import json
import logging
import uuid

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, IntegerType, StringType, StructField, StructType

from pyspark_contracts._check import check
from pyspark_contracts._contract import Contract
from pyspark_contracts._field import Field
from pyspark_contracts._logging import get_logger
from pyspark_contracts._report import (
    ContractViolationError,
    Severity,
    Violation,
    ViolationKind,
    ViolationReport,
)

SCHEMA = StructType(
    [
        StructField("vin", StringType()),
        StructField("odometer", FloatType()),
        StructField("note", StringType()),
    ]
)


class _MessageOnlyFormatter(logging.Formatter):
    """What a typical shared formatter does: prints the message and drops `extra`."""

    def format(self, record):
        return f"{record.levelname}:{record.name}:{record.getMessage()}"


def _message_only_logger():
    logger = logging.getLogger(f"test-{uuid.uuid4()}")
    logger.propagate = False
    lines = []

    class _Capture(logging.Handler):
        def emit(self, record):
            lines.append(self.format(record))

    handler = _Capture()
    handler.setFormatter(_MessageOnlyFormatter())
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    return logger, lines


def test_summary_of_a_single_violation():
    v = Violation(
        kind=ViolationKind.VALUE_OUT_OF_RANGE,
        column="odometerEnd",
        constraint="min_value=0",
        row_pct=2.3,
        failure_count=42,
    )
    report = ViolationReport("C", [v], 1842)
    assert report.summary() == (
        "1 violation(s) — value_out_of_range odometerEnd (min_value=0): 42 rows"
    )


def test_summary_uses_singular_for_one_row():
    v = Violation(kind="null_violation", column="vin", constraint="nullable", failure_count=1)
    assert ViolationReport("C", [v], 10).summary().endswith("null_violation vin (nullable): 1 row")


def test_summary_describes_each_kind_of_violation():
    violations = [
        Violation(kind="missing_column", column="a", expected_type="float"),
        Violation(
            kind="type_mismatch", column="b", expected_type="decimal(26,12)", actual_type="int"
        ),
        Violation(kind="unexpected_column", column="c", actual_type="string"),
        Violation(kind="row_count_out_of_range", constraint="min_rows=100 (actual: 3)"),
        Violation(
            kind="check_failed", check="no_negative", constraint="must be >= 0", failure_count=2
        ),
    ]
    assert ViolationReport("C", violations, 3).summary() == (
        "5 violation(s) — "
        "missing_column a (expected float); "
        "type_mismatch b (expected decimal(26,12), got int); "
        "unexpected_column c (got string); "
        "row_count_out_of_range (min_rows=100 (actual: 3)); "
        "check_failed no_negative (must be >= 0): 2 rows"
    )


def test_summary_counts_errors_and_warnings_and_marks_warnings():
    violations = [
        Violation(kind="null_violation", column="vin", constraint="nullable", failure_count=3),
        Violation(
            kind="length_out_of_range",
            severity=Severity.WARN,
            column="note",
            constraint="max_length=2",
            failure_count=1,
        ),
    ]
    assert ViolationReport("C", violations, 10).summary() == (
        "1 error(s), 1 warning(s) — "
        "null_violation vin (nullable): 3 rows; "
        "length_out_of_range note (max_length=2): 1 row [warn]"
    )


def test_summary_is_capped_and_says_how_many_were_left_out():
    violations = [Violation(kind="missing_column", column=f"c{i}") for i in range(25)]
    summary = ViolationReport("C", violations, 0).summary()
    assert summary.startswith("25 violation(s) — missing_column c0; ")
    assert "missing_column c9" in summary
    assert "missing_column c10" not in summary
    assert summary.endswith("; … and 15 more")


def test_summary_never_includes_sample_values():
    v = Violation(
        kind="null_violation",
        column="vin",
        constraint="nullable",
        failure_count=1,
        sample_values=["1HGCM82633A004352"],
    )
    assert "1HGCM" not in ViolationReport("C", [v], 1).summary()


def test_hard_mode_log_message_names_the_failures(spark):
    class C(Contract):
        vin = Field(StringType(), nullable=False)
        odometer = Field(FloatType(), min_value=0)

    logger, lines = _message_only_logger()
    df = spark.createDataFrame([(None, -1.0, "x"), ("A", 5.0, "y")], SCHEMA)
    with pytest.raises(ContractViolationError):
        C().validate(df, lazy=True, logger=logger)

    assert lines == [
        "ERROR:" + logger.name + ":contract violation — job aborted: 2 violation(s) — "
        "null_violation vin (nullable): 1 row; "
        "value_out_of_range odometer (min_value=0): 1 row"
    ]


def test_soft_mode_log_message_names_the_failures(spark):
    class C(Contract):
        odometer = Field(FloatType(), min_value=0)

    logger, lines = _message_only_logger()
    C().validate(spark.createDataFrame([("A", -1.0, "x")], SCHEMA), mode="soft", logger=logger)
    assert lines == [
        "WARNING:" + logger.name + ":contract violation — continuing: 1 violation(s) — "
        "value_out_of_range odometer (min_value=0): 1 row"
    ]


def test_warnings_only_log_message(spark):
    class C(Contract):
        note = Field(StringType(), max_length=0, severity="warn")

        @check("odometer must be non-negative", severity="warn")
        def non_negative(self, df):
            return df.filter(F.col("odometer") < 0)

    logger, lines = _message_only_logger()
    df = spark.createDataFrame([("A", -1.0, "x")], SCHEMA)
    C().validate(df, logger=logger)
    assert lines == [
        "WARNING:" + logger.name + ":contract violation — continuing: "
        "0 error(s), 2 warning(s) — "
        "length_out_of_range note (max_length=0): 1 row [warn]; "
        "check_failed non_negative (odometer must be non-negative): 1 row [warn]"
    ]


def test_structured_extra_fields_are_unchanged(spark, caplog):
    class C(Contract):
        odometer = Field(FloatType(), min_value=0)

    logger = logging.getLogger(f"test-{uuid.uuid4()}")
    with caplog.at_level(logging.DEBUG, logger=logger.name):
        C().validate(spark.createDataFrame([("A", -1.0, "x")], SCHEMA), mode="soft", logger=logger)

    record = next(r for r in caplog.records if r.name == logger.name)
    assert record.contract == "C"
    assert record.mode == "soft"
    assert record.row_count == 1
    assert record.violations[0]["kind"] == "value_out_of_range"
    assert record.violations[0]["sample_values"] == [-1.0]


def test_json_logger_still_carries_every_field(spark, capsys):
    class C(Contract):
        odometer = Field(IntegerType(), nullable=False)

    # a fresh logger per test: get_logger binds sys.stdout when the logger is first created
    logger = get_logger(f"test-{uuid.uuid4()}")
    schema = StructType([StructField("odometer", IntegerType())])
    C().validate(spark.createDataFrame([(None,)], schema), mode="soft", logger=logger)

    entry = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert entry["message"].startswith("contract violation — continuing: 1 violation(s) — ")
    assert entry["violations"][0]["kind"] == "null_violation"
    assert entry["row_count"] == 1
