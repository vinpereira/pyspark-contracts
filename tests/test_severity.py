import logging
import uuid

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from pyspark_contracts._check import check
from pyspark_contracts._contract import Contract
from pyspark_contracts._field import Field
from pyspark_contracts._report import ContractViolationError, Severity, Violation, ViolationReport
from pyspark_contracts._schema import ContractSchema

SCHEMA = StructType(
    [
        StructField("a", IntegerType()),
        StructField("b", StringType()),
        StructField("c", StringType()),
    ]
)
# a: 1 null in 4 rows (25%) | b: 1 value longer than 2 | c: every value shorter than 5
ROWS = [(1, "x", "c"), (None, "toolong", "c"), (3, "y", "c"), (4, "z", "c")]


def _logger():
    return logging.getLogger(f"test-{uuid.uuid4()}")


def _records(caplog, logger):
    return [(r.levelname, r.getMessage()) for r in caplog.records if r.name == logger.name]


def _kinds_and_severities(report):
    return [(v.kind, v.column or v.check, v.severity) for v in report.violations]


def test_severity_compares_and_formats_as_its_string():
    assert Severity.WARN == "warn"
    assert str(Severity.WARN) == "warn"
    assert f"{Severity.ERROR}" == "error"


def test_violation_severity_defaults_to_error_and_coerces_strings():
    assert Violation(kind="null_violation").severity is Severity.ERROR
    assert Violation(kind="null_violation", severity="warn").severity is Severity.WARN
    with pytest.raises(ValueError):
        Violation(kind="null_violation", severity="fatal")


def test_to_dict_emits_severity_right_after_kind():
    v = Violation(kind="null_violation", column="a")
    entry = ViolationReport("C", [v], 4).to_dict()["violations"][0]
    assert list(entry)[:2] == ["kind", "severity"]
    assert type(entry["severity"]) is str
    assert entry["severity"] == "error"


@pytest.mark.parametrize(
    "kwargs", [{"severity": "fatal"}, {"max_failure_pct": -1}, {"max_failure_pct": 100.5}]
)
def test_field_rejects_invalid_severity_settings(kwargs):
    with pytest.raises(ValueError):
        Field(IntegerType(), **kwargs)


def test_check_rejects_invalid_severity_at_decoration_time():
    with pytest.raises(ValueError):
        check("anything", severity="fatal")


def test_hard_mode_does_not_raise_on_warnings_only(spark, caplog):
    class C(Contract):
        b = Field(StringType(), max_length=2, severity="warn")

    logger = _logger()
    with caplog.at_level(logging.DEBUG, logger=logger.name):
        report = C().validate(spark.createDataFrame(ROWS, SCHEMA), logger=logger)

    assert report
    assert not report.errors
    assert _kinds_and_severities(report) == [("length_out_of_range", "b", "warn")]
    assert _records(caplog, logger) == [
        (
            "WARNING",
            "contract violation — continuing: 0 error(s), 1 warning(s) — "
            "length_out_of_range b (max_length=2): 1 row [warn]",
        )
    ]


def test_hard_mode_raises_on_errors_and_message_counts_both(spark, caplog):
    class C(Contract):
        a = Field(IntegerType(), nullable=False)
        b = Field(StringType(), max_length=2, severity="warn")

    logger = _logger()
    with (
        caplog.at_level(logging.DEBUG, logger=logger.name),
        pytest.raises(ContractViolationError) as exc_info,
    ):
        C().validate(spark.createDataFrame(ROWS, SCHEMA), lazy=True, logger=logger)

    assert str(exc_info.value) == "C — 1 error(s), 1 warning(s) — job aborted"
    assert _records(caplog, logger) == [
        (
            "ERROR",
            "contract violation — job aborted: 1 error(s), 1 warning(s) — "
            "null_violation a (nullable): 1 row; "
            "length_out_of_range b (max_length=2): 1 row [warn]",
        )
    ]
    report = exc_info.value.report
    assert [v.column for v in report.errors] == ["a"]
    assert [v.column for v in report.warnings] == ["b"]


def test_error_message_is_unchanged_without_warnings(spark):
    class C(Contract):
        a = Field(IntegerType(), nullable=False)

    with pytest.raises(ContractViolationError) as exc_info:
        C().validate(spark.createDataFrame(ROWS, SCHEMA))
    assert str(exc_info.value) == "C — 1 violation(s) — job aborted"


def test_fail_fast_keeps_warnings_and_stops_at_first_error(spark):
    class C(Contract):
        b = Field(StringType(), max_length=2, severity="warn")
        a = Field(IntegerType(), nullable=False)
        c = Field(StringType(), min_length=5)

    report = C().validate(spark.createDataFrame(ROWS, SCHEMA), mode="soft", lazy=False)
    assert _kinds_and_severities(report) == [
        ("length_out_of_range", "b", "warn"),
        ("null_violation", "a", "error"),
    ]


def test_fail_fast_runs_checks_after_a_warning(spark):
    class C(Contract):
        b = Field(StringType(), max_length=2, severity="warn")

        @check("c must be at least 5 characters")
        def long_c(self, df):
            return df.filter(F.length("c") < 5)

    with pytest.raises(ContractViolationError) as exc_info:
        C().validate(spark.createDataFrame(ROWS, SCHEMA))
    assert _kinds_and_severities(exc_info.value.report) == [
        ("length_out_of_range", "b", "warn"),
        ("check_failed", "long_c", "error"),
    ]


@pytest.mark.parametrize(("max_failure_pct", "severity"), [(25, "warn"), (24.9, "error")])
def test_max_failure_pct_turns_failures_within_tolerance_into_warnings(
    spark, max_failure_pct, severity
):
    class C(Contract):
        a = Field(IntegerType(), nullable=False, max_failure_pct=max_failure_pct)

    report = C().validate(spark.createDataFrame(ROWS, SCHEMA), mode="soft")
    assert _kinds_and_severities(report) == [("null_violation", "a", severity)]
    assert report.violations[0].row_pct == 25.0


def test_max_failure_pct_compares_the_unrounded_share(spark):
    class C(Contract):
        a = Field(IntegerType(), nullable=False, max_failure_pct=33.3)

    df = spark.createDataFrame([(1,), (2,), (None,)], StructType([SCHEMA["a"]]))
    violation = C().validate(df, mode="soft").violations[0]
    assert violation.row_pct == 33.3  # 1/3 rounded
    assert violation.severity == "error"  # 33.33...% is above the 33.3% tolerance


def test_max_failure_pct_applies_to_unique_and_condition(spark):
    class C(Contract):
        a = Field(
            IntegerType(),
            unique=True,
            condition=lambda f: F.col(f) > 1,
            max_failure_pct=50,
        )

    df = spark.createDataFrame([(1,), (1,), (2,), (3,)], StructType([SCHEMA["a"]]))
    report = C().validate(df, mode="soft")
    assert _kinds_and_severities(report) == [
        ("duplicate_value", "a", "warn"),
        ("condition_failed", "a", "warn"),
    ]


def test_warn_severity_with_tolerance_exceeded_stays_a_warning(spark):
    class C(Contract):
        a = Field(IntegerType(), nullable=False, severity="warn", max_failure_pct=1)

    report = C().validate(spark.createDataFrame(ROWS, SCHEMA), mode="soft")
    assert _kinds_and_severities(report) == [("null_violation", "a", "warn")]


def test_structural_violations_are_always_errors(spark):
    class C(Contract):
        missing = Field(IntegerType(), nullable=False, severity="warn", max_failure_pct=100)

    with pytest.raises(ContractViolationError) as exc_info:
        C().validate(spark.createDataFrame(ROWS, SCHEMA))
    assert _kinds_and_severities(exc_info.value.report) == [("missing_column", "missing", "error")]


def test_check_severity_warn(spark):
    class C(Contract):
        @check("c should be at least 5 characters", severity="warn")
        def long_c(self, df):
            return df.filter(F.length("c") < 5)

    report = C().validate(spark.createDataFrame(ROWS, SCHEMA))
    assert _kinds_and_severities(report) == [("check_failed", "long_c", "warn")]


class Documented(Contract):
    a = Field(IntegerType(), nullable=False, max_failure_pct=0.1)
    b = Field(StringType(), max_length=2, severity="warn")
    c = Field(StringType())

    @check("c should be at least 5 characters", severity="warn")
    def long_c(self, df):
        return df.filter(F.length("c") < 5)


def test_to_dict_includes_severity_and_tolerance_only_when_set():
    fields = Documented.to_dict()["fields"]
    assert fields["a"]["max_failure_pct"] == 0.1
    assert "severity" not in fields["a"]
    assert fields["b"]["severity"] == "warn"
    assert "max_failure_pct" not in fields["b"]
    assert "severity" not in fields["c"] and "max_failure_pct" not in fields["c"]


def test_describe_shows_severity_and_tolerance():
    text = Documented.describe()
    assert "max_failure_pct=0.1" in text
    assert "severity=warn" in text
    assert "long_c (warn): c should be at least 5 characters" in text


def test_reloaded_schema_keeps_severity_and_tolerance(spark):
    with pytest.warns(UserWarning):
        schema = ContractSchema.from_dict(Documented.to_dict())
    rows = [(i, "x", "c") for i in range(999)] + [(None, "toolong", "c")]
    report = schema.validate(spark.createDataFrame(rows, SCHEMA), mode="soft")
    assert _kinds_and_severities(report) == [
        ("null_violation", "a", "warn"),
        ("length_out_of_range", "b", "warn"),
    ]
