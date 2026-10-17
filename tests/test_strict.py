import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from pyspark_contracts._check import check
from pyspark_contracts._contract import Contract
from pyspark_contracts._depth import ValidationDepth
from pyspark_contracts._field import Field
from pyspark_contracts._report import ContractViolationError
from pyspark_contracts._schema import ContractSchema

SCHEMA = StructType(
    [
        StructField("vin", StringType()),
        StructField("debug_flag", StringType()),
        StructField("odometer", IntegerType()),
        StructField("_ingested_by", IntegerType()),
    ]
)


class LooseContract(Contract):
    vin = Field(StringType())
    odometer = Field(IntegerType())


class StrictContract(Contract):
    strict = True
    vin = Field(StringType())
    odometer = Field(IntegerType())


def test_extra_columns_are_allowed_by_default(spark):
    df = spark.createDataFrame([], SCHEMA)
    assert not LooseContract().validate(df, mode="soft")


def test_strict_flags_each_undeclared_column_in_dataframe_order(spark):
    df = spark.createDataFrame([], SCHEMA)
    report = StrictContract().validate(df, mode="soft")
    assert [(v.kind, v.column, v.actual_type) for v in report.violations] == [
        ("unexpected_column", "debug_flag", "string"),
        ("unexpected_column", "_ingested_by", "int"),
    ]


def test_strict_passes_when_columns_match_exactly(spark):
    schema = StructType([StructField("vin", StringType()), StructField("odometer", IntegerType())])
    assert not StrictContract().validate(spark.createDataFrame([], schema), mode="soft")


def test_unexpected_columns_come_after_declared_field_violations(spark):
    schema = StructType([StructField("extra", StringType()), StructField("vin", IntegerType())])
    report = StrictContract().validate(spark.createDataFrame([], schema), mode="soft")
    assert [(v.kind, v.column) for v in report.violations] == [
        ("type_mismatch", "vin"),
        ("missing_column", "odometer"),
        ("unexpected_column", "extra"),
    ]


def test_strict_hard_mode_raises(spark):
    with pytest.raises(ContractViolationError) as exc_info:
        StrictContract().validate(spark.createDataFrame([], SCHEMA))
    assert exc_info.value.report.violations[0].kind == "unexpected_column"


def test_strict_fail_fast_stops_at_first_unexpected_column(spark):
    report = StrictContract().validate(spark.createDataFrame([], SCHEMA), mode="soft", lazy=False)
    assert [v.column for v in report.violations] == ["debug_flag"]


def test_strict_is_checked_under_schema_only(spark):
    report = StrictContract().validate(
        spark.createDataFrame([], SCHEMA), mode="soft", depth=ValidationDepth.SCHEMA_ONLY
    )
    assert [v.kind for v in report.violations] == ["unexpected_column", "unexpected_column"]
    assert report.row_count is None


def test_strict_is_skipped_under_data_only(spark):
    report = StrictContract().validate(
        spark.createDataFrame([], SCHEMA), mode="soft", depth=ValidationDepth.DATA_ONLY
    )
    assert not report


def test_strict_is_inherited(spark):
    class Sub(StrictContract):
        pass

    report = Sub().validate(spark.createDataFrame([], SCHEMA), mode="soft")
    assert {v.column for v in report.violations} == {"debug_flag", "_ingested_by"}


def test_unexpected_column_does_not_block_cross_column_checks(spark):
    class MyContract(Contract):
        strict = True
        odometer = Field(IntegerType(), condition=lambda f: F.col(f) >= 0)

        @check("odometer must be below 1000")
        def plausible(self, df):
            return df.filter(F.col("odometer") >= 1000)

    schema = StructType([StructField("odometer", IntegerType()), StructField("x", StringType())])
    df = spark.createDataFrame([(-1, "a"), (5000, "b")], schema)
    report = MyContract().validate(df, mode="soft")
    assert [v.kind for v in report.violations] == [
        "unexpected_column",
        "condition_failed",
        "check_failed",
    ]


def test_to_dict_includes_strict_only_when_set():
    assert StrictContract.to_dict()["strict"] is True
    assert "strict" not in LooseContract.to_dict()


def test_describe_mentions_strict():
    assert "Strict: undeclared columns are violations" in StrictContract.describe().splitlines()
    assert not any(line.startswith("Strict:") for line in LooseContract.describe().splitlines())


def test_reloaded_schema_enforces_strict(spark):
    report = ContractSchema.from_dict(StrictContract.to_dict()).validate(
        spark.createDataFrame([], SCHEMA), mode="soft"
    )
    assert [v.column for v in report.violations] == ["debug_flag", "_ingested_by"]
