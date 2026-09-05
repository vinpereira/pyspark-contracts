from pyspark.sql.types import FloatType, StringType, StructField, StructType

from pyspark_contracts._contract import Contract
from pyspark_contracts._depth import ValidationDepth
from pyspark_contracts._field import Field


def test_validation_depth_has_three_members():
    assert ValidationDepth.SCHEMA_ONLY.value == "schema_only"
    assert ValidationDepth.DATA_ONLY.value == "data_only"
    assert ValidationDepth.SCHEMA_AND_DATA.value == "schema_and_data"


def test_schema_only_skips_data_checks_and_row_count(spark):
    class MyContract(Contract):
        odometer = Field(FloatType(), min_value=0.0)

    schema = StructType([StructField("odometer", FloatType())])
    df = spark.createDataFrame([(-1.0,)], schema)
    report = MyContract().validate(df, mode="soft", depth=ValidationDepth.SCHEMA_ONLY)
    assert not report
    assert report.row_count is None


def test_schema_only_still_detects_missing_column(spark):
    class MyContract(Contract):
        odometer = Field(FloatType())

    df = spark.createDataFrame([], StructType([]))
    report = MyContract().validate(df, mode="soft", depth=ValidationDepth.SCHEMA_ONLY)
    assert len(report.violations) == 1
    assert report.violations[0].kind == "missing_column"
    assert report.row_count is None


def test_schema_only_skips_nullable_check(spark):
    class MyContract(Contract):
        vin = Field(StringType(), nullable=False)

    schema = StructType([StructField("vin", StringType())])
    df = spark.createDataFrame([(None,)], schema)
    report = MyContract().validate(df, mode="soft", depth=ValidationDepth.SCHEMA_ONLY)
    assert not report


def test_data_only_skips_structural_checks(spark):
    class MyContract(Contract):
        odometer = Field(FloatType(), min_value=0.0)

    schema = StructType([StructField("odometer", FloatType())])
    df = spark.createDataFrame([(-1.0,)], schema)
    report = MyContract().validate(df, mode="soft", depth=ValidationDepth.DATA_ONLY)
    assert len(report.violations) == 1
    assert report.violations[0].kind == "value_out_of_range"


def test_data_only_ignores_missing_column(spark):
    class MyContract(Contract):
        vin = Field(StringType())
        odometer = Field(FloatType(), min_value=0.0)

    schema = StructType([StructField("odometer", FloatType())])
    df = spark.createDataFrame([(-1.0,)], schema)
    report = MyContract().validate(df, mode="soft", depth=ValidationDepth.DATA_ONLY)
    kinds = [v.kind for v in report.violations]
    assert "missing_column" not in kinds
    assert "value_out_of_range" in kinds


def test_data_only_runs_nullable_check(spark):
    class MyContract(Contract):
        vin = Field(StringType(), nullable=False)

    schema = StructType([StructField("vin", StringType())])
    df = spark.createDataFrame([(None,)], schema)
    report = MyContract().validate(df, mode="soft", depth=ValidationDepth.DATA_ONLY)
    assert len(report.violations) == 1
    assert report.violations[0].kind == "null_violation"


def test_schema_and_data_is_default(spark):
    class MyContract(Contract):
        vin = Field(StringType(), nullable=False)

    schema = StructType([StructField("vin", StringType())])
    df = spark.createDataFrame([(None,)], schema)
    report = MyContract().validate(df, mode="soft")
    assert report.depth == ValidationDepth.SCHEMA_AND_DATA
    assert len(report.violations) == 1


def test_depth_included_in_report_to_dict(spark):
    class MyContract(Contract):
        odometer = Field(FloatType())

    schema = StructType([StructField("odometer", FloatType())])
    df = spark.createDataFrame([(1.0,)], schema)
    report = MyContract().validate(df, mode="soft", depth=ValidationDepth.SCHEMA_ONLY)
    assert report.to_dict()["depth"] == "schema_only"
