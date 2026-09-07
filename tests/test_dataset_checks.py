import pytest
from pyspark.sql.types import FloatType, StructField, StructType

from pyspark_contracts._contract import Contract
from pyspark_contracts._depth import ValidationDepth
from pyspark_contracts._field import Field
from pyspark_contracts._report import ContractViolationError


def test_min_rows_violation(spark):
    class MyContract(Contract):
        min_rows = 3
        odometer = Field(FloatType())

    schema = StructType([StructField("odometer", FloatType())])
    df = spark.createDataFrame([(1.0,), (2.0,)], schema)
    report = MyContract().validate(df, mode="soft")
    assert len(report.violations) == 1
    v = report.violations[0]
    assert v.kind == "row_count_out_of_range"
    assert v.column is None
    assert "min_rows=3" in v.constraint
    assert "actual: 2" in v.constraint


def test_max_rows_violation(spark):
    class MyContract(Contract):
        max_rows = 2
        odometer = Field(FloatType())

    schema = StructType([StructField("odometer", FloatType())])
    df = spark.createDataFrame([(1.0,), (2.0,), (3.0,)], schema)
    report = MyContract().validate(df, mode="soft")
    assert len(report.violations) == 1
    assert report.violations[0].kind == "row_count_out_of_range"
    assert "max_rows=2" in report.violations[0].constraint


def test_no_violation_when_row_count_within_bounds(spark):
    class MyContract(Contract):
        min_rows = 1
        max_rows = 10
        odometer = Field(FloatType())

    schema = StructType([StructField("odometer", FloatType())])
    df = spark.createDataFrame([(1.0,), (2.0,)], schema)
    report = MyContract().validate(df, mode="soft")
    assert not report


def test_min_rows_defaults_to_none(spark):
    class MyContract(Contract):
        odometer = Field(FloatType())

    schema = StructType([StructField("odometer", FloatType())])
    df = spark.createDataFrame([], schema)
    report = MyContract().validate(df, mode="soft")
    assert not report


def test_dataset_violation_stops_quality_checks_in_fail_fast_mode(spark):
    class MyContract(Contract):
        min_rows = 10
        odometer = Field(FloatType(), min_value=0.0)

    schema = StructType([StructField("odometer", FloatType())])
    df = spark.createDataFrame([(-1.0,)], schema)
    report = MyContract().validate(df, mode="soft", lazy=False)
    assert len(report.violations) == 1
    assert report.violations[0].kind == "row_count_out_of_range"


def test_dataset_violation_raises_in_hard_mode(spark):
    class MyContract(Contract):
        min_rows = 10
        odometer = Field(FloatType())

    schema = StructType([StructField("odometer", FloatType())])
    df = spark.createDataFrame([(1.0,)], schema)
    with pytest.raises(ContractViolationError):
        MyContract().validate(df)


def test_schema_only_skips_dataset_checks(spark):
    class MyContract(Contract):
        min_rows = 10
        odometer = Field(FloatType())

    schema = StructType([StructField("odometer", FloatType())])
    df = spark.createDataFrame([(1.0,)], schema)
    report = MyContract().validate(df, mode="soft", depth=ValidationDepth.SCHEMA_ONLY)
    assert not report
