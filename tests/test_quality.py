import pytest
from pyspark.sql.types import FloatType, IntegerType, StringType, StructField, StructType

from pyspark_contracts._contract import Contract
from pyspark_contracts._field import Field


def test_min_value_violation(spark):
    class MyContract(Contract):
        odometer = Field(FloatType(), nullable=False, min_value=0.0)

    schema = StructType([StructField("odometer", FloatType())])
    df = spark.createDataFrame([(-1.0,), (100.0,), (-5.0,)], schema)
    report = MyContract().validate(df, mode="soft")
    assert len(report.violations) == 1
    assert report.violations[0].kind == "value_out_of_range"
    assert "min_value" in report.violations[0].constraint
    assert report.violations[0].row_pct == pytest.approx(66.7, abs=0.1)


def test_max_value_violation(spark):
    class MyContract(Contract):
        score = Field(FloatType(), max_value=100.0)

    schema = StructType([StructField("score", FloatType())])
    df = spark.createDataFrame([(50.0,), (150.0,)], schema)
    report = MyContract().validate(df, mode="soft")
    assert len(report.violations) == 1
    assert report.violations[0].kind == "value_out_of_range"
    assert "max_value" in report.violations[0].constraint


def test_no_violation_when_values_in_range(spark):
    class MyContract(Contract):
        odometer = Field(FloatType(), min_value=0.0, max_value=999999.0)

    schema = StructType([StructField("odometer", FloatType())])
    df = spark.createDataFrame([(0.0,), (50000.0,), (999999.0,)], schema)
    report = MyContract().validate(df, mode="soft")
    assert not report


def test_min_length_violation(spark):
    class MyContract(Contract):
        vin = Field(StringType(), min_length=17)

    schema = StructType([StructField("vin", StringType())])
    df = spark.createDataFrame([("SHORT",), ("A" * 17,)], schema)
    report = MyContract().validate(df, mode="soft")
    assert len(report.violations) == 1
    assert report.violations[0].kind == "length_out_of_range"
    assert "min_length" in report.violations[0].constraint


def test_max_length_violation(spark):
    class MyContract(Contract):
        code = Field(StringType(), max_length=5)

    schema = StructType([StructField("code", StringType())])
    df = spark.createDataFrame([("AB",), ("TOOLONG",)], schema)
    report = MyContract().validate(df, mode="soft")
    assert len(report.violations) == 1
    assert report.violations[0].kind == "length_out_of_range"


def test_regex_violation(spark):
    class MyContract(Contract):
        vin = Field(StringType(), regex=r"^[A-Z0-9]{17}$")

    schema = StructType([StructField("vin", StringType())])
    df = spark.createDataFrame([("ABC1234567890ABCD",), ("bad",), ("ALSO_BAD!",)], schema)
    report = MyContract().validate(df, mode="soft")
    assert len(report.violations) == 1
    assert report.violations[0].kind == "regex_mismatch"
    assert report.violations[0].row_pct == pytest.approx(66.7, abs=0.1)


def test_no_regex_violation_when_all_match(spark):
    class MyContract(Contract):
        status = Field(StringType(), regex=r"^[A-Z]+$")

    schema = StructType([StructField("status", StringType())])
    df = spark.createDataFrame([("ACTIVE",), ("PENDING",)], schema)
    report = MyContract().validate(df, mode="soft")
    assert not report


def test_allowed_values_violation(spark):
    class MyContract(Contract):
        status = Field(StringType(), allowed_values=["active", "inactive"])

    schema = StructType([StructField("status", StringType())])
    df = spark.createDataFrame([("active",), ("inactive",), ("unknown",)], schema)
    report = MyContract().validate(df, mode="soft")
    assert len(report.violations) == 1
    assert report.violations[0].kind == "value_not_allowed"
    assert report.violations[0].row_pct == pytest.approx(33.3, abs=0.1)


def test_no_allowed_values_violation_when_all_valid(spark):
    class MyContract(Contract):
        status = Field(StringType(), allowed_values=["active", "inactive"])

    schema = StructType([StructField("status", StringType())])
    df = spark.createDataFrame([("active",), ("inactive",)], schema)
    report = MyContract().validate(df, mode="soft")
    assert not report


def test_quality_skipped_for_columns_with_schema_violations(spark):
    class MyContract(Contract):
        odometer = Field(FloatType(), min_value=0.0)

    schema = StructType([StructField("odometer", IntegerType())])
    df = spark.createDataFrame([(-1,)], schema)
    report = MyContract().validate(df, mode="soft")
    kinds = [v.kind for v in report.violations]
    assert "type_mismatch" in kinds
    assert "value_out_of_range" not in kinds
