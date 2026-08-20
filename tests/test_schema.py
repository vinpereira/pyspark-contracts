import pytest
from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from pyspark_contracts._contract import Contract
from pyspark_contracts._field import Field
from pyspark_contracts._report import ContractViolationError


def test_contract_metaclass_collects_fields():
    class OdometerContract(Contract):
        vin = Field(StringType())
        odometer = Field(FloatType())

    assert "vin" in OdometerContract._fields
    assert "odometer" in OdometerContract._fields
    assert isinstance(OdometerContract._fields["vin"].dtype, StringType)


def test_validate_passes_when_schema_matches(spark):
    class MyContract(Contract):
        vin = Field(StringType())
        odometer = Field(FloatType())

    schema = StructType(
        [
            StructField("vin", StringType()),
            StructField("odometer", FloatType()),
        ]
    )
    df = spark.createDataFrame([], schema)
    report = MyContract().validate(df, mode="soft")
    assert not report


def test_validate_extra_columns_are_ignored(spark):
    class MyContract(Contract):
        vin = Field(StringType())

    schema = StructType(
        [
            StructField("vin", StringType()),
            StructField("extra", IntegerType()),
        ]
    )
    df = spark.createDataFrame([], schema)
    report = MyContract().validate(df, mode="soft")
    assert not report


def test_validate_detects_missing_column(spark):
    class MyContract(Contract):
        vin = Field(StringType())
        odometer = Field(FloatType())

    df = spark.createDataFrame([], StructType([StructField("vin", StringType())]))
    report = MyContract().validate(df, mode="soft")
    assert len(report.violations) == 1
    assert report.violations[0].kind == "missing_column"
    assert report.violations[0].column == "odometer"


def test_validate_detects_type_mismatch(spark):
    class MyContract(Contract):
        vin = Field(StringType())

    df = spark.createDataFrame([], StructType([StructField("vin", IntegerType())]))
    report = MyContract().validate(df, mode="soft")
    assert len(report.violations) == 1
    assert report.violations[0].kind == "type_mismatch"
    assert report.violations[0].expected_type == "StringType"
    assert report.violations[0].actual_type == "IntegerType"


def test_validate_hard_fail_raises_contract_violation_error(spark):
    class MyContract(Contract):
        odometer = Field(FloatType())

    df = spark.createDataFrame([], StructType([]))
    with pytest.raises(ContractViolationError) as exc_info:
        MyContract().validate(df)
    assert exc_info.value.report.violations[0].kind == "missing_column"


def test_validate_soft_fail_returns_report(spark):
    class MyContract(Contract):
        odometer = Field(FloatType())

    df = spark.createDataFrame([], StructType([]))
    report = MyContract().validate(df, mode="soft")
    assert report
    assert len(report.violations) == 1


def test_validate_report_includes_mode(spark):
    class MyContract(Contract):
        odometer = Field(FloatType())

    df = spark.createDataFrame([], StructType([]))
    report = MyContract().validate(df, mode="soft")
    assert report.mode == "soft"


def test_external_logger_is_used(spark, caplog):
    import logging

    class MyContract(Contract):
        odometer = Field(FloatType())

    df = spark.createDataFrame([], StructType([]))
    external_logger = logging.getLogger("test_external_logger")
    with caplog.at_level(logging.ERROR, logger="test_external_logger"):
        with pytest.raises(ContractViolationError):
            MyContract().validate(df, logger=external_logger)
    assert any("missing_column" in str(getattr(r, "violations", "")) for r in caplog.records)
