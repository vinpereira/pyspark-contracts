import pytest
from pyspark.sql.types import FloatType, StringType, StructType

from pyspark_contracts._contract import Contract
from pyspark_contracts._field import Field
from pyspark_contracts._report import ContractViolationError


def test_hard_mode_fails_fast_by_default(spark):
    class MyContract(Contract):
        vin = Field(StringType())
        odometer = Field(FloatType())

    df = spark.createDataFrame([], StructType([]))
    with pytest.raises(ContractViolationError) as exc_info:
        MyContract().validate(df)
    assert len(exc_info.value.report.violations) == 1


def test_hard_mode_lazy_true_collects_all_violations(spark):
    class MyContract(Contract):
        vin = Field(StringType())
        odometer = Field(FloatType())

    df = spark.createDataFrame([], StructType([]))
    with pytest.raises(ContractViolationError) as exc_info:
        MyContract().validate(df, lazy=True)
    assert len(exc_info.value.report.violations) == 2


def test_soft_mode_defaults_to_lazy_collects_all_violations(spark):
    class MyContract(Contract):
        vin = Field(StringType())
        odometer = Field(FloatType())

    df = spark.createDataFrame([], StructType([]))
    report = MyContract().validate(df, mode="soft")
    assert len(report.violations) == 2


def test_soft_mode_lazy_false_stops_at_first_violation(spark):
    class MyContract(Contract):
        vin = Field(StringType())
        odometer = Field(FloatType())

    df = spark.createDataFrame([], StructType([]))
    report = MyContract().validate(df, mode="soft", lazy=False)
    assert len(report.violations) == 1
