from pyspark.sql.types import FloatType, StructType

from pyspark_contracts._contract import Contract
from pyspark_contracts._field import Field


def test_validate_skipped_when_globally_disabled(spark, monkeypatch):
    monkeypatch.setenv("PYSPARK_CONTRACTS_ENABLED", "false")

    class MyContract(Contract):
        odometer = Field(FloatType())

    df = spark.createDataFrame([], StructType([]))
    report = MyContract().validate(df, mode="hard")
    assert not report


def test_validate_runs_when_globally_enabled(spark, monkeypatch):
    monkeypatch.setenv("PYSPARK_CONTRACTS_ENABLED", "true")

    class MyContract(Contract):
        odometer = Field(FloatType())

    df = spark.createDataFrame([], StructType([]))
    report = MyContract().validate(df, mode="soft")
    assert report
    assert len(report.violations) == 1


def test_validate_runs_when_env_var_unset(spark, monkeypatch):
    monkeypatch.delenv("PYSPARK_CONTRACTS_ENABLED", raising=False)

    class MyContract(Contract):
        odometer = Field(FloatType())

    df = spark.createDataFrame([], StructType([]))
    report = MyContract().validate(df, mode="soft")
    assert report
