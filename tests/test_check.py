import pytest
from pyspark.sql.types import FloatType, StringType, StructField, StructType

from pyspark_contracts._check import check
from pyspark_contracts._contract import Contract
from pyspark_contracts._field import Field


def test_check_decorator_stores_description():
    @check("must be positive")
    def my_check(self, df):
        return df

    assert my_check._check_description == "must be positive"


def test_contract_metaclass_collects_checks():
    class MyContract(Contract):
        odometer = Field(FloatType())

        @check("odometer must be non-negative")
        def no_negative(self, df):
            return df.filter(df.odometer < 0)

    assert "no_negative" in MyContract._checks
    assert MyContract._checks["no_negative"]._check_description == ("odometer must be non-negative")
    assert "odometer" not in MyContract._checks


def test_check_failed_violation(spark):
    from pyspark.sql import functions as F

    class MyContract(Contract):
        odometer = Field(FloatType())

        @check("odometer must be non-negative")
        def no_negative(self, df):
            return df.filter(F.col("odometer") < 0)

    schema = StructType([StructField("odometer", FloatType())])
    df = spark.createDataFrame([(-1.0,), (5.0,)], schema)
    report = MyContract().validate(df, mode="soft")
    assert len(report.violations) == 1
    v = report.violations[0]
    assert v.kind == "check_failed"
    assert v.column == "no_negative"
    assert v.constraint == "odometer must be non-negative"
    assert v.failure_count == 1
    assert v.sample_values == [{"odometer": -1.0}]


def test_check_passes_when_no_failing_rows(spark):
    from pyspark.sql import functions as F

    class MyContract(Contract):
        odometer = Field(FloatType())

        @check("odometer must be non-negative")
        def no_negative(self, df):
            return df.filter(F.col("odometer") < 0)

    schema = StructType([StructField("odometer", FloatType())])
    df = spark.createDataFrame([(5.0,), (10.0,)], schema)
    report = MyContract().validate(df, mode="soft")
    assert not report


def test_check_receives_kwargs_from_validate(spark):
    from pyspark.sql import functions as F

    class MyContract(Contract):
        odometer = Field(FloatType())

        @check("odometer must not exceed threshold")
        def under_threshold(self, df, threshold: float = 100.0):
            return df.filter(F.col("odometer") > threshold)

    schema = StructType([StructField("odometer", FloatType())])
    df = spark.createDataFrame([(50.0,)], schema)
    report = MyContract().validate(df, mode="soft", threshold=10.0)
    assert len(report.violations) == 1
    assert report.violations[0].column == "under_threshold"


def test_check_does_not_fire_when_kwarg_leaves_default(spark):
    from pyspark.sql import functions as F

    class MyContract(Contract):
        odometer = Field(FloatType())

        @check("odometer must not exceed threshold")
        def under_threshold(self, df, threshold: float = 100.0):
            return df.filter(F.col("odometer") > threshold)

    schema = StructType([StructField("odometer", FloatType())])
    df = spark.createDataFrame([(50.0,)], schema)
    report = MyContract().validate(df, mode="soft")
    assert not report


def test_validate_raises_type_error_for_unknown_kwarg(spark):
    class MyContract(Contract):
        odometer = Field(FloatType())

        @check("odometer must not exceed threshold")
        def under_threshold(self, df, threshold: float = 100.0):
            return df

    schema = StructType([StructField("odometer", FloatType())])
    df = spark.createDataFrame([(50.0,)], schema)
    with pytest.raises(TypeError):
        MyContract().validate(df, mode="soft", threshhold=10.0)


def test_check_lazy_false_stops_at_first_failing_check(spark):
    class MyContract(Contract):
        odometer = Field(FloatType())

        @check("check a")
        def check_a(self, df):
            return df

        @check("check b")
        def check_b(self, df):
            return df

    schema = StructType([StructField("odometer", FloatType())])
    df = spark.createDataFrame([(1.0,)], schema)
    report = MyContract().validate(df, mode="soft", lazy=False)
    assert len(report.violations) == 1
    assert report.violations[0].column == "check_a"


def test_check_skipped_when_missing_column(spark):
    class MyContract(Contract):
        vin = Field(StringType())
        odometer = Field(FloatType())

        @check("always fails")
        def always_fails(self, df):
            return df

    schema = StructType([StructField("vin", StringType())])
    df = spark.createDataFrame([("ABC",)], schema)
    report = MyContract().validate(df, mode="soft", lazy=True)
    kinds = [v.kind for v in report.violations]
    assert "missing_column" in kinds
    assert "check_failed" not in kinds


def test_check_still_runs_when_only_quality_violation_present(spark):
    class MyContract(Contract):
        odometer = Field(FloatType(), min_value=0.0)

        @check("always fails")
        def always_fails(self, df):
            return df

    schema = StructType([StructField("odometer", FloatType())])
    df = spark.createDataFrame([(-1.0,)], schema)
    report = MyContract().validate(df, mode="soft", lazy=True)
    kinds = [v.kind for v in report.violations]
    assert "value_out_of_range" in kinds
    assert "check_failed" in kinds
