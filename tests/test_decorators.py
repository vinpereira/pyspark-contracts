import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, StructField, StructType

from pyspark_contracts._check import check
from pyspark_contracts._contract import Contract
from pyspark_contracts._decorators import check_output
from pyspark_contracts._field import Field
from pyspark_contracts._report import ContractViolationError


def test_check_output_returns_original_result(spark):
    class MyContract(Contract):
        odometer = Field(FloatType())

    @check_output(MyContract)
    def build(df):
        return df

    schema = StructType([StructField("odometer", FloatType())])
    df = spark.createDataFrame([(1.0,)], schema)
    result = build(df)
    assert result is df


def test_check_output_raises_on_violation(spark):
    class MyContract(Contract):
        odometer = Field(FloatType())
        vin = Field(FloatType())

    @check_output(MyContract)
    def build(df):
        return df

    schema = StructType([StructField("odometer", FloatType())])
    df = spark.createDataFrame([(1.0,)], schema)
    with pytest.raises(ContractViolationError):
        build(df)


def test_check_output_soft_mode_does_not_raise(spark):
    class MyContract(Contract):
        odometer = Field(FloatType())
        vin = Field(FloatType())

    @check_output(MyContract, mode="soft")
    def build(df):
        return df

    schema = StructType([StructField("odometer", FloatType())])
    df = spark.createDataFrame([(1.0,)], schema)
    result = build(df)
    assert result is df


def test_check_output_forwards_kwargs_to_validate(spark):
    class MyContract(Contract):
        odometer = Field(FloatType())

        @check("odometer must not exceed threshold")
        def under_threshold(self, df, threshold: float = 100.0):
            return df.filter(F.col("odometer") > threshold)

    @check_output(MyContract, threshold=10.0)
    def build(df):
        return df

    schema = StructType([StructField("odometer", FloatType())])
    df = spark.createDataFrame([(50.0,)], schema)
    with pytest.raises(ContractViolationError):
        build(df)


def test_check_output_preserves_function_metadata(spark):
    class MyContract(Contract):
        odometer = Field(FloatType())

    @check_output(MyContract)
    def build(df):
        """Docstring."""
        return df

    assert build.__name__ == "build"
    assert build.__doc__ == "Docstring."


def test_check_output_rejects_non_contract_class():
    with pytest.raises(TypeError):
        check_output(object)
