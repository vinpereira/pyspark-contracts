import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, StructField, StructType

from pyspark_contracts._check import check
from pyspark_contracts._contract import Contract
from pyspark_contracts._decorators import check_input, check_output
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


def test_check_input_validates_before_function_runs(spark):
    class MyContract(Contract):
        odometer = Field(FloatType())
        vin = Field(FloatType())

    calls = []

    @check_input(MyContract, param="df")
    def process(df):
        calls.append("ran")
        return df

    schema = StructType([StructField("odometer", FloatType())])
    df = spark.createDataFrame([(1.0,)], schema)
    with pytest.raises(ContractViolationError):
        process(df)
    assert calls == []


def test_check_input_passes_through_valid_input(spark):
    class MyContract(Contract):
        odometer = Field(FloatType())

    @check_input(MyContract, param="df")
    def process(df):
        return df

    schema = StructType([StructField("odometer", FloatType())])
    df = spark.createDataFrame([(1.0,)], schema)
    result = process(df)
    assert result is df


def test_check_input_finds_param_passed_positionally(spark):
    class MyContract(Contract):
        odometer = Field(FloatType())
        vin = Field(FloatType())

    @check_input(MyContract, param="df")
    def process(other_arg, df):
        return df

    schema = StructType([StructField("odometer", FloatType())])
    df = spark.createDataFrame([(1.0,)], schema)
    with pytest.raises(ContractViolationError):
        process("ignored", df)


def test_check_input_finds_param_passed_by_keyword(spark):
    class MyContract(Contract):
        odometer = Field(FloatType())
        vin = Field(FloatType())

    @check_input(MyContract, param="df")
    def process(other_arg, df):
        return df

    schema = StructType([StructField("odometer", FloatType())])
    df = spark.createDataFrame([(1.0,)], schema)
    with pytest.raises(ContractViolationError):
        process(other_arg="ignored", df=df)


def test_check_input_raises_at_decoration_time_for_unknown_param():
    class MyContract(Contract):
        odometer = Field(FloatType())

    with pytest.raises(TypeError):

        @check_input(MyContract, param="does_not_exist")
        def process(df):
            return df


def test_check_input_rejects_non_contract_class():
    with pytest.raises(TypeError):
        check_input(object, param="df")


def test_stacked_check_input_validates_both_params(spark):
    class ContractA(Contract):
        a_col = Field(FloatType())
        vin = Field(FloatType())

    class ContractB(Contract):
        b_col = Field(FloatType())

    @check_input(ContractA, param="df_a")
    @check_input(ContractB, param="df_b")
    def process(df_a, df_b):
        return df_a

    schema_a = StructType([StructField("a_col", FloatType())])
    schema_b = StructType([StructField("b_col", FloatType())])
    df_a = spark.createDataFrame([(1.0,)], schema_a)
    df_b = spark.createDataFrame([(1.0,)], schema_b)
    with pytest.raises(ContractViolationError):
        process(df_a, df_b)
