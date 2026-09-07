import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, FloatType, StringType, StructField, StructType

from pyspark_contracts._check import check
from pyspark_contracts._contract import Contract
from pyspark_contracts._field import Field
from pyspark_contracts._report import ContractViolationError
from pyspark_contracts._schema import ContractSchema


def test_from_dict_reconstructs_field_type_and_nullable():
    class MyContract(Contract):
        odometer = Field(FloatType(), nullable=False)

    schema = ContractSchema.from_dict(MyContract.to_dict())
    field = schema._fields["odometer"]
    assert field.dtype == FloatType()
    assert field.nullable is False


def test_from_dict_reconstructs_decimal_precision_and_scale():
    class MyContract(Contract):
        meter_reading_value = Field(DecimalType(26, 12))

    schema = ContractSchema.from_dict(MyContract.to_dict())
    assert schema._fields["meter_reading_value"].dtype == DecimalType(26, 12)


def test_from_dict_reconstructs_unique():
    class MyContract(Contract):
        vin = Field(StringType(), unique=True)

    schema = ContractSchema.from_dict(MyContract.to_dict())
    assert schema._fields["vin"].unique is True


def test_from_dict_reconstructs_quality_constraints():
    class MyContract(Contract):
        odometer = Field(FloatType(), min_value=0.0, max_value=100.0)

    schema = ContractSchema.from_dict(MyContract.to_dict())
    field = schema._fields["odometer"]
    assert field.min_value == 0.0
    assert field.max_value == 100.0


def test_validate_detects_missing_column(spark):
    class MyContract(Contract):
        vin = Field(StringType())
        odometer = Field(FloatType())

    schema = ContractSchema.from_dict(MyContract.to_dict())
    df = spark.createDataFrame([], StructType([StructField("vin", StringType())]))
    report = schema.validate(df, mode="soft")
    assert len(report.violations) == 1
    assert report.violations[0].kind == "missing_column"


def test_validate_detects_quality_violation(spark):
    class MyContract(Contract):
        odometer = Field(FloatType(), min_value=0.0)

    schema = ContractSchema.from_dict(MyContract.to_dict())
    df_schema = StructType([StructField("odometer", FloatType())])
    df = spark.createDataFrame([(-1.0,)], df_schema)
    report = schema.validate(df, mode="soft")
    assert len(report.violations) == 1
    assert report.violations[0].kind == "value_out_of_range"


def test_report_uses_original_contract_name(spark):
    class MyContract(Contract):
        odometer = Field(FloatType())

    schema = ContractSchema.from_dict(MyContract.to_dict())
    df = spark.createDataFrame([], StructType([]))
    report = schema.validate(df, mode="soft")
    assert report.contract_name == "MyContract"


def test_hard_mode_error_message_uses_original_contract_name(spark):
    class MyContract(Contract):
        odometer = Field(FloatType())

    schema = ContractSchema.from_dict(MyContract.to_dict())
    df = spark.createDataFrame([], StructType([]))
    with pytest.raises(ContractViolationError) as exc_info:
        schema.validate(df)
    assert "MyContract" in str(exc_info.value)


def test_from_dict_warns_about_unreconstructable_checks():
    class MyContract(Contract):
        odometer = Field(FloatType())

        @check("odometer must be non-negative")
        def no_negative(self, df):
            return df.filter(F.col("odometer") < 0)

    with pytest.warns(UserWarning, match="no_negative"):
        ContractSchema.from_dict(MyContract.to_dict())


def test_from_dict_warns_about_unreconstructable_condition():
    class MyContract(Contract):
        start_dt = Field(
            StringType(),
            condition=lambda f: F.col(f) < F.col("end_dt"),
            condition_description="start before end",
        )
        end_dt = Field(StringType())

    with pytest.warns(UserWarning, match="start_dt"):
        ContractSchema.from_dict(MyContract.to_dict())


def test_from_dict_does_not_warn_without_checks_or_condition(recwarn):
    class MyContract(Contract):
        vin = Field(StringType())

    ContractSchema.from_dict(MyContract.to_dict())
    assert len(recwarn) == 0


def test_from_json_round_trips():
    class MyContract(Contract):
        odometer = Field(FloatType(), nullable=False)

    schema = ContractSchema.from_json(MyContract.to_json())
    assert schema._fields["odometer"].nullable is False


def test_check_kwarg_rejected_since_no_checks_exist(spark):
    class MyContract(Contract):
        odometer = Field(FloatType())

    schema = ContractSchema.from_dict(MyContract.to_dict())
    df = spark.createDataFrame([(1.0,)], StructType([StructField("odometer", FloatType())]))
    with pytest.raises(TypeError):
        schema.validate(df, mode="soft", threshold=10.0)


def test_from_dict_reconstructs_min_rows_and_max_rows():
    class MyContract(Contract):
        min_rows = 10
        max_rows = 1000
        vin = Field(StringType())

    schema = ContractSchema.from_dict(MyContract.to_dict())
    assert schema.min_rows == 10
    assert schema.max_rows == 1000


def test_reloaded_schema_enforces_min_rows(spark):
    class MyContract(Contract):
        min_rows = 5
        vin = Field(StringType())

    schema = ContractSchema.from_dict(MyContract.to_dict())
    df = spark.createDataFrame([("a",), ("b",)], StructType([StructField("vin", StringType())]))
    report = schema.validate(df, mode="soft")
    assert len(report.violations) == 1
    assert report.violations[0].kind == "row_count_out_of_range"
