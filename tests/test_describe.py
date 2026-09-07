from pyspark.sql.types import DecimalType, LongType, StringType

from pyspark_contracts._check import check
from pyspark_contracts._contract import Contract
from pyspark_contracts._field import Field


def test_describe_includes_contract_name():
    class MyContract(Contract):
        vin = Field(StringType())

    output = MyContract.describe()
    assert "MyContract" in output


def test_describe_includes_field_name_type_and_not_null():
    class MyContract(Contract):
        asset_hdr_id = Field(LongType(), nullable=False)

    output = MyContract.describe()
    assert "asset_hdr_id" in output
    assert "bigint" in output
    assert "not null" in output


def test_describe_marks_nullable_field():
    class MyContract(Contract):
        readingDate = Field(StringType())

    output = MyContract.describe()
    assert "nullable" in output


def test_describe_includes_constraint_summary():
    class MyContract(Contract):
        meter_reading_value = Field(DecimalType(26, 12), nullable=False, min_value=0)

    output = MyContract.describe()
    assert "decimal(26,12)" in output
    assert "min=0" in output


def test_describe_includes_description_and_metadata():
    class MyContract(Contract):
        meter_reading_value = Field(
            DecimalType(26, 12),
            description="Odometer reading in km",
            metadata={"unit": "km"},
        )

    output = MyContract.describe()
    assert "Odometer reading in km" in output
    assert "unit=km" in output


def test_describe_includes_checks_section():
    class MyContract(Contract):
        odometer = Field(LongType())

        @check("odometer must be non-negative")
        def no_negative(self, df):
            return df

    output = MyContract.describe()
    assert "Checks:" in output
    assert "no_negative" in output
    assert "odometer must be non-negative" in output


def test_describe_omits_checks_section_when_no_checks():
    class MyContract(Contract):
        vin = Field(StringType())

    output = MyContract.describe()
    assert "Checks:" not in output


def test_describe_includes_unique():
    class MyContract(Contract):
        vin = Field(StringType(), unique=True)

    output = MyContract.describe()
    assert "unique" in output


def test_describe_includes_row_bounds():
    class MyContract(Contract):
        min_rows = 10
        max_rows = 1000
        vin = Field(StringType())

    output = MyContract.describe()
    assert "min_rows=10" in output
    assert "max_rows=1000" in output


def test_describe_omits_row_bounds_line_when_unset():
    class MyContract(Contract):
        vin = Field(StringType())

    output = MyContract.describe()
    assert "Rows:" not in output
