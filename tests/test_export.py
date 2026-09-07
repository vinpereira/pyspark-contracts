import json

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, LongType, StringType

from pyspark_contracts._check import check
from pyspark_contracts._contract import Contract
from pyspark_contracts._dtype_json import dtype_from_json
from pyspark_contracts._field import Field


def test_to_dict_includes_contract_name():
    class MyContract(Contract):
        vin = Field(StringType())

    d = MyContract.to_dict()
    assert d["contract"] == "MyContract"


def test_to_dict_field_includes_type_and_nullable():
    class MyContract(Contract):
        asset_hdr_id = Field(LongType(), nullable=False)

    d = MyContract.to_dict()
    field = d["fields"]["asset_hdr_id"]
    assert field["type"] == "bigint"
    assert field["nullable"] is False


def test_to_dict_field_includes_description_and_metadata():
    class MyContract(Contract):
        meter_reading_value = Field(
            DecimalType(26, 12),
            nullable=False,
            min_value=0,
            description="Odometer reading in km",
            metadata={"unit": "km"},
        )

    d = MyContract.to_dict()
    field = d["fields"]["meter_reading_value"]
    assert field["type"] == "decimal(26,12)"
    assert field["min_value"] == 0
    assert field["description"] == "Odometer reading in km"
    assert field["metadata"] == {"unit": "km"}


def test_to_dict_omits_unset_field_attributes():
    class MyContract(Contract):
        vin = Field(StringType())

    d = MyContract.to_dict()
    field = d["fields"]["vin"]
    assert "min_value" not in field
    assert "description" not in field
    assert "metadata" not in field


def test_to_dict_includes_condition_description_not_condition_itself():
    class MyContract(Contract):
        start_dt = Field(
            StringType(),
            condition=lambda f: F.col(f) < F.col("end_dt"),
            condition_description="start_dt must precede end_dt",
        )
        end_dt = Field(StringType())

    d = MyContract.to_dict()
    field = d["fields"]["start_dt"]
    assert field["condition_description"] == "start_dt must precede end_dt"
    assert "condition" not in field


def test_to_dict_includes_checks():
    class MyContract(Contract):
        odometer = Field(LongType())

        @check("odometer must be non-negative")
        def no_negative(self, df):
            return df

    d = MyContract.to_dict()
    assert d["checks"] == {"no_negative": "odometer must be non-negative"}


def test_to_dict_checks_empty_when_no_checks():
    class MyContract(Contract):
        vin = Field(StringType())

    d = MyContract.to_dict()
    assert d["checks"] == {}


def test_to_json_returns_valid_json_matching_to_dict():
    class MyContract(Contract):
        vin = Field(StringType(), nullable=False)

    json_str = MyContract.to_json()
    assert json.loads(json_str) == MyContract.to_dict()


def test_to_dict_field_includes_type_json_that_round_trips():
    class MyContract(Contract):
        meter_reading_value = Field(DecimalType(26, 12), nullable=False)

    d = MyContract.to_dict()
    field = d["fields"]["meter_reading_value"]
    assert "type_json" in field
    reconstructed = dtype_from_json(field["type_json"])
    assert reconstructed == DecimalType(26, 12)


def test_to_dict_field_includes_unique():
    class MyContract(Contract):
        vin = Field(StringType(), unique=True)

    d = MyContract.to_dict()
    assert d["fields"]["vin"]["unique"] is True


def test_to_dict_omits_unique_when_not_set():
    class MyContract(Contract):
        vin = Field(StringType())

    d = MyContract.to_dict()
    assert "unique" not in d["fields"]["vin"]


def test_to_dict_includes_min_rows_and_max_rows():
    class MyContract(Contract):
        min_rows = 10
        max_rows = 1000
        vin = Field(StringType())

    d = MyContract.to_dict()
    assert d["min_rows"] == 10
    assert d["max_rows"] == 1000


def test_to_dict_omits_min_rows_and_max_rows_when_unset():
    class MyContract(Contract):
        vin = Field(StringType())

    d = MyContract.to_dict()
    assert "min_rows" not in d
    assert "max_rows" not in d
