from pyspark.sql.types import (
    ArrayType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from pyspark_contracts._contract import Contract
from pyspark_contracts._field import Field
from pyspark_contracts._schema import ContractSchema


class ReadingContract(Contract):
    strict = True
    vin = Field(StringType(), nullable=False, min_length=17, description="Vehicle identifier")
    odometer = Field(DecimalType(18, 2), min_value=0)
    tags = Field(ArrayType(StringType()))


def test_to_struct_type_follows_declaration_order_types_and_nullability():
    struct = ReadingContract.to_struct_type()
    assert [(f.name, f.dataType, f.nullable) for f in struct.fields] == [
        ("vin", StringType(), False),
        ("odometer", DecimalType(18, 2), True),
        ("tags", ArrayType(StringType()), True),
    ]


def test_to_struct_type_carries_description_as_column_comment():
    struct = ReadingContract.to_struct_type()
    assert struct["vin"].metadata == {"comment": "Vehicle identifier"}
    assert struct["odometer"].metadata == {}


def test_to_struct_type_includes_inherited_fields():
    class WithAsset(ReadingContract):
        asset_id = Field(IntegerType())

    assert WithAsset.to_struct_type().fieldNames() == ["vin", "odometer", "tags", "asset_id"]


def test_empty_df_has_the_contract_schema_and_no_rows(spark):
    df = ReadingContract.empty_df(spark)
    assert df.count() == 0
    assert [(f.name, f.dataType) for f in df.schema.fields] == [
        ("vin", StringType()),
        ("odometer", DecimalType(18, 2)),
        ("tags", ArrayType(StringType())),
    ]


def test_empty_df_passes_its_own_strict_contract(spark):
    assert not ReadingContract().validate(ReadingContract.empty_df(spark), mode="soft")


def test_to_struct_type_as_read_schema(spark, tmp_path):
    class CsvContract(Contract):
        vin = Field(StringType())
        km = Field(IntegerType(), min_value=0)

    path = tmp_path / "readings.csv"
    path.write_text("ABC,10\nDEF,-5\n")

    df = spark.read.schema(CsvContract.to_struct_type()).csv(str(path))
    report = CsvContract().validate(df, mode="soft")
    assert [(v.kind, v.column, v.failure_count) for v in report.violations] == [
        ("value_out_of_range", "km", 1)
    ]


def test_reloaded_schema_builds_the_same_struct_type(spark):
    schema = ContractSchema.from_dict(ReadingContract.to_dict())
    assert schema.to_struct_type() == ReadingContract.to_struct_type()
    assert schema.empty_df(spark).schema == ReadingContract.empty_df(spark).schema


def test_nested_struct_types_pass_through_unchanged():
    point = StructType(
        [StructField("lat", DecimalType(9, 6)), StructField("lon", DecimalType(9, 6))]
    )

    class GeoContract(Contract):
        location = Field(point, nullable=False)

    assert GeoContract.to_struct_type()["location"].dataType == point
