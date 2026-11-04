import pytest
from pyspark.sql.types import LongType, StringType, StructField, StructType

from pyspark_contracts._contract import Contract
from pyspark_contracts._field import Field
from pyspark_contracts._schema import ContractSchema

DOTTED = "accumulatedData.duration"


class Telemetry(Contract):
    duration = Field(LongType(), nullable=False, min_value=0, column=DOTTED)
    vin = Field(StringType(), nullable=False)


def _df(spark, rows):
    schema = StructType([StructField(DOTTED, LongType()), StructField("vin", StringType())])
    return spark.createDataFrame(rows, schema)


def test_attribute_stays_a_field_and_the_real_name_keys_the_contract():
    assert isinstance(Telemetry.duration, Field)
    assert list(Telemetry._fields) == [DOTTED, "vin"]
    assert Telemetry._fields[DOTTED] is Telemetry.duration


def test_validates_the_real_column_and_reports_its_real_name(spark):
    report = Telemetry().validate(_df(spark, [(5, "A"), (-1, "B"), (None, "C")]), mode="soft")
    assert [(v.kind, v.column, v.failure_count) for v in report.violations] == [
        ("null_violation", DOTTED, 1),
        ("value_out_of_range", DOTTED, 1),
    ]


def test_missing_column_is_reported_by_real_name(spark):
    df = spark.createDataFrame([("A",)], StructType([StructField("vin", StringType())]))
    report = Telemetry().validate(df, mode="soft")
    assert [(v.kind, v.column) for v in report.violations] == [("missing_column", DOTTED)]


def test_to_dict_and_reload_use_the_real_name(spark):
    exported = Telemetry.to_dict()
    assert list(exported["fields"]) == [DOTTED, "vin"]
    assert "duration" not in exported["fields"]
    report = ContractSchema.from_dict(exported).validate(_df(spark, [(-1, "A")]), mode="soft")
    assert [(v.kind, v.column) for v in report.violations] == [("value_out_of_range", DOTTED)]


def test_describe_and_struct_type_use_the_real_name():
    assert DOTTED in Telemetry.describe()
    assert Telemetry.to_struct_type().fieldNames() == [DOTTED, "vin"]


def test_subclass_overrides_by_real_column_name():
    class Relaxed(Telemetry):
        renamed = Field(LongType(), column=DOTTED)

    assert list(Relaxed._fields) == [DOTTED, "vin"]
    assert Relaxed._fields[DOTTED] is Relaxed.renamed
    assert Relaxed._fields[DOTTED].nullable is True
    assert Telemetry._fields[DOTTED].nullable is False


def test_two_attributes_for_the_same_column_are_rejected():
    with pytest.raises(TypeError, match="both declare column 'x.y'"):

        class Clash(Contract):
            a = Field(LongType(), column="x.y")
            b = Field(LongType(), column="x.y")


def test_column_may_name_another_attributes_column_only_across_inheritance():
    class Base(Contract):
        vin = Field(StringType())

    class Sub(Base):
        other = Field(LongType(), column="vin")

    assert Sub._fields["vin"] is Sub.other


def test_empty_column_is_rejected():
    with pytest.raises(ValueError, match="column"):
        Field(LongType(), column="")
