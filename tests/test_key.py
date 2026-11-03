import json
import uuid

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from pyspark_contracts._check import check
from pyspark_contracts._contract import Contract
from pyspark_contracts._field import Field
from pyspark_contracts._logging import get_logger
from pyspark_contracts._schema import ContractSchema

SCHEMA = StructType(
    [
        StructField("vin", StringType()),
        StructField("day", IntegerType()),
        StructField("km", FloatType()),
        StructField("note", StringType()),
    ]
)
ROWS = [
    ("V1", 1, -1.0, "ok"),
    ("V2", 1, 5.0, "toolong"),
    ("V3", 2, None, "ok"),
    ("V4", 2, -9.0, "ok"),
]


class Readings(Contract):
    key = ["vin", "day"]
    vin = Field(StringType())
    day = Field(IntegerType())
    km = Field(FloatType(), nullable=False, min_value=0)
    note = Field(StringType(), max_length=3)


def _by_kind(report):
    return {(v.kind.value, v.constraint): v for v in report.violations}


def test_sample_keys_line_up_with_sample_values(spark):
    violations = _by_kind(Readings().validate(spark.createDataFrame(ROWS, SCHEMA), mode="soft"))
    below_zero = violations[("value_out_of_range", "min_value=0")]
    assert below_zero.sample_values == [-1.0, -9.0]
    assert below_zero.sample_keys == [{"vin": "V1", "day": 1}, {"vin": "V4", "day": 2}]
    nulls = violations[("null_violation", "nullable")]
    assert nulls.sample_values == [None]
    assert nulls.sample_keys == [{"vin": "V3", "day": 2}]
    too_long = violations[("length_out_of_range", "max_length=3")]
    assert too_long.sample_keys == [{"vin": "V2", "day": 1}]


def test_sample_keys_for_every_row_level_rule(spark):
    class Rules(Contract):
        key = ["vin"]
        vin = Field(StringType())
        note = Field(StringType(), regex="[a-z]{2}", allowed_values=["ok"], min_length=3)
        day = Field(IntegerType(), condition=lambda f: F.col(f) < 2)

    report = Rules().validate(spark.createDataFrame(ROWS, SCHEMA), mode="soft")
    assert {v.kind.value for v in report.violations} == {
        "regex_mismatch",
        "value_not_allowed",
        "length_out_of_range",
        "condition_failed",
    }
    assert all(v.sample_keys for v in report.violations)
    condition = next(v for v in report.violations if v.kind == "condition_failed")
    assert condition.sample_keys == [{"vin": "V3"}, {"vin": "V4"}]


def test_no_key_means_no_sample_keys(spark):
    class NoKey(Contract):
        km = Field(FloatType(), min_value=0)

    violation = NoKey().validate(spark.createDataFrame(ROWS, SCHEMA), mode="soft").violations[0]
    assert violation.sample_keys is None
    assert (
        "sample_keys"
        not in NoKey()
        .validate(spark.createDataFrame(ROWS, SCHEMA), mode="soft")
        .to_dict()["violations"][0]
    )


def test_key_on_the_failing_column_itself(spark):
    class Self(Contract):
        key = ["km"]
        km = Field(FloatType(), min_value=0)

    violation = Self().validate(spark.createDataFrame(ROWS, SCHEMA), mode="soft").violations[0]
    assert violation.sample_keys == [{"km": -1.0}, {"km": -9.0}]


def test_unique_and_check_violations_have_no_sample_keys(spark):
    class Others(Contract):
        key = ["vin"]
        vin = Field(StringType())
        day = Field(IntegerType(), unique=True)

        @check("km must be non-negative")
        def non_negative(self, df):
            return df.filter(F.col("km") < 0)

    report = Others().validate(spark.createDataFrame(ROWS, SCHEMA), mode="soft")
    assert {v.kind.value for v in report.violations} == {"duplicate_value", "check_failed"}
    assert all(v.sample_keys is None for v in report.violations)


def test_keys_are_in_the_structured_report_but_never_in_the_summary_or_message(spark):
    report = Readings().validate(spark.createDataFrame(ROWS, SCHEMA), mode="soft")
    assert report.to_dict()["violations"][0]["sample_keys"]
    assert "V1" not in report.summary() and "V4" not in report.summary()


def test_log_message_has_no_keys_but_the_fields_do(spark, capsys):
    logger = get_logger(f"test-{uuid.uuid4()}")
    Readings().validate(spark.createDataFrame(ROWS, SCHEMA), mode="soft", logger=logger)
    entry = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert "V1" not in entry["message"]
    below_zero = next(v for v in entry["violations"] if v["constraint"] == "min_value=0")
    assert below_zero["sample_keys"] == [{"vin": "V1", "day": 1}, {"vin": "V4", "day": 2}]


def test_key_with_dotted_and_aliased_columns(spark):
    class Dotted(Contract):
        key = ["a.id"]
        ident = Field(LongType(), column="a.id")
        value = Field(LongType(), min_value=0, column="a.value")

    schema = StructType([StructField("a.id", LongType()), StructField("a.value", LongType())])
    df = spark.createDataFrame([(1, 5), (2, -3)], schema)
    violation = Dotted().validate(df, mode="soft").violations[0]
    assert (violation.column, violation.sample_values, violation.sample_keys) == (
        "a.value",
        [-3],
        [{"a.id": 2}],
    )


def test_key_is_inherited_and_can_be_overridden():
    class Child(Readings):
        pass

    class Other(Readings):
        key = ["vin"]

    assert Child.key == ("vin", "day")
    assert Other.key == ("vin",)


def test_key_must_name_declared_fields():
    with pytest.raises(TypeError, match="key column 'nope' is not a declared field"):

        class Bad(Contract):
            key = ["vin", "nope"]
            vin = Field(StringType())


def test_key_entries_must_be_strings():
    with pytest.raises(TypeError, match="key"):

        class Bad(Contract):
            key = ["vin", 3]
            vin = Field(StringType())


def test_key_given_as_a_bare_string_is_rejected():
    with pytest.raises(TypeError, match="list of column names"):

        class Bad(Contract):
            key = "vin"
            vin = Field(StringType())


def test_export_describe_and_reload(spark):
    assert Readings.to_dict()["key"] == ["vin", "day"]
    assert "Key: vin, day" in Readings.describe().splitlines()
    assert "key" not in type("Plain", (Contract,), {"vin": Field(StringType())}).to_dict()
    schema = ContractSchema.from_dict(Readings.to_dict())
    violations = _by_kind(schema.validate(spark.createDataFrame(ROWS, SCHEMA), mode="soft"))
    below_zero = violations[("value_out_of_range", "min_value=0")]
    assert below_zero.sample_keys == [{"vin": "V1", "day": 1}, {"vin": "V4", "day": 2}]


def test_reload_rejects_a_key_that_is_not_a_field():
    exported = Readings.to_dict()
    exported["key"] = ["vin", "nope"]
    with pytest.raises(ValueError, match="key column 'nope'"):
        ContractSchema.from_dict(exported)
