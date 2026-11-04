import pytest
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from pyspark_contracts._contract import Contract
from pyspark_contracts._field import Field
from pyspark_contracts._report import Severity
from pyspark_contracts._schema import ContractSchema

SCHEMA = StructType([StructField("name", StringType())])
BLANKS = ["", "   ", "\t", "\n", " ", "　"]  # empty, spaces, tab, newline, NBSP, CJK space


def _df(spark, values):
    return spark.createDataFrame([(v,) for v in values], SCHEMA)


class Required(Contract):
    name = Field(StringType(), nullable=False, blank_is_null=True)


def test_every_blank_variant_and_null_are_missing(spark):
    df = _df(spark, BLANKS + [None, "x", " x ", "0"])
    violation = Required().validate(df, mode="soft").violations[0]
    assert violation.kind == "null_violation"
    assert violation.failure_count == len(BLANKS) + 1
    assert violation.constraint == "nullable, blank counts as null"
    assert violation.row_pct == 70.0  # 7 of 10 rows


def test_samples_are_the_blank_values_themselves(spark):
    violation = Required().validate(_df(spark, ["", "ok"]), mode="soft").violations[0]
    assert violation.sample_values == [""]


def test_filled_values_pass(spark):
    assert not Required().validate(_df(spark, ["a", " b ", "0"]), mode="soft")


def test_without_the_flag_blank_strings_are_fine(spark):
    class Plain(Contract):
        name = Field(StringType(), nullable=False)

    report = Plain().validate(_df(spark, BLANKS + [None]), mode="soft")
    assert report.violations[0].failure_count == 1
    assert report.violations[0].constraint == "nullable"


def test_only_the_nullable_check_is_affected(spark):
    class Both(Contract):
        name = Field(StringType(), nullable=False, blank_is_null=True, min_length=1)

    report = Both().validate(_df(spark, [""]), mode="soft")
    assert [(v.kind, v.constraint) for v in report.violations] == [
        ("null_violation", "nullable, blank counts as null"),
        ("length_out_of_range", "min_length=1"),
    ]


def test_works_with_severity(spark):
    class Soft(Contract):
        name = Field(StringType(), nullable=False, blank_is_null=True, severity="warn")

    report = Soft().validate(_df(spark, ["  "]))
    assert report.violations[0].severity is Severity.WARN


def test_on_a_column_with_a_dotted_name(spark):
    class Dotted(Contract):
        value = Field(StringType(), nullable=False, blank_is_null=True, column="a.b")

    df = spark.createDataFrame([(" ",), ("x",)], StructType([StructField("a.b", StringType())]))
    violation = Dotted().validate(df, mode="soft").violations[0]
    assert (violation.column, violation.failure_count) == ("a.b", 1)


def test_requires_a_required_field():
    with pytest.raises(ValueError, match="nullable=False"):
        Field(StringType(), blank_is_null=True)


def test_requires_a_string_type():
    with pytest.raises(ValueError, match="StringType"):
        Field(IntegerType(), nullable=False, blank_is_null=True)


def test_export_describe_and_reload(spark):
    assert Required.to_dict()["fields"]["name"]["blank_is_null"] is True
    plain = type("Plain", (Contract,), {"name": Field(StringType(), nullable=False)})
    assert "blank_is_null" not in plain.to_dict()["fields"]["name"]
    assert "blank_is_null" in Required.describe()
    report = ContractSchema.from_dict(Required.to_dict()).validate(_df(spark, [" "]), mode="soft")
    assert report.violations[0].failure_count == 1
