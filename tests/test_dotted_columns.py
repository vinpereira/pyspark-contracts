import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

from pyspark_contracts._contract import Contract
from pyspark_contracts._field import Field
from pyspark_contracts._schema import ContractSchema

# Glue's relationalize() produces names like these; a class attribute can't hold a dot, so
# the contract is built from a namespace dict.
DOTTED = "accumulatedData.duration"
TICKED = "odd`name"


def _contract(**fields):
    return type("DottedContract", (Contract,), fields)


def _df(spark, name, rows, dtype=LongType()):
    return spark.createDataFrame(rows, StructType([StructField(name, dtype)]))


def test_row_level_constraints_on_a_dotted_column(spark):
    contract = _contract(**{DOTTED: Field(LongType(), nullable=False, min_value=0, max_value=100)})
    df = _df(spark, DOTTED, [(5,), (None,), (-1,), (500,)])
    report = contract().validate(df, mode="soft")
    assert [(v.kind, v.column, v.failure_count, v.sample_values) for v in report.violations] == [
        ("null_violation", DOTTED, 1, [None]),
        ("value_out_of_range", DOTTED, 1, [-1]),
        ("value_out_of_range", DOTTED, 1, [500]),
    ]


def test_string_constraints_on_a_dotted_column(spark):
    contract = _contract(
        **{
            "a.b": Field(
                StringType(),
                min_length=2,
                max_length=3,
                regex="[a-z]+",
                allowed_values=["ab", "xy"],
            )
        }
    )
    df = _df(spark, "a.b", [("ab",), ("x",), ("TOOLONG",)], StringType())
    report = contract().validate(df, mode="soft")
    assert {v.kind for v in report.violations} == {
        "length_out_of_range",
        "regex_mismatch",
        "value_not_allowed",
    }


def test_unique_on_a_dotted_column(spark):
    contract = _contract(**{DOTTED: Field(LongType(), unique=True)})
    report = contract().validate(_df(spark, DOTTED, [(1,), (1,), (2,), (None,)]), mode="soft")
    violation = report.violations[0]
    assert (violation.kind, violation.column) == ("duplicate_value", DOTTED)
    assert violation.failure_count == 2
    assert violation.sample_values == [1]


def test_unique_on_a_column_named_count(spark):
    # unique counts duplicates with a helper column; it must not collide with the user's
    contract = _contract(count=Field(LongType(), unique=True))
    report = contract().validate(_df(spark, "count", [(1,), (1,), (2,)]), mode="soft")
    violation = report.violations[0]
    assert (violation.kind, violation.column) == ("duplicate_value", "count")
    assert (violation.failure_count, violation.sample_values) == (2, [1])


def test_valid_dotted_column_passes(spark):
    contract = _contract(**{DOTTED: Field(LongType(), nullable=False, min_value=0, unique=True)})
    assert not contract().validate(_df(spark, DOTTED, [(1,), (2,)]), mode="soft")


def test_a_backtick_in_the_column_name(spark):
    contract = _contract(**{TICKED: Field(LongType(), nullable=False, unique=True)})
    report = contract().validate(_df(spark, TICKED, [(1,), (1,), (None,)]), mode="soft")
    assert [v.kind for v in report.violations] == ["null_violation", "duplicate_value"]


def test_sample_values_come_from_the_flat_column_not_a_nested_field(spark):
    # `a` is a struct with a field `b`, and there is also a flat column literally named "a.b"
    schema = StructType(
        [
            StructField("a", StructType([StructField("b", LongType())])),
            StructField("a.b", LongType()),
        ]
    )
    df = spark.createDataFrame([((1,), None), ((2,), 7)], schema)
    contract = _contract(**{"a.b": Field(LongType(), nullable=False)})
    violation = contract().validate(df, mode="soft").violations[0]
    assert (violation.failure_count, violation.sample_values) == (1, [None])


def test_condition_receives_the_raw_column_name(spark):
    received = []

    def condition(name):
        received.append(name)
        return F.col(f"`{name}`") >= 0

    contract = _contract(**{DOTTED: Field(LongType(), condition=condition)})
    report = contract().validate(_df(spark, DOTTED, [(1,), (-1,)]), mode="soft")
    assert received == [DOTTED]
    assert [(v.kind, v.column) for v in report.violations] == [("condition_failed", DOTTED)]


def test_reloaded_schema_validates_dotted_columns(spark):
    contract = _contract(**{DOTTED: Field(LongType(), nullable=False, min_value=0, unique=True)})
    schema = ContractSchema.from_dict(contract.to_dict())
    report = schema.validate(_df(spark, DOTTED, [(1,), (1,), (None,), (-5,)]), mode="soft")
    assert {v.kind for v in report.violations} == {
        "null_violation",
        "value_out_of_range",
        "duplicate_value",
    }


@pytest.mark.parametrize("lazy", [True, False])
def test_missing_dotted_column_is_reported_not_raised(spark, lazy):
    contract = _contract(**{DOTTED: Field(LongType())})
    df = _df(spark, "other", [(1,)])
    report = contract().validate(df, mode="soft", lazy=lazy)
    assert [(v.kind, v.column) for v in report.violations] == [("missing_column", DOTTED)]
