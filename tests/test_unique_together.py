import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

from pyspark_contracts._check import check
from pyspark_contracts._contract import Contract
from pyspark_contracts._field import Field
from pyspark_contracts._report import ContractViolationError
from pyspark_contracts._schema import ContractSchema

SCHEMA = StructType(
    [
        StructField("customer", StringType()),
        StructField("day", IntegerType()),
        StructField("note", StringType()),
    ]
)


class PerDay(Contract):
    unique_together = [("customer", "day")]
    customer = Field(StringType())
    day = Field(IntegerType())
    note = Field(StringType())


def _df(spark, rows):
    return spark.createDataFrame(rows, SCHEMA)


def test_duplicate_combinations_are_reported_per_row(spark):
    df = _df(spark, [("A", 1, "x"), ("A", 1, "y"), ("A", 2, "z"), ("B", 1, "w"), ("B", 1, "v")])
    violation = PerDay().validate(df, mode="soft").violations[0]
    assert violation.kind == "duplicate_value"
    assert violation.column is None
    assert violation.constraint == "unique_together=(customer, day)"
    assert violation.failure_count == 4
    assert violation.row_pct == 80.0
    assert sorted(violation.sample_values, key=lambda d: d["customer"]) == [
        {"customer": "A", "day": 1},
        {"customer": "B", "day": 1},
    ]


def test_same_value_in_one_column_alone_is_fine(spark):
    df = _df(spark, [("A", 1, "x"), ("A", 2, "x"), ("B", 1, "x")])
    assert not PerDay().validate(df, mode="soft")


def test_a_combination_with_a_null_is_never_a_duplicate(spark):
    df = _df(spark, [("A", None, "x"), ("A", None, "y"), (None, None, "z"), (None, None, "w")])
    assert not PerDay().validate(df, mode="soft")


def test_three_columns(spark):
    class Triple(Contract):
        unique_together = [("customer", "day", "note")]
        customer = Field(StringType())
        day = Field(IntegerType())
        note = Field(StringType())

    df = _df(spark, [("A", 1, "x"), ("A", 1, "x"), ("A", 1, "y")])
    violation = Triple().validate(df, mode="soft").violations[0]
    assert violation.constraint == "unique_together=(customer, day, note)"
    assert violation.failure_count == 2


def test_several_constraints_are_reported_in_declaration_order(spark):
    class Two(Contract):
        unique_together = [("customer", "day"), ("customer", "note")]
        customer = Field(StringType())
        day = Field(IntegerType())
        note = Field(StringType())

    df = _df(spark, [("A", 1, "x"), ("A", 1, "y"), ("B", 2, "z"), ("B", 3, "z")])
    report = Two().validate(df, mode="soft")
    assert [v.constraint for v in report.violations] == [
        "unique_together=(customer, day)",
        "unique_together=(customer, note)",
    ]


def test_evaluated_after_every_field_rule(spark):
    class Mixed(Contract):
        unique_together = [("customer", "day")]
        customer = Field(StringType(), nullable=False)
        day = Field(IntegerType(), unique=True)
        note = Field(StringType(), max_length=1)

    df = _df(spark, [("A", 1, "xx"), ("A", 1, "y"), (None, 2, "z")])
    assert [(v.kind.value, v.column) for v in Mixed().validate(df, mode="soft").violations] == [
        ("null_violation", "customer"),
        ("duplicate_value", "day"),
        ("length_out_of_range", "note"),
        ("duplicate_value", None),
    ]


def test_fail_fast_stops_at_the_first_error_before_it_runs(spark):
    class Early(Contract):
        unique_together = [("customer", "day")]
        customer = Field(StringType(), nullable=False)
        day = Field(IntegerType())

    df = _df(spark, [(None, 1, "x"), ("A", 1, "y"), ("A", 1, "z")])
    report = Early().validate(df, mode="soft", lazy=False)
    assert [(v.kind.value, v.column) for v in report.violations] == [("null_violation", "customer")]


def test_hard_mode_raises(spark):
    with pytest.raises(ContractViolationError) as exc_info:
        PerDay().validate(_df(spark, [("A", 1, "x"), ("A", 1, "y")]))
    assert exc_info.value.report.violations[0].kind == "duplicate_value"


def test_skipped_when_one_of_its_columns_is_missing(spark):
    df = spark.createDataFrame([("A", "x")], StructType(SCHEMA.fields[:1] + SCHEMA.fields[2:]))
    report = PerDay().validate(df, mode="soft")
    assert [(v.kind.value, v.column) for v in report.violations] == [("missing_column", "day")]


def test_always_an_error_even_next_to_warning_fields(spark):
    class Soft(Contract):
        unique_together = [("customer", "day")]
        customer = Field(StringType(), severity="warn", max_failure_pct=100)
        day = Field(IntegerType(), severity="warn", max_failure_pct=100)

    report = Soft().validate(_df(spark, [("A", 1, "x"), ("A", 1, "y")]), mode="soft")
    assert report.violations[0].severity == "error"


def test_summary_reads_naturally(spark):
    report = PerDay().validate(_df(spark, [("A", 1, "x"), ("A", 1, "y")]), mode="soft")
    assert report.summary() == (
        "1 violation(s) — duplicate_value (unique_together=(customer, day)): 2 rows"
    )


def test_with_aliased_dotted_columns(spark):
    class Dotted(Contract):
        unique_together = [("a.id", "a.day")]
        ident = Field(LongType(), column="a.id")
        day = Field(LongType(), column="a.day")

    schema = StructType([StructField("a.id", LongType()), StructField("a.day", LongType())])
    df = spark.createDataFrame([(1, 1), (1, 1), (1, 2)], schema)
    violation = Dotted().validate(df, mode="soft").violations[0]
    assert violation.failure_count == 2
    assert violation.sample_values == [{"a.id": 1, "a.day": 1}]


def test_inherited_and_overridable():
    class Child(PerDay):
        pass

    class Other(PerDay):
        unique_together = [("customer", "note")]

    assert Child.unique_together == (("customer", "day"),)
    assert Other.unique_together == (("customer", "note"),)


def test_works_next_to_a_check(spark):
    class WithCheck(Contract):
        unique_together = [("customer", "day")]
        customer = Field(StringType())
        day = Field(IntegerType())

        @check("day must be positive")
        def positive(self, df):
            return df.filter(F.col("day") <= 0)

    report = WithCheck().validate(
        _df(spark, [("A", 1, "x"), ("A", 1, "y"), ("B", 0, "z")]), mode="soft"
    )
    assert [v.kind.value for v in report.violations] == ["duplicate_value", "check_failed"]


def test_one_column_points_to_field_unique():
    with pytest.raises(ValueError, match=r"Field\(unique=True\)"):

        class One(Contract):
            unique_together = [("customer",)]
            customer = Field(StringType())


def test_must_name_declared_fields():
    with pytest.raises(TypeError, match="unique_together column 'nope' is not a declared field"):

        class Bad(Contract):
            unique_together = [("customer", "nope")]
            customer = Field(StringType())


def test_malformed_values_are_rejected():
    with pytest.raises(TypeError, match="unique_together"):

        class NotTuples(Contract):
            unique_together = ["customer", "day"]
            customer = Field(StringType())
            day = Field(IntegerType())

    with pytest.raises(ValueError, match="repeats"):

        class Repeated(Contract):
            unique_together = [("customer", "customer")]
            customer = Field(StringType())


def test_export_describe_and_reload(spark):
    assert PerDay.to_dict()["unique_together"] == [["customer", "day"]]
    assert "Unique together: (customer, day)" in PerDay.describe().splitlines()
    assert "unique_together" not in type("P", (Contract,), {"a": Field(StringType())}).to_dict()
    schema = ContractSchema.from_dict(PerDay.to_dict())
    report = schema.validate(_df(spark, [("A", 1, "x"), ("A", 1, "y")]), mode="soft")
    assert report.violations[0].constraint == "unique_together=(customer, day)"


def test_reload_rejects_an_undeclared_column():
    exported = PerDay.to_dict()
    exported["unique_together"] = [["customer", "nope"]]
    with pytest.raises(ValueError, match="unique_together column 'nope'"):
        ContractSchema.from_dict(exported)
