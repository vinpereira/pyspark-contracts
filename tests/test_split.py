import uuid
import warnings

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
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
from pyspark_contracts._report import ContractViolationError
from pyspark_contracts._schema import ContractSchema
from pyspark_contracts._split import SplitResult

SCHEMA = StructType(
    [
        StructField("vin", StringType()),
        StructField("day", IntegerType()),
        StructField("km", FloatType()),
        StructField("note", StringType()),
    ]
)
ROWS = [
    ("V1", 1, 10.0, "ok"),  # clean
    ("V2", 1, -5.0, "ok"),  # km below 0
    (None, 2, 3.0, "ok"),  # vin missing
    ("V4", 2, None, "ok"),  # km null: not rejected by min_value (null < 0 is null)
    ("V5", 3, 1.0, "  "),  # blank note
    ("V6", 3, -1.0, ""),  # km below 0 and blank note
    ("V1", 1, 7.0, "ok"),  # duplicates V1 on (vin, day)
]


class Readings(Contract):
    unique_together = [("vin", "day")]

    vin = Field(StringType(), nullable=False)
    day = Field(IntegerType())
    km = Field(FloatType(), min_value=0)
    note = Field(StringType(), nullable=False, blank_is_null=True)


def _df(spark, rows=ROWS):
    return spark.createDataFrame(rows, SCHEMA)


def _violations(rejected, errors="_violations"):
    return {
        row["vin"] if row["vin"] is not None else "<null>": [
            (v["kind"], v["column"], v["constraint"]) for v in row[errors]
        ]
        for row in rejected.collect()
    }


def _spark_jobs(spark, fn) -> int:
    try:
        sc = spark.sparkContext
    except Exception:
        pytest.skip("counting Spark jobs needs a SparkContext, which Spark Connect doesn't have")
    group = f"test-{uuid.uuid4().hex}"
    sc.setJobGroup(group, group)
    fn()
    return len(sc.statusTracker().getJobIdsForGroup(group))


def test_every_row_lands_in_exactly_one_side(spark):
    df = _df(spark)
    valid, rejected = Readings().split(df)
    assert valid.count() + rejected.count() == df.count()
    assert valid.union(rejected.drop("_violations")).exceptAll(df).count() == 0
    assert df.exceptAll(valid.union(rejected.drop("_violations"))).count() == 0
    assert {r["vin"] for r in valid.collect()} == {"V4"}


def test_valid_keeps_the_original_columns_and_rejected_adds_the_errors_column(spark):
    valid, rejected = Readings().split(_df(spark))
    assert valid.columns == ["vin", "day", "km", "note"]
    assert rejected.columns == ["vin", "day", "km", "note", "_violations"]
    errors_type = rejected.schema["_violations"].dataType
    assert (
        errors_type.simpleString() == "array<struct<kind:string,column:string,constraint:string>>"
    )


def test_rejected_rows_list_every_rule_they_broke_in_declaration_order(spark):
    _, rejected = Readings().split(_df(spark))
    got = _violations(rejected)
    got.pop("V1")  # two rows share this vin, so the dict can't hold both; checked below
    assert got == {
        "V2": [("value_out_of_range", "km", "min_value=0")],
        "<null>": [("null_violation", "vin", "nullable")],
        "V5": [("null_violation", "note", "nullable, blank counts as null")],
        "V6": [
            ("value_out_of_range", "km", "min_value=0"),
            ("null_violation", "note", "nullable, blank counts as null"),
        ],
    }


def test_both_rows_of_a_duplicate_group_are_rejected(spark):
    _, rejected = Readings().split(_df(spark))
    dup = [r for r in rejected.collect() if r["vin"] == "V1"]
    assert len(dup) == 2
    assert all(
        [(v["kind"], v["column"], v["constraint"]) for v in r["_violations"]]
        == [("duplicate_value", None, "unique_together=(vin, day)")]
        for r in dup
    )


def test_a_null_predicate_does_not_reject_the_row(spark):
    valid, _ = Readings().split(_df(spark))
    assert [r["vin"] for r in valid.collect()] == ["V4"]


def test_matches_validate_rule_by_rule(spark):
    class Everything(Contract):
        unique_together = [("a", "b")]
        a = Field(IntegerType(), nullable=False, min_value=0, max_value=9, unique=True)
        b = Field(StringType(), nullable=False, blank_is_null=True, min_length=2, max_length=3)
        c = Field(
            StringType(),
            regex="[a-z]+",
            allowed_values=["ab", "xy", "q"],
            condition=lambda f: F.length(F.col(f)) < 3,
        )

    schema = StructType(
        [
            StructField("a", IntegerType()),
            StructField("b", StringType()),
            StructField("c", StringType()),
        ]
    )
    rows = [
        (1, "ab", "ab"),
        (1, "ab", "ab"),
        (2, "x", "XY"),
        (None, "  ", "zzz"),
        (11, "toolong", None),
        (-1, "ok", "q"),
        (3, None, "xy"),
        (3, "", "xy"),
        (4, "yz", "ab"),
        (4, "yz", "ab"),
    ]
    df = spark.createDataFrame(rows, schema)

    report = Everything().validate(df, mode="soft", lazy=True)
    expected = {(v.kind.value, v.column, v.constraint): v.failure_count for v in report.violations}

    _, rejected = Everything().split(df)
    exploded = rejected.select(F.explode("_violations").alias("v")).select("v.*")
    got = {
        (r["kind"], r["column"], r["constraint"]): r["count"]
        for r in exploded.groupBy("kind", "column", "constraint").count().collect()
    }
    assert got == expected
    assert len(expected) >= 9  # the contract really exercises every rule kind


def test_warn_rules_never_reject_a_row(spark):
    class Soft(Contract):
        km = Field(FloatType(), min_value=0, severity="warn")
        vin = Field(StringType(), nullable=False)

    valid, rejected = Soft().split(_df(spark))
    assert {r["vin"] for r in rejected.collect()} == {None}
    assert valid.count() == len(ROWS) - 1
    assert {v["column"] for r in rejected.collect() for v in r["_violations"]} == {"vin"}


def test_tolerance_is_ignored_by_split(spark):
    class Tolerant(Contract):
        km = Field(FloatType(), min_value=0, max_failure_pct=100)

    assert not Tolerant().validate(_df(spark), mode="soft").errors  # a warning in validate
    _, rejected = Tolerant().split(_df(spark))
    assert rejected.count() == 2  # but split still takes the rows out


def test_checks_are_not_evaluated_and_a_warning_names_them(spark):
    class WithChecks(Contract):
        km = Field(FloatType())

        @check("km must be positive")
        def positive(self, df):
            return df.filter(F.col("km") <= 0)

        @check("km must be small")
        def small(self, df):
            return df.filter(F.col("km") > 1000)

    with pytest.warns(UserWarning, match="positive.*small|small.*positive"):
        valid, rejected = WithChecks().split(_df(spark))
    assert rejected.count() == 0
    assert valid.count() == len(ROWS)


def test_no_warning_without_checks(spark):
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        Readings().split(_df(spark))


def test_dataset_level_row_counts_are_ignored(spark):
    class Bounded(Contract):
        min_rows = 100
        km = Field(FloatType())

    valid, rejected = Bounded().split(_df(spark))
    assert (valid.count(), rejected.count()) == (len(ROWS), 0)


@pytest.mark.parametrize(
    ("df_schema", "kinds"),
    [
        (StructType([f for f in SCHEMA.fields if f.name != "km"]), ["missing_column"]),
        (
            StructType(
                [StructField("vin", IntegerType())] + [f for f in SCHEMA.fields if f.name != "vin"]
            ),
            ["type_mismatch"],
        ),
    ],
)
def test_structural_problems_raise(spark, df_schema, kinds):
    class C(Contract):
        vin = Field(StringType())
        day = Field(IntegerType())
        km = Field(FloatType())
        note = Field(StringType())

    df = spark.createDataFrame([], df_schema)
    with pytest.raises(ContractViolationError) as exc_info:
        C().split(df)
    assert [v.kind.value for v in exc_info.value.report.violations] == kinds


def test_strict_contract_raises_on_unexpected_columns(spark):
    class Strict(Contract):
        strict = True
        vin = Field(StringType())

    with pytest.raises(ContractViolationError) as exc_info:
        Strict().split(_df(spark))
    assert {v.kind.value for v in exc_info.value.report.violations} == {"unexpected_column"}


def test_errors_column_name_can_be_changed_and_must_be_free(spark):
    _, rejected = Readings().split(_df(spark), errors_column="dq_errors")
    assert rejected.columns[-1] == "dq_errors"
    df = _df(spark).withColumn("_violations", F.lit(1))
    with pytest.raises(ValueError, match="_violations"):
        Readings().split(df)


def test_empty_input_gives_two_empty_frames(spark):
    valid, rejected = Readings().split(spark.createDataFrame([], SCHEMA))
    assert (valid.count(), rejected.count()) == (0, 0)


def test_a_contract_without_row_rules_rejects_nothing(spark):
    class Bare(Contract):
        vin = Field(StringType())

    valid, rejected = Bare().split(_df(spark))
    assert (valid.count(), rejected.count()) == (len(ROWS), 0)
    assert rejected.schema["_violations"].dataType.simpleString().startswith("array<struct<")


def test_result_unpacks_and_has_named_fields(spark):
    result = Readings().split(_df(spark))
    assert isinstance(result, SplitResult)
    valid, rejected = result
    assert result.valid is valid and result.rejected is rejected


def test_is_lazy_building_it_launches_no_spark_job(spark):
    jobs = _spark_jobs(spark, lambda: Readings().split(_df(spark)))
    assert jobs == 0


def test_validation_disabled_keeps_everything_valid(spark, monkeypatch):
    monkeypatch.setenv("PYSPARK_CONTRACTS_ENABLED", "false")
    valid, rejected = Readings().split(_df(spark))
    assert (valid.count(), rejected.count()) == (len(ROWS), 0)
    assert rejected.columns[-1] == "_violations"


def test_with_dotted_and_aliased_columns(spark):
    class Dotted(Contract):
        unique_together = [("a.id", "a.day")]
        ident = Field(LongType(), nullable=False, column="a.id")
        day = Field(LongType(), min_value=0, column="a.day")

    schema = StructType([StructField("a.id", LongType()), StructField("a.day", LongType())])
    df = spark.createDataFrame([(1, 1), (1, 1), (2, -1), (None, 3), (3, 4)], schema)
    valid, rejected = Dotted().split(df)
    assert [tuple(r) for r in valid.collect()] == [(3, 4)]
    assert rejected.count() == 4
    kinds = {v["kind"] for r in rejected.collect() for v in r["_violations"]}
    assert kinds == {"duplicate_value", "value_out_of_range", "null_violation"}


def test_a_reloaded_schema_can_split(spark):
    schema = ContractSchema.from_dict(Readings.to_dict())
    valid, rejected = schema.split(_df(spark))
    assert [r["vin"] for r in valid.collect()] == ["V4"]
    assert rejected.count() == len(ROWS) - 1


def test_unique_ignores_nulls(spark):
    class Unique(Contract):
        km = Field(FloatType(), unique=True)

    df = _df(
        spark, [("A", 1, None, "x"), ("B", 1, None, "x"), ("C", 1, 2.0, "x"), ("D", 1, 2.0, "x")]
    )
    valid, rejected = Unique().split(df)
    assert sorted(r["vin"] for r in rejected.collect()) == ["C", "D"]
    assert sorted(r["vin"] for r in valid.collect()) == ["A", "B"]


def test_array_columns_pass_through_untouched(spark):
    class WithArray(Contract):
        tags = Field(ArrayType(StringType()))
        km = Field(FloatType(), min_value=0)

    schema = StructType(
        [StructField("tags", ArrayType(StringType())), StructField("km", FloatType())]
    )
    df = spark.createDataFrame([(["a", "b"], -1.0), (["c"], 2.0)], schema)
    valid, rejected = WithArray().split(df)
    assert valid.collect()[0]["tags"] == ["c"]
    assert rejected.collect()[0]["tags"] == ["a", "b"]
