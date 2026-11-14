import uuid

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

import pyspark_contracts._quality_checks as quality_checks
from pyspark_contracts._check import check
from pyspark_contracts._contract import Contract
from pyspark_contracts._field import Field

SCHEMA = StructType(
    [
        StructField("id", LongType()),
        StructField("code", StringType()),
        StructField("qty", IntegerType()),
    ]
)


def _spark_jobs(spark, fn) -> int:
    try:
        sc = spark.sparkContext
    except Exception:
        pytest.skip("counting Spark jobs needs a SparkContext, which Spark Connect doesn't have")
    group = f"test-{uuid.uuid4().hex}"
    sc.setJobGroup(group, group)
    fn()
    return len(sc.statusTracker().getJobIdsForGroup(group))


def _rows(n_good, bad):
    return [(i, "ok", 1) for i in range(n_good)] + bad


class OneRule(Contract):
    qty = Field(IntegerType(), min_value=0)


class FiveRules(Contract):
    key = ["id"]
    id = Field(LongType())
    code = Field(StringType(), min_length=2, regex="[a-z]+", allowed_values=["ok"])
    qty = Field(IntegerType(), min_value=0)

    @check("qty must be below 100")
    def small(self, df):
        return F.col("qty") >= 100


BAD = [(1001, "X", -1), (1002, "Y", 500)]  # code breaks 3 rules, qty breaks min or the check


def test_samples_for_many_failing_rules_cost_like_one(spark):
    df = spark.createDataFrame(_rows(50, [(1001, "ok", -1)]), SCHEMA).cache()
    df.count()
    many = spark.createDataFrame(_rows(50, BAD), SCHEMA).cache()
    many.count()
    one_rule = _spark_jobs(spark, lambda: OneRule().validate(df, mode="soft"))
    five_rules = _spark_jobs(spark, lambda: FiveRules().validate(many, mode="soft"))
    assert five_rules <= one_rule + 2  # one aggregation + one sample query (+ AQE stage jobs)


def test_values_and_keys_line_up_across_batched_rules(spark):
    df = spark.createDataFrame(_rows(20, BAD), SCHEMA)
    report = FiveRules().validate(df, mode="soft")
    by_rule = {(v.kind.value, v.constraint or v.check): v for v in report.violations}

    allowed = by_rule[("value_not_allowed", "allowed_values=['ok']")]
    assert sorted(zip(allowed.sample_values, (k["id"] for k in allowed.sample_keys))) == [
        ("X", 1001),
        ("Y", 1002),
    ]
    below = by_rule[("value_out_of_range", "min_value=0")]
    assert (below.sample_values, below.sample_keys) == ([-1], [{"id": 1001}])
    small = by_rule[("check_failed", "qty must be below 100")]
    assert small.sample_values == [{"id": 1002, "code": "Y", "qty": 500}]
    assert small.sample_keys is None


def test_same_samples_as_fetching_rule_by_rule(spark, monkeypatch):
    df = spark.createDataFrame(_rows(20, BAD), SCHEMA)
    batched = FiveRules().validate(df, mode="soft")
    monkeypatch.setattr(quality_checks, "_SAMPLE_BATCH_MAX_FAILURES", -1)  # batch nothing
    one_by_one = FiveRules().validate(df, mode="soft")

    def normalize(report):
        return [
            (
                v.kind.value,
                v.constraint,
                sorted(map(repr, v.sample_values)),
                sorted(map(repr, v.sample_keys or [])),
            )
            for v in report.violations
        ]

    assert normalize(batched) == normalize(one_by_one)


def test_at_most_five_samples_per_rule(spark):
    df = spark.createDataFrame(_rows(5, [(100 + i, "X", -i - 1) for i in range(12)]), SCHEMA)
    report = FiveRules().validate(df, mode="soft")
    assert all(len(v.sample_values) == 5 for v in report.violations)
    assert all(len(v.sample_keys) == 5 for v in report.violations if v.sample_keys is not None)


def test_a_frequent_failure_skips_the_batch(spark, monkeypatch):
    monkeypatch.setattr(quality_checks, "_SAMPLE_BATCH_MAX_FAILURES", 3)
    # code fails on all 10 rows (above the cut-off); qty fails on 2 (batched with nothing else)
    df = spark.createDataFrame([(i, "X", -1 if i < 2 else 1) for i in range(10)], SCHEMA)
    report = FiveRules().validate(df, mode="soft")
    counts = {v.constraint: (v.failure_count, len(v.sample_values)) for v in report.violations}
    assert counts["min_length=2"] == (10, 5)
    assert counts["min_value=0"] == (2, 2)


def test_null_predicates_never_produce_samples(spark):
    df = spark.createDataFrame(_rows(3, [(9, None, None), (10, "X", -1)]), SCHEMA)
    report = FiveRules().validate(df, mode="soft")
    assert all(None not in v.sample_values for v in report.violations)


def test_fail_fast_fetches_samples_only_for_what_it_reports(spark):
    df = spark.createDataFrame(_rows(5, BAD), SCHEMA)
    report = FiveRules().validate(df, mode="soft", lazy=False)
    assert len(report.violations) == 1
    assert sorted(report.violations[0].sample_values) == ["X", "Y"]


def test_dotted_column_names_in_a_batch(spark):
    class Dotted(Contract):
        key = ["a.id"]
        ident = Field(LongType(), column="a.id")
        value = Field(IntegerType(), min_value=0, max_value=10, column="a.value")

    schema = StructType([StructField("a.id", LongType()), StructField("a.value", IntegerType())])
    df = spark.createDataFrame([(1, 5), (2, -1), (3, 99)], schema)
    report = Dotted().validate(df, mode="soft")
    assert [(v.sample_values, v.sample_keys) for v in report.violations] == [
        ([-1], [{"a.id": 2}]),
        ([99], [{"a.id": 3}]),
    ]


def test_a_user_column_named_like_the_helpers_is_untouched(spark):
    class Clash(Contract):
        qty = Field(IntegerType(), min_value=0, max_value=10)

    schema = StructType(
        [
            StructField("qty", IntegerType()),
            StructField("__pyspark_contracts_rule", StringType()),
        ]
    )
    df = spark.createDataFrame([(-1, "a"), (99, "b"), (5, "c")], schema)
    report = Clash().validate(df, mode="soft")
    assert [v.sample_values for v in report.violations] == [[-1], [99]]
