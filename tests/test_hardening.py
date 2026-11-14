import dataclasses
import uuid

import pytest
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

from pyspark_contracts import Contract, Field, Violation, check

SCHEMA = StructType([StructField("vin", StringType()), StructField("n", LongType())])
ROWS = [("a", 1), ("a", 2), ("b", 3), ("c", -1)]


def _spark_jobs(spark, fn) -> int:
    try:
        sc = spark.sparkContext
    except Exception:
        pytest.skip("counting Spark jobs needs a SparkContext, which Spark Connect doesn't have")
    group = f"test-{uuid.uuid4().hex}"
    sc.setJobGroup(group, group)
    fn()
    return len(sc.statusTracker().getJobIdsForGroup(group))


# --- Violation is keyword-only -------------------------------------------------------------


def test_violation_rejects_positional_arguments():
    with pytest.raises(TypeError):
        Violation("null_violation", "error", "vin")  # type: ignore[misc]


def test_violation_keyword_construction_still_works():
    v = Violation(kind="null_violation", column="vin", failure_count=2)
    assert (v.kind, v.column, v.failure_count) == ("null_violation", "vin", 2)


def test_violation_is_a_keyword_only_dataclass():
    assert all(f.kw_only for f in dataclasses.fields(Violation))


# --- window functions in conditions -------------------------------------------------------


class OnePerVin(Contract):
    key = ["vin"]
    vin = Field(StringType())
    n = Field(LongType())

    @check("one row per vin")
    def unique_vin(self, df):
        return F.count(F.lit(1)).over(Window.partitionBy("vin")) > 1


def test_a_window_function_in_a_condition_check(spark):
    violation = OnePerVin().validate(spark.createDataFrame(ROWS, SCHEMA), mode="soft").violations[0]
    assert (violation.check, violation.failure_count) == ("unique_vin", 2)
    assert sorted(r["n"] for r in violation.sample_values) == [1, 2]
    assert all(set(r) == {"vin", "n"} for r in violation.sample_values)


def test_a_window_function_in_a_field_condition(spark):
    class Running(Contract):
        n = Field(
            LongType(),
            condition=lambda f: F.col(f) <= F.max(f).over(Window.partitionBy("vin")),
            condition_description="n is at most the max of its vin",
        )
        vin = Field(
            StringType(), condition=lambda f: F.count(F.lit(1)).over(Window.partitionBy(f)) == 1
        )

    report = Running().validate(spark.createDataFrame(ROWS, SCHEMA), mode="soft")
    assert [(v.kind.value, v.column, v.failure_count) for v in report.violations] == [
        ("condition_failed", "vin", 2)
    ]


def test_window_conditions_batched_with_other_failing_rules(spark):
    class Mixed(OnePerVin):
        n = Field(LongType(), min_value=0)

    report = Mixed().validate(spark.createDataFrame(ROWS, SCHEMA), mode="soft")
    by_name = {v.check or v.constraint: v for v in report.violations}
    assert by_name["min_value=0"].sample_values == [-1]
    assert by_name["min_value=0"].sample_keys == [{"vin": "c"}]
    assert by_name["unique_vin"].failure_count == 2


def test_split_with_a_window_condition_matches_validate(spark):
    df = spark.createDataFrame(ROWS, SCHEMA)
    _, rejected = OnePerVin().split(df)
    assert sorted(r["n"] for r in rejected.collect()) == [1, 2]


def test_projecting_conditions_adds_no_spark_job(spark):
    class Plain(Contract):
        n = Field(LongType(), min_value=0, max_value=10)
        vin = Field(StringType(), nullable=False)

    df = spark.createDataFrame([("a", 1), ("b", 2)], SCHEMA)

    class Nothing(Contract):
        n = Field(LongType(), min_value=0)

    assert _spark_jobs(spark, lambda: Plain().validate(df)) == _spark_jobs(
        spark, lambda: Nothing().validate(df)
    )
