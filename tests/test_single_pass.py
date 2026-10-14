import uuid

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from pyspark_contracts._contract import Contract
from pyspark_contracts._field import Field


def _spark_jobs(spark, fn) -> int:
    """Number of Spark jobs one call of ``fn`` launches."""
    try:
        sc = spark.sparkContext
    except Exception:
        pytest.skip("counting Spark jobs needs a SparkContext, which Spark Connect doesn't have")
    group = f"test-{uuid.uuid4().hex}"
    sc.setJobGroup(group, group)
    fn()
    return len(sc.statusTracker().getJobIdsForGroup(group))


def test_passing_validation_cost_does_not_grow_with_constraint_count(spark):
    class OneConstraint(Contract):
        a = Field(IntegerType(), nullable=False)

    class TenConstraints(Contract):
        a = Field(IntegerType(), nullable=False, min_value=0, max_value=100, allowed_values=[1, 2])
        b = Field(StringType(), nullable=False, min_length=1, max_length=3, regex="[a-z]+")
        c = Field(IntegerType(), condition=lambda f: F.col(f) >= F.col("a"))

    schema = StructType(
        [
            StructField("a", IntegerType()),
            StructField("b", StringType()),
            StructField("c", IntegerType()),
        ]
    )
    df = spark.createDataFrame([(1, "ab", 5), (2, "xyz", 2)], schema)

    one = _spark_jobs(spark, lambda: OneConstraint().validate(df))
    ten = _spark_jobs(spark, lambda: TenConstraints().validate(df))
    assert ten == one


def test_fail_fast_skips_unique_after_earlier_violation(spark):
    class NotNull(Contract):
        a = Field(IntegerType(), nullable=False)

    class NotNullAndUnique(Contract):
        a = Field(IntegerType(), nullable=False, unique=True)

    df = spark.createDataFrame([(1,), (1,), (None,)], StructType([StructField("a", IntegerType())]))

    def run(contract):
        return lambda: contract().validate(df, mode="soft", lazy=False)

    assert _spark_jobs(spark, run(NotNullAndUnique)) == _spark_jobs(spark, run(NotNull))


def test_violations_keep_declaration_order(spark):
    class MyContract(Contract):
        a = Field(
            IntegerType(),
            nullable=False,
            unique=True,
            condition=lambda f: F.col(f) > 0,
        )
        b = Field(StringType(), max_length=2)

    schema = StructType([StructField("a", IntegerType()), StructField("b", StringType())])
    df = spark.createDataFrame([(-1, "ok"), (-1, "toolong"), (None, "ok")], schema)
    report = MyContract().validate(df, mode="soft")
    assert [(v.kind, v.column) for v in report.violations] == [
        ("null_violation", "a"),
        ("duplicate_value", "a"),
        ("condition_failed", "a"),
        ("length_out_of_range", "b"),
    ]
    assert [v.failure_count for v in report.violations] == [1, 2, 2, 1]
