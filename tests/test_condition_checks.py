import uuid
import warnings

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, IntegerType, StringType, StructField, StructType

from pyspark_contracts import Violation as ExportedViolation
from pyspark_contracts._check import check
from pyspark_contracts._contract import Contract
from pyspark_contracts._field import Field
from pyspark_contracts._report import ContractViolationError, Violation
from pyspark_contracts._schema import ContractSchema

SCHEMA = StructType(
    [
        StructField("vin", StringType()),
        StructField("start", FloatType()),
        StructField("end", FloatType()),
    ]
)
ROWS = [("V1", 0.0, 10.0), ("V2", 5.0, 3.0), ("V3", 1.0, None), ("V4", 2.0, 1.0)]


def _df(spark, rows=ROWS):
    return spark.createDataFrame(rows, SCHEMA)


def _spark_jobs(spark, fn) -> int:
    try:
        sc = spark.sparkContext
    except Exception:
        pytest.skip("counting Spark jobs needs a SparkContext, which Spark Connect doesn't have")
    group = f"test-{uuid.uuid4().hex}"
    sc.setJobGroup(group, group)
    fn()
    return len(sc.statusTracker().getJobIdsForGroup(group))


class Odometer(Contract):
    key = ["vin"]
    vin = Field(StringType())
    start = Field(FloatType())
    end = Field(FloatType())

    @check("end must not be below start")
    def no_regression(self, df):
        return F.col("end") < F.col("start")


def test_a_condition_check_reports_like_a_dataframe_check(spark):
    violation = Odometer().validate(_df(spark), mode="soft").violations[0]
    assert violation.kind == "check_failed"
    assert (violation.check, violation.column) == ("no_regression", None)
    assert violation.constraint == "end must not be below start"
    assert (violation.failure_count, violation.row_pct) == (2, 50.0)
    assert sorted(violation.sample_values, key=lambda r: r["vin"]) == [
        {"vin": "V2", "start": 5.0, "end": 3.0},
        {"vin": "V4", "start": 2.0, "end": 1.0},
    ]
    assert violation.sample_keys is None  # whole rows already include the key


def test_same_result_as_the_equivalent_dataframe_check(spark):
    class AsDataFrame(Odometer):
        @check("end must not be below start")
        def no_regression(self, df):
            return df.filter(F.col("end") < F.col("start"))

    as_condition = Odometer().validate(_df(spark), mode="soft").violations[0]
    as_dataframe = AsDataFrame().validate(_df(spark), mode="soft").violations[0]
    assert (as_condition.failure_count, as_condition.check, as_condition.constraint) == (
        as_dataframe.failure_count,
        as_dataframe.check,
        as_dataframe.constraint,
    )


def test_a_null_condition_does_not_fail(spark):
    # V3 has end = null: null < 1.0 is null, so the row is not counted
    violation = Odometer().validate(_df(spark), mode="soft").violations[0]
    assert "V3" not in {r["vin"] for r in violation.sample_values}


def test_a_passing_condition_check_costs_no_extra_spark_job(spark):
    class NoCheck(Contract):
        vin = Field(StringType())
        start = Field(FloatType(), min_value=0)

    class WithConditionCheck(NoCheck):
        @check("start must be finite")
        def finite(self, df):
            return F.isnan(F.col("start"))

    df = _df(spark, [("V1", 1.0, 2.0)])
    assert _spark_jobs(spark, lambda: WithConditionCheck().validate(df)) == _spark_jobs(
        spark, lambda: NoCheck().validate(df)
    )


def test_condition_and_dataframe_checks_keep_declaration_order(spark):
    class Mixed(Contract):
        vin = Field(StringType())
        start = Field(FloatType())
        end = Field(FloatType())

        @check("c1")
        def first(self, df):
            return F.col("start") > 1

        @check("d2")
        def second(self, df):
            return df.filter(F.col("vin") != "V1")

        @check("c3")
        def third(self, df):
            return F.col("end") < 5

    report = Mixed().validate(_df(spark), mode="soft")
    assert [v.check for v in report.violations] == ["first", "second", "third"]


def test_checks_come_after_field_rules(spark):
    class Both(Odometer):
        start = Field(FloatType(), max_value=4)

    report = Both().validate(_df(spark), mode="soft")
    assert [v.kind.value for v in report.violations] == ["value_out_of_range", "check_failed"]


def test_parameters_are_routed_to_condition_checks(spark):
    class Threshold(Contract):
        end = Field(FloatType())

        @check("end must stay under the threshold")
        def under(self, df, threshold: float = 100.0):
            return F.col("end") > threshold

    assert not Threshold().validate(_df(spark), mode="soft")
    report = Threshold().validate(_df(spark), mode="soft", threshold=5.0)
    assert report.violations[0].failure_count == 1


def test_unknown_parameters_raise_even_on_an_empty_dataframe(spark):
    with pytest.raises(TypeError, match="thresold"):
        Odometer().validate(_df(spark, []), mode="soft", thresold=5.0)


def test_returning_something_else_raises_naming_the_check(spark):
    class Broken(Contract):
        vin = Field(StringType())

        @check("forgot to return")
        def forgot(self, df):
            F.col("vin").isNull()

    with pytest.raises(TypeError, match="forgot"):
        Broken().validate(_df(spark), mode="soft")


def test_skipped_when_a_column_is_missing(spark):
    df = spark.createDataFrame([("V1", 1.0)], StructType(SCHEMA.fields[:2]))
    report = Odometer().validate(df, mode="soft")
    assert [(v.kind.value, v.column) for v in report.violations] == [("missing_column", "end")]


def test_fail_fast_stops_at_a_field_error_before_the_checks(spark):
    class Early(Odometer):
        vin = Field(StringType(), regex="X.*")

    report = Early().validate(_df(spark), mode="soft", lazy=False)
    assert [v.kind.value for v in report.violations] == ["regex_mismatch"]


def test_hard_mode_raises_on_a_failing_condition_check(spark):
    with pytest.raises(ContractViolationError) as exc_info:
        Odometer().validate(_df(spark))
    assert exc_info.value.report.violations[0].check == "no_regression"


@pytest.mark.parametrize(("max_failure_pct", "severity"), [(50, "warn"), (49.9, "error")])
@pytest.mark.parametrize("returns", ["condition", "dataframe"])
def test_max_failure_pct_on_checks(spark, max_failure_pct, severity, returns):
    class Tolerant(Contract):
        vin = Field(StringType())

        @check("end must not be below start", max_failure_pct=max_failure_pct)
        def no_regression(self, df):
            condition = F.col("end") < F.col("start")
            return condition if returns == "condition" else df.filter(condition)

    violation = Tolerant().validate(_df(spark), mode="soft").violations[0]
    assert violation.row_pct == 50.0
    assert violation.severity == severity


@pytest.mark.parametrize("value", [-1, 100.5])
def test_check_rejects_an_invalid_max_failure_pct(value):
    with pytest.raises(ValueError, match="max_failure_pct"):
        check("x", max_failure_pct=value)


def test_describe_shows_check_severity_and_tolerance():
    class Described(Contract):
        vin = Field(StringType())

        @check("a", severity="warn", max_failure_pct=0.5)
        def both(self, df):
            return F.col("vin").isNull()

        @check("b", max_failure_pct=1)
        def tolerance(self, df):
            return F.col("vin").isNull()

    lines = Described.describe().splitlines()
    assert "  both (warn, max_failure_pct=0.5): a" in lines
    assert "  tolerance (max_failure_pct=1): b" in lines


def test_a_reloaded_schema_warns_about_condition_checks_too():
    with pytest.warns(UserWarning, match="no_regression"):
        ContractSchema.from_dict(Odometer.to_dict())


# --- split() ---------------------------------------------------------------------------


def test_split_rejects_rows_failing_an_error_condition_check(spark):
    valid, rejected = Odometer().split(_df(spark))
    assert sorted(r["vin"] for r in valid.collect()) == ["V1", "V3"]
    entries = {r["vin"]: [tuple(v) for v in r["_violations"]] for r in rejected.collect()}
    assert entries == {
        "V2": [("check_failed", None, "no_regression", "end must not be below start")],
        "V4": [("check_failed", None, "no_regression", "end must not be below start")],
    }


def test_split_errors_column_has_the_check_field(spark):
    _, rejected = Odometer().split(_df(spark))
    assert rejected.schema["_violations"].dataType.simpleString() == (
        "array<struct<kind:string,column:string,check:string,constraint:string>>"
    )


def test_split_field_rules_have_a_null_check(spark):
    class FieldRule(Contract):
        start = Field(FloatType(), max_value=4)

    _, rejected = FieldRule().split(_df(spark))
    assert [tuple(v) for v in rejected.collect()[0]["_violations"]] == [
        ("value_out_of_range", "start", None, "max_value=4")
    ]


def test_split_ignores_warning_condition_checks(spark):
    class Soft(Contract):
        vin = Field(StringType())

        @check("end must not be below start", severity="warn")
        def no_regression(self, df):
            return F.col("end") < F.col("start")

    valid, rejected = Soft().split(_df(spark))
    assert (valid.count(), rejected.count()) == (len(ROWS), 0)


def test_split_warns_only_about_dataframe_checks(spark):
    class Mixed(Odometer):
        @check("needs the whole frame")
        def whole_frame(self, df):
            return df.groupBy("vin").count().filter("count > 1")

    with pytest.warns(UserWarning) as record:
        Mixed().split(_df(spark))
    messages = [str(w.message) for w in record]
    assert any("whole_frame" in m for m in messages)
    assert not any("no_regression" in m for m in messages)


def test_split_without_dataframe_checks_does_not_warn(spark):
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        Odometer().split(_df(spark))


def test_split_routes_parameters(spark):
    class Threshold(Contract):
        end = Field(FloatType())

        @check("end must stay under the threshold")
        def under(self, df, threshold: float = 100.0):
            return F.col("end") > threshold

    _, rejected = Threshold().split(_df(spark), threshold=5.0)
    assert [r["vin"] for r in rejected.collect()] == ["V1"]
    with pytest.raises(TypeError, match="thresold"):
        Threshold().split(_df(spark), thresold=5.0)


def test_split_matches_validate_for_condition_checks(spark):
    class Combined(Odometer):
        start = Field(FloatType(), max_value=4)

    report = Combined().validate(_df(spark), mode="soft")
    expected = {(v.kind.value, v.column, v.check): v.failure_count for v in report.violations}
    _, rejected = Combined().split(_df(spark))
    exploded = rejected.select(F.explode("_violations").alias("v")).select("v.*")
    got = {
        (r["kind"], r["column"], r["check"]): r["count"]
        for r in exploded.groupBy("kind", "column", "check").count().collect()
    }
    assert got == expected


def test_violation_is_exported_from_the_package_root():
    assert ExportedViolation is Violation


def test_integer_columns_in_conditions(spark):
    class Ints(Contract):
        n = Field(IntegerType())

        @check("n must be even")
        def even(self, df):
            return F.col("n") % 2 != 0

    df = spark.createDataFrame([(1,), (2,), (3,)], StructType([StructField("n", IntegerType())]))
    assert Ints().validate(df, mode="soft").violations[0].failure_count == 2
