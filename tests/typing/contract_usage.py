"""Type-checked in CI (mypy), not run by pytest: a contract written the way the README shows
must type-check cleanly, since the package ships ``py.typed``."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, FloatType, LongType, StringType

from pyspark_contracts import (
    Contract,
    ContractSchema,
    Field,
    Severity,
    SplitResult,
    ValidationDepth,
    Violation,
    ViolationKind,
    check,
)


class OdometerContract(Contract):
    key = ["vin", "readingDate"]
    unique_together = [("vin", "readingDate")]
    min_rows = 1
    max_rows = 1_000_000
    strict = True

    vin = Field(StringType(), nullable=False, blank_is_null=True, description="Vehicle id")
    readingDate = Field(DateType(), nullable=False, max_failure_pct=0.1)
    odometerEnd = Field(FloatType(), min_value=0, max_value=2_000_000.5, severity="warn")
    duration = Field(LongType(), unique=True, column="accumulatedData.duration")

    @check("delta must be non-negative", severity="warn", max_failure_pct=0.5)
    def no_regression(self, df: DataFrame):
        return F.col("odometerEnd") < 0

    @check("one reading per vin per day")
    def one_per_day(self, df: DataFrame, max_per_day: int = 1) -> DataFrame:
        return df.groupBy("vin", "readingDate").count().filter(F.col("count") > max_per_day)


def run(spark: SparkSession, df: DataFrame) -> None:
    report = OdometerContract().validate(df, mode="soft", depth=ValidationDepth.DATA_ONLY)
    first: Violation = report.violations[0]
    kind: ViolationKind = first.kind
    severity: Severity = first.severity
    print(kind, severity, first.sample_keys, report.errors, report.warnings, report.summary())

    result: SplitResult = OdometerContract().split(df, errors_column="dq", max_per_day=2)
    valid, rejected = result
    valid.count()
    rejected.count()

    OdometerContract.empty_df(spark).printSchema()
    reloaded = ContractSchema.from_dict(OdometerContract.to_dict())
    reloaded.validate(df, mode="soft")
    print(OdometerContract.describe(), OdometerContract.to_json(), OdometerContract.key)


def build_violation() -> Violation:
    return Violation(kind=ViolationKind.NULL_VIOLATION, column="vin", failure_count=1)
