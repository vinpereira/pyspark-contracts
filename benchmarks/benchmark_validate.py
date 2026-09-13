"""Benchmark validation overhead for a typical ETL schema.

Builds a synthetic DataFrame with ROW_COUNT rows and validates it against a Contract
that exercises every constraint kind (nullable, min/max, length, regex, allowed_values,
unique, condition, @check, min_rows), at each ValidationDepth, to show where the Spark
actions actually go.

Run: uv run python benchmarks/benchmark_validate.py
"""

import statistics
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, LongType, StringType, TimestampType

from pyspark_contracts import Contract, Field, ValidationDepth, check

ROW_COUNT = 1_000_000
REPEATS = 3


class OdometerReadingContract(Contract):
    min_rows = 1

    asset_hdr_id = Field(LongType(), nullable=False, unique=True)
    vin = Field(
        StringType(),
        nullable=False,
        min_length=17,
        max_length=17,
        regex=r"^[A-Z0-9]{17}$",
    )
    status = Field(StringType(), nullable=False, allowed_values=["active", "inactive", "retired"])
    odometer_start = Field(DecimalType(18, 2), nullable=False, min_value=0)
    odometer_end = Field(
        DecimalType(18, 2),
        nullable=False,
        min_value=0,
        condition=lambda f: F.col(f) >= F.col("odometer_start"),
        condition_description="odometer_end must be >= odometer_start",
    )
    reading_date = Field(TimestampType(), nullable=False)

    @check("delta_km must not exceed a plausible daily max")
    def plausible_delta(self, df):
        delta = F.col("odometer_end") - F.col("odometer_start")
        return df.filter(delta > 2000)


def build_dataframe(spark, n):
    statuses = F.array(F.lit("active"), F.lit("inactive"), F.lit("retired"))
    return spark.range(n).select(
        F.col("id").alias("asset_hdr_id"),
        F.concat(
            F.lit("1HGCM82633A"), F.lpad((F.col("id") % 1_000_000).cast("string"), 6, "0")
        ).alias("vin"),
        F.element_at(statuses, (F.col("id") % 3 + 1).cast("int")).alias("status"),
        (F.col("id") % 100_000).cast(DecimalType(18, 2)).alias("odometer_start"),
        ((F.col("id") % 100_000) + (F.col("id") % 50))
        .cast(DecimalType(18, 2))
        .alias("odometer_end"),
        F.to_timestamp(F.lit("2026-01-01")).alias("reading_date"),
    )


def time_it(label, fn):
    fn()  # warmup — primes Catalyst whole-stage codegen so it isn't billed to the first repeat
    times = []
    for _ in range(REPEATS):
        start = time.perf_counter()
        fn()
        times.append(time.perf_counter() - start)
    formatted = ", ".join(f"{t:.2f}s" for t in times)
    print(f"{label:<42} median={statistics.median(times):6.2f}s   runs=[{formatted}]")


def main():
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("pyspark-contracts-benchmark")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    df = build_dataframe(spark, ROW_COUNT).cache()
    df.count()  # materialize the cache before timing anything

    print(
        f"Rows: {ROW_COUNT:,}  |  Contract: {len(OdometerReadingContract._fields)} fields, "
        f"{len(OdometerReadingContract._checks)} @check, "
        f"min_rows={OdometerReadingContract.min_rows}\n"
    )

    time_it("Baseline: df.count() only", lambda: df.count())
    time_it(
        "validate(depth=SCHEMA_ONLY)",
        lambda: OdometerReadingContract().validate(
            df, mode="soft", depth=ValidationDepth.SCHEMA_ONLY
        ),
    )
    time_it(
        "validate(depth=DATA_ONLY)",
        lambda: OdometerReadingContract().validate(
            df, mode="soft", depth=ValidationDepth.DATA_ONLY
        ),
    )
    time_it(
        "validate(depth=SCHEMA_AND_DATA, default)",
        lambda: OdometerReadingContract().validate(df, mode="soft"),
    )

    spark.stop()


if __name__ == "__main__":
    main()
