# Integration Guide

How to bring `pyspark-contracts` into AWS Glue, Databricks, and a local pytest suite. This
assumes you're already familiar with the core API — see the [README](README.md) for that.

## AWS Glue

### Installing the package

Glue 3.0+ jobs can pull the package straight from PyPI at job start via a job parameter —
no wheel to build or upload:

```
--additional-python-modules pyspark-contracts
```

For Glue 1.0/2.0, or air-gapped environments without PyPI access, upload the wheel to S3
and reference it instead:

```
--extra-py-files s3://your-bucket/wheels/pyspark_contracts-0.8.0-py3-none-any.whl
```

Either way, install *without* the `[pyspark]` extra — Glue already provides PySpark on the
job's classpath, and reinstalling it is unnecessary (and, on some Glue versions, breaks the
job's bundled Spark).

### A validating Glue job

```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.types import StringType, TimestampType

from pyspark_contracts import Contract, Field

args = getResolvedOptions(sys.argv, ["JOB_NAME", "source_path"])
glue_context = GlueContext(SparkContext.getOrCreate())
spark = glue_context.spark_session


class AssetHdrTelemetryReadingContract(Contract):
    asset_hdr_id = Field(StringType(), nullable=False)
    input_dt = Field(TimestampType(), nullable=False)


df = spark.read.parquet(args["source_path"])

# Hard fail — raises ContractViolationError, which aborts the job with a
# non-zero exit code. Glue marks the run FAILED and the structured JSON error
# lands in the job's CloudWatch log group automatically — no extra logging
# config needed.
AssetHdrTelemetryReadingContract().validate(df)

# ... rest of the ETL ...
```

### Toggling validation per job run

Glue job parameters are the natural place to control the library's
[global toggle](README.md#disabling-validation) — e.g. to skip validation on a reprocessing
run of data you already validated once:

```python
import os

args = getResolvedOptions(sys.argv, ["JOB_NAME", "enable_validation"])
os.environ["PYSPARK_CONTRACTS_ENABLED"] = args.get("enable_validation", "true")
```

Set `--enable_validation false` as a job parameter (or in the trigger/workflow that starts
the run) instead of editing the script.

### Failing fast before the expensive part of the job

`depth=ValidationDepth.SCHEMA_ONLY` right after reading raw/landing data costs nothing (no
Spark action) and catches a changed upstream schema before the job spends time on joins,
aggregations, or writes:

```python
from pyspark_contracts import ValidationDepth

raw_df = spark.read.parquet(args["source_path"])
AssetHdrTelemetryReadingContract().validate(raw_df, depth=ValidationDepth.SCHEMA_ONLY)
# ... proceed only if that passed; the full SCHEMA_AND_DATA validate() can run later,
# after the expensive transforms, right before the write.
```

## Databricks

### Installing the package

**Cluster library** (available to every notebook/job attached to the cluster): Compute →
your cluster → Libraries → Install new → PyPI → `pyspark-contracts`. Don't add the
`[pyspark]` extra — the Databricks Runtime already ships PySpark.

**Notebook-scoped** (current session only, e.g. for trying it out):

```python
%pip install pyspark-contracts
```

`%pip install` restarts the Python process, so run it in the first cell, before any other
imports.

### A validating notebook cell

```python
from pyspark.sql.types import DecimalType, LongType
from pyspark_contracts import Contract, Field

class OdometerContract(Contract):
    asset_hdr_id = Field(LongType(), nullable=False, unique=True)
    odometer_start = Field(DecimalType(18, 2), nullable=False, min_value=0)
    odometer_end = Field(DecimalType(18, 2), nullable=False, min_value=0)

df = spark.table("bronze.odometer_readings")

report = OdometerContract().validate(df, mode="soft")
if report:
    dbutils.notebook.exit(f"{len(report.violations)} contract violation(s) — see driver logs")
```

### Different strictness per environment

A common pattern is `mode="soft"` on dev/staging job clusters (so you can inspect a full
`ViolationReport` while iterating) and `mode="hard"` in production (fail the job outright).
Widgets make that a job parameter instead of a code change:

```python
dbutils.widgets.dropdown("validation_mode", "soft", ["soft", "hard"])
mode = dbutils.widgets.get("validation_mode")

report = OdometerContract().validate(df, mode=mode)
```

### Multi-task Workflows

For a job with several tasks feeding into each other, `@check_input`/`@check_output`
(see the [README](README.md#validating-pipeline-functions)) keep the contract attached to
each task's transform function instead of relying on every task remembering to call
`validate()`:

```python
from pyspark_contracts import check_input, check_output

@check_input(BronzeReadingsContract, param="raw_df")
@check_output(OdometerContract)
def build_odometer_table(raw_df):
    return raw_df.select(...)
```

No Unity Catalog-specific handling is needed — validation happens on the DataFrame in
memory, after the read and before the write, regardless of which catalog or table format
supplied it.

## Local pytest

### Installing for development

```bash
pip install pyspark-contracts[pyspark]
```

(or add it to your `dev` dependency group with `uv`/`poetry`/`pip-tools` — the `[pyspark]`
extra pulls in PySpark itself, since your local machine doesn't have a cluster providing it.)

### A session-scoped Spark fixture

This is the same fixture pattern `pyspark-contracts`' own test suite uses — one Spark session
shared across the whole test run, tuned for fast local tests rather than throughput:

```python
# conftest.py
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("my-project-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
```

### Testing a contract against a factory-built DataFrame

When your test builds the DataFrame itself (rather than reading real data), the schema is
already guaranteed by construction — `depth=ValidationDepth.SCHEMA_ONLY` is redundant there.
What you actually want to test is the *data* constraints, so use `DATA_ONLY` (or the default,
`SCHEMA_AND_DATA`, if you'd rather also lock in the schema as documentation):

```python
from pyspark.sql.types import FloatType, StructField, StructType
from pyspark_contracts import Field, ValidationDepth

from my_project.contracts import OdometerContract


def test_odometer_contract_rejects_negative_values(spark):
    schema = StructType([StructField("odometer_start", FloatType())])
    df = spark.createDataFrame([(-1.0,)], schema)

    report = OdometerContract().validate(df, mode="soft", depth=ValidationDepth.DATA_ONLY)

    assert report
    assert report.violations[0].kind == "value_out_of_range"
```

`mode="soft"` is almost always what you want in tests: it returns a `ViolationReport` you can
assert against, instead of raising `ContractViolationError` (which you'd otherwise have to
wrap in `pytest.raises` for every single test, including the ones checking that valid data
passes).
