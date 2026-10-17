# pyspark-contracts

[![Tests](https://github.com/vinpereira/pyspark-contracts/actions/workflows/test.yml/badge.svg)](https://github.com/vinpereira/pyspark-contracts/actions/workflows/test.yml)
[![Lint](https://github.com/vinpereira/pyspark-contracts/actions/workflows/lint.yml/badge.svg)](https://github.com/vinpereira/pyspark-contracts/actions/workflows/lint.yml)
[![Documentation Status](https://app.readthedocs.org/projects/pyspark-contracts/badge/?version=latest)](https://pyspark-contracts.readthedocs.io/en/latest/?badge=latest)
[![PyPI](https://img.shields.io/pypi/v/pyspark-contracts)](https://pypi.org/project/pyspark-contracts/)
[![Python versions](https://img.shields.io/pypi/pyversions/pyspark-contracts)](https://pypi.org/project/pyspark-contracts/)
[![License](https://img.shields.io/pypi/l/pyspark-contracts)](https://github.com/vinpereira/pyspark-contracts/blob/main/LICENSE)

Lightweight schema and data quality validation for PySpark DataFrames.

Full documentation: [pyspark-contracts.readthedocs.io](https://pyspark-contracts.readthedocs.io/) ·
What's next: [ROADMAP.md](https://github.com/vinpereira/pyspark-contracts/blob/main/ROADMAP.md)

Validates at runtime — before your pipeline runs — with a structured JSON report
queryable in CloudWatch Logs Insights, Datadog, or any log aggregator.

## Install

```bash
pip install pyspark-contracts
```

PySpark is assumed to be on your classpath (Glue, Databricks, EMR).
For local development: `pip install pyspark-contracts[pyspark]`

Tested on PySpark 3.3–4.x and Python 3.10–3.14, in both classic mode and Spark Connect.

## Quick start

```python
from pyspark.sql.types import DateType, FloatType, StringType
from pyspark_contracts import Contract, Field

class OdometerContract(Contract):
    vin           = Field(StringType(), nullable=False, min_length=17, max_length=17)
    odometerStart = Field(FloatType(),  nullable=False, min_value=0)
    odometerEnd   = Field(FloatType(),  nullable=False, min_value=0)
    readingDate   = Field(DateType())

# Hard fail (default) — logs ERROR + raises ContractViolationError
OdometerContract().validate(df)

# Soft fail — logs WARNING + returns ViolationReport
report = OdometerContract().validate(df, mode="soft")
if report:
    print(f"{len(report.violations)} violation(s) found")
```

## Why pyspark-contracts

A contract is a Python class that lives next to the pipeline code, gets enforced on the
DataFrames going into and out of your functions, and reports failures as structured JSON in
the logs you already have. The library has no dependencies besides PySpark itself.

It does less than the data quality frameworks below, on purpose. Pick by what you need:

| Tool | Pick it when |
|------|--------------|
| **pyspark-contracts** | You want schema and row-level rules as code, checked at function boundaries in PySpark jobs, with nothing extra to install or run. |
| [Pandera](https://pandera.readthedocs.io/) | You also validate pandas or polars DataFrames, or need statistical and hypothesis checks. |
| [Great Expectations](https://greatexpectations.io/) | You want a data quality platform: expectation suites managed as configuration, Data Docs reports, and checkpoints across many datasets. |
| [PyDeequ](https://github.com/awslabs/python-deequ) | You want metrics tracked over time, anomaly detection, or suggested constraints, and can put the Deequ JAR matching your Spark version on the classpath. |
| [AWS Glue Data Quality](https://docs.aws.amazon.com/glue/latest/dg/glue-data-quality.html) | You run on AWS Glue and want managed rulesets with CloudWatch metrics and console integration. It complements contracts rather than replacing them; you can use both. |

What the library deliberately won't do is listed under *Non-goals* in the
[roadmap](https://github.com/vinpereira/pyspark-contracts/blob/main/ROADMAP.md).

## Validation modes

| Mode | On violation | Return |
|------|-------------|--------|
| `"hard"` (default) | Logs `ERROR` (JSON), raises `ContractViolationError` | — |
| `"soft"` | Logs `WARNING` (JSON), continues | `ViolationReport` |

Violations are errors by default. A rule marked as a warning (see
[Severity and tolerance](#severity-and-tolerance)) never raises: if every violation is a
warning, `"hard"` mode logs `WARNING` and returns the report too.

## Lazy vs fail-fast

By default, `"hard"` mode stops at the first error found (fewer Spark actions, faster
feedback), while `"soft"` mode collects every violation so you get the full picture in one
run. Warnings found before that first error are kept — stopping at a warning could hide an
error that should abort the job. Override either default with `lazy`:

```python
# Hard mode, but collect every violation before raising
OdometerContract().validate(df, lazy=True)

# Soft mode, but stop at the first error
report = OdometerContract().validate(df, mode="soft", lazy=False)
```

## Severity and tolerance

Every violation has a `severity`: `"error"` (the default) or `"warn"`. Warnings are reported
and logged like errors, but never make `"hard"` mode raise — useful for a signal you want to
watch without aborting the job, or a rule that tolerates a little noise:

```python
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, StringType
from pyspark_contracts import Contract, Field, check

class OdometerContract(Contract):
    vin = Field(StringType(), nullable=False, regex="[A-Z0-9]{17}")

    # Up to 0.1% null readings is a warning; more than that is an error
    readingDate = Field(DateType(), nullable=False, max_failure_pct=0.1)

    # Never more than a warning
    note = Field(StringType(), max_length=200, severity="warn")

    @check("delta_km should be under a plausible daily max", severity="warn")
    def plausible_delta(self, df: DataFrame) -> DataFrame:
        return df.filter(F.col("odometerEnd") - F.col("odometerStart") > 2000)
```

- `severity` on a `Field` applies to all of its data constraints; on `@check`, to that check.
- `max_failure_pct` is a share of rows (0–100, like `row_pct`): a constraint failing on at most
  that share is a warning instead of an error. It's compared against the exact share, so
  `row_pct`'s rounding can't flip it.
- Structural violations (`missing_column`, `type_mismatch`, `unexpected_column`) and
  `row_count_out_of_range` are always errors.

```python
report = OdometerContract().validate(df)   # raises only if report.errors would be non-empty
for v in report.warnings:
    print(v.kind, v.column or v.check, v.row_pct)
```

`if report:` is still true when there are only warnings; use `report.errors` /
`report.warnings` to tell them apart.

## Cross-column checks

`Field` constraints are column-local. For rules spanning more than one column, use
`condition` (simple two-column comparisons) or `@check` (arbitrary logic):

```python
from pyspark_contracts import Contract, Field, check
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, TimestampType

class FinalDfContract(Contract):
    end_dt   = Field(TimestampType(), nullable=False)
    start_dt = Field(TimestampType(), nullable=False,
                     condition=lambda f: F.col(f) < F.col("end_dt"),
                     condition_description="start_dt must precede end_dt")

    delta_km = Field(DoubleType(), nullable=False)

    @check("delta_km must be non-negative (no regression)")
    def no_regression(self, df: DataFrame) -> DataFrame:
        return df.filter(F.col("delta_km") < 0)

    @check("delta_km must not exceed daily threshold")
    def no_abnormal_jump(self, df: DataFrame, threshold: float = 1000.0) -> DataFrame:
        return df.filter(F.col("delta_km") > threshold)
```

A `@check` method receives the full DataFrame and returns the **failing rows** — an empty
result means it passed. Extra parameters (`threshold`) are supplied via `validate()` and
routed to the checks that declare them:

```python
report = FinalDfContract().validate(df, mode="soft", threshold=2000.0)
```

Both `condition` and `@check` are skipped entirely when the DataFrame has a missing column
or type mismatch, since either can reference arbitrary columns.

## Validating pipeline functions

`@check_input` and `@check_output` move contract validation from the function body onto its
signature, so a refactor can't accidentally drop it:

```python
from pyspark_contracts import check_input, check_output

@check_input(AccumulatedDataContract, param="accumulated_df")
@check_output(FinalDfContract)
def build_final_df(accumulated_df: DataFrame, contracts_df: DataFrame) -> DataFrame:
    return accumulated_df.join(contracts_df, ...)
```

`check_input` validates the named parameter *before* the function runs — a bad step never
executes. `check_output` validates the return value *after* the function runs, before it's
returned to the caller. Both accept `mode` and any extra kwargs, forwarded to `validate()`
exactly as if you'd called it directly:

```python
@check_output(FinalDfContract, mode="soft", threshold=2000.0)
def build_final_df(df: DataFrame) -> DataFrame:
    ...
```

A function needing more than one input validated stacks `@check_input`:

```python
@check_input(AccumulatedDataContract, param="accumulated_df")
@check_input(ContractsContract, param="contracts_df")
def build_final_df(accumulated_df, contracts_df): ...
```

## Validation depth

```python
from pyspark_contracts import ValidationDepth

# Column names and types only — reads df.schema, triggers zero Spark actions
OdometerContract().validate(df, depth=ValidationDepth.SCHEMA_ONLY)

# Skip structural checks, run only nullable/range/length/regex/condition/@check
OdometerContract().validate(df, depth=ValidationDepth.DATA_ONLY)

# Default: both
OdometerContract().validate(df, depth=ValidationDepth.SCHEMA_AND_DATA)
```

`SCHEMA_AND_DATA` is the default and runs the full pipeline described everywhere else in this
README — every schema and quality check, exactly as if `depth` were never passed.

`SCHEMA_ONLY` is particularly useful in unit tests where the DataFrame is built from a known
factory: the schema is guaranteed by construction, so only the data constraints are worth
checking — the reverse case, `DATA_ONLY`, skips column/type checks and assumes the schema is
already right, without adding a safety net if it isn't.

## Dataset-level checks

`min_rows` / `max_rows` are declared as plain class attributes, not `Field` options — they
assert something about the DataFrame as a whole rather than a single column:

```python
class MyContract(Contract):
    min_rows = 100
    max_rows = 1_000_000
    vin = Field(StringType())
```

Checked right after the row count is known, so they apply under `DATA_ONLY` and
`SCHEMA_AND_DATA` depth (not `SCHEMA_ONLY`, which never computes a row count). Unlike
`@check`/`condition`, `min_rows`/`max_rows` are plain data, so they round-trip through
`to_dict()`/`ContractSchema` with no "can't reconstruct" warning.

## Strict contracts

By default a DataFrame may carry columns the contract doesn't declare. Set `strict = True` to
report each of them as an `unexpected_column` violation — useful for an output contract right
before writing to a table, where a leftover debug or join column shouldn't slip through:

```python
class FinalTableContract(Contract):
    strict = True
    vin = Field(StringType(), nullable=False)
    odometer = Field(FloatType())
```

It's a structural check like `missing_column`: it reads only `df.schema`, so it runs under
`SCHEMA_ONLY` and `SCHEMA_AND_DATA` (not `DATA_ONLY`) with no Spark action, and it round-trips
through `to_dict()`/`ContractSchema`. Unexpected columns don't stop `condition` or `@check`
from running, since every column they could reference is still there.

## Disabling validation

```bash
PYSPARK_CONTRACTS_ENABLED=false  # skips all validation, no code changes needed
```

Useful when performance is critical and the schema is already trusted — e.g. the
read right after a validated write.

## Bringing your own logger

```python
import logging
logger = logging.getLogger("my-etl-job")
OdometerContract().validate(df, logger=logger)
```

If no logger is passed, the library emits JSON to stdout.

## Field options

| Option | Type | Description |
|--------|------|-------------|
| `nullable` | `bool` | Default `True`. Set `False` to fail on null values. |
| `min_value` | numeric / date | Minimum allowed value. |
| `max_value` | numeric / date | Maximum allowed value. |
| `min_length` | `int` | Minimum string length. |
| `max_length` | `int` | Maximum string length. |
| `regex` | `str` | Regex pattern the entire value must match — anchored for you, so `"[0-9]+"` rejects `"abc123"`. |
| `allowed_values` | `list` | Allowlist of valid values. |
| `unique` | `bool` | Default `False`. Set `True` to fail when the column has duplicate (non-null) values. |
| `severity` | `str` | `"error"` (default) or `"warn"`, for every data constraint on the field. |
| `max_failure_pct` | `float` | Percentage of rows (0–100) a constraint may fail on and still be only a warning. |

## Documenting a contract

`description` and `metadata` on `Field` are pure documentation — they never affect validation:

```python
class AssetHdrTelemetryReadingContract(Contract):
    asset_hdr_id = Field(LongType(), nullable=False,
                         description="AF internal vehicle identifier")
    meter_reading_value = Field(DecimalType(26, 12), nullable=False, min_value=0,
                                description="Odometer reading in km",
                                metadata={"unit": "km", "source": "asset_hdr_telemetry_reading"})
```

Export the whole contract as data — types, constraints, descriptions, and the name/description
of every `@check` (never the executable code, which can't be serialized):

```python
schema_dict = AssetHdrTelemetryReadingContract.to_dict()
json_str = AssetHdrTelemetryReadingContract.to_json()
```

Or print a quick human-readable summary:

```python
print(AssetHdrTelemetryReadingContract.describe())
```

## Sharing a schema without the Python class

Reload an exported schema and validate with it, without importing the original `Contract`
subclass:

```python
from pyspark_contracts import ContractSchema

schema_dict = AssetHdrTelemetryReadingContract.to_dict()
ContractSchema.from_dict(schema_dict).validate(df)

# Or straight from JSON
ContractSchema.from_json(json_str).validate(df)
```

`ContractSchema` reconstructs everything `to_dict()` captured as data — types, nullability,
and every quality constraint. It cannot reconstruct `@check` methods or `condition` lambdas
(arbitrary Python code was never serialized) — `from_dict()`/`from_json()` emit a warning
naming any such rules the original contract had, so you know they won't be enforced.

## Reusing the contract as a Spark schema

`to_struct_type()` returns the `StructType` the contract declares, so the same declaration can
drive the read:

```python
df = spark.read.schema(OdometerContract.to_struct_type()).parquet(path)
OdometerContract().validate(df)
```

Each field's `nullable` carries over, and its `description` becomes the column's `comment`
metadata (the column comment when the DataFrame is saved as a table). Spark relaxes
`nullable=False` to `True` when reading files, so `validate()` is still what enforces it.

`empty_df(spark)` gives you an empty DataFrame with that schema, for tests:

```python
df = OdometerContract.empty_df(spark)
```

Both also work on a `ContractSchema` reloaded with `from_dict()`/`from_json()`.

## Log output

Both modes emit the same structured JSON before acting:

```json
{
  "level": "ERROR",
  "message": "contract violation — job aborted",
  "contract": "OdometerContract",
  "mode": "hard",
  "depth": "schema_and_data",
  "violations": [
    {"kind": "missing_column", "severity": "error", "column": "odometerStart", "expected_type": "float"},
    {
      "kind": "value_out_of_range",
      "severity": "error",
      "column": "odometerEnd",
      "constraint": "min_value=0",
      "row_pct": 2.3,
      "failure_count": 42,
      "sample_values": [-1.0, -5.5, -12.0]
    }
  ],
  "row_count": 1842
}
```

### CloudWatch Logs Insights

```sql
-- All contract violations in the last 7 days
filter message = "contract violation — job aborted"
| stats count(*) as failures by contract

-- Violations by column across all runs
filter ispresent(violations)
| flatten violations
| stats count(*) by violations.kind, violations.column

-- Errors only, ignoring warnings
filter ispresent(violations)
| flatten violations
| filter violations.severity = "error"
| stats count(*) by contract, violations.kind

-- Failing @check methods across all runs
filter ispresent(violations)
| flatten violations
| filter violations.kind = "check_failed"
| stats count(*) by contract, violations.check
```

## Violation kinds

Each violation's `kind` is a `ViolationKind` member. It's a `str` enum, so comparing it to
the plain string still works:

```python
from pyspark_contracts import ViolationKind

nulls = [v for v in report.violations if v.kind == ViolationKind.NULL_VIOLATION]
```

| Kind | Trigger |
|------|---------|
| `missing_column` | Column declared in contract is absent from the DataFrame |
| `type_mismatch` | Column exists but type differs from declared type (reported as Spark type strings, e.g. `decimal(26,12)`). Metadata on nested struct fields is ignored. |
| `null_violation` | `nullable=False` column contains null values |
| `value_out_of_range` | Value below `min_value` or above `max_value` |
| `length_out_of_range` | String length below `min_length` or above `max_length` |
| `regex_mismatch` | Value does not match `regex` pattern |
| `value_not_allowed` | Value not in `allowed_values` list |
| `condition_failed` | `Field(condition=...)` expression evaluated to false |
| `check_failed` | `@check`-decorated method returned non-empty failing rows (named in `check`, not `column`) |
| `duplicate_value` | `Field(unique=True)` column has repeated (non-null) values |
| `row_count_out_of_range` | DataFrame row count below `min_rows` or above `max_rows` |
| `unexpected_column` | `strict = True` contract, and the DataFrame has a column it doesn't declare |

## Performance note

The row count and every column constraint (`nullable`, ranges, lengths, `regex`,
`allowed_values`, `condition`) are checked together in a single Spark action, however many
the contract declares. On top of that:

| Adds one Spark action each | Why |
|---|---|
| `unique` field | Needs a `groupBy`, which can't share the single pass |
| `@check` method | Counts the rows it returns |
| Violation found | Fetches its `sample_values` |

So a contract without `unique` or `@check` reads a passing DataFrame once. If yours has
several of them, or you expect violations, and the DataFrame is expensive to recompute,
cache it before validating:

```python
df = df.cache()
OdometerContract().validate(df)
```

See [BENCHMARKS.md](https://github.com/vinpereira/pyspark-contracts/blob/main/BENCHMARKS.md) for measured overhead.
