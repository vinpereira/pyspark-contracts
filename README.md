# pyspark-contracts

Lightweight schema and data quality validation for PySpark DataFrames.

Validates at runtime — before your pipeline runs — with a structured JSON report
queryable in CloudWatch Logs Insights, Datadog, or any log aggregator.

## Install

```bash
pip install pyspark-contracts
```

PySpark is assumed to be on your classpath (Glue, Databricks, EMR).
For local development: `pip install pyspark-contracts[pyspark]`

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

## Validation modes

| Mode | On violation | Return |
|------|-------------|--------|
| `"hard"` (default) | Logs `ERROR` (JSON), raises `ContractViolationError` | — |
| `"soft"` | Logs `WARNING` (JSON), continues | `ViolationReport` |

## Lazy vs fail-fast

By default, `"hard"` mode stops at the first violation found (fewer Spark actions,
faster feedback), while `"soft"` mode collects every violation so you get the full
picture in one run. Override either with `lazy`:

```python
# Hard mode, but collect every violation before raising
OdometerContract().validate(df, lazy=True)

# Soft mode, but stop at the first violation
report = OdometerContract().validate(df, mode="soft", lazy=False)
```

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
| `regex` | `str` | Regex pattern (must match entire value via `rlike`). |
| `allowed_values` | `list` | Allowlist of valid values. |

## Log output

Both modes emit the same structured JSON before acting:

```json
{
  "level": "ERROR",
  "message": "contract violation — job aborted",
  "contract": "OdometerContract",
  "mode": "hard",
  "violations": [
    {"kind": "missing_column", "column": "odometerStart", "expected_type": "FloatType"},
    {
      "kind": "value_out_of_range",
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
```

## Violation kinds

| Kind | Trigger |
|------|---------|
| `missing_column` | Column declared in contract is absent from the DataFrame |
| `type_mismatch` | Column exists but type differs from declared type |
| `null_violation` | `nullable=False` column contains null values |
| `value_out_of_range` | Value below `min_value` or above `max_value` |
| `length_out_of_range` | String length below `min_length` or above `max_length` |
| `regex_mismatch` | Value does not match `regex` pattern |
| `value_not_allowed` | Value not in `allowed_values` list |
| `condition_failed` | `Field(condition=...)` expression evaluated to false |
| `check_failed` | `@check`-decorated method returned non-empty failing rows |

## Performance note

Each quality constraint triggers a Spark action (`filter + count`).
For DataFrames with many constraints, consider caching before validation:

```python
df = df.cache()
OdometerContract().validate(df)
```
