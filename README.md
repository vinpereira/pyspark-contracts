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
    {"kind": "value_out_of_range", "column": "odometerEnd", "constraint": "min_value=0", "row_pct": 2.3}
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

## Performance note

Each quality constraint triggers a Spark action (`filter + count`).
For DataFrames with many constraints, consider caching before validation:

```python
df = df.cache()
OdometerContract().validate(df)
```
