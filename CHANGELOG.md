# Changelog

All notable changes to this project are documented in this file.

## [0.2.1] - 2026-09-01

### Fixed
- `validate()` now reports `row_count=None` (instead of `0`) when
  `PYSPARK_CONTRACTS_ENABLED=false` skips validation, so a skipped run is no
  longer indistinguishable from a genuinely empty DataFrame.

## [0.2.0] - 2026-08-31

### Added
- `lazy` parameter on `validate()` to control whether every violation is
  collected or validation stops at the first one found. Defaults to
  fail-fast in hard mode and collect-all in soft mode; either can be
  overridden explicitly.
- `failure_count` and `sample_values` (up to 5 offending values) on each
  `Violation`, alongside `column` and `constraint` — including
  `constraint="nullable"` on null violations.
- `PYSPARK_CONTRACTS_ENABLED=false` global toggle to skip all validation
  with no code changes, for environments where the schema is already
  trusted (e.g. a read right after a validated write).

## [0.1.0] - 2026-08-23

### Added
- `Contract` base class and `Field` descriptor.
- Type validation (`StringType`, `IntegerType`, `DoubleType`, `DecimalType`,
  `TimestampType`, etc.).
- Column-level constraints: `nullable`, `min_value`, `max_value`,
  `min_length`, `max_length`, `regex`, `allowed_values`.
- Hard mode (raises `ContractViolationError`) and soft mode (returns
  `ViolationReport`).
- Structured JSON logging compatible with CloudWatch Logs Insights,
  Datadog, and similar aggregators.
- No runtime dependencies beyond PySpark.
