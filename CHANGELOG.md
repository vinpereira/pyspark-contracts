# Changelog

All notable changes to this project are documented in this file.

## [0.7.0] - 2026-09-06

### Added
- `ContractSchema.from_dict()` / `from_json()` — reload an exported schema and validate a
  DataFrame with it, without importing the original `Contract` subclass. Reconstructs types,
  nullability, and every quality constraint; warns (not silently) about `@check`/`condition`
  rules the original contract had that can't be re-run, since their logic was never part of
  the export.
- `to_dict()` field entries gain `"type_json"` alongside the existing human-readable `"type"`
  string, giving `ContractSchema` a PySpark-native, officially supported way to reconstruct
  the exact type (via `StructType.jsonValue()`/`fromJson()`, wrapping the single field so
  nested/composite types round-trip too — plain `DataType` has no generic `fromJson()`).

### Changed
- Internal refactor: `validate()` moved from `Contract` onto a shared `_ValidationMixin`, so
  `ContractSchema` reuses the identical validation pipeline instead of a second
  implementation. No observable behavior change for `Contract`.

## [0.6.0] - 2026-09-06

### Added
- `description` and `metadata` on `Field` — pure documentation, never affect validation.
- `Contract.to_dict()` / `Contract.to_json()` — export the full contract (types, nullability,
  every quality constraint, descriptions/metadata, and `@check` names/descriptions) as plain
  data. `condition` and `@check` logic itself is never serialized, only its description text.
- `Contract.describe()` — a human-readable, printable summary of a contract's fields and
  checks.

## [0.5.0] - 2026-09-05

### Added
- `ValidationDepth` enum and a `depth` parameter on `validate()`: `SCHEMA_ONLY` (column names
  and types only, zero Spark actions), `DATA_ONLY` (skip structural checks, run only data
  constraints), and `SCHEMA_AND_DATA` (default — today's behavior).
- `depth` is now included in `ViolationReport.to_dict()` and the structured log output,
  alongside `mode`.

### Changed
- The nullable check moved from the schema stage to the quality stage internally (no
  observable behavior change for the default `SCHEMA_AND_DATA` depth) — this is what lets
  `SCHEMA_ONLY` trigger zero Spark actions and `DATA_ONLY` still catch null violations.

## [0.4.0] - 2026-09-04

### Added
- `@check_output` decorator — validates a function's return value against a `Contract`
  after the function runs, then returns the value unchanged.
- `@check_input` decorator — validates a named parameter against a `Contract` before the
  function runs, using signature binding so the parameter is found whether the caller
  passed it positionally or by keyword. Stack multiple `@check_input` calls to validate
  more than one parameter.
- Both decorators fail fast with `TypeError` at decoration time — `check_input` if the
  named parameter doesn't exist on the function, either decorator if `contract_cls` isn't
  a `Contract` subclass — instead of failing on first call.

## [0.3.0] - 2026-09-03

### Added
- `@check` method decorator for arbitrary cross-column validation logic. The method
  receives the full DataFrame and returns the failing rows; extra parameters are supplied
  via `validate(df, **kwargs)` and routed to the checks that declare them.
- `condition` / `condition_description` on `Field` for simple two-column comparisons
  anchored to one field (e.g. `start_dt` must precede `end_dt`).
- Two new violation kinds: `check_failed` and `condition_failed`.

### Changed
- `@check` and `condition` are skipped entirely when the DataFrame has a `missing_column`
  or `type_mismatch` violation, since both can reference arbitrary columns and would
  otherwise risk crashing on a column that doesn't exist.

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
