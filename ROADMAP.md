# Roadmap

What's planned for pyspark-contracts, and what's deliberately out of scope. Plans can change;
once something ships it moves from here to the [CHANGELOG](https://github.com/vinpereira/pyspark-contracts/blob/main/CHANGELOG.md).

## Next: v0.11.0

Contract features that real pipelines keep needing.

- **Strict contracts** — a `strict = True` class attribute that flags columns present in the
  DataFrame but not declared on the contract (`unexpected_column`). Off by default. Useful for
  output contracts, right before writing to a table.
- **Reuse the declaration** — `Contract.to_struct_type()` to read data with the contract as
  its schema (`spark.read.schema(MyContract.to_struct_type())`), and `Contract.empty_df(spark)`
  as a test fixture.
- **Severity and tolerance** — every violation gets a `severity` (`"error"` or `"warn"`):
  - `Field(severity="warn")` / `@check(..., severity="warn")` makes a rule's violations
    warnings.
  - `Field(max_failure_pct=0.5)` turns failures within that share of rows into warnings — still
    reported, so drift stays visible.
  - `mode="hard"` raises only when there's at least one error; `report.errors` and
    `report.warnings` split the two.
  - Structural (`missing_column`, `type_mismatch`) and row-count violations are always errors.
- **Nested type comparison ignores field metadata** — a `StructType` column whose fields
  differ from the contract only in metadata (documentation) is no longer a `type_mismatch`.

## v1.0.0 — Stable API

Once v0.11.0 lands, v1.0.0 commits to the public API — `Contract`, `Field`, `@check`,
`@check_input`, `@check_output`, `ValidationDepth`, `ContractSchema`, `ViolationReport`,
`ViolationKind`, `ContractViolationError`, and the JSON log format: no breaking changes without
a major version bump.

## Ideas, not scheduled

- Fetch `sample_values` for several failing constraints in one Spark action instead of one each.
- Run several `@check` methods in fewer Spark actions.

## Non-goals

- **Multi-backend support** (pandas, polars, Dask). pyspark-contracts is PySpark-only by design;
  [Pandera](https://pandera.readthedocs.io/) already covers several backends.
- **Statistical checks** (mean, standard deviation, distribution tests). These belong in data
  quality frameworks, not schema contracts.
- **Replacing AWS Glue Data Quality.** Glue DQ provides CloudWatch metrics, row-level outcomes,
  and Glue console integration that this library deliberately doesn't replicate. The two are
  complementary.
- **Schema inference from data.** Contracts are declarations, not inferences.
