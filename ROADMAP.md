# Roadmap

What's planned for pyspark-contracts, and what's deliberately out of scope. Plans can change;
once something ships it moves from here to the [CHANGELOG](https://github.com/vinpereira/pyspark-contracts/blob/main/CHANGELOG.md).

## Next: v1.0.0 — Stable API

v1.0.0 commits to the public API — `Contract`, `Field`, `@check`, `@check_input`,
`@check_output`, `ValidationDepth`, `ContractSchema`, `ViolationReport`,
`ViolationKind`, `Severity`, `ContractViolationError`, and the JSON log format: no breaking
changes without a major version bump.

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
