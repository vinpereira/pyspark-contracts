# Roadmap

What's planned for pyspark-contracts, and what's deliberately out of scope. Plans can change;
once something ships it moves from here to the [CHANGELOG](https://github.com/vinpereira/pyspark-contracts/blob/main/CHANGELOG.md).

## Next: v0.11.1 — Fixes

- **Column names containing dots** (e.g. produced by Glue's `relationalize`, like
  `accumulatedData.duration`) currently make validation fail with an `AnalysisException`,
  because Spark reads the dot as a nested field. Column references will be quoted so these
  columns validate like any other.
- **Your own logger keeps the details.** Violations are attached to the log record as `extra`
  fields, which many formatters drop — with a logger that prints only the message, the log
  says a contract failed but not which rules. The message itself will carry the violations.

## v0.12.0 — Row-level outcomes

What a pipeline needs to act on failures, not just detect them.

- **Split valid and rejected rows** — e.g. `valid, rejected = MyContract().split(df)`, where
  each rejected row lists the rules it broke. Lets a job quarantine bad rows and carry on
  with the good ones, instead of only aborting or only logging.
- **Key columns in samples** — declare a contract's key (e.g. `key = ["vin"]`) so
  `sample_values` say *which* records failed, not just the offending values.
- **Uniqueness across several columns** — e.g. one row per key and reference date, which a
  single-column `unique` can't express.
- **Blank strings as missing** — an option to treat `""` and whitespace-only strings like
  nulls for required fields, common in text files where an empty field isn't a null.

## v0.13.0 — Cheaper checks

Cut the Spark actions that are left after v0.10.0's single-pass validation.

- **`@check` can return a condition.** Most checks are a `df.filter(...)`. Such a check can
  return just the `Column` condition that selects the failing rows, and it's then evaluated
  in the same single scan as the column constraints — no extra Spark action, even when the
  contract passes. Checks that need joins, windows, or aggregations keep returning a
  DataFrame, exactly as today.

  ```python
  @check("delta_km must be non-negative")
  def no_regression(self, df):
      return F.col("delta_km") < 0          # instead of df.filter(F.col("delta_km") < 0)
  ```

- **Sample values in one scan.** When several constraints fail, their `sample_values` are
  fetched in a single pass over the data instead of one Spark action per failing
  constraint. Matters most for rare failures on large, uncached DataFrames, where each of
  those actions reads almost the whole input to find a handful of rows.

## v1.0.0 — Stable API

After v0.13.0, v1.0.0 commits to the public API — `Contract`, `Field`, `@check`,
`@check_input`, `@check_output`, `ValidationDepth`, `ContractSchema`, `ViolationReport`,
`ViolationKind`, `Severity`, `ContractViolationError`, and the JSON log format: no breaking
changes without a major version bump.

## Non-goals

- **Multi-backend support** (pandas, polars, Dask). pyspark-contracts is PySpark-only by design;
  [Pandera](https://pandera.readthedocs.io/) already covers several backends.
- **Statistical checks** (mean, standard deviation, distribution tests). These belong in data
  quality frameworks, not schema contracts.
- **Replacing AWS Glue Data Quality.** Glue DQ provides CloudWatch metrics, row-level outcomes,
  and Glue console integration that this library deliberately doesn't replicate. The two are
  complementary.
- **Schema inference from data.** Contracts are declarations, not inferences.
