# Benchmarks

Validation overhead for a typical ETL schema, measured against a synthetic 1,000,000-row
DataFrame. Run it yourself:

```bash
uv run python benchmarks/benchmark_validate.py
```

## Setup

The benchmark contract (`OdometerReadingContract` in `benchmarks/benchmark_validate.py`)
exercises every constraint kind in one schema: `nullable`, `min_value`, `min_length`/
`max_length`, `regex`, `allowed_values`, `unique`, a cross-column `condition`, one `@check`,
and `min_rows` — 6 fields, 1 check, deliberately denser than most real contracts, so the
numbers below are closer to a worst case than a typical one.

Each scenario runs once unmeasured (warmup, so Catalyst's whole-stage codegen is already
compiled) and then 3 timed repeats; the table reports the median. The warmup run also counts
the Spark jobs the scenario launches (via a job group on the `SparkContext`) — a number that,
unlike wall-clock time, doesn't depend on the machine. `mode="soft"` is used
throughout — `mode` only changes what happens *after* violations are found (log level, raise
or not), not how much work `validate()` does to find them, so it doesn't affect timing.

## Results

Measured on an Apple M3 (8 cores), `local[*]`, PySpark 4.2.0, cached input DataFrame, before
and after v0.10.0's single-pass validation:

| Scenario | v0.9.0 | v0.10.0 | Spark jobs (v0.9.0 → v0.10.0) |
|---|---|---|---|
| `df.count()` only (baseline) | 0.06s | 0.06s | 2 → 2 |
| `validate(depth=SCHEMA_ONLY)` | 0.00s | 0.00s | 0 → 0 |
| `validate(depth=DATA_ONLY)` | 0.77s | 0.33s | 30 → 6 |
| `validate(depth=SCHEMA_AND_DATA)` (default) | 0.73s | 0.29s | 30 → 6 |

Jobs aren't actions: Spark can split one action into several jobs (adaptive query execution
plans a shuffle stage as its own job), which is why `df.count()` alone shows 2. In actions:

- **v0.9.0** ran 16: `df.count()`, one `filter().count()` for each of the 13 column
  constraints, the `unique` `groupBy`, and the `@check`.
- **v0.10.0** runs 3: one aggregation for the row count and all 13 column constraints
  together, the `unique` `groupBy`, and the `@check`.

`SCHEMA_AND_DATA` and `DATA_ONLY` land within noise of each other, which is expected —
`_check_schema` reads only `df.schema` (metadata, no action), so adding it on top of
`DATA_ONLY` costs nothing measurable. `SCHEMA_ONLY` reliably rounds to `0.00s` with 0 jobs,
confirming it never triggers a Spark action.

## Reading these numbers

- **Column constraints are nearly free to add** — `nullable`, ranges, lengths, `regex`,
  `allowed_values`, and `condition` all share one scan, so a contract with twice as many costs
  about the same. Adding rows makes that scan longer, but doesn't add actions.
- **`unique` and `@check` each add an action** — `unique` needs a `groupBy()`/shuffle, and a
  `@check` counts the rows it returns. They're what's left of this benchmark's cost.
- **Violations add an action each**, to fetch their `sample_values`. These numbers are for a
  passing validation, the common case in production.
- **Caching matters less than before, not zero** — this benchmark validates a `.cache()`d
  DataFrame. A contract with no `unique`/`@check` now reads a passing DataFrame once, so
  caching only pays off with several `unique`/`@check` or expected violations, on a DataFrame
  that's expensive to recompute (see the "Performance note" in the README).
- These are wall-clock numbers from one laptop, not a guarantee — re-run
  `benchmarks/benchmark_validate.py` on your own cluster/data shape before relying on them for
  capacity planning.
