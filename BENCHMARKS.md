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
compiled) and then 3 timed repeats; the table reports the median. `mode="soft"` is used
throughout — `mode` only changes what happens *after* violations are found (log level, raise
or not), not how much work `validate()` does to find them, so it doesn't affect timing.

## Results

Measured on an Apple M3 (8 cores), `local[*]`, PySpark 4.2.0, cached input DataFrame:

| Scenario | Median | What's actually running |
|---|---|---|
| `df.count()` only (baseline) | 0.04s | One action, no validation at all |
| `validate(depth=SCHEMA_ONLY)` | 0.00s | Reads `df.schema` only — no Spark action, as designed |
| `validate(depth=DATA_ONLY)` | 0.57s | ~16 actions: nullable + range/length/regex/allowed_values/unique/condition per field, plus the `@check` |
| `validate(depth=SCHEMA_AND_DATA)` (default) | 0.50s | Everything `DATA_ONLY` does, plus the (free) schema check |

`SCHEMA_AND_DATA` and `DATA_ONLY` land within noise of each other, which is expected —
`_check_schema` reads only `df.schema` (metadata, no action), so adding it on top of
`DATA_ONLY` costs nothing measurable. `SCHEMA_ONLY` reliably rounds to `0.00s`, confirming it
never triggers a Spark action.

## Reading these numbers

- **This scales with the number of constraints, not directly with row count** — each
  constraint (`min_value`, `unique`, a `@check`, ...) is its own `filter().count()` (or
  `groupBy()` for `unique`), so a schema with twice the constraints costs roughly twice as
  much, at any row count. Adding rows makes each of those actions scan more data, but doesn't
  add more actions.
- **`unique` is the most expensive constraint** — it needs a `groupBy()`/shuffle, not a plain
  `filter().count()` like every other constraint. A schema with several `unique` fields will
  cost more than this benchmark's single one.
- **Caching the DataFrame before validating matters** (see the "Performance note" in the
  README) — this benchmark validates a `.cache()`d DataFrame; without it, every one of those
  ~16 actions would re-read from source.
- These are wall-clock numbers from one laptop, not a guarantee — re-run
  `benchmarks/benchmark_validate.py` on your own cluster/data shape before relying on them for
  capacity planning.
