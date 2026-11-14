# Benchmarks

Validation overhead for a typical ETL schema, measured against a synthetic 1,000,000-row
DataFrame. Run it yourself:

```bash
uv run python benchmarks/benchmark_validate.py
```

## Setup

The benchmark contract (`OdometerReadingContract` in `benchmarks/benchmark_validate.py`)
exercises every constraint kind in one schema: `nullable`, `min_value`, `min_length`/
`max_length`, `regex`, `allowed_values`, `unique`, a cross-column `condition`, one `@check`
(returning a condition), and `min_rows` — 6 fields, 1 check, deliberately denser than most
real contracts, so the numbers below are closer to a worst case than a typical one.

Besides the passing DataFrame, a copy with 4 bad rows in 1M breaks 5 rules (vin length and
regex, status, odometer_start, and the check), to show what a failing validation costs.

Each scenario runs once unmeasured (warmup, so Catalyst's whole-stage codegen is already
compiled) and then 3 timed repeats; the table reports the median. The warmup run also counts
the Spark jobs the scenario launches (via a job group on the `SparkContext`) — a number that,
unlike wall-clock time, doesn't depend on the machine. `mode="soft"` is used
throughout — `mode` only changes what happens *after* violations are found (log level, raise
or not), not how much work `validate()` does to find them, so it doesn't affect timing.

## Results

Measured on an Apple M3 (8 cores), `local[*]`, PySpark 4.3.0, cached input DataFrames, v0.12.0
against v0.13.0 on the same machine and day. (v0.12.0 runs the same check written as
`df.filter(...)`, since it can't take a condition.)

| Scenario | v0.12.0 | v0.13.0 | Spark jobs (v0.12.0 → v0.13.0) |
|---|---|---|---|
| `df.count()` only (baseline) | 0.05s | 0.06s | 2 → 2 |
| `validate(depth=SCHEMA_ONLY)` | 0.00s | 0.00s | 0 → 0 |
| `validate(depth=DATA_ONLY)` | 0.34s | 0.31s | 6 → 4 |
| `validate(depth=SCHEMA_AND_DATA)` (default) | 0.32s | 0.28s | 6 → 4 |
| `validate()`, 5 rules failing on 4 rows | 0.62s | 0.44s | 21 → 6 |

Jobs aren't actions: Spark can split one action into several jobs (adaptive query execution
plans a shuffle stage as its own job), which is why `df.count()` alone shows 2. In actions:

- **Passing**, v0.12.0 ran 3: one aggregation for the row count and the 13 column
  constraints, the `unique` `groupBy`, and a count of the `@check`'s rows. v0.13.0 runs 2: the
  condition check joins the aggregation.
- **Failing**, v0.12.0 ran 8: those 3, plus one sample lookup per failing rule (5). v0.13.0 runs
  3: the aggregation, the `unique` `groupBy`, and a single query that fetches the samples of all
  5 rules.

Earlier versions: v0.9.0 ran 16 actions (30 jobs) on the passing case, one per column constraint;
v0.10.0's single pass brought it to 3 (6 jobs).

`SCHEMA_AND_DATA` and `DATA_ONLY` land within noise of each other, which is expected —
`_check_schema` reads only `df.schema` (metadata, no action), so adding it on top of
`DATA_ONLY` costs nothing measurable. `SCHEMA_ONLY` reliably rounds to `0.00s` with 0 jobs,
confirming it never triggers a Spark action.

## Reading these numbers

- **Column constraints and condition checks are nearly free to add** — `nullable`, ranges,
  lengths, `regex`, `allowed_values`, `condition`, and every `@check` that returns a condition
  share one scan, so a contract with twice as many costs about the same.
- **`unique`, `unique_together`, and DataFrame checks each add an action** — the first two need
  a `groupBy()`/shuffle, and a check returning a DataFrame is counted on its own.
- **A failing validation adds one action for samples, not one per rule** — rules failing on up
  to 10,000 rows share a single sample query. A rule failing on more keeps its own lookup, which
  stops reading as soon as it has five rows.
- **Caching matters less than before, not zero** — this benchmark validates `.cache()`d
  DataFrames. A contract without `unique`/`unique_together` or DataFrame checks reads a passing
  DataFrame once; cache when you have several of those, or expect violations, on a DataFrame
  that's expensive to recompute (see the "Performance note" in the README). Without a cache,
  every action saved is a full read of the source saved.
- These are wall-clock numbers from one laptop, not a guarantee — re-run
  `benchmarks/benchmark_validate.py` on your own cluster/data shape before relying on them for
  capacity planning.
