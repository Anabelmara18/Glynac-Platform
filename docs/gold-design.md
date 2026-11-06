# Gold design notes (for the review)

Gold is the business-facing layer: a few small aggregate tables built from Silver,
one DAG each, every one grouped by `organisation_id`.

## Engine choice, per aggregate (the review question)

| Aggregate | Engine | Why this one, and why not the others |
|---|---|---|
| `aum_daily` | ReplacingMergeTree(_loaded_at) | AUM is a point-in-time **total**. Re-running a day must replace the number. SummingMergeTree would add the re-run to the original and double-count. |
| `transaction_volume_daily` | SummingMergeTree((txn_count, total_amount)) | Counts and amounts are **additive flows**: partial rows for a key can be summed on merge, and reads can use plain `sum()`. Safe because each load drops the day's partition first. |
| `advisor_book_daily` | AggregatingMergeTree | A **distinct count** of accounts is not additive. It is stored as an aggregate state (`uniqExactState`) and finalised with `uniqExactMerge` in the `_v` view. |

The rule of thumb: snapshot total -> Replacing; additive measure -> Summing;
non-additive measure (distinct, quantiles, averages) -> Aggregating with states.

## The tenant guard (`gold/guard.py`)

Static analysis of the SQL itself (sqlglot, ClickHouse dialect), no database needed:
R1 top-level SELECT groups by `organisation_id`; R2 any nested aggregating SELECT does too;
R3 every JOIN compares `organisation_id` on both sides; R4 the output has the column;
R5 table ORDER BY starts with it; R6 it is a declared column.

It runs in three places: `build_gold.py` (refuses to build), the DAG factory (a bad config
becomes an Airflow import error), and `tests/test_gold_guard.py`.

**Proof it would fail if the grouping were removed:** the tests build *mutants* - copies of
every config with `organisation_id` removed from the GROUP BY or a JOIN - and assert the
guard flags each one. `test_builder_refuses_a_config_with_the_grouping_removed` edits the
real YAML on disk and checks the real builder aborts.

## Reconciliation (`gold/verify_gold.py`)

Every aggregate, every day, per organisation, is compared with an independent recompute
from Silver. A leaky join (no `organisation_id`) is run as a negative control and
overstates AUM by ~1.4M because the seeded collision key exists in two organisations.

## A real bug the reconcile caught

The first version joined fee schedules with an INNER JOIN. The seeded collision accounts had
no fee schedule, so their holdings silently dropped out of the advisor book and AUM was short
by 1,422,625.72 against `aum_daily`. Fixes: the generator now gives those accounts a schedule,
and the join is a LEFT JOIN so a missing schedule can never remove AUM (fees = 0 instead).
