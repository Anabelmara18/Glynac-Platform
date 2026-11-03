# Silver design notes (for the review)

## Why ReplacingMergeTree
Silver is rebuilt per (table, day) and must be safe to re-run. `ReplacingMergeTree(_loaded_at)`
keeps the newest copy of a row with the same sorting key, so a re-run or a late duplicate insert
collapses to one row. Reads use `FINAL`. Two caveats we handle explicitly:
- Replacing only dedups **within a partition** (we partition by processing day).
  Event tables (transactions, trades) therefore dedup across days at load time with a
  tenant-aware `NOT IN (org, key)` check; resent rows are skipped.
- Merges are asynchronous, so correctness never relies on a merge having run: loads drop
  the day's partition first, and every read path uses `FINAL`.

## Grain
- Snapshot tables: one row per (organisation_id, business key, processing_date). Inside a
  day, stale duplicates are collapsed with `argMax(col, version_column)` - the latest
  `updated_at` wins.
- Event tables: one row per (organisation_id, business key) across all days.
- `ORDER BY` starts with `organisation_id` on every table, so the sorting/dedup key can never
  merge two tenants. The seeded collision (ACC-99001 etc.) proves it.

## Change history
`<table>_history` turns the daily snapshots into versions (valid_from, valid_to, is_current).
A new version starts when any tracked column changes between consecutive days
(accounts: status/advisor/type; contacts: email/phone/city/risk; fee_schedules: type/rate/frequency).
`<table>_current` is the latest day per key per organisation.

## Parent-child join
`holding_detail` joins holdings -> portfolios -> accounts -> contacts (+ securities).
Every ON clause must compare `organisation_id`; the builder refuses to render a join that
doesn't. Without it, the seeded collision key fans out (2 rows become 4).

## Cleaning rules
Trim all strings; lower-case emails/statuses/sides/types; upper-case currencies/tickers.
