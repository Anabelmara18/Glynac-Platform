# Medallion Data Platform (wealth-management)

Config-driven Bronze → Silver → Gold pipeline on MinIO + ClickHouse + Airflow.

## Quick start

```bash
pip install -r requirements.txt

# 1. start storage + warehouse
docker compose up -d

# 2. generate data and upload to MinIO (bucket `bronze`)
cd generator
python generate.py --target minio

# or generate locally without Docker, to inspect the files
python generate.py --target local --out ./out
```

MinIO console: http://localhost:9001 (minioadmin / minioadmin)
ClickHouse HTTP: http://localhost:8123 (default / clickhouse)

## Landing layout

`bronze/<source>/<table>/<organisation_id>/<processing_date>/data.orc`

`organisation_id` and `processing_date` are **not** columns in the files; they
exist only in the path and must be populated by the Bronze layer.

## Dataset (10 tables)

| Source | Tables |
|---|---|
| salesforce | advisors, contacts, accounts |
| reference_data | securities, fee_schedules |
| blackdiamond | portfolios, holdings, transactions, trades, statements |

3 orgs (`org_8812`, `org_4410`, `org_9027`) × 7 days. `_manifest.json` records
every seeded collision, stale duplicate, resent row and state change.

## Seeded test conditions

- **Cross-org collision:** `CON-99001`, `ACC-99001`, `PF-99001`, `HLD-99001`
  exist in `org_8812` and `org_4410` with different data.
- **Stale duplicates** inside a single daily file (latest `updated_at` wins).
- **Resent transactions/trades** delivered again the next day.
- **Change history:** contact details (day 3), account status (day 4),
  fee rates (day 5), advisor reassignment (day 6).

## Roadmap

- [x] Step 1: schemas, generator, MinIO + ClickHouse
- [ ] Step 2: Bronze tables in ClickHouse (partition columns from path)
- [ ] Step 3: Silver YAML configs + dedup + history
- [ ] Step 4: Airflow + generator DAG
- [ ] Step 5: Gold layer + guard tests
- [ ] Step 6: Schema helper CLI
