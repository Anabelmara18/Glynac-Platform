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

## Bronze layer (no Docker needed)

```bash
# one-time: get the standalone ClickHouse binary (Linux/WSL)
curl https://clickhouse.com/ | sh

python generator/generate.py --target local --out generator/out
python generator/export_bronze_configs.py          # writes configs/bronze/*.yml
python bronze/build_bronze.py --mode local --apply # builds 10 Bronze views
python bronze/verify_bronze.py                     # must print BRONZE OK
```

Each Bronze view exposes the raw columns plus `organisation_id` and
`processing_date` (parsed from the folder path) and `_source_file`.
With a real ClickHouse server + MinIO use `--mode s3` instead, which renders
S3-engine tables and the same views.

## Silver layer

```bash
python generator/export_silver_configs.py            # writes configs/silver/*.yml
python silver/build_silver.py --apply                # loads all days, builds views
python silver/verify_silver.py                       # must print SILVER OK
```

Design rationale (engines, grain, history, tenant-safe join): `docs/silver-design.md`.

## Airflow (DAG factory, no Docker)

`dags/medallion_dag_factory.py` reads `configs/silver/*.yml` and generates one DAG per
table (`silver_<table>`): `check_bronze_ready -> load_silver -> validate_silver`.
There is no table-specific code; add a YAML and a DAG appears.
Schedule and date range live in `configs/pipeline.yml`.

```bash
# separate venv for Airflow (it has heavy dependencies)
python3 -m venv .venv-airflow && source .venv-airflow/bin/activate
pip install apache-airflow pyyaml pytest

export AIRFLOW_HOME=$HOME/airflow
export AIRFLOW__CORE__DAGS_FOLDER=$PWD/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export CLICKHOUSE_BIN=$PWD/clickhouse

pytest tests -q                                   # DAG factory tests
airflow db migrate
airflow dags test silver_transactions 2026-10-26  # one real run, 3 tasks
airflow standalone                                # UI on http://localhost:8080
```

In the UI, unpause the `silver_*` DAGs; `catchup=True` runs all 7 days.
When the loads finish, create the views once:
`python silver/build_silver.py --views-only --apply` then `python silver/verify_silver.py`.

ClickHouse calls are serialised with a file lock (the standalone binary allows one
process per data folder). To use a real server instead set `CLICKHOUSE_URL=http://localhost:8123`.

## Roadmap

- [x] Step 1: schemas, generator, MinIO + ClickHouse
- [x] Step 2: Bronze tables in ClickHouse (partition columns from path)
- [x] Step 3: Silver YAML configs + dedup + history
- [x] Step 4: Airflow DAG factory (one DAG per YAML)
- [ ] Step 5: Gold layer + guard tests
- [ ] Step 6: Schema helper CLI
