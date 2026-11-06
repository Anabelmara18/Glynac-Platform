# Medallion Data Platform (wealth-management)

Config-driven Bronze → Silver → Gold pipeline on ClickHouse + Airflow, multi-tenant by `organisation_id`.

**How it was run:** on the standalone ClickHouse binary inside WSL (no Docker), because Docker image
pulls failed on the author's network. `docker-compose.yml` is provided for environments where Docker works
and is untested here. Everything is selected by environment (`CLICKHOUSE_URL`, `CLICKHOUSE_BIN`), not code.

Order: generate data -> `bronze/build_bronze.py` -> `silver/build_silver.py` -> `gold/build_gold.py`,
then the three `verify_*.py` scripts (each prints `... OK`). Details in the sections below.

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

## Gold layer

```bash
python gold/build_gold.py --apply          # refuses to build if the tenant guard fails
python gold/verify_gold.py                 # must print GOLD OK
pytest tests -q                            # guard + DAG factory tests (no database needed)
```

Three aggregates, three engines (`configs/gold/*.yml`, each with an `engine_rationale`):
`aum_daily` (ReplacingMergeTree), `transaction_volume_daily` (SummingMergeTree),
`advisor_book_daily` (AggregatingMergeTree + `_v` view). Every one groups by
`organisation_id`; `gold/guard.py` enforces it and the tests prove it fails if removed.
Design notes: `docs/gold-design.md`. Airflow generates a `gold_<name>` DAG per YAML.

## Running a ClickHouse server (no Docker)

```bash
./scripts/clickhouse_server.sh                 # terminal A, leave running
export CLICKHOUSE_URL=http://localhost:8123    # terminal B: every script now uses the server
# browse the data at http://localhost:8123/play
```

## Schema helper CLI

Reads an ORC file's schema and drafts a Bronze YAML (needs only the file).

```bash
# one file (source/table inferred from the landing path)
python cli/schema_helper.py generator/out/bronze/salesforce/accounts/org_8812/2026-10-26/data.orc

# whole landing zone: scans EVERY file, fails on schema drift
python cli/schema_helper.py --all generator/out/bronze --out-dir configs/bronze

# does it reproduce the committed configs? (exit 1 if not)
python cli/schema_helper.py --all generator/out/bronze --out-dir configs/bronze --check
```

It refuses files that already contain `organisation_id`/`processing_date` (the path-derived
partition columns would shadow them), files of one table whose schemas disagree, and column
types with no ClickHouse mapping (it names the column). ORC timestamps are nanosecond
precision; Bronze uses `DateTime64(6)` unless `--timestamp-precision` says otherwise.
`tests/test_schema_helper.py` proves the CLI reproduces all 10 hand-written configs.

## Tests

```bash
pytest tests -q   # in .venv: Gold guard + schema helper; in .venv-airflow: Gold guard + DAG factory
```
Tests whose dependency is missing (Airflow, pyarrow) skip themselves, so the command works in both.

## Roadmap

- [x] Step 1: schemas, generator, MinIO + ClickHouse
- [x] Step 2: Bronze tables in ClickHouse (partition columns from path)
- [x] Step 3: Silver YAML configs + dedup + history
- [x] Step 4: Airflow DAG factory (one DAG per YAML)
- [x] Step 5: Gold layer + tenant guard + tests
- [x] Step 6: Schema helper CLI
