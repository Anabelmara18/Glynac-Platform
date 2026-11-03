"""DAG factory: one Airflow DAG per table, generated from the YAML configs.

There is NO table-specific code in this file. Drop a new YAML into
configs/silver/ and a new DAG appears on the next parse; delete it and the DAG
goes away. (tests/test_dag_factory.py proves this.)

Each DAG:   check_bronze_ready  ->  load_silver  ->  validate_silver
  check_bronze_ready  fails (and retries) until files for that day have landed
  load_silver         idempotent: drops that day's partition, re-inserts it
  validate_silver     rows loaded, no duplicate keys, org_id never empty,
                      and (snapshots) row count equals Bronze's distinct count

Environment (all optional):
  MEDALLION_ROOT      repo root (default: parent of dags/)
  MEDALLION_CONFIGS   config root (default: <root>/configs)
  CLICKHOUSE_BIN      path to the clickhouse binary (default: <root>/clickhouse)
  CLICKHOUSE_URL      use a ClickHouse server over HTTP instead of the binary
"""
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import yaml
from airflow.sdk import DAG

try:
    from airflow.sdk.exceptions import AirflowFailException  # Airflow 3.1+
except ImportError:
    from airflow.exceptions import AirflowFailException

try:  # Airflow 3
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:  # Airflow 2
    from airflow.operators.python import PythonOperator

ROOT = Path(os.environ.get("MEDALLION_ROOT", Path(__file__).resolve().parent.parent))
CONFIGS = Path(os.environ.get("MEDALLION_CONFIGS", ROOT / "configs"))
CLICKHOUSE = os.environ.get("CLICKHOUSE_BIN", str(ROOT / "clickhouse"))
for sub in ("bronze", "silver"):
    sys.path.insert(0, str(ROOT / sub))
import build_silver  # noqa: E402
import ch  # noqa: E402


def _pipeline():
    return yaml.safe_load((CONFIGS / "pipeline.yml").read_text())


def _keys(cfg):
    return ", ".join(cfg["business_key"])


# --------------------------------------------------------------------------- #
# task callables (generic over any config)
# --------------------------------------------------------------------------- #
def check_bronze_ready(table, day):
    cfg = _load(table)
    n = ch.query(CLICKHOUSE, f"SELECT count() AS n FROM bronze.{cfg['bronze_table']} "
                             f"WHERE processing_date = toDate('{day}')")[0]["n"]
    if int(n) == 0:
        raise RuntimeError(f"no Bronze rows for {table} on {day} yet (files not landed?)")
    return int(n)


def load_silver(table, day):
    cfg = _load(table)
    ch.execute(CLICKHOUSE, "CREATE DATABASE IF NOT EXISTS silver")
    ch.execute(CLICKHOUSE, build_silver.ddl(cfg))          # idempotent: IF NOT EXISTS
    for stmt in build_silver.load_sql(cfg, day):
        ch.execute(CLICKHOUSE, stmt)


def validate_silver(table, day):
    cfg = _load(table)
    k = _keys(cfg)
    grp = f"organisation_id, processing_date, {k}" if cfg["grain"] == "snapshot" else f"organisation_id, {k}"
    loaded = int(ch.query(CLICKHOUSE, f"SELECT count() AS n FROM silver.{table} FINAL "
                                      f"WHERE processing_date = toDate('{day}')")[0]["n"])
    dups = int(ch.query(CLICKHOUSE, f"SELECT count() AS n FROM (SELECT 1 FROM silver.{table} FINAL "
                                    f"GROUP BY {grp} HAVING count() > 1)")[0]["n"])
    empty = int(ch.query(CLICKHOUSE, f"SELECT count() AS n FROM silver.{table} FINAL "
                                     f"WHERE processing_date = toDate('{day}') AND organisation_id = ''")[0]["n"])
    problems = []
    if loaded == 0:
        problems.append("no rows loaded")
    if dups:
        problems.append(f"{dups} duplicate key groups")
    if empty:
        problems.append(f"{empty} rows with empty organisation_id")
    if cfg["grain"] == "snapshot":
        expected = int(ch.query(CLICKHOUSE, f"SELECT count() AS n FROM (SELECT 1 FROM bronze.{cfg['bronze_table']} "
                                            f"WHERE processing_date = toDate('{day}') GROUP BY {grp})")[0]["n"])
        if loaded != expected:
            problems.append(f"row count {loaded} != Bronze distinct {expected}")
    if problems:
        raise AirflowFailException(f"silver.{table} {day}: " + "; ".join(problems))
    return loaded


def _load(table):
    return next(c for c in build_silver.load_cfgs(CONFIGS / "silver") if c["table"] == table)


# --------------------------------------------------------------------------- #
# factory
# --------------------------------------------------------------------------- #
def build_dag(layer, cfg, settings):
    table = cfg["table"]
    with DAG(
        dag_id=f"{layer}_{table}",
        schedule=settings["schedule"],
        start_date=datetime.fromisoformat(settings["start_date"]),
        end_date=datetime.fromisoformat(settings["end_date"]),
        catchup=True,
        max_active_runs=1,
        default_args={"retries": settings["retries"],
                      "retry_delay": timedelta(minutes=settings["retry_delay_minutes"])},
        tags=[layer, cfg["grain"], "generated"],
        doc_md=f"Generated from `configs/{layer}/{table}.yml` ({cfg['grain']} grain, "
               f"key: {', '.join(cfg['business_key'])}).",
    ) as dag:
        kw = {"table": table, "day": "{{ ds }}"}
        ready = PythonOperator(task_id="check_bronze_ready", python_callable=check_bronze_ready, op_kwargs=kw)
        load = PythonOperator(task_id="load_silver", python_callable=load_silver, op_kwargs=kw)
        check = PythonOperator(task_id="validate_silver", python_callable=validate_silver, op_kwargs=kw)
        ready >> load >> check
    return dag


def generate():
    settings = _pipeline()
    dags = {}
    for cfg in build_silver.load_cfgs(CONFIGS / "silver"):
        dag = build_dag("silver", cfg, settings)
        dags[dag.dag_id] = dag
    return dags


globals().update(generate())     # Airflow discovers DAG objects in module globals
