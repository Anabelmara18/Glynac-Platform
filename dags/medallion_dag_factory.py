"""DAG factory: one Airflow DAG per Silver table and per Gold aggregate,
generated from the YAML configs.

There is NO table-specific code in this file. Drop a YAML into configs/silver/
or configs/gold/ and a new DAG appears on the next parse; delete it and the DAG
goes away (tests/test_dag_factory.py proves this). A Gold config that breaks the
tenant guard (not grouped by organisation_id, join without organisation_id)
makes the file fail to import, so it shows up as an import error in the UI.

  silver_<table>:  check_bronze_ready -> load_silver -> validate_silver
  gold_<name>:     check_silver_ready -> load_gold   -> validate_gold

Every load is idempotent (drops that day's partition, re-inserts it).

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
for sub in ("bronze", "silver", "gold"):
    sys.path.insert(0, str(ROOT / sub))
import build_gold  # noqa: E402
import build_silver  # noqa: E402
import ch  # noqa: E402
import guard  # noqa: E402


def _pipeline():
    return yaml.safe_load((CONFIGS / "pipeline.yml").read_text())


def _n(sql):
    return int(ch.query(CLICKHOUSE, sql)[0]["n"])


# --------------------------------------------------------------------------- #
# SILVER task callables (generic over any config)
# --------------------------------------------------------------------------- #
def _silver(name):
    return next(c for c in build_silver.load_cfgs(CONFIGS / "silver") if c["table"] == name)


def check_bronze_ready(name, day):
    cfg = _silver(name)
    n = _n(f"SELECT count() AS n FROM bronze.{cfg['bronze_table']} WHERE processing_date = toDate('{day}')")
    if n == 0:
        raise RuntimeError(f"no Bronze rows for {name} on {day} yet (files not landed?)")
    return n


def load_silver(name, day):
    cfg = _silver(name)
    ch.execute(CLICKHOUSE, "CREATE DATABASE IF NOT EXISTS silver")
    ch.execute(CLICKHOUSE, build_silver.ddl(cfg))          # idempotent: IF NOT EXISTS
    for stmt in build_silver.load_sql(cfg, day):
        ch.execute(CLICKHOUSE, stmt)


def validate_silver(name, day):
    cfg = _silver(name)
    k = ", ".join(cfg["business_key"])
    grp = f"organisation_id, processing_date, {k}" if cfg["grain"] == "snapshot" else f"organisation_id, {k}"
    loaded = _n(f"SELECT count() AS n FROM silver.{name} FINAL WHERE processing_date = toDate('{day}')")
    dups = _n(f"SELECT count() AS n FROM (SELECT 1 FROM silver.{name} FINAL GROUP BY {grp} HAVING count() > 1)")
    empty = _n(f"SELECT count() AS n FROM silver.{name} FINAL "
               f"WHERE processing_date = toDate('{day}') AND organisation_id = ''")
    problems = []
    if loaded == 0:
        problems.append("no rows loaded")
    if dups:
        problems.append(f"{dups} duplicate key groups")
    if empty:
        problems.append(f"{empty} rows with empty organisation_id")
    if cfg["grain"] == "snapshot":
        expected = _n(f"SELECT count() AS n FROM (SELECT 1 FROM bronze.{cfg['bronze_table']} "
                      f"WHERE processing_date = toDate('{day}') GROUP BY {grp})")
        if loaded != expected:
            problems.append(f"row count {loaded} != Bronze distinct {expected}")
    if problems:
        raise AirflowFailException(f"silver.{name} {day}: " + "; ".join(problems))
    return loaded


# --------------------------------------------------------------------------- #
# GOLD task callables (generic over any config)
# --------------------------------------------------------------------------- #
def _gold(name):
    return next(c for c in build_gold.load_cfgs(CONFIGS / "gold") if c["name"] == name)


def check_silver_ready(name, day):
    cfg = _gold(name)
    for dep in cfg["depends_on"]:
        if _n(f"SELECT count() AS n FROM silver.{dep} WHERE processing_date = toDate('{day}')") == 0:
            raise RuntimeError(f"silver.{dep} has no rows for {day} yet; gold.{name} must wait")


def load_gold(name, day):
    cfg = _gold(name)
    ch.execute(CLICKHOUSE, "CREATE DATABASE IF NOT EXISTS gold")
    for stmt in build_gold.ddl(cfg):
        ch.execute(CLICKHOUSE, stmt)
    for stmt in build_gold.load_sql(cfg, day):
        ch.execute(CLICKHOUSE, stmt)


def validate_gold(name, day):
    cfg = _gold(name)
    rows = _n(f"SELECT count() AS n FROM gold.{name} FINAL WHERE processing_date = toDate('{day}')")
    orgs = _n(f"SELECT uniqExact(organisation_id) AS n FROM gold.{name} FINAL WHERE processing_date = toDate('{day}')")
    src = cfg["depends_on"][0]
    src_orgs = _n(f"SELECT uniqExact(organisation_id) AS n FROM silver.{src} FINAL WHERE processing_date = toDate('{day}')")
    empty = _n(f"SELECT count() AS n FROM gold.{name} FINAL "
               f"WHERE processing_date = toDate('{day}') AND organisation_id = ''")
    mism = _n(cfg["reconcile"].replace("{day}", day).replace("AS mismatches", "AS n"))
    problems = []
    if rows == 0:
        problems.append("no rows loaded")
    if orgs != src_orgs:
        problems.append(f"{orgs} organisations in gold vs {src_orgs} in silver.{src} (an organisation went missing or leaked)")
    if empty:
        problems.append(f"{empty} rows with empty organisation_id")
    if mism:
        problems.append(f"{mism} organisations do not reconcile with Silver")
    if problems:
        raise AirflowFailException(f"gold.{name} {day}: " + "; ".join(problems))
    return rows


# --------------------------------------------------------------------------- #
# factory
# --------------------------------------------------------------------------- #
LAYERS = {
    "silver": {
        "configs": lambda: build_silver.load_cfgs(CONFIGS / "silver"),
        "name": lambda c: c["table"],
        "tasks": [("check_bronze_ready", check_bronze_ready), ("load_silver", load_silver),
                  ("validate_silver", validate_silver)],
        "tags": lambda c: [c["grain"]],
        "doc": lambda c: f"Generated from `configs/silver/{c['table']}.yml` ({c['grain']} grain, "
                         f"key: {', '.join(c['business_key'])}).",
    },
    "gold": {
        "configs": lambda: build_gold.load_cfgs(CONFIGS / "gold"),
        "name": lambda c: c["name"],
        "tasks": [("check_silver_ready", check_silver_ready), ("load_gold", load_gold),
                  ("validate_gold", validate_gold)],
        "tags": lambda c: [c["engine"]],
        "doc": lambda c: f"Generated from `configs/gold/{c['name']}.yml`. {c['description']} "
                         f"Engine: {c['engine']}. {' '.join(c['engine_rationale'].split())}",
    },
}


def build_dag(layer, cfg, settings):
    spec = LAYERS[layer]
    name = spec["name"](cfg)
    with DAG(
        dag_id=f"{layer}_{name}",
        schedule=settings["schedule"],
        start_date=datetime.fromisoformat(settings["start_date"]),
        end_date=datetime.fromisoformat(settings["end_date"]),
        catchup=True,
        max_active_runs=1,
        default_args={"retries": settings["retries"],
                      "retry_delay": timedelta(minutes=settings["retry_delay_minutes"])},
        tags=[layer, *spec["tags"](cfg), "generated"],
        doc_md=spec["doc"](cfg),
    ) as dag:
        prev = None
        for task_id, fn in spec["tasks"]:
            op = PythonOperator(task_id=task_id, python_callable=fn, op_kwargs={"name": name, "day": "{{ ds }}"})
            if prev:
                prev >> op
            prev = op
    return dag


def generate():
    settings = _pipeline()
    problems = [p for c in LAYERS["gold"]["configs"]() for p in guard.check_config(c)]
    if problems:  # surfaces as an import error in the Airflow UI
        raise ValueError("Gold tenant guard failed:\n  " + "\n  ".join(problems))
    dags = {}
    for layer, spec in LAYERS.items():
        for cfg in spec["configs"]():
            dag = build_dag(layer, cfg, settings)
            dags[dag.dag_id] = dag
    return dags


globals().update(generate())     # Airflow discovers DAG objects in module globals
