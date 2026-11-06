"""Tests for the DAG factory. Run:  pytest tests/ -q   (needs apache-airflow)

Proves the "no code duplication" requirement:
  * one DAG per Silver YAML, each with the same three tasks in order
  * adding a YAML (no code change) adds a DAG; removing it removes the DAG
  * the factory contains no table-specific names
"""
import importlib.util
import shutil
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parent.parent
FACTORY = ROOT / "dags" / "medallion_dag_factory.py"
TABLES = ["accounts", "advisors", "contacts", "fee_schedules", "holdings",
          "portfolios", "securities", "statements", "trades", "transactions"]
AGGREGATES = ["aum_daily", "advisor_book_daily", "transaction_volume_daily"]
SILVER_TASKS = ["check_bronze_ready", "load_silver", "validate_silver"]
GOLD_TASKS = ["check_silver_ready", "load_gold", "validate_gold"]


def load_factory(monkeypatch, configs):
    monkeypatch.setenv("MEDALLION_CONFIGS", str(configs))
    monkeypatch.setenv("MEDALLION_ROOT", str(ROOT))
    name = f"factory_{abs(hash(str(configs)))}"
    spec = importlib.util.spec_from_file_location(name, FACTORY)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def configs(tmp_path):
    dst = tmp_path / "configs"
    shutil.copytree(ROOT / "configs", dst)
    return dst


def test_one_dag_per_yaml(monkeypatch, configs):
    dags = load_factory(monkeypatch, configs).generate()
    assert sorted(dags) == sorted([f"silver_{t}" for t in TABLES] + [f"gold_{a}" for a in AGGREGATES])


def test_every_dag_has_same_tasks_in_order(monkeypatch, configs):
    for dag_id, dag in load_factory(monkeypatch, configs).generate().items():
        expected = SILVER_TASKS if dag_id.startswith("silver_") else GOLD_TASKS
        assert [t.task_id for t in dag.topological_sort()] == expected


def test_schedule_comes_from_config(monkeypatch, configs):
    cfg = yaml.safe_load((configs / "pipeline.yml").read_text())
    dag = load_factory(monkeypatch, configs).generate()["silver_accounts"]
    assert dag.start_date.date().isoformat() == cfg["start_date"]
    assert dag.end_date.date().isoformat() == cfg["end_date"]
    assert dag.max_active_runs == 1 and dag.catchup is True


def test_new_yaml_adds_dag_without_code_change(monkeypatch, configs):
    src = yaml.safe_load((configs / "silver" / "advisors.yml").read_text())
    src["table"] = "advisors_copy"
    (configs / "silver" / "advisors_copy.yml").write_text(yaml.safe_dump(src, sort_keys=False))
    dags = load_factory(monkeypatch, configs).generate()
    assert "silver_advisors_copy" in dags and len(dags) == len(TABLES) + len(AGGREGATES) + 1
    (configs / "silver" / "advisors_copy.yml").unlink()
    assert "silver_advisors_copy" not in load_factory(monkeypatch, configs).generate()


def test_new_gold_yaml_adds_dag_without_code_change(monkeypatch, configs):
    src = yaml.safe_load((configs / "gold" / "aum_daily.yml").read_text())
    src["name"] = "aum_daily_copy"
    (configs / "gold" / "aum_daily_copy.yml").write_text(yaml.safe_dump(src, sort_keys=False))
    assert "gold_aum_daily_copy" in load_factory(monkeypatch, configs).generate()


def test_gold_config_without_organisation_grouping_breaks_the_import(monkeypatch, configs):
    f = configs / "gold" / "aum_daily.yml"
    f.write_text(f.read_text().replace("GROUP BY h.organisation_id\n", ""))
    with pytest.raises(ValueError, match="tenant guard failed"):
        load_factory(monkeypatch, configs)


def test_factory_has_no_table_specific_code():
    text = FACTORY.read_text().lower()
    leaked = [t for t in TABLES + AGGREGATES if t in text]
    assert not leaked, f"table names hard-coded in the factory: {leaked}"
