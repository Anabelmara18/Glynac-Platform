"""Tenant guard tests for Gold. No database and no Airflow needed.  pytest tests -q

These prove the guard FAILS if organisation_id is removed from a GROUP BY or a JOIN.
"""
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "gold"))
sys.path.insert(0, str(ROOT / "bronze"))
import build_gold  # noqa: E402
import guard  # noqa: E402

CFGS = build_gold.load_cfgs(ROOT / "configs" / "gold")


def test_there_are_gold_configs():
    assert len(CFGS) >= 3


@pytest.mark.parametrize("cfg", CFGS, ids=lambda c: c["name"])
def test_real_configs_pass_the_guard(cfg):
    assert guard.check_config(cfg) == []


@pytest.mark.parametrize("cfg", CFGS, ids=lambda c: c["name"])
def test_removing_group_by_organisation_id_is_caught(cfg):
    problems = guard.check_config(guard.drop_group_org(cfg))
    assert any("GROUP BY" in p for p in problems), problems


@pytest.mark.parametrize("cfg", CFGS, ids=lambda c: c["name"])
def test_removing_join_organisation_id_is_caught(cfg):
    mutant = guard.drop_join_org(cfg)
    if mutant is None:
        pytest.skip("no joins in this aggregate")
    assert any("JOIN" in p for p in guard.check_config(mutant))


def test_order_by_must_start_with_organisation_id():
    bad = dict(CFGS[0], order_by=["processing_date", "organisation_id"])
    assert any("ORDER BY" in p for p in guard.check_config(bad))


def test_builder_refuses_a_config_with_the_grouping_removed(tmp_path):
    """End to end: edit the real YAML on disk, run the real builder."""
    cfg_dir = tmp_path / "gold"
    shutil.copytree(ROOT / "configs" / "gold", cfg_dir)
    f = cfg_dir / "aum_daily.yml"
    text = f.read_text()
    assert "GROUP BY h.organisation_id\n" in text
    f.write_text(text.replace("GROUP BY h.organisation_id\n", ""))
    r = subprocess.run([sys.executable, str(ROOT / "gold" / "build_gold.py"), "--config-dir", str(cfg_dir),
                        "--dates", "2026-10-26", "--out", str(tmp_path / "x.sql")], capture_output=True, text=True)
    assert r.returncode != 0
    assert "tenant guard failed" in r.stderr and not (tmp_path / "x.sql").exists()
