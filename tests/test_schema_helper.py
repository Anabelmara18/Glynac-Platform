"""Schema helper CLI tests. Need pyarrow + faker (the normal .venv); skipped elsewhere.

Headline test: ORC files alone -> configs identical to the hand-written
configs/bronze/*.yml for all 10 tables.
"""
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

pa = pytest.importorskip("pyarrow")
pytest.importorskip("faker")
import pyarrow.orc as orc  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "cli"))
import schema_helper as sh  # noqa: E402

COMMITTED = ROOT / "configs" / "bronze"


@pytest.fixture(scope="module")
def landing(tmp_path_factory):
    out = tmp_path_factory.mktemp("landing")
    subprocess.run([sys.executable, str(ROOT / "generator" / "generate.py"), "--target", "local",
                    "--out", str(out), "--days", "2", "--scale", "0.3"], check=True, capture_output=True)
    return out / "bronze"


def write_orc(path, table):
    path.parent.mkdir(parents=True, exist_ok=True)
    orc.write_table(table, str(path))


def test_all_ten_tables_reproduce_the_committed_configs(landing):
    assert sh.main(["--all", str(landing), "--out-dir", str(COMMITTED), "--check"]) == 0


def test_generated_yaml_files_equal_the_committed_ones(landing, tmp_path):
    assert sh.main(["--all", str(landing), "--out-dir", str(tmp_path)]) == 0
    files = sorted(p.name for p in COMMITTED.glob("*.yml"))
    assert len(files) == 10 and sorted(p.name for p in tmp_path.glob("*.yml")) == files
    for name in files:
        assert yaml.safe_load((tmp_path / name).read_text()) == yaml.safe_load((COMMITTED / name).read_text())


def test_single_file_infers_source_and_table_from_the_path(landing):
    f = next(landing.glob("salesforce/accounts/*/*/data.orc"))
    cfg = sh.draft(f)
    assert (cfg["source"], cfg["table"], cfg["path_glob"]) == ("salesforce", "accounts", "salesforce/accounts/*/*/data.orc")


def test_cannot_infer_without_the_landing_layout(tmp_path, capsys):
    write_orc(tmp_path / "data.orc", pa.table({"x": ["a"]}))
    assert sh.main([str(tmp_path / "data.orc")]) == 2
    assert "pass --source and --table" in capsys.readouterr().err
    assert sh.main([str(tmp_path / "data.orc"), "--source", "s", "--table", "t"]) == 0


def test_file_containing_a_partition_column_is_refused(tmp_path, capsys):
    f = tmp_path / "src" / "tbl" / "org_1" / "2026-10-26" / "data.orc"
    write_orc(f, pa.table({"id": ["a"], "organisation_id": ["org_1"]}))
    assert sh.main([str(f)]) == 2
    assert "shadow" in capsys.readouterr().err


def test_schema_drift_between_files_is_detected(tmp_path, capsys):
    base = tmp_path / "landing" / "src" / "tbl"
    write_orc(base / "org_1" / "2026-10-26" / "data.orc", pa.table({"id": ["a"], "amount": pa.array([1], pa.int32())}))
    write_orc(base / "org_1" / "2026-10-27" / "data.orc", pa.table({"id": ["a"], "amount": pa.array([1], pa.int64())}))
    assert sh.main(["--all", str(tmp_path / "landing"), "--out-dir", str(tmp_path / "out")]) == 2
    err = capsys.readouterr().err
    assert "schema drift" in err and "amount" in err


def test_unsupported_type_names_the_column(tmp_path, capsys):
    f = tmp_path / "src" / "tbl" / "org_1" / "2026-10-26" / "data.orc"
    write_orc(f, pa.table({"id": ["a"], "tags": pa.array([[1, 2]], pa.list_(pa.int64()))}))
    assert sh.main([str(f)]) == 2
    assert "column 'tags'" in capsys.readouterr().err


def test_check_fails_when_a_committed_config_has_drifted(landing, tmp_path):
    cfg_dir = tmp_path / "bronze"
    shutil.copytree(COMMITTED, cfg_dir)
    f = cfg_dir / "accounts.yml"
    f.write_text(f.read_text().replace("Date32", "String", 1))
    assert sh.main(["--all", str(landing), "--out-dir", str(cfg_dir), "--check"]) == 1


def test_timestamp_precision_flag(landing):
    f = next(landing.glob("salesforce/accounts/*/*/data.orc"))
    types = {c["name"]: c["type"] for c in sh.draft(f, ts_precision=3)["columns"]}
    assert types["updated_at"] == "DateTime64(3)"
