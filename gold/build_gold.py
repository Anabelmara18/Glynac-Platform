#!/usr/bin/env python3
"""Render Gold SQL from configs/gold/*.yml and (optionally) apply it.

  gold.<name>      aggregate table, engine chosen per config (see engine_rationale)
  gold.<name>_v    finalised view, for configs that define `view`

Every Gold SELECT must pass gold/guard.py (grouped by organisation_id, joins
tenant-aware) or the build refuses to run.

  python gold/build_gold.py --apply --clickhouse ./clickhouse
"""
import argparse
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "bronze"))
sys.path.insert(0, str(ROOT / "gold"))
import ch  # noqa: E402
import guard  # noqa: E402


def load_cfgs(d):
    return [yaml.safe_load(p.read_text()) for p in sorted(Path(d).glob("*.yml")) if not p.name.startswith("_")]


def ddl(cfg):
    """[CREATE TABLE, (CREATE VIEW)]"""
    n = cfg["name"]
    cols = ", ".join(f"{c['name']} {c['type']}" + (f" DEFAULT {c['default']}" if c.get("default") else "")
                     for c in cfg["columns"])
    engine = f"{cfg['engine']}({cfg.get('engine_args', '')})"
    out = [f"CREATE TABLE IF NOT EXISTS gold.{n} ({cols})\nENGINE = {engine}\n"
           f"PARTITION BY {cfg['partition_by']}\nORDER BY ({', '.join(cfg['order_by'])});\n"]
    if cfg.get("view"):
        out.append(f"CREATE OR REPLACE VIEW gold.{n}_v AS\n{cfg['view'].strip()};\n")
    return out


def load_sql(cfg, day):
    """Idempotent reload of one aggregate for one day: [drop partition, insert]."""
    n = cfg["name"]
    cols = ", ".join(c["name"] for c in cfg["columns"] if not c.get("default"))
    return [f"ALTER TABLE gold.{n} DROP PARTITION {day.replace('-', '')};\n",
            f"INSERT INTO gold.{n} ({cols})\n{cfg['select'].replace('{day}', day).strip()};\n"]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config-dir", default=str(ROOT / "configs" / "gold"))
    ap.add_argument("--out", default=str(ROOT / "sql" / "gold.sql"))
    ap.add_argument("--dates", default="all")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--clickhouse", default=str(ROOT / "clickhouse"))
    a = ap.parse_args()

    cfgs = load_cfgs(a.config_dir)
    bad = [p for c in cfgs for p in guard.check_config(c)]
    if bad:
        raise SystemExit("tenant guard failed, refusing to build:\n  " + "\n  ".join(bad))

    if a.dates == "all":
        dates = [r["d"] for r in ch.query(a.clickhouse, "SELECT DISTINCT toString(processing_date) AS d FROM silver.accounts ORDER BY d")]
    else:
        dates = a.dates.split(",")

    sql = ["CREATE DATABASE IF NOT EXISTS gold;\n"]
    for c in cfgs:
        sql += ddl(c)
    for d in dates:
        for c in cfgs:
            sql += load_sql(c, d)
    out = Path(a.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(sql))
    print(f"rendered Gold: {len(cfgs)} aggregates x {len(dates)} dates (guard passed) -> {out}")
    if a.apply:
        ch.run_file(a.clickhouse, out)
        print("applied")


if __name__ == "__main__":
    main()
