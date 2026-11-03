#!/usr/bin/env python3
"""Render Silver SQL from configs/silver/*.yml and (optionally) apply it.

What gets built
  silver.<t>            cleaned, deduplicated table. One row per
                        (organisation_id, business key) per processing_date.
                        Event tables (transactions, trades): one row per
                        (organisation_id, key) across ALL days.
  silver.<t>_current    latest state of each key per organisation
  silver.<t>_history    change history (valid_from / valid_to / is_current)
                        for tables with `history.enabled`
  silver.<join name>    parent-child join views from _joins.yml

Every load function is (table, date) -> SQL, so an Airflow task can call the
same code for exactly one table and one day (next step).

  python silver/build_silver.py --apply --clickhouse ./clickhouse
"""
import argparse
import re
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "bronze"))   # shared ClickHouse runner
import ch  # noqa: E402


# --------------------------------------------------------------------------- #
# tables
# --------------------------------------------------------------------------- #
def ddl(cfg):
    t, keys = cfg["table"], cfg["business_key"]
    coldefs = ", ".join(f"{c['name']} {c['type']}" for c in cfg["columns"])
    order = ["organisation_id"] + (["processing_date"] if cfg["grain"] == "snapshot" else []) + keys
    return (
        f"CREATE TABLE IF NOT EXISTS silver.{t} ({coldefs}, organisation_id String, processing_date Date,\n"
        f"  _loaded_at DateTime64(3) DEFAULT now64(3))\n"
        f"ENGINE = ReplacingMergeTree(_loaded_at)\n"
        f"PARTITION BY toYYYYMMDD(processing_date)\n"
        f"ORDER BY ({', '.join(order)});\n"
    )


def load_sql(cfg, day):
    """Idempotent reload of ONE table for ONE processing date.
    Returns [drop partition, insert]."""
    t, keys, cols = cfg["table"], cfg["business_key"], cfg["columns"]
    ver = cfg.get("version_column")
    part = day.replace("-", "")

    inner_cols = ", ".join(f"{c['expr']} AS {c['name']}" for c in cols)
    inner = (f"SELECT {inner_cols}, b.organisation_id AS organisation_id, b.processing_date AS processing_date "
             f"FROM bronze.{cfg['bronze_table']} AS b WHERE b.processing_date = toDate('{day}')")

    sel = []
    for c in cols:
        n = c["name"]
        if n in keys:
            sel.append(f"s.{n} AS {n}")
        elif ver:
            sel.append(f"argMax(s.{n}, s.{ver}) AS {n}")      # latest version wins
        else:
            sel.append(f"any(s.{n}) AS {n}")                  # resent rows are exact copies
    group = ["s.organisation_id", "s.processing_date"] + [f"s.{k}" for k in keys]

    where = ""
    if cfg["grain"] == "event":
        # ReplacingMergeTree only dedups inside one partition, so cross-day
        # dedup (resent transactions) is done here, tenant-aware.
        k = ", ".join(keys)
        where = (f" WHERE (s.organisation_id, {', '.join('s.' + x for x in keys)}) NOT IN "
                 f"(SELECT organisation_id, {k} FROM silver.{t})")

    names = ", ".join([c["name"] for c in cols] + ["organisation_id", "processing_date"])
    insert = (f"INSERT INTO silver.{t} ({names})\n"
              f"SELECT {', '.join(sel)}, s.organisation_id AS organisation_id, s.processing_date AS processing_date\n"
              f"FROM ({inner}) AS s{where}\nGROUP BY {', '.join(group)};\n")
    return [f"ALTER TABLE silver.{t} DROP PARTITION {part};\n", insert]


# --------------------------------------------------------------------------- #
# views
# --------------------------------------------------------------------------- #
def current_view(cfg):
    t, keys = cfg["table"], cfg["business_key"]
    return (f"CREATE OR REPLACE VIEW silver.{t}_current AS\n"
            f"SELECT * EXCEPT (_loaded_at) FROM silver.{t} FINAL\n"
            f"ORDER BY processing_date DESC LIMIT 1 BY organisation_id, {', '.join(keys)};\n")


def history_view(cfg):
    """SCD2-style versions. A new version starts whenever a tracked column
    changes between consecutive processing dates."""
    t, keys, tracked = cfg["table"], cfg["business_key"], cfg["history"]["tracked"]
    names = [c["name"] for c in cfg["columns"]]
    part = "PARTITION BY organisation_id, " + ", ".join(keys)
    w_full = f"({part} ORDER BY processing_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"
    w_cum = f"({part} ORDER BY processing_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
    hash_expr = f"cityHash64({', '.join(tracked)})"
    agg = [f"f.{k} AS {k}" for k in keys] + [
        f"argMin(f.{n}, f.processing_date) AS {n}" for n in names if n not in keys]
    k_list = ", ".join(f"f.{k}" for k in keys)
    return (
        f"CREATE OR REPLACE VIEW silver.{t}_history AS\n"
        f"WITH flagged AS (\n"
        f"  SELECT *, sum(_chg) OVER {w_cum} AS _ver FROM (\n"
        f"    SELECT *, if(_h != lagInFrame(_h, 1, toUInt64(0)) OVER {w_full}, 1, 0) AS _chg FROM (\n"
        f"      SELECT * EXCEPT (_loaded_at), {hash_expr} AS _h FROM silver.{t} FINAL))),\n"
        f"versions AS (\n"
        f"  SELECT f.organisation_id AS organisation_id, {', '.join(agg)},\n"
        f"         min(f.processing_date) AS valid_from\n"
        f"  FROM flagged AS f GROUP BY f.organisation_id, {k_list}, f._ver)\n"
        f"SELECT *, isNull(valid_to) AS is_current FROM (\n"
        f"  SELECT *, nullIf(leadInFrame(valid_from, 1, toDate('1970-01-01')) OVER\n"
        f"    (PARTITION BY organisation_id, {', '.join(keys)} ORDER BY valid_from\n"
        f"     ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), toDate('1970-01-01')) AS valid_to\n"
        f"  FROM versions);\n"
    )


def join_view(j):
    for step in j["joins"]:
        if len(re.findall(r"organisation_id", step["condition"])) < 2:
            raise ValueError(
                f"join '{j['name']}' -> {step['table']}: ON clause must compare organisation_id "
                f"on both sides (cross-tenant join refused): {step['condition']}")
    base = j["base"]
    joins = " ".join(f"{s.get('type', 'LEFT')} JOIN silver.{s['table']} AS {s['alias']} ON {s['condition']}"
                     for s in j["joins"])
    return (f"CREATE OR REPLACE VIEW silver.{j['name']} AS\nSELECT {', '.join(j['columns'])}\n"
            f"FROM silver.{base['table']} AS {base['alias']} {joins};\n")


# --------------------------------------------------------------------------- #
def load_cfgs(d):
    return [yaml.safe_load(p.read_text()) for p in sorted(Path(d).glob("*.yml")) if not p.name.startswith("_")]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config-dir", default=str(ROOT / "configs" / "silver"))
    ap.add_argument("--out", default=str(ROOT / "sql" / "silver.sql"))
    ap.add_argument("--dates", default="all", help="'all' (every date found in Bronze) or comma list")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--views-only", action="store_true", help="only (re)create the _current/_history/join views; use after Airflow has loaded the tables")
    ap.add_argument("--clickhouse", default=str(ROOT / "clickhouse"))
    a = ap.parse_args()

    cfgs = load_cfgs(a.config_dir)
    if a.dates == "all":
        dates = [r["d"] for r in ch.query(a.clickhouse, "SELECT DISTINCT toString(processing_date) AS d FROM bronze.accounts ORDER BY d")]
    else:
        dates = a.dates.split(",")

    sql = ["CREATE DATABASE IF NOT EXISTS silver;\n"]
    if not a.views_only:
        sql += [ddl(c) for c in cfgs]
        for d in dates:
            for c in cfgs:
                sql += load_sql(c, d)
    for c in cfgs:
        if c["grain"] == "snapshot":
            sql.append(current_view(c))
        if c.get("history", {}).get("enabled"):
            sql.append(history_view(c))
    for j in yaml.safe_load((Path(a.config_dir) / "_joins.yml").read_text()):
        sql.append(join_view(j))

    out = Path(a.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(sql))
    print(f"rendered Silver for {len(cfgs)} tables x {len(dates)} dates -> {out}")
    if a.apply:
        ch.run_file(a.clickhouse, out)
        print("applied to", ch.CHDATA)


if __name__ == "__main__":
    main()
