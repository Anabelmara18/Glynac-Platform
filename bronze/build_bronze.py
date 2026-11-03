#!/usr/bin/env python3
"""Render Bronze DDL from configs/bronze/*.yml and (optionally) apply it.

Two modes:
  local  views over file(...) on disk       -> `clickhouse local`, no Docker
  s3     S3-engine tables + views over MinIO -> real ClickHouse server

In both modes the view exposes the raw columns PLUS:
  organisation_id  taken from the folder path (3rd segment from the end)
  processing_date  taken from the folder path (2nd segment from the end)
  _source_file     the full file path, for lineage/debugging

Examples:
  python bronze/build_bronze.py --mode local --apply --clickhouse ./clickhouse
  python bronze/build_bronze.py --mode s3 --out sql/bronze_s3.sql
"""
import argparse
from pathlib import Path

import yaml

import ch

ROOT = Path(__file__).resolve().parent.parent


def partition_exprs(cfg):
    exprs = []
    for p in cfg["partitions"]:
        seg = f"splitByChar('/', _path)[-{p['path_segment_from_end']}]"
        if p["type"] == "Date":
            seg = f"toDateOrZero({seg})"   # bad folder name -> 1970-01-01, caught by verify
        exprs.append(f"{seg} AS {p['name']}")
    return exprs


def render(cfg, mode, data_root, endpoint, bucket, access, secret):
    t = cfg["table"]
    cols = [c["name"] for c in cfg["columns"]]
    structure = ", ".join(f"{c['name']} {c['type']}" for c in cfg["columns"])
    select = ",\n  ".join(cols + partition_exprs(cfg) + ["_path AS _source_file"])
    if mode == "local":
        src = f"file('{data_root}/{cfg['path_glob']}', '{cfg['format']}', '{structure}')"
        return f"CREATE OR REPLACE VIEW bronze.{t} AS\nSELECT\n  {select}\nFROM {src};\n"
    raw = (f"CREATE TABLE IF NOT EXISTS bronze.raw_{t} ({structure})\n"
           f"ENGINE = S3('{endpoint}/{bucket}/{cfg['path_glob']}', '{access}', '{secret}', '{cfg['format']}');\n")
    view = f"CREATE OR REPLACE VIEW bronze.{t} AS\nSELECT\n  {select}\nFROM bronze.raw_{t};\n"
    return raw + view


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--mode", choices=["local", "s3"], default="local")
    ap.add_argument("--config-dir", default=str(ROOT / "configs" / "bronze"))
    ap.add_argument("--data-root", default=str(ROOT / "generator" / "out" / "bronze"))
    ap.add_argument("--endpoint", default="http://minio:9000")
    ap.add_argument("--bucket", default="bronze")
    ap.add_argument("--access-key", default="minioadmin")
    ap.add_argument("--secret-key", default="minioadmin")
    ap.add_argument("--out", default=str(ROOT / "sql" / "bronze.sql"))
    ap.add_argument("--apply", action="store_true", help="local mode: run the SQL with clickhouse local")
    ap.add_argument("--clickhouse", default=str(ROOT / "clickhouse"), help="path to the clickhouse binary")
    a = ap.parse_args()

    cfgs = [yaml.safe_load(p.read_text()) for p in sorted(Path(a.config_dir).glob("*.yml"))]
    sql = "CREATE DATABASE IF NOT EXISTS bronze;\n\n" + "\n".join(
        render(c, a.mode, a.data_root, a.endpoint, a.bucket, a.access_key, a.secret_key) for c in cfgs)
    out = Path(a.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(sql)
    print(f"rendered {len(cfgs)} Bronze tables ({a.mode}) -> {out}")
    if a.apply:
        if a.mode != "local":
            raise SystemExit("--apply only supports local mode; load the s3 SQL into your ClickHouse server")
        ch.run_file(a.clickhouse, out)
        print("applied to", ch.CHDATA)


if __name__ == "__main__":
    main()
