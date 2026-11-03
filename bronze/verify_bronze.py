#!/usr/bin/env python3
"""Prove the Bronze layer is correct. Exits non-zero on any failure.

Checks, for every Bronze table:
  1. organisation_id is never empty            (partition column populated)
  2. processing_date is never the 1970 default (partition column populated)
  3. distinct orgs / dates match the generator manifest
  4. row count equals what the generator wrote
and, for the seeded collision keys, that each exists under exactly 2 orgs.
"""
import argparse
import json
from pathlib import Path

import yaml

import ch

ROOT = Path(__file__).resolve().parent.parent


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--clickhouse", default=str(ROOT / "clickhouse"))
    ap.add_argument("--manifest", default=str(ROOT / "generator" / "out" / "_manifest.json"))
    ap.add_argument("--config-dir", default=str(ROOT / "configs" / "bronze"))
    a = ap.parse_args()

    manifest = json.loads(Path(a.manifest).read_text())
    exp_orgs, exp_dates = len(manifest["orgs"]), len(manifest["dates"])
    failures = 0
    print(f"{'table':<15}{'rows':>7}{'expect':>8}{'empty_org':>10}{'empty_date':>11}{'orgs':>6}{'dates':>6}  result")
    for p in sorted(Path(a.config_dir).glob("*.yml")):
        t = yaml.safe_load(p.read_text())["table"]
        r = ch.query(a.clickhouse, f"""
            SELECT count() AS n,
                   countIf(organisation_id = '') AS empty_org,
                   countIf(processing_date = toDate('1970-01-01')) AS empty_date,
                   uniqExact(organisation_id) AS orgs,
                   uniqExact(processing_date) AS dates
            FROM bronze.{t}""")[0]
        expected = sum(sum(days.values()) for days in manifest["row_counts"][t].values())
        ok = (int(r["empty_org"]) == 0 and int(r["empty_date"]) == 0 and int(r["orgs"]) == exp_orgs
              and int(r["dates"]) == exp_dates and int(r["n"]) == expected)
        failures += not ok
        print(f"{t:<15}{r['n']:>7}{expected:>8}{r['empty_org']:>10}{r['empty_date']:>11}{r['orgs']:>6}{r['dates']:>6}  {'PASS' if ok else 'FAIL'}")

    print("\nSeeded collision keys (must appear under exactly 2 organisations):")
    keycol = {"contacts": "contact_id", "accounts": "account_id", "portfolios": "portfolio_id", "holdings": "holding_id"}
    for t, key in manifest["seeded_collision"]["keys"].items():
        r = ch.query(a.clickhouse, f"SELECT uniqExact(organisation_id) AS orgs FROM bronze.{t} WHERE {keycol[t]} = '{key}'")[0]
        ok = int(r["orgs"]) == 2
        failures += not ok
        print(f"  {t:<12}{key:<12} orgs={r['orgs']}  {'PASS' if ok else 'FAIL'}")

    print("\nBRONZE OK" if not failures else f"\n{failures} CHECK(S) FAILED")
    raise SystemExit(1 if failures else 0)


if __name__ == "__main__":
    main()
