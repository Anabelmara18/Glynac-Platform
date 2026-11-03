#!/usr/bin/env python3
"""Prove the Silver layer is correct. Exits non-zero on any failure.

  1. DEDUP      one row per (org, key, day); matches Bronze's distinct count;
                the surviving row is the LATEST version (stale copies lose)
  2. EVENTS     resent transactions/trades appear exactly once
  3. HISTORY    every change recorded by the generator produced a new version;
                every key has exactly one current version
  4. COLLISION  seeded cross-org keys stay distinct; a join WITHOUT
                organisation_id would fan out, the org-aware join does not
  5. CLEANING   normalisation rules held (lower-case emails, upper-case currency)
  6. TENANCY    no empty organisation_id; a join missing organisation_id is refused
"""
import argparse
import json
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "bronze"))
sys.path.insert(0, str(ROOT / "silver"))
import ch  # noqa: E402
import build_silver  # noqa: E402

fails = 0


def check(label, ok, detail=""):
    global fails
    fails += not ok
    print(f"  {'PASS' if ok else 'FAIL'}  {label}{('  ' + detail) if detail else ''}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--clickhouse", default=str(ROOT / "clickhouse"))
    ap.add_argument("--manifest", default=str(ROOT / "generator" / "out" / "_manifest.json"))
    ap.add_argument("--config-dir", default=str(ROOT / "configs" / "silver"))
    a = ap.parse_args()
    q = lambda sql: ch.query(a.clickhouse, sql)  # noqa: E731
    one = lambda sql: int(list(q(sql)[0].values())[0])  # noqa: E731
    manifest = json.loads(Path(a.manifest).read_text())
    cfgs = build_silver.load_cfgs(a.config_dir)

    print("1/2. DEDUP and EVENT grain")
    for c in cfgs:
        t, keys, ver = c["table"], c["business_key"], c.get("version_column")
        k = ", ".join(keys)
        if c["grain"] == "snapshot":
            grp = f"organisation_id, processing_date, {k}"
            exp = one(f"SELECT count() FROM (SELECT 1 FROM bronze.{t} GROUP BY {grp})")
        else:
            grp = f"organisation_id, {k}"
            exp = one(f"SELECT count() FROM (SELECT 1 FROM bronze.{t} GROUP BY {grp})")
        got = one(f"SELECT count() FROM silver.{t} FINAL")
        dup = one(f"SELECT count() FROM (SELECT 1 FROM silver.{t} FINAL GROUP BY {grp} HAVING count() > 1)")
        detail = f"rows={got} expected={exp} dup_groups={dup}"
        if ver:  # surviving row must carry the newest version Bronze had for that key/day
            join_on = " AND ".join(f"s.{x} = b.{x}" for x in ["organisation_id", "processing_date"] + keys)
            stale = one(f"""SELECT count() FROM silver.{t} AS s FINAL INNER JOIN
                (SELECT {grp}, max({ver}) AS mv FROM bronze.{t} GROUP BY {grp}) AS b ON {join_on}
                WHERE s.{ver} != b.mv""")
            detail += f" stale_winners={stale}"
            check(f"{t:<14}", got == exp and dup == 0 and stale == 0, detail)
        else:
            check(f"{t:<14}", got == exp and dup == 0, detail)
    resent = [r for r in manifest["resent_rows"] if r["table"] in ("transactions", "trades")]
    bad = 0
    for r in resent:
        key = "transaction_id" if r["table"] == "transactions" else "trade_id"
        n = one(f"SELECT count() FROM silver.{r['table']} FINAL WHERE organisation_id='{r['org']}' AND {key}='{r['key']}'")
        bad += n != 1
    check(f"{len(resent)} resent transactions/trades each stored exactly once", bad == 0, f"violations={bad}")

    print("3. HISTORY")
    for c in cfgs:
        if not c.get("history", {}).get("enabled"):
            continue
        t, keys = c["table"], c["business_key"]
        k = ", ".join(keys)
        multi = one(f"SELECT count() FROM (SELECT 1 FROM silver.{t}_history GROUP BY organisation_id, {k} HAVING countIf(is_current) != 1)")
        changes = [(x["org"], x["key"]) for x in manifest["changes"] if x["table"] == t]
        short = 0
        if changes:
            tuples = ", ".join(f"('{o}','{key}')" for o, key in sorted(set(changes)))
            short = one(f"""SELECT count() FROM (SELECT 1 FROM silver.{t}_history
                WHERE (organisation_id, {k}) IN ({tuples}) GROUP BY organisation_id, {k} HAVING count() < 2)""")
        check(f"{t:<14}", multi == 0 and short == 0,
              f"keys_without_exactly_one_current={multi} changed_keys={len(set(changes))} missing_new_version={short}")

    print("4. COLLISION")
    keycol = {"contacts": "contact_id", "accounts": "account_id", "portfolios": "portfolio_id", "holdings": "holding_id"}
    for t, key in manifest["seeded_collision"]["keys"].items():
        n = one(f"SELECT uniqExact(organisation_id) FROM silver.{t}_current WHERE {keycol[t]} = '{key}'")
        check(f"{t}_current  {key} kept under {n} orgs", n == 2)
    r = q("SELECT uniqExact(account_type) AS types, uniqExact(base_currency) AS ccy FROM silver.accounts_current WHERE account_id = 'ACC-99001'")[0]
    check("ACC-99001 data stayed distinct per org", int(r["types"]) == 2 and int(r["ccy"]) == 2, f"account_types={r['types']} currencies={r['ccy']}")
    safe = one("SELECT count() FROM silver.holding_detail WHERE holding_id = 'HLD-99001'")
    naive = one("""SELECT count() FROM silver.holdings_current AS h
                   LEFT JOIN silver.portfolios_current AS p ON p.portfolio_id = h.portfolio_id
                   WHERE h.holding_id = 'HLD-99001'""")
    names = one("SELECT uniqExact(client_name) FROM silver.holding_detail WHERE holding_id = 'HLD-99001'")
    check("org-aware join returns 2 rows with 2 different clients", safe == 2 and names == 2, f"rows={safe} clients={names}")
    check("naive join (no organisation_id) would fan out", naive > safe, f"naive_rows={naive} vs safe_rows={safe}  <- the leak this prevents")

    print("5. CLEANING")
    check("emails lower-case", one("SELECT countIf(email != lower(email)) FROM silver.contacts FINAL") == 0)
    check("currencies upper-case", one("SELECT countIf(base_currency != upper(base_currency)) FROM silver.accounts FINAL") == 0)
    check("statuses lower-case", one("SELECT countIf(status != lower(status)) FROM silver.accounts FINAL") == 0)

    print("6. TENANCY")
    empty = sum(one(f"SELECT countIf(organisation_id = '') FROM silver.{c['table']} FINAL") for c in cfgs)
    check("no empty organisation_id in any Silver table", empty == 0, f"empty={empty}")
    try:
        build_silver.join_view({"name": "bad", "base": {"table": "x", "alias": "x"},
                                "joins": [{"table": "y", "alias": "y", "condition": "y.id = x.id"}], "columns": ["x.id"]})
        check("join without organisation_id is refused", False)
    except ValueError:
        check("join without organisation_id is refused", True)

    print("\nSILVER OK" if not fails else f"\n{fails} CHECK(S) FAILED")
    raise SystemExit(1 if fails else 0)


if __name__ == "__main__":
    main()
