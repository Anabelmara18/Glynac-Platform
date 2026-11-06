#!/usr/bin/env python3
"""Prove the Gold layer is correct and tenant-safe. Exits non-zero on failure.

  1. GUARD       static rules pass on every config
  2. MUTANTS     the guard flags configs with organisation_id removed from the
                 GROUP BY or from a JOIN (it would fail if grouping were removed)
  3. RECONCILE   every day, every aggregate, per organisation, equals an
                 independent recompute from Silver
  4. CROSS-CHECK distinct-account state (AggregatingMergeTree) agrees with the
                 plain count in aum_daily; estimated fees match a float recompute
  5. LEAK        a join WITHOUT organisation_id overstates AUM (negative control)
  6. IDEMPOTENT  reloading a day twice leaves every total unchanged
  7. TENANCY     no empty organisation_id in any Gold table
"""
import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "bronze"))
sys.path.insert(0, str(ROOT / "gold"))
import build_gold  # noqa: E402
import ch  # noqa: E402
import guard  # noqa: E402

fails = 0


def check(label, ok, detail=""):
    global fails
    fails += not ok
    print(f"  {'PASS' if ok else 'FAIL'}  {label}{('  ' + detail) if detail else ''}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--clickhouse", default=str(ROOT / "clickhouse"))
    ap.add_argument("--config-dir", default=str(ROOT / "configs" / "gold"))
    a = ap.parse_args()
    q = lambda sql: ch.query(a.clickhouse, sql)  # noqa: E731
    one = lambda sql: float(list(q(sql)[0].values())[0])  # noqa: E731
    cfgs = build_gold.load_cfgs(a.config_dir)
    dates = [r["d"] for r in q("SELECT DISTINCT toString(processing_date) AS d FROM gold.aum_daily ORDER BY d")]
    last = dates[-1]

    print("1. GUARD (static rules R1-R6)")
    for c in cfgs:
        p = guard.check_config(c)
        check(f"{c['name']:<26} engine={c['engine']}", not p, "; ".join(p))

    print("2. MUTANTS (guard must catch each broken copy)")
    for c in cfgs:
        check(f"{c['name']:<26} GROUP BY organisation_id removed -> flagged", bool(guard.check_config(guard.drop_group_org(c))))
        m = guard.drop_join_org(c)
        if m:
            check(f"{c['name']:<26} JOIN organisation_id removed    -> flagged", bool(guard.check_config(m)))

    print(f"3. RECONCILE per organisation, {len(dates)} days x {len(cfgs)} aggregates")
    for c in cfgs:
        bad = [d for d in dates if one(c["reconcile"].replace("{day}", d)) != 0]
        check(f"{c['name']:<26}", not bad, f"days_with_mismatch={bad}" if bad else f"{len(dates)} days match Silver")

    print("4. CROSS-CHECKS")
    r = one(f"""SELECT count() FROM (
        SELECT b.organisation_id FROM (SELECT organisation_id, sum(accounts) AS n FROM gold.advisor_book_daily_v
                                       WHERE processing_date = toDate('{last}') GROUP BY organisation_id) AS b
        INNER JOIN (SELECT organisation_id, accounts_with_holdings AS n FROM gold.aum_daily FINAL
                    WHERE processing_date = toDate('{last}')) AS g ON g.organisation_id = b.organisation_id
        WHERE b.n != g.n)""")
    check("advisor distinct-account states sum to aum_daily account counts", r == 0, f"mismatched_orgs={int(r)}")
    r = one(f"""SELECT count() FROM (
        SELECT b.organisation_id FROM (SELECT organisation_id, sum(est_annual_fees) AS fees FROM gold.advisor_book_daily_v
                                       WHERE processing_date = toDate('{last}') GROUP BY organisation_id) AS b
        INNER JOIN (SELECT hh.organisation_id AS organisation_id, sum(toFloat64(hh.market_value) * ff.rate_bps / 10000) AS fees
                    FROM (SELECT * FROM silver.holdings FINAL WHERE processing_date = toDate('{last}')) AS hh
                    INNER JOIN (SELECT organisation_id, portfolio_id, account_id FROM silver.portfolios FINAL
                                WHERE processing_date = toDate('{last}')) AS pp
                      ON pp.organisation_id = hh.organisation_id AND pp.portfolio_id = hh.portfolio_id
                    INNER JOIN (SELECT organisation_id, account_id, rate_bps FROM silver.fee_schedules FINAL
                                WHERE processing_date = toDate('{last}')) AS ff
                      ON ff.organisation_id = pp.organisation_id AND ff.account_id = pp.account_id
                    GROUP BY hh.organisation_id) AS s ON s.organisation_id = b.organisation_id
        WHERE abs(toFloat64(b.fees) - s.fees) > 1)""")
    check("estimated annual fees match an independent float recompute", r == 0, f"mismatched_orgs={int(r)}")

    print("5. LEAK negative control")
    correct = one(f"SELECT sum(total_aum) FROM gold.aum_daily FINAL WHERE processing_date = toDate('{last}')")
    leaky = one(f"""SELECT sum(h.market_value) FROM (SELECT * FROM silver.holdings FINAL WHERE processing_date = toDate('{last}')) AS h
                    INNER JOIN (SELECT portfolio_id, account_id FROM silver.portfolios FINAL WHERE processing_date = toDate('{last}')) AS p
                      ON p.portfolio_id = h.portfolio_id""")
    check("join WITHOUT organisation_id overstates AUM (the leak the guard prevents)", leaky > correct,
          f"leaky={leaky:,.2f} vs correct={correct:,.2f} (+{leaky - correct:,.2f})")

    print(f"6. IDEMPOTENT reload of {last}")
    totals = lambda: (one(f"SELECT sum(total_aum) FROM gold.aum_daily FINAL WHERE processing_date = toDate('{last}')"),  # noqa: E731
                      one(f"SELECT sum(txn_count) FROM gold.transaction_volume_daily WHERE processing_date = toDate('{last}')"),
                      one(f"SELECT sum(total_amount) FROM gold.transaction_volume_daily WHERE processing_date = toDate('{last}')"),
                      one(f"SELECT sum(aum) FROM gold.advisor_book_daily_v WHERE processing_date = toDate('{last}')"))
    before = totals()
    for _ in range(2):
        for c in cfgs:
            for stmt in build_gold.load_sql(c, last):
                ch.execute(a.clickhouse, stmt)
    after = totals()
    check("totals unchanged after reloading the day twice", before == after, f"before={before} after={after}")

    print("7. TENANCY")
    empty = sum(one(f"SELECT countIf(organisation_id = '') FROM gold.{c['name']}") for c in cfgs)
    check("no empty organisation_id in any Gold table", empty == 0, f"empty={int(empty)}")

    print("\nGOLD OK" if not fails else f"\n{fails} CHECK(S) FAILED")
    raise SystemExit(1 if fails else 0)


if __name__ == "__main__":
    main()
