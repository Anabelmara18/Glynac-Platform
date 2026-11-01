#!/usr/bin/env python3
"""Synthetic wealth-management data generator.

Writes ORC files for 10 Bronze tables x 3 organisations x N days using the
landing layout:

    bronze/<source>/<table>/<organisation_id>/<processing_date>/data.orc

Usage:
    python generate.py --target local --out ./out            # no Docker needed
    python generate.py --target minio                        # upload to MinIO

What gets seeded on purpose (all recorded in _manifest.json):
  * CROSS-ORG COLLISION: contact CON-99001, account ACC-99001, portfolio
    PF-99001 and holding HLD-99001 exist in BOTH org_8812 and org_4410 with
    different data. Silver must keep them as distinct rows.
  * STALE DUPLICATES: inside a single daily file, a few accounts/contacts/
    holdings appear twice with different updated_at. Latest must win.
  * RESENT TRANSACTIONS/TRADES: rows re-delivered the next day, exact copies.
  * CHANGE HISTORY: contact details, account status/advisor and fee rates
    change on specific days so Silver can track "current" vs historical state.
"""
import argparse
import io
import json
import random
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.orc as orc
from faker import Faker

from schemas import ORGS, TABLES

BUCKET = "bronze"
COLLISION_ID = "99001"
COLLISION_ORGS = (ORGS[0], ORGS[1])

# ticker, name, asset class, exchange, base price
SECURITIES = [
    ("AAPL", "Apple Inc.", "Equity", "NASDAQ", 190), ("MSFT", "Microsoft Corp.", "Equity", "NASDAQ", 410),
    ("GOOGL", "Alphabet Inc.", "Equity", "NASDAQ", 150), ("AMZN", "Amazon.com Inc.", "Equity", "NASDAQ", 175),
    ("NVDA", "NVIDIA Corp.", "Equity", "NASDAQ", 880), ("JPM", "JPMorgan Chase & Co.", "Equity", "NYSE", 195),
    ("JNJ", "Johnson & Johnson", "Equity", "NYSE", 160), ("XOM", "Exxon Mobil Corp.", "Equity", "NYSE", 115),
    ("PG", "Procter & Gamble Co.", "Equity", "NYSE", 160), ("V", "Visa Inc.", "Equity", "NYSE", 275),
    ("VTI", "Vanguard Total Stock Market ETF", "Equity ETF", "NYSE", 255),
    ("VOO", "Vanguard S&P 500 ETF", "Equity ETF", "NYSE", 470),
    ("SPY", "SPDR S&P 500 ETF", "Equity ETF", "NYSE", 515), ("QQQ", "Invesco QQQ Trust", "Equity ETF", "NASDAQ", 440),
    ("SCHD", "Schwab US Dividend Equity ETF", "Equity ETF", "NYSE", 78),
    ("VEA", "Vanguard FTSE Developed Markets ETF", "Equity ETF", "NYSE", 50),
    ("VWO", "Vanguard FTSE Emerging Markets ETF", "Equity ETF", "NYSE", 42),
    ("BND", "Vanguard Total Bond Market ETF", "Fixed Income ETF", "NASDAQ", 72),
    ("AGG", "iShares Core US Aggregate Bond ETF", "Fixed Income ETF", "NYSE", 98),
    ("TLT", "iShares 20+ Year Treasury Bond ETF", "Fixed Income ETF", "NASDAQ", 92),
    ("IEF", "iShares 7-10 Year Treasury Bond ETF", "Fixed Income ETF", "NASDAQ", 96),
    ("LQD", "iShares iBoxx Investment Grade Corporate Bond ETF", "Fixed Income ETF", "NYSE", 108),
    ("HYG", "iShares iBoxx High Yield Corporate Bond ETF", "Fixed Income ETF", "NYSE", 77),
    ("GLD", "SPDR Gold Shares", "Commodity ETF", "NYSE", 215),
    ("VNQ", "Vanguard Real Estate ETF", "Real Estate ETF", "NYSE", 85),
]

ACCOUNT_TYPES = ["Individual", "Joint", "IRA", "Roth IRA", "Trust", "Corporate"]
STRATEGIES = ["Growth", "Balanced", "Income", "Conservative", "Aggressive Growth"]
BENCHMARKS = ["S&P 500", "MSCI World", "Bloomberg US Agg", "60/40 Blend"]
RISK = ["Conservative", "Moderate", "Growth", "Aggressive"]
REGIONS = ["Northeast", "Southeast", "Midwest", "West", "Southwest"]
TXN_TYPES = ["deposit", "withdrawal", "dividend", "fee", "transfer_in", "transfer_out"]


def money(x):
    return Decimal(str(round(float(x), 2))).quantize(Decimal("0.01"))


def qty(x):
    return Decimal(str(round(float(x), 4))).quantize(Decimal("0.0001"))


def mk(prefix, org_idx, n):
    """IDs are unique per org by construction (org_idx baked in), except the
    deliberately seeded collision ids which use the literal 99001."""
    return f"{prefix}-{(org_idx + 1) * 100000 + n}"


# --------------------------------------------------------------------------- #
# Initial state
# --------------------------------------------------------------------------- #
def build_org(org, idx, rng, fake, scale, start):
    st = {t: [] for t in TABLES}
    t0 = datetime.combine(start - timedelta(days=60), time(2, 0))

    def past(max_days=900):
        return datetime.combine(start - timedelta(days=rng.randint(61, max_days)), time(rng.randint(0, 23), 0))

    for i in range(1, max(3, round(8 * scale)) + 1):
        fn, ln = fake.first_name(), fake.last_name()
        st["advisors"].append(dict(
            advisor_id=mk("ADV", idx, i), first_name=fn, last_name=ln,
            email=f"{fn}.{ln}@{org.replace('_', '')}-wealth.example".lower(),
            region=rng.choice(REGIONS), hire_date=past().date(), status="active", updated_at=t0))

    for i in range(1, max(10, round(60 * scale)) + 1):
        fn, ln = fake.first_name(), fake.last_name()
        st["contacts"].append(dict(
            contact_id=mk("CON", idx, i), first_name=fn, last_name=ln,
            email=f"{fn}.{ln}{rng.randint(1, 99)}@example.com".lower(), phone=fake.phone_number(),
            city=fake.city(), risk_profile=rng.choice(RISK), created_at=past(), updated_at=t0))

    adv_ids = [a["advisor_id"] for a in st["advisors"]]
    for i, c in enumerate(st["contacts"], start=1):
        st["accounts"].append(dict(
            account_id=mk("ACC", idx, i), contact_id=c["contact_id"], advisor_id=rng.choice(adv_ids),
            account_type=rng.choice(ACCOUNT_TYPES), status="active", base_currency="USD",
            opened_date=past().date(), updated_at=t0))

    for i, a in enumerate(st["accounts"], start=1):
        st["fee_schedules"].append(dict(
            fee_schedule_id=mk("FEE", idx, i), account_id=a["account_id"], fee_type="AUM",
            rate_bps=rng.choice([50, 65, 75, 85, 100]), billing_frequency=rng.choice(["monthly", "quarterly"]),
            effective_from=a["opened_date"], updated_at=t0))

    for tk, name, ac, ex, px in SECURITIES:
        st["securities"].append(dict(
            security_id=f"SEC-{tk}", ticker=tk, name=name, asset_class=ac, currency="USD",
            exchange=ex, last_price=money(px * rng.uniform(0.97, 1.03)), updated_at=t0))
    prices = {s["security_id"]: s["last_price"] for s in st["securities"]}

    pf_n = h_n = s_n = 0
    for a in st["accounts"]:
        for _ in range(rng.choice([1, 1, 2])):
            pf_n += 1
            pf = dict(portfolio_id=mk("PF", idx, pf_n), account_id=a["account_id"],
                      name=f"{rng.choice(STRATEGIES)} Portfolio", strategy=rng.choice(STRATEGIES),
                      benchmark=rng.choice(BENCHMARKS), inception_date=a["opened_date"], updated_at=t0)
            st["portfolios"].append(pf)
            for sid in rng.sample(list(prices), rng.randint(3, 6)):
                h_n += 1
                q = rng.uniform(10, 500)
                st["holdings"].append(dict(
                    holding_id=mk("HLD", idx, h_n), portfolio_id=pf["portfolio_id"], security_id=sid,
                    quantity=qty(q), cost_basis=money(q * float(prices[sid]) * rng.uniform(0.7, 1.05)),
                    market_value=money(q * float(prices[sid])), as_of_date=start - timedelta(days=1), updated_at=t0))
        s_n += 1
        st["statements"].append(dict(
            statement_id=mk("STM", idx, s_n), account_id=a["account_id"],
            period_start=date(start.year, start.month, 1) - timedelta(days=30),
            period_end=date(start.year, start.month, 1) - timedelta(days=1),
            opening_balance=money(rng.uniform(20_000, 900_000)), closing_balance=money(rng.uniform(20_000, 900_000)),
            generated_at=datetime.combine(date(start.year, start.month, 1), time(1, 0))))

    # ---- seeded cross-org collision (same business keys, different data) ----
    if org in COLLISION_ORGS:
        side = "A" if org == COLLISION_ORGS[0] else "B"
        st["contacts"].append(dict(
            contact_id=f"CON-{COLLISION_ID}", first_name=f"Org{side}", last_name="SeededCollision",
            email=f"collision.{side}@example.com".lower(), phone="555-0199", city="Collisionville",
            risk_profile="Moderate" if side == "A" else "Aggressive", created_at=t0, updated_at=t0))
        st["accounts"].append(dict(
            account_id=f"ACC-{COLLISION_ID}", contact_id=f"CON-{COLLISION_ID}", advisor_id=adv_ids[0],
            account_type="Individual" if side == "A" else "Trust", status="active",
            base_currency="USD" if side == "A" else "CAD", opened_date=date(2020, 1, 15), updated_at=t0))
        st["portfolios"].append(dict(
            portfolio_id=f"PF-{COLLISION_ID}", account_id=f"ACC-{COLLISION_ID}", name=f"Collision Portfolio {side}",
            strategy="Balanced" if side == "A" else "Aggressive Growth", benchmark="S&P 500",
            inception_date=date(2020, 1, 15), updated_at=t0))
        q = 100 if side == "A" else 7500
        st["holdings"].append(dict(
            holding_id=f"HLD-{COLLISION_ID}", portfolio_id=f"PF-{COLLISION_ID}", security_id="SEC-AAPL",
            quantity=qty(q), cost_basis=money(q * 150), market_value=money(q * float(prices["SEC-AAPL"])),
            as_of_date=start - timedelta(days=1), updated_at=t0))
    return st


# --------------------------------------------------------------------------- #
# Daily evolution
# --------------------------------------------------------------------------- #
def evolve(st, day_i, day, rng, fake):
    """Mutate state for the given day and return list of change notes."""
    ts = datetime.combine(day, time(2, 0))
    changes = []
    prices = {}
    for s in st["securities"]:
        s["last_price"] = money(float(s["last_price"]) * (1 + rng.gauss(0, 0.01)))
        s["updated_at"] = ts
        prices[s["security_id"]] = s["last_price"]

    for h in st["holdings"]:
        if rng.random() < 0.08:
            h["quantity"] = qty(float(h["quantity"]) * rng.uniform(0.9, 1.1))
        h["market_value"] = money(float(h["quantity"]) * float(prices[h["security_id"]]))
        h["as_of_date"], h["updated_at"] = day, ts

    if day_i == 2:
        for c in rng.sample(st["contacts"], 3):
            c["email"], c["phone"], c["updated_at"] = f"new.{c['email']}", fake.phone_number(), ts
            changes.append(("contacts", c["contact_id"], "email/phone changed"))
    if day_i == 3:
        pool = [a for a in st["accounts"] if not a["account_id"].endswith(COLLISION_ID)]
        for a in rng.sample(pool, max(2, len(pool) // 16)):
            a["status"], a["updated_at"] = "restricted", ts
            changes.append(("accounts", a["account_id"], "active -> restricted"))
        a = rng.choice([x for x in pool if x["status"] == "active"])
        a["status"], a["updated_at"] = "closed", ts
        changes.append(("accounts", a["account_id"], "active -> closed"))
    if day_i == 4:
        for f in rng.sample(st["fee_schedules"], 3):
            f["rate_bps"], f["updated_at"] = max(25, f["rate_bps"] - 10), ts
            changes.append(("fee_schedules", f["fee_schedule_id"], "rate reduced by 10 bps"))
    if day_i == 5:
        advs = [a["advisor_id"] for a in st["advisors"]]
        for a in rng.sample(st["accounts"], max(2, len(st["accounts"]) // 20)):
            if a["status"] == "closed" or a["account_id"].endswith(COLLISION_ID):
                continue
            if a["status"] == "restricted":
                a["status"] = "active"
                changes.append(("accounts", a["account_id"], "restricted -> active"))
            else:
                a["advisor_id"] = rng.choice(advs)
                changes.append(("accounts", a["account_id"], "advisor reassigned"))
            a["updated_at"] = ts
    return changes


def stale_copy(table, row):
    r = dict(row)
    r["updated_at"] = row["updated_at"] - timedelta(days=1)
    if table == "accounts":
        r["status"] = "pending"
    elif table == "contacts":
        r["email"] = f"old.{row['email']}"
    elif table == "holdings":
        r["quantity"] = qty(float(row["quantity"]) * 0.9)
    return r


# --------------------------------------------------------------------------- #
# Output sinks
# --------------------------------------------------------------------------- #
class LocalSink:
    def __init__(self, out):
        self.root = Path(out)

    def put(self, key, data):
        p = self.root / BUCKET / key
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)


class MinioSink:
    def __init__(self, endpoint, access, secret):
        import boto3
        self.s3 = boto3.client("s3", endpoint_url=endpoint, aws_access_key_id=access,
                               aws_secret_access_key=secret, region_name="us-east-1")
        try:
            self.s3.head_bucket(Bucket=BUCKET)
        except Exception:
            self.s3.create_bucket(Bucket=BUCKET)

    def put(self, key, data):
        self.s3.put_object(Bucket=BUCKET, Key=key, Body=data)


def to_orc_bytes(table, rows):
    tbl = pa.Table.from_pylist(rows, schema=TABLES[table]["schema"])
    buf = io.BytesIO()
    orc.write_table(tbl, buf)
    return buf.getvalue()


# --------------------------------------------------------------------------- #
def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--target", choices=["local", "minio"], default="local")
    ap.add_argument("--out", default="./out", help="local output dir (also where _manifest.json goes)")
    ap.add_argument("--endpoint", default="http://localhost:9000")
    ap.add_argument("--access-key", default="minioadmin")
    ap.add_argument("--secret-key", default="minioadmin")
    ap.add_argument("--start", default="2026-10-26", help="first processing date (YYYY-MM-DD)")
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--scale", type=float, default=1.0, help="row-count multiplier")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    Faker.seed(args.seed)
    fake = Faker("en_US")
    start = date.fromisoformat(args.start)
    days = [start + timedelta(days=i) for i in range(args.days)]
    sink = LocalSink(args.out) if args.target == "local" else MinioSink(args.endpoint, args.access_key, args.secret_key)

    manifest = {
        "seed": args.seed, "orgs": ORGS, "dates": [d.isoformat() for d in days],
        "tables": {t: {"source": m["source"], "business_key": m["business_key"]} for t, m in TABLES.items()},
        "seeded_collision": {"orgs": list(COLLISION_ORGS),
                             "keys": {"contacts": f"CON-{COLLISION_ID}", "accounts": f"ACC-{COLLISION_ID}",
                                      "portfolios": f"PF-{COLLISION_ID}", "holdings": f"HLD-{COLLISION_ID}"}},
        "stale_duplicates": [], "resent_rows": [], "changes": [], "row_counts": {},
    }

    for idx, org in enumerate(ORGS):
        rng = random.Random(f"{args.seed}-{org}")
        st = build_org(org, idx, rng, fake, args.scale, start)
        prev_txn, prev_trd = [], []
        txn_n = trd_n = 0

        for day_i, day in enumerate(days):
            for tbl, rid, note in evolve(st, day_i, day, rng, fake):
                manifest["changes"].append(dict(org=org, date=day.isoformat(), table=tbl, key=rid, change=note))
            ts = datetime.combine(day, time(2, 0))
            prices = {s["security_id"]: s["last_price"] for s in st["securities"]}

            new_txn, new_trd = [], []
            for _ in range(max(5, round(30 * args.scale))):
                txn_n += 1
                t = rng.choice(TXN_TYPES)
                amt = rng.uniform(100, 25_000) * (-1 if t in ("withdrawal", "fee", "transfer_out") else 1)
                new_txn.append(dict(
                    transaction_id=mk("TXN", idx, txn_n), account_id=rng.choice(st["accounts"])["account_id"],
                    transaction_type=t, amount=money(amt), currency="USD", trade_date=day,
                    settle_date=day + timedelta(days=2), description=f"{t.replace('_', ' ').title()} {fake.word()}",
                    created_at=ts + timedelta(minutes=rng.randint(0, 600))))
            for _ in range(max(3, round(10 * args.scale))):
                trd_n += 1
                sid = rng.choice(list(prices))
                new_trd.append(dict(
                    trade_id=mk("TRD", idx, trd_n), portfolio_id=rng.choice(st["portfolios"])["portfolio_id"],
                    security_id=sid, side=rng.choice(["buy", "sell"]), quantity=qty(rng.uniform(1, 200)),
                    price=prices[sid], executed_at=datetime.combine(day, time(rng.randint(9, 15), rng.randint(0, 59))),
                    status="settled" if rng.random() > 0.1 else "pending"))

            resent_txn, resent_trd = prev_txn[:2], prev_trd[:1]
            for r in resent_txn:
                manifest["resent_rows"].append(dict(org=org, date=day.isoformat(), table="transactions", key=r["transaction_id"]))
            for r in resent_trd:
                manifest["resent_rows"].append(dict(org=org, date=day.isoformat(), table="trades", key=r["trade_id"]))

            payload = {t: [dict(r) for r in st[t]] for t in TABLES if t not in ("transactions", "trades")}
            payload["transactions"] = new_txn + [dict(r) for r in resent_txn]
            payload["trades"] = new_trd + [dict(r) for r in resent_trd]

            for tbl in ("accounts", "contacts", "holdings"):
                pool = [r for r in payload[tbl] if not str(r[TABLES[tbl]["business_key"][0]]).endswith(COLLISION_ID)]
                for r in rng.sample(pool, 2):
                    payload[tbl].append(stale_copy(tbl, r))
                    manifest["stale_duplicates"].append(dict(
                        org=org, date=day.isoformat(), table=tbl, key=r[TABLES[tbl]["business_key"][0]]))
            for tbl, rows in payload.items():
                rng.shuffle(rows)
                key = f"{TABLES[tbl]['source']}/{tbl}/{org}/{day.isoformat()}/data.orc"
                sink.put(key, to_orc_bytes(tbl, rows))
                manifest["row_counts"].setdefault(tbl, {}).setdefault(org, {})[day.isoformat()] = len(rows)

            prev_txn, prev_trd = new_txn, new_trd
        print(f"[{org}] wrote {len(days)} days x {len(TABLES)} tables")

    mp = Path(args.out)
    mp.mkdir(parents=True, exist_ok=True)
    (mp / "_manifest.json").write_text(json.dumps(manifest, indent=2, default=str))
    print(f"manifest -> {mp / '_manifest.json'}")


if __name__ == "__main__":
    main()
