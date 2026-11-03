#!/usr/bin/env python3
"""Write configs/silver/<table>.yml (+ _joins.yml) from the Bronze configs.

Silver rules applied per column (all values are cleaned, never dropped):
  strings   trim whitespace
  email     lower + trim
  status / side / transaction_type / fee_type / billing_frequency   lower + trim
  currency / base_currency / ticker                                  upper + trim
"""
from pathlib import Path

import yaml

from schemas import TABLES

ROOT = Path(__file__).resolve().parent.parent
BRONZE = ROOT / "configs" / "bronze"
OUT = ROOT / "configs" / "silver"

LOWER = {"email", "status", "side", "transaction_type", "fee_type", "billing_frequency"}
UPPER = {"currency", "base_currency", "ticker"}
VERSION = {"statements": "generated_at"}          # default for snapshots is updated_at
EVENT_TABLES = {"transactions", "trades"}          # append-only facts, dedup across days
HISTORY = {                                        # SCD2-style change history
    "accounts": ["status", "advisor_id", "account_type"],
    "contacts": ["email", "phone", "city", "risk_profile"],
    "fee_schedules": ["fee_type", "rate_bps", "billing_frequency"],
}

JOINS = """\
# Parent-child joins. Every ON clause MUST compare organisation_id; the builder
# refuses to render a join that does not (tenant-safety guard).
- name: holding_detail
  base: {table: holdings_current, alias: h}
  joins:
    - {table: portfolios_current, alias: p, condition: "p.organisation_id = h.organisation_id AND p.portfolio_id = h.portfolio_id"}
    - {table: accounts_current,   alias: a, condition: "a.organisation_id = p.organisation_id AND a.account_id = p.account_id"}
    - {table: contacts_current,   alias: c, condition: "c.organisation_id = a.organisation_id AND c.contact_id = a.contact_id"}
    - {table: securities_current, alias: s, condition: "s.organisation_id = h.organisation_id AND s.security_id = h.security_id"}
  columns:
    - h.organisation_id AS organisation_id
    - h.holding_id AS holding_id
    - h.quantity AS quantity
    - h.market_value AS market_value
    - p.portfolio_id AS portfolio_id
    - p.name AS portfolio_name
    - p.strategy AS strategy
    - a.account_id AS account_id
    - a.account_type AS account_type
    - a.status AS account_status
    - c.contact_id AS contact_id
    - concat(c.first_name, ' ', c.last_name) AS client_name
    - s.ticker AS ticker
    - s.asset_class AS asset_class
"""


def expr(name, ctype):
    if ctype != "String":
        return f"b.{name}"
    if name == "email" or name in LOWER:
        return f"lower(trim(b.{name}))"
    if name in UPPER:
        return f"upper(trim(b.{name}))"
    return f"trim(b.{name})"


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    for name, meta in TABLES.items():
        b = yaml.safe_load((BRONZE / f"{name}.yml").read_text())
        cfg = {
            "table": name,
            "bronze_table": name,
            "grain": "event" if name in EVENT_TABLES else "snapshot",
            "business_key": meta["business_key"],
        }
        if name not in EVENT_TABLES:
            cfg["version_column"] = VERSION.get(name, "updated_at")
        if name in HISTORY:
            cfg["history"] = {"enabled": True, "tracked": HISTORY[name]}
        cfg["columns"] = [{"name": c["name"], "type": c["type"], "expr": expr(c["name"], c["type"])}
                          for c in b["columns"]]
        (OUT / f"{name}.yml").write_text(yaml.safe_dump(cfg, sort_keys=False, width=200))
        print(f"wrote configs/silver/{name}.yml")
    (OUT / "_joins.yml").write_text(JOINS)
    print("wrote configs/silver/_joins.yml")


if __name__ == "__main__":
    main()
