"""Tenant-safety guard for Gold SQL. Pure static analysis - needs no database.

Rules enforced on every Gold config:
  R1  the top-level SELECT must GROUP BY organisation_id
  R2  any nested SELECT that aggregates or groups must also GROUP BY organisation_id
  R3  every JOIN condition must compare organisation_id on both sides
  R4  the top-level SELECT must output an organisation_id column
  R5  the table's ORDER BY must start with organisation_id
  R6  organisation_id must be a declared column

`drop_group_org` and `drop_join_org` produce deliberately broken copies of a
config (the "mutants"). The tests assert the guard flags every mutant, which
proves the guard would fail if someone removed the grouping.
"""
import copy

import sqlglot
from sqlglot import exp

ORG = "organisation_id"


def _parse(sql):
    return sqlglot.parse_one(sql.replace("{day}", "2000-01-01"), read="clickhouse")


def _is_org(e):
    return isinstance(e, exp.Column) and e.name == ORG


def _group_has_org(sel):
    g = sel.args.get("group")
    return bool(g) and any(_is_org(e) for e in g.expressions)


def _aggregates(sel):
    return any(e.find(exp.AggFunc) for e in sel.expressions)


def check_sql(sql, label):
    problems = []
    tree = _parse(sql)
    top = tree if isinstance(tree, exp.Select) else tree.find(exp.Select)
    if not _group_has_org(top):
        problems.append(f"{label}: top-level SELECT does not GROUP BY {ORG} (R1)")
    for sel in tree.find_all(exp.Select):
        if sel is top:
            continue
        if (sel.args.get("group") or _aggregates(sel)) and not _group_has_org(sel):
            problems.append(f"{label}: a nested SELECT aggregates without grouping by {ORG} (R2)")
    for j in tree.find_all(exp.Join):
        on = j.args.get("on")
        n = sum(1 for c in on.find_all(exp.Column) if c.name == ORG) if on is not None else 0
        if n < 2:
            problems.append(f"{label}: JOIN condition does not compare {ORG} on both sides (R3)")
    if ORG not in {e.alias_or_name for e in top.expressions}:
        problems.append(f"{label}: SELECT does not output {ORG} (R4)")
    return problems


def check_config(cfg):
    name = cfg["name"]
    problems = check_sql(cfg["select"], f"{name}.select")
    if cfg.get("view"):
        problems += check_sql(cfg["view"], f"{name}.view")
    if not cfg["order_by"] or cfg["order_by"][0] != ORG:
        problems.append(f"{name}: ORDER BY must start with {ORG} (R5)")
    if ORG not in [c["name"] for c in cfg["columns"]]:
        problems.append(f"{name}: {ORG} is not a declared column (R6)")
    return problems


# --------------------------------------------------------------------------- #
# mutants (deliberately broken copies)
# --------------------------------------------------------------------------- #
def drop_group_org(cfg):
    new = copy.deepcopy(cfg)
    tree = _parse(cfg["select"])
    top = tree if isinstance(tree, exp.Select) else tree.find(exp.Select)
    g = top.args["group"]
    kept = [e for e in g.expressions if not _is_org(e)]
    top.set("group", exp.Group(expressions=kept) if kept else None)
    new["select"] = tree.sql(dialect="clickhouse")
    return new


def drop_join_org(cfg):
    """Remove the organisation_id comparison from every JOIN. None if no joins."""
    tree = _parse(cfg["select"])
    joins = list(tree.find_all(exp.Join))
    if not joins:
        return None
    for j in joins:
        on = j.args["on"]
        conjuncts = [c for c in on.flatten() if not any(col.name == ORG for col in c.find_all(exp.Column))]
        j.set("on", exp.and_(*conjuncts) if conjuncts else exp.true())
    new = copy.deepcopy(cfg)
    new["select"] = tree.sql(dialect="clickhouse")
    return new
