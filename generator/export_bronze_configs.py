#!/usr/bin/env python3
"""Write configs/bronze/<table>.yml for every table in schemas.py.

These are the hand-seeded Bronze configs. Later, the schema helper CLI
(cli/) must reproduce the same files from the raw ORC schema alone.
"""
from pathlib import Path

import sys

import yaml

from schemas import TABLES

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "cli"))
from schema_helper import ch_type  # noqa: E402  (single source of truth for type mapping)

OUT = Path(__file__).resolve().parent.parent / "configs" / "bronze"


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    for name, meta in TABLES.items():
        cfg = {
            "table": name,
            "source": meta["source"],
            # landing layout: <source>/<table>/<organisation_id>/<processing_date>/data.orc
            "path_glob": f"{meta['source']}/{name}/*/*/data.orc",
            "format": "ORC",
            "partitions": [
                {"name": "organisation_id", "type": "String", "path_segment_from_end": 3},
                {"name": "processing_date", "type": "Date", "path_segment_from_end": 2},
            ],
            "columns": [{"name": f.name, "type": ch_type(f.type)} for f in meta["schema"]],
        }
        (OUT / f"{name}.yml").write_text(yaml.safe_dump(cfg, sort_keys=False))
        print(f"wrote configs/bronze/{name}.yml")


if __name__ == "__main__":
    main()
