#!/usr/bin/env python3
"""
Generate a domain_kpis.json file by grouping KPIs from a CSV catalog.

The CSV must include columns: Section, Section_Description, Code, Code_Description.
Each unique Section becomes a domain entry with its KPIs.
"""

import argparse
import csv
import json
from collections import OrderedDict
from pathlib import Path
from typing import Dict, List


def slugify(text: str) -> str:
    slug = (
        text.strip()
        .lower()
        .replace("%", "pct")
        .replace("<", "lt")
        .replace(">", "gt")
        .replace("+", "plus")
        .replace("(", "")
        .replace(")", "")
        .replace(".", "")
        .replace(",", "")
        .replace("/", "_")
        .replace(" ", "_")
        .replace("-", "_")
    )
    while "__" in slug:
        slug = slug.replace("__", "_")
    return slug.strip("_")


def load_csv(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        required = {"Section", "Section_Description", "Code", "Code_Description"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"CSV missing required columns: {missing}")
        return list(reader)


def build_domain_map(rows: List[Dict[str, str]]) -> Dict[str, Dict]:
    domains: Dict[str, Dict] = OrderedDict()

    for row in rows:
        section = row["Section"].strip()
        section_desc = row["Section_Description"].strip()
        code = row["Code"].strip()
        code_desc = row["Code_Description"].strip()

        if not section or not code:
            continue

        if section not in domains:
            domains[section] = {
                "section": section,
                "description": section_desc,
                "kpis": [],
            }

        alias = slugify(code_desc or code)

        if any(k["code"] == code for k in domains[section]["kpis"]):
            continue

        domains[section]["kpis"].append(
            {
                "code": code,
                "name": code_desc,
                "alias": alias,
            }
        )

    return domains


def main():
    parser = argparse.ArgumentParser(
        description="Build domain_kpis.json from a Section-based KPI CSV."
    )
    parser.add_argument("csv_path", type=Path, help="Input CSV path")
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=Path("data/domain_kpis.json"),
        help="Output JSON file (default: data/domain_kpis.json)",
    )
    args = parser.parse_args()

    rows = load_csv(args.csv_path)
    domain_map = build_domain_map(rows)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8") as fh:
        json.dump(domain_map, fh, indent=2, ensure_ascii=False)

    print(f"Wrote {len(domain_map)} domains to {args.output}")


if __name__ == "__main__":
    main()
