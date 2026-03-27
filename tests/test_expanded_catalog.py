"""
Tests for the expanded KPI catalog and domain configuration.

Tests:
  1. Expanded catalog loads correctly with 161 KPIs
  2. All codes are unique, all aliases are unique
  3. Domain config maps every KPI to exactly one domain
  4. Code-to-alias resolution works (the cache filtering fix)
  5. No orphan KPIs — every catalog entry belongs to a domain
  6. Parquet inspector — reads a real cached parquet and reports contents

Usage:
    conda run -n mcp python -m tests.test_expanded_catalog
    conda run -n mcp python -m tests.test_expanded_catalog --inspect GM
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

# ── helpers ──────────────────────────────────────────────────────────────────

def _ok(msg: str):
    print(f"  ✅  {msg}")

def _fail(msg: str):
    print(f"  ❌  {msg}")
    sys.exit(1)

def _check(condition: bool, ok_msg: str, fail_msg: str):
    if condition:
        _ok(ok_msg)
    else:
        _fail(fail_msg)

def _section(title: str):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


# ── test 1: catalog loads correctly ──────────────────────────────────────────

def test_catalog_loads():
    _section("Test 1 — Expanded catalog loads with correct size")

    from mcp_orchestration.dairy_kpi_client import DairyKPIClient
    client = DairyKPIClient(api_base_url="https://example.com")
    catalog = client._get_kpi_list()

    _check(len(catalog) == 161, f"Catalog has {len(catalog)} KPIs (expected 161)", f"Got {len(catalog)}")
    _check("alias" in catalog.columns, "Alias column exists", "Missing alias column")
    _check("Code" in catalog.columns, "Code column exists", "Missing Code column")
    _check("Description" in catalog.columns, "Description column exists", "Missing Description column")
    _check("Context" in catalog.columns, "Context column exists", "Missing Context column")


# ── test 2: uniqueness ──────────────────────────────────────────────────────

def test_uniqueness():
    _section("Test 2 — Codes and aliases are unique")

    with open(REPO_ROOT / "data" / "expanded_kpi_catalog.json") as f:
        catalog = json.load(f)

    codes = catalog["Code"]
    _check(len(codes) == len(set(codes)), f"All {len(codes)} codes are unique", f"Duplicate codes: {[c for c in codes if codes.count(c) > 1]}")

    from mcp_orchestration.dairy_kpi_client import DairyKPIClient
    client = DairyKPIClient(api_base_url="https://example.com")
    df = client._get_kpi_list()
    aliases = list(df["alias"])
    dupes = [a for a in aliases if aliases.count(a) > 1]
    _check(len(aliases) == len(set(aliases)), f"All {len(aliases)} aliases are unique", f"Duplicate aliases: {set(dupes)}")


# ── test 3: domain config covers all domains ────────────────────────────────

def test_domain_config():
    _section("Test 3 — Domain config maps KPIs to all 6 domains")

    from agents.domain_config import get_domain_kpi_aliases

    expected_domains = {
        "Fertility": 38,
        "Production": 31,
        "Health": 34,
        "Calf Raising": 27,
        "Culling": 22,
        "Herd Demographics": 9,
    }

    total = 0
    for domain, expected_count in expected_domains.items():
        aliases = get_domain_kpi_aliases(domain)
        _check(
            len(aliases) == expected_count,
            f"{domain}: {len(aliases)} KPIs (expected {expected_count})",
            f"{domain}: got {len(aliases)}, expected {expected_count}"
        )
        total += len(aliases)

    _check(total == 161, f"Total across domains: {total} (expected 161)", f"Got {total}")


# ── test 4: code-to-alias resolution ────────────────────────────────────────

def test_code_to_alias_resolution():
    _section("Test 4 — Code-to-alias resolution works")

    sys.path.insert(0, str(REPO_ROOT))
    from mcp_orchestration.mcp_server import _resolve_to_aliases

    # Test known mappings
    test_cases = [
        ("255d", "pct_partos_logrados"),
        ("224d", "pct_fiebre_de_leche"),
        ("79", "pico_de_prod_1a_lact"),
        ("228e", "pct_cetosis_1a_lact"),
        ("315a", "pct_desecho_baja_produccion"),
        ("92", "cuenta_std"),
    ]

    for code, expected_alias in test_cases:
        result = _resolve_to_aliases([code])
        _check(
            result[0] == expected_alias,
            f"{code} → {result[0]}",
            f"{code}: expected {expected_alias}, got {result[0]}"
        )

    # Test alias passthrough
    result = _resolve_to_aliases(["pct_cetosis"])
    _check(result[0] == "pct_cetosis", "Alias passthrough works", f"Got {result[0]}")

    # Test batch resolution
    batch = _resolve_to_aliases(["255d", "224d", "79", "pct_cetosis"])
    _check(len(batch) == 4, f"Batch resolved {len(batch)} items", f"Expected 4, got {len(batch)}")


# ── test 5: no orphan KPIs ──────────────────────────────────────────────────

def test_no_orphans():
    _section("Test 5 — Every catalog KPI belongs to a domain")

    from agents.domain_config import get_domain_kpi_aliases
    from mcp_orchestration.dairy_kpi_client import DairyKPIClient

    client = DairyKPIClient(api_base_url="https://example.com")
    catalog = client._get_kpi_list()
    all_aliases = set(catalog["alias"])

    domain_aliases = set()
    for domain in ["Fertility", "Production", "Health", "Calf Raising", "Culling", "Herd Demographics"]:
        domain_aliases.update(get_domain_kpi_aliases(domain))

    orphans = all_aliases - domain_aliases
    _check(
        len(orphans) == 0,
        f"All {len(all_aliases)} catalog KPIs are assigned to a domain",
        f"{len(orphans)} orphan KPIs: {orphans}"
    )

    extras = domain_aliases - all_aliases
    _check(
        len(extras) == 0,
        "No domain KPIs missing from catalog",
        f"{len(extras)} domain KPIs not in catalog: {extras}"
    )


# ── test 6: parquet inspector ────────────────────────────────────────────────

def inspect_parquet(farm_code: str):
    _section(f"Parquet Inspector — farm: {farm_code}")

    from agents.kpi_cache import CACHE_DIR
    farm_dir = CACHE_DIR / farm_code

    if not farm_dir.exists():
        print(f"  No cache directory for farm '{farm_code}'")
        print(f"  Expected at: {farm_dir}")
        return

    parquet_files = sorted(farm_dir.glob("*.parquet"))
    if not parquet_files:
        print(f"  Cache directory exists but no parquet files found")
        return

    import pandas as pd

    for pf in parquet_files:
        print(f"\n  📄 {pf.name} ({pf.stat().st_size / 1024:.1f} KB)")
        df = pd.read_parquet(pf)

        print(f"  Rows: {len(df)}")
        print(f"  Columns: {len(df.columns)}")

        # Date range
        if "Date" in df.columns:
            dates = pd.to_datetime(df["Date"])
            print(f"  Date range: {dates.min().strftime('%Y-%m-%d')} → {dates.max().strftime('%Y-%m-%d')}")

        # KPI columns (non-Date)
        kpi_cols = [c for c in df.columns if c != "Date"]
        print(f"  KPI columns: {len(kpi_cols)}")

        # Check for expected expanded catalog coverage
        from mcp_orchestration.dairy_kpi_client import DairyKPIClient
        client = DairyKPIClient(api_base_url="https://example.com")
        expected_aliases = set(client._get_kpi_list()["alias"])
        cached_kpis = set(kpi_cols)
        present = expected_aliases & cached_kpis
        missing = expected_aliases - cached_kpis
        extra = cached_kpis - expected_aliases

        print(f"\n  Catalog coverage:")
        print(f"    Present: {len(present)}/{len(expected_aliases)} ({100*len(present)/len(expected_aliases):.0f}%)")
        if missing:
            print(f"    Missing ({len(missing)}): {sorted(missing)[:20]}{'...' if len(missing) > 20 else ''}")
        if extra:
            print(f"    Extra ({len(extra)}): {sorted(extra)[:10]}{'...' if len(extra) > 10 else ''}")

        # Data quality: nulls per KPI
        null_counts = df[kpi_cols].isnull().sum()
        high_null = null_counts[null_counts > len(df) * 0.5]
        if len(high_null) > 0:
            print(f"\n  ⚠️  KPIs with >50% nulls ({len(high_null)}):")
            for col, cnt in high_null.sort_values(ascending=False).head(10).items():
                print(f"    {col}: {cnt}/{len(df)} null ({100*cnt/len(df):.0f}%)")

        # Sample values for first 5 KPIs
        print(f"\n  Sample values (last row):")
        last_row = df.iloc[-1]
        for col in kpi_cols[:10]:
            val = last_row[col]
            print(f"    {col}: {val}")
        if len(kpi_cols) > 10:
            print(f"    ... and {len(kpi_cols) - 10} more")


# ── entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if "--inspect" in sys.argv:
        idx = sys.argv.index("--inspect")
        farm = sys.argv[idx + 1] if idx + 1 < len(sys.argv) else "GM"
        inspect_parquet(farm)
    else:
        test_catalog_loads()
        test_uniqueness()
        test_domain_config()
        test_code_to_alias_resolution()
        test_no_orphans()
        print("\n✅ All expanded catalog tests passed.\n")
        print("Tip: run with --inspect GM to inspect a cached parquet file")
