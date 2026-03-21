"""
Tests for agents/kpi_cache.py

Tests three things:
  1. put/get round-trip — data survives write/read correctly
  2. TTL cleanup — files older than max_age_months are deleted, recent ones kept
  3. Cache miss returns None

Usage:
    conda run -n mcp python -m tests.test_kpi_cache
"""

from __future__ import annotations

import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

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
    print(f"\n{'='*55}")
    print(f"  {title}")
    print(f"{'='*55}")


# ── patch cache dir to a temp location ───────────────────────────────────────

import agents.kpi_cache as cache_module

def _with_temp_cache(fn):
    """Decorator: runs test with CACHE_DIR pointing to a fresh temp directory."""
    def wrapper(*args, **kwargs):
        with tempfile.TemporaryDirectory() as tmp:
            original = cache_module.CACHE_DIR
            cache_module.CACHE_DIR = Path(tmp)
            try:
                fn(*args, **kwargs)
            finally:
                cache_module.CACHE_DIR = original
    return wrapper


# ── tests ────────────────────────────────────────────────────────────────────

@_with_temp_cache
def test_put_get_roundtrip():
    _section("Test 1 — put/get round-trip")

    df = pd.DataFrame({
        "Date": pd.to_datetime(["2026-01-01", "2026-02-01", "2026-03-01"]),
        "pct_cetosis": [0.12, 0.15, 0.18],
        "taza_prenez_21_dias": [0.21, 0.19, 0.17],
    })

    path = cache_module.put("TEST", df)
    _check(path.exists(), f"Parquet file created at {path.name}", "File not created")

    result = cache_module.get("TEST")
    _check(result is not None, "Cache hit after put", "Cache returned None")
    _check(list(result.columns) == list(df.columns), "Columns match", f"Got {list(result.columns)}")
    _check(len(result) == len(df), f"Row count matches ({len(result)})", f"Expected {len(df)}, got {len(result)}")
    _check(
        abs(result["pct_cetosis"].iloc[-1] - 0.18) < 1e-6,
        "Last value preserved correctly",
        f"Got {result['pct_cetosis'].iloc[-1]}"
    )


@_with_temp_cache
def test_cache_miss():
    _section("Test 2 — cache miss returns None")

    result = cache_module.get("NONEXISTENT_FARM")
    _check(result is None, "Cache miss returns None for unknown farm", f"Expected None, got {result}")

    result2 = cache_module.get("TEST", year_month="1999-01")
    _check(result2 is None, "Cache miss returns None for old month", f"Expected None, got {result2}")


@_with_temp_cache
def test_ttl_cleanup():
    _section("Test 3 — TTL cleanup")

    farm_dir = cache_module.CACHE_DIR / "FARMX"
    farm_dir.mkdir(parents=True)

    df = pd.DataFrame({"Date": pd.to_datetime(["2026-01-01"]), "val": [1.0]})

    # Write files spanning different ages
    old_files = ["2024-01", "2024-06", "2024-12"]   # >12 months ago (relative to 2026-03)
    recent_files = ["2025-04", "2025-10", "2026-02"] # <=12 months ago

    for ym in old_files + recent_files:
        p = farm_dir / f"{ym}.parquet"
        df.to_parquet(p, index=False)

    deleted = cache_module.cleanup("FARMX", max_age_months=12)

    _check(
        len(deleted) == len(old_files),
        f"Deleted {len(deleted)} old file(s) (expected {len(old_files)})",
        f"Expected {len(old_files)} deletions, got {len(deleted)}: {[d.name for d in deleted]}"
    )

    surviving = list(farm_dir.glob("*.parquet"))
    _check(
        len(surviving) == len(recent_files),
        f"{len(surviving)} recent file(s) kept (expected {len(recent_files)})",
        f"Expected {len(recent_files)} survivors, got {len(surviving)}: {[s.name for s in surviving]}"
    )

    deleted_names = {d.stem for d in deleted}
    for ym in old_files:
        _check(ym in deleted_names, f"{ym} was deleted", f"{ym} was NOT deleted")
    for ym in recent_files:
        _check((farm_dir / f"{ym}.parquet").exists(), f"{ym} was kept", f"{ym} was deleted unexpectedly")


@_with_temp_cache
def test_no_duplicate_write():
    _section("Test 4 — same-month write returns existing file")

    df = pd.DataFrame({"Date": pd.to_datetime(["2026-03-01"]), "val": [42.0]})
    path1 = cache_module.put("FARM1", df)

    df2 = pd.DataFrame({"Date": pd.to_datetime(["2026-03-01"]), "val": [99.0]})
    path2 = cache_module.put("FARM1", df2)

    _check(path1 == path2, "Second put to same month returns same path", f"{path1} != {path2}")

    result = cache_module.get("FARM1")
    _check(
        result["val"].iloc[0] == 42.0,
        "Original data preserved (no overwrite)",
        f"Expected 42.0, got {result['val'].iloc[0]}"
    )


# ── entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    test_put_get_roundtrip()
    test_cache_miss()
    test_ttl_cleanup()
    test_no_duplicate_write()
    print("\n✅ All cache tests passed.\n")
