"""
Local monthly KPI cache.

Files are stored as:
    data/kpi_cache/{farm_code}/{YYYY-MM}.parquet

One file per farm per month. Data is valid for the entire month it was fetched.
Files older than `max_age_months` are deleted automatically on every write.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
CACHE_DIR = REPO_ROOT / "data" / "kpi_cache"

MAX_AGE_MONTHS = 12


def _cache_path(farm_code: str, year_month: str) -> Path:
    """Return the parquet path for a given farm and YYYY-MM string."""
    return CACHE_DIR / farm_code / f"{year_month}.parquet"


def _current_ym() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m")


def get(farm_code: str, year_month: str | None = None) -> pd.DataFrame | None:
    """
    Return cached DataFrame for the given farm and month, or None if not cached.
    Defaults to the current month if year_month is omitted.
    """
    ym = year_month or _current_ym()
    path = _cache_path(farm_code, ym)
    if not path.exists():
        return None
    try:
        return pd.read_parquet(path)
    except Exception:
        return None


def put(farm_code: str, df: pd.DataFrame, year_month: str | None = None) -> Path:
    """
    Save a DataFrame to the cache for the given farm and month.
    Runs TTL cleanup for this farm after writing.
    """
    ym = year_month or _current_ym()
    path = _cache_path(farm_code, ym)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        return path  # already cached this month, don't overwrite
    df.to_parquet(path, index=False)
    cleanup(farm_code)
    return path


def cleanup(farm_code: str, max_age_months: int = MAX_AGE_MONTHS) -> list[Path]:
    """
    Delete cached files for a farm that are older than max_age_months.
    Returns list of deleted paths.
    """
    farm_dir = CACHE_DIR / farm_code
    if not farm_dir.exists():
        return []

    now = datetime.now(timezone.utc)
    deleted = []

    for parquet_file in farm_dir.glob("*.parquet"):
        stem = parquet_file.stem  # e.g. "2024-11"
        try:
            file_date = datetime.strptime(stem, "%Y-%m").replace(tzinfo=timezone.utc)
        except ValueError:
            continue

        age_months = (now.year - file_date.year) * 12 + (now.month - file_date.month)
        if age_months > max_age_months:
            parquet_file.unlink()
            deleted.append(parquet_file)

    if deleted:
        print(f"🗑️  Cache cleanup [{farm_code}]: deleted {len(deleted)} file(s) older than {max_age_months} months")

    return deleted


def cleanup_all(max_age_months: int = MAX_AGE_MONTHS) -> int:
    """
    Run TTL cleanup across all farms in the cache directory.
    Returns total number of deleted files.
    """
    if not CACHE_DIR.exists():
        return 0
    total = 0
    for farm_dir in CACHE_DIR.iterdir():
        if farm_dir.is_dir():
            total += len(cleanup(farm_dir.name, max_age_months))
    return total
