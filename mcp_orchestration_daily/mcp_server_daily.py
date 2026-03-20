import logging
import os
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
from mcp.server.fastmcp import FastMCP

from mcp_orchestration_daily.daily_data_client import DailyDataClient

API_BASE_URL = os.getenv("DAILY_API_BASE_URL", "http://200.23.18.75:8074")
daily_client = DailyDataClient(api_base_url=API_BASE_URL)

mcp = FastMCP("Daily Dairy Data")


def _sanitize_df(df: pd.DataFrame) -> List[Dict]:
    if df.empty:
        return []
    for col in df.select_dtypes(include=["datetime64[ns]", "datetime64[ns, UTC]"]).columns:
        df[col] = df[col].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    df = df.replace([np.inf, -np.inf], np.nan).where(pd.notnull(df), None)
    return df.to_dict(orient="records")


@mcp.tool()
def get_milk_feed_data_group(
    farm_code: str,
    group_code: str,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    days: int = 30,
    columns: Optional[List[str]] = None,
) -> List[Dict]:
    """
    Return daily milk/feed rows for the requested farm/group.
    Dates can be set explicitly (YYYY-MM-DD) or inferred from `days`.
    """
    df = daily_client.fetch_milk_feed_data_group(
        farm_code=farm_code,
        group_code=group_code,
        from_date=from_date,
        to_date=to_date,
        days=days,
        columns=columns,
    )
    return _sanitize_df(df)


@mcp.tool()
def summarize_daily_metric(
    farm_code: str,
    group_code: str,
    metric: str,
    days: int = 30,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
) -> Dict:
    """
    Compute summary stats for a specific metric in the daily data window.
    """
    df = daily_client.fetch_milk_feed_data_group(
        farm_code=farm_code,
        group_code=group_code,
        from_date=from_date,
        to_date=to_date,
        days=days,
    )
    if df.empty:
        return {"error": "No data returned for the requested window."}
    if metric not in df.columns:
        return {"error": f"Unknown metric '{metric}'. Available: {sorted(df.columns)}"}

    series = pd.to_numeric(df[metric], errors="coerce").dropna()
    if series.empty:
        return {"error": f"No numeric data found for metric '{metric}'."}

    summary = {
        "farm_code": farm_code,
        "group_code": group_code,
        "metric": metric,
        "rows": int(series.count()),
        "mean": float(series.mean()),
        "std": float(series.std()) if len(series) > 1 else None,
        "min": float(series.min()),
        "max": float(series.max()),
        "trend": float(series.diff().mean()) if len(series) > 1 else None,
    }
    if "Date" in df.columns and not df["Date"].isna().all():
        start = df["Date"].min()
        end = df["Date"].max()
        summary["start_date"] = (
            start.strftime("%Y-%m-%dT%H:%M:%SZ") if hasattr(start, "strftime") else str(start)
        )
        summary["end_date"] = (
            end.strftime("%Y-%m-%dT%H:%M:%SZ") if hasattr(end, "strftime") else str(end)
        )
    return summary


@mcp.tool()
def list_daily_fields(
    farm_code: str,
    group_code: str,
    days: int = 7,
) -> Dict:
    """
    Inspect the daily dataset to list available fields for the farm/group.
    """
    df = daily_client.fetch_milk_feed_data_group(
        farm_code=farm_code,
        group_code=group_code,
        days=days,
    )
    if df.empty:
        return {"fields": [], "note": "No data returned for preview window."}
    numeric_fields = [
        c for c in df.columns if c != "Date" and pd.api.types.is_numeric_dtype(df[c])
    ]
    other_fields = [c for c in df.columns if c not in numeric_fields and c != "Date"]
    return {
        "fields": list(df.columns),
        "numeric_fields": numeric_fields,
        "other_fields": other_fields,
    }


app = mcp.streamable_http_app()
