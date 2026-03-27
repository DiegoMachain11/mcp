# mcp_server.py
from typing import List, Dict, Optional
import numpy as np
import pandas as pd
import logging
import sys
from pathlib import Path
from mcp.server.fastmcp import FastMCP

from mcp_orchestration.dairy_kpi_client import DairyKPIClient

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from agents.kpi_cache import get as cache_get, put as cache_put

# Init your data client
kpi_client = DairyKPIClient(api_base_url="https://rgiomadero.com:9091/IREGIOService")

# Build MCP server
mcp = FastMCP("Dairy KPIs")


def _resolve_to_aliases(selected_kpis: List[str]) -> List[str]:
    """
    Resolve a list of KPI codes, aliases, or descriptions to alias column names.
    Accepts codes like '255d', aliases like 'pct_partos_logrados', or mixed.
    """
    catalog = kpi_client._get_kpi_list()
    code_to_alias = dict(zip(catalog["Code"].astype(str), catalog["alias"]))
    known_aliases = set(catalog["alias"])

    resolved = []
    for kpi in selected_kpis:
        kpi = str(kpi).strip()
        if kpi in known_aliases:
            resolved.append(kpi)
        elif kpi in code_to_alias:
            resolved.append(code_to_alias[kpi])
        else:
            slug = kpi_client._make_alias(kpi)
            if slug in known_aliases:
                resolved.append(slug)
            else:
                resolved.append(kpi)
    return resolved


CACHE_MAX_MONTHS = 24  # Always fetch the max window so any subsequent request is served from cache


def _fetch_with_cache(farm_code: str, language: str, months: int) -> pd.DataFrame:
    """
    Return full KPI DataFrame for a farm, using the monthly cache.
    On cache miss: fetches ALL KPIs for the max window (24 months) and caches.
    On cache hit: returns the cached DataFrame instantly.
    The caller is responsible for filtering by the requested time window.
    """
    cached = cache_get(farm_code)
    if cached is not None:
        logging.info(f"Cache hit for farm {farm_code}")
        return cached

    logging.info(f"Cache miss for farm {farm_code} — fetching {CACHE_MAX_MONTHS} months from IREGIO")
    df = kpi_client.fetch_farm_kpis(
        farm_code=farm_code,
        language=language,
        months=CACHE_MAX_MONTHS,
    )
    cache_put(farm_code, df)
    return df


def _sanitize_df(df: pd.DataFrame) -> List[Dict]:
    # Convert datetimes → ISO strings
    for col in df.select_dtypes(
        include=["datetime64[ns]", "datetime64[ns, UTC]"]
    ).columns:
        df[col] = df[col].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    # Replace inf/NaN → None
    df = df.replace([np.inf, -np.inf], np.nan).where(pd.notnull(df), None)
    return df.to_dict(orient="records")


@mcp.tool()
def get_farm_kpis(
    farm_code: str,
    language: str = "es",
    months: int = 13,
    selected_kpis: Optional[List[str]] = None,
) -> List[Dict]:
    """
    Return time-series KPI rows for a farm.
    Each row contains Date and KPI columns.
    """
    logging.info(
        f"Fetching KPIs for farm {farm_code}, language={language}, months={months}"
    )
    df = _fetch_with_cache(farm_code, language, months)

    if selected_kpis:
        aliases = _resolve_to_aliases(selected_kpis)
        cols = ["Date"] + [c for c in aliases if c in df.columns]
        df = df[cols]

    now = pd.Timestamp.now(tz="UTC").tz_localize(None)
    df = df[df["Date"] > (now - pd.Timedelta(days=int(30 * months)))]

    return _sanitize_df(df)


@mcp.tool()
def analyze_kpis(
    farm_code: str, metric: str, days: int = 90, language: str = "es"
) -> Dict:
    """
    Compute simple stats for a KPI over the last N days.
    """
    df = kpi_client.fetch_farm_kpis(
        farm_code=farm_code, language=language, selected_kpis=[metric]
    )
    if metric not in df.columns:
        return {"error": f"Unknown metric '{metric}'"}
    now = pd.Timestamp.now(tz="UTC").tz_localize(None)
    recent = df[df["Date"] > (now - pd.Timedelta(days=days))]
    recent = recent.dropna(subset=[metric])
    avg = float(recent[metric].mean()) if not recent.empty else None
    trend = float(recent[metric].diff().mean()) if not recent.empty else None
    return {
        "farm_code": farm_code,
        "metric": metric,
        "days": days,
        "average": avg,
        "trend_per_row": trend,
    }


@mcp.tool()
def summarize_kpis(
    farm_code: str,
    language: str = "es",
    months: int = 13,
    selected_kpis: Optional[List[str]] = None,
) -> Dict:
    """
    Summarize all KPIs for a farm over the last N days.
    Computes mean, std, min, max, and trend for each numeric KPI.
    Returns a dictionary of KPI summaries.
    """
    logging.info(
        f"Summarizing KPIs for farm {farm_code}, selected_kpis={selected_kpis}, months={months}"
    )
    df = _fetch_with_cache(farm_code, language, months)
    days = int(30 * months)
    now = pd.Timestamp.now(tz="UTC").tz_localize(None)
    recent = df[df["Date"] > (now - pd.Timedelta(days=days))].copy()

    if selected_kpis:
        aliases = _resolve_to_aliases(selected_kpis)
        keep = ["Date"] + [c for c in aliases if c in recent.columns]
        recent = recent[keep]

    logging.info(df)

    # Identify numeric KPI columns
    numeric_cols = [
        c
        for c in recent.columns
        if c != "Date" and pd.api.types.is_numeric_dtype(recent[c])
    ]
    summaries = {}
    for col in numeric_cols:
        s = recent[col].dropna()
        if len(s) < 2:
            continue
        summaries[col] = {
            "mean": round(float(s.mean()), 3),
            "std": round(float(s.std()), 3),
            "min": round(float(s.min()), 3),
            "max": round(float(s.max()), 3),
            "trend": round(float(s.diff().mean()), 5),
            "count": int(s.count()),
        }

    return {
        "farm_code": farm_code,
        "days": days,
        "summary_count": len(summaries),
        "summaries": summaries,
    }


@mcp.tool()
def plot_selected_kpis(
    farm_code: str, selected_kpis: List[str], language: str = "es", days: int = 90
) -> Dict:
    """
    Plot only the specified KPIs for a farm over the last N days.
    Returns a base64-encoded PNG image.
    """
    df = kpi_client.fetch_farm_kpis(
        farm_code=farm_code, language=language, selected_kpis=selected_kpis
    )
    now = pd.Timestamp.now(tz="UTC").tz_localize(None)
    recent = df[df["Date"] > (now - pd.Timedelta(days=days))].copy()

    if "Date" not in recent.columns:
        return {"error": "Missing 'Date' column in data."}

    valid_kpis = [k for k in selected_kpis if k in recent.columns]
    if not valid_kpis:
        return {"error": f"No valid KPIs found among {selected_kpis}"}

    import io, base64, matplotlib.pyplot as plt
    import math as _math

    n_kpis = len(valid_kpis)
    cols = min(n_kpis, 3)
    rows = _math.ceil(n_kpis / cols)
    fig, axes = plt.subplots(rows, cols, figsize=(5 * cols, 3 * rows), squeeze=False)

    for idx, kpi in enumerate(valid_kpis):
        r, c = divmod(idx, cols)
        ax = axes[r][c]
        series = recent[["Date", kpi]].dropna()
        ax.plot(series["Date"], series[kpi], marker="o", markersize=3, linewidth=1.5)
        ax.set_title(kpi.replace("_", " ").title(), fontsize=9, fontweight="bold")
        ax.tick_params(axis="both", labelsize=7)
        ax.grid(True, alpha=0.3)
        plt.setp(ax.get_xticklabels(), rotation=30, ha="right")

    # Hide unused subplots
    for idx in range(n_kpis, rows * cols):
        r, c = divmod(idx, cols)
        axes[r][c].set_visible(False)

    fig.suptitle(f"AI-Selected KPIs — {farm_code}", fontsize=12, fontweight="bold")
    plt.tight_layout()

    buf = io.BytesIO()
    plt.savefig(buf, format="png")
    plt.close(fig)
    buf.seek(0)
    image_base64 = base64.b64encode(buf.read()).decode("utf-8")

    return {
        "farm_code": farm_code,
        "selected_kpis": valid_kpis,
        "image_base64": image_base64,
    }


# --- Expose as HTTP (Streamable HTTP transport) ---
app = mcp.streamable_http_app()
