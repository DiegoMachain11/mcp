# mcp_server.py
from typing import List, Dict, Optional
import numpy as np
import pandas as pd

from mcp.server.fastmcp import FastMCP
from fastapi import FastAPI

# ---- Your existing API wrapper (simplified import) ----
from dairy_kpi_client import DairyKPIClient

# Init your data client
kpi_client = DairyKPIClient(api_base_url="http://200.23.18.75:8074/IREGIOService")

# Build MCP server
mcp = FastMCP("Dairy KPIs")


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
def get_farm_kpis(farm_code: str, language: str = "es") -> List[Dict]:
    """
    Return time-series KPI rows for a farm.
    Each row contains Date and KPI columns.
    """
    df = kpi_client.fetch_farm_kpis(farm_code=farm_code, language=language)
    return _sanitize_df(df)


@mcp.tool()
def analyze_kpis(
    farm_code: str, metric: str, days: int = 90, language: str = "es"
) -> Dict:
    """
    Compute simple stats for a KPI over the last N days.
    """
    df = kpi_client.fetch_farm_kpis(farm_code=farm_code, language=language)
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


# --- Expose as HTTP (Streamable HTTP transport) ---
# Option A: standalone ASGI app just for MCP on "/"
app = mcp.streamable_http_app()
