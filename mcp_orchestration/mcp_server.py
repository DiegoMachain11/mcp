# mcp_server.py
from typing import List, Dict, Optional
import numpy as np
import pandas as pd

from mcp.server.fastmcp import FastMCP

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
def get_farm_kpis(farm_code: str, language: str = "es", months: int = 13) -> List[Dict]:
    """
    Return time-series KPI rows for a farm.
    Each row contains Date and KPI columns.
    """
    df = kpi_client.fetch_farm_kpis(
        farm_code=farm_code, language=language, months=months
    )
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


@mcp.tool()
def plot_selected_kpis(
    farm_code: str, selected_kpis: List[str], language: str = "es", days: int = 90
) -> Dict:
    """
    Plot only the specified KPIs for a farm over the last N days.
    Returns a base64-encoded PNG image.
    """
    df = kpi_client.fetch_farm_kpis(farm_code=farm_code, language=language)
    now = pd.Timestamp.now(tz="UTC").tz_localize(None)
    recent = df[df["Date"] > (now - pd.Timedelta(days=days))].copy()

    if "Date" not in recent.columns:
        return {"error": "Missing 'Date' column in data."}

    valid_kpis = [k for k in selected_kpis if k in recent.columns]
    if not valid_kpis:
        return {"error": f"No valid KPIs found among {selected_kpis}"}

    import io, base64, matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(10, 6))
    for kpi in valid_kpis:
        ax.plot(recent["Date"], recent[kpi], label=kpi.replace("_", " ").title())

    ax.set_title(f"AI-Selected KPIs for {farm_code}")
    ax.set_xlabel("Date")
    ax.set_ylabel("Value")
    ax.legend()
    ax.grid(True)
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
# Option A: standalone ASGI app just for MCP on "/"
app = mcp.streamable_http_app()
