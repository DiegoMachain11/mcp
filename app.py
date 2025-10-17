from fastapi import FastAPI, Query
from fastapi.responses import ORJSONResponse
from dairy_kpi_client import DairyKPIClient
import pandas as pd
import numpy as np

app = FastAPI(title="Dairy Farm KPI MCP Service")

# Initialize your client
client = DairyKPIClient(api_base_url="http://200.23.18.75:8074/IREGIOService")


@app.get("/mcp/resources/farm_kpis", response_class=ORJSONResponse)
def get_farm_kpis(
    farm_code: str = Query(..., description="Farm code to fetch KPIs for"),
    language: str = Query("es", description="Language code: 'es' or 'en'"),
):
    """Main MCP resource endpoint - fetches farm KPIs."""
    df = client.fetch_farm_kpis(farm_code, language)

    for col in df.select_dtypes(
        include=["datetime64[ns]", "datetime64[ns, UTC]"]
    ).columns:
        df[col] = df[col].dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    # sanitize infinities/NaN
    df = df.replace([np.inf, -np.inf], np.nan)

    return df.to_dict(orient="records")
    # """Main MCP resource endpoint - fetches farm KPIs."""
    # df = client.fetch_farm_kpis(farm_code, language)
    # return df.to_dict(orient="records")


@app.get("/mcp/resources/farm_kpis/schema")
def get_kpi_schema():
    """Expose JSON schema for KPIs."""
    return client.get_kpi_schema()


@app.post("/mcp/tools/analyze_kpis")
def analyze_kpis(
    farm_code: str = Query(...),
    metric: str = Query(...),
    days: int = Query(90, description="Number of days to analyze"),
):
    """Simple KPI analysis (average + trend)."""
    df = client.fetch_farm_kpis(farm_code)
    df = df.dropna(subset=[metric])
    df = df[df["Date"] > (pd.Timestamp.now() - pd.Timedelta(days=days))]
    avg = df[metric].mean()
    trend = df[metric].diff().mean()
    return {
        "farm_code": farm_code,
        "metric": metric,
        "average": avg,
        "trend": trend,
        "summary": f"{metric} average over {days} days is {avg:.2f} (trend {trend:.2f}/day)",
    }


@app.get("/")
def root():
    return {"message": "Dairy Farm KPI MCP Service running"}
