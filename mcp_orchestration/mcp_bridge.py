# mcp_bridge.py
from fastapi import FastAPI
import asyncio
from typing import Dict
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

app = FastAPI()
MCP_SERVER_URL = "http://localhost:8080/mcp"


async def _call_tool(tool, args):
    async with streamablehttp_client(MCP_SERVER_URL) as (r, w, _):
        async with ClientSession(r, w) as s:
            await s.initialize()
            res = await s.call_tool(tool, args)
            return res.structuredContent or res.content


@app.get("/get_farm_kpis")
async def get_farm_kpis(farm_code: str, language: str, months: int = 13):
    return await _call_tool(
        "get_farm_kpis",
        {"farm_code": farm_code, "language": language, "months": months},
    )


@app.get("/analyze_kpis")
async def analyze_kpis(farm_code: str, metric: str, days: int):
    return await _call_tool(
        "analyze_kpis", {"farm_code": farm_code, "metric": metric, "days": days}
    )


@app.get("/summarize_kpis")
async def summarize_kpis(farm_code: str, language: str, months: int):
    return await _call_tool(
        "summarize_kpis",
        {"farm_code": farm_code, "language": language, "months": months},
    )


@app.post("/plot_selected_kpis")
async def plot_selected_kpis(payload: Dict):
    """
    Plot the specific KPIs provided by the AI selection.
    """
    return await _call_tool("plot_selected_kpis", payload)
