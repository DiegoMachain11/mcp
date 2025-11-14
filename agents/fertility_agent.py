import json
import os
import requests
from openai import OpenAI

from agents.helpers import _extract_rows, normalize_kpi_list
from agents.domain_config import build_domain_kpi_list

# === CONFIG ===
BRIDGE_URL = "http://localhost:8090"
OPENAI_MODEL = "gpt-4o-mini"
openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


def run_fertility_agent(
    farm_code: str, kpis: list[str], language: str = "es", months: int = 3
):
    """Deep-dive analysis for fertility KPIs."""

    normalized_kpis = normalize_kpi_list(kpis)
    kpi_names = build_domain_kpi_list("Fertility", normalized_kpis)

    # --- Fetch detailed time series ---
    params = {"farm_code": farm_code, "language": language, "months": months}
    if kpi_names:
        params["selected_kpis"] = kpi_names

    resp = requests.get(f"{BRIDGE_URL}/get_farm_kpis", params=params)
    resp.raise_for_status()
    rows = _extract_rows(resp.json())
    if not rows:
        return {
            "domain": "Production",
            "summary": "No KPI data available for analysis.",
            "issues": [],
            "recommendations": {"Immediate": [], "Short": [], "Medium": [], "Long": []},
            "kpis_to_plot": kpi_names,
        }

    print("Fertility rows fetched:", rows)

    # Filter only relevant KPIs
    fertility_data = [
        {k: row.get(k) for k in ["Date", *kpi_names] if k in row} for row in rows
    ]

    # --- Build LLM prompt ---
    prompt = f"""
    You are an expert dairy reproduction analyst.

    Given fertility KPI data from farm '{farm_code}', analyze:
    - Conception, pregnancy and fertility trends
    - Estrus detection efficiency
    - Days open, abortions, service ages
    - Key risks or anomalies (e.g., high abortions, low heat detection)
    - Return exact percentages and numbers where relevant.
    - Practical recommendations by timeframe:
        Immediate (0–1 month)
        Short (1–3 months)
        Medium (3–6 months)
        Long (6+ months)

    Return JSON strictly as:
    {{
        "domain": "Fertility",
        "summary": "...short paragraph overview...",
        "issues": [ "list of detected problems" ],
        "recommendations": {{
            "Immediate": [ "..." ],
            "Short": [ "..." ],
            "Medium": [ "..." ],
            "Long": [ "..." ]
        }},
        "kpis_to_plot": [ "list of the key KPI column names" ]
    }}

    KPI data (sample):
    {json.dumps(fertility_data[-10:], indent=2, ensure_ascii=False)}
    """

    response = openai_client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=[
            {
                "role": "system",
                "content": "You are a domain-specific dairy farm fertility advisor.",
            },
            {"role": "user", "content": prompt},
        ],
        response_format={"type": "json_object"},
    )

    result = json.loads(response.choices[0].message.content)
    return result


if __name__ == "__main__":
    # Example test
    kpis = [
        "pct_partos_logrados",
        "taza_prenez_21_dias",
        "deteccion_de_celos_ult2",
        "dias_abiertos_mx",
        "%_total_abortos_vaquillas_m",
    ]
    output = run_fertility_agent("GM", kpis)
    print(json.dumps(output, indent=2, ensure_ascii=False))
