import json
import os
import requests
from openai import OpenAI

from helpers import _extract_rows

# === CONFIG ===
BRIDGE_URL = "http://localhost:8090"
OPENAI_MODEL = "gpt-4o-mini"
openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


def run_production_agent(
    farm_code: str, kpis: list[str], language: str = "es", months: int = 3
):
    """Deep-dive analysis for production KPIs."""

    # --- Fetch detailed time series ---
    resp = requests.get(
        f"{BRIDGE_URL}/get_farm_kpis",
        params={"farm_code": farm_code, "language": language, "months": months},
    )
    resp.raise_for_status()

    rows = _extract_rows(resp.json())
    if not rows:
        return {
            "domain": "Production",
            "summary": "No KPI data available for analysis.",
            "issues": [],
            "recommendations": {"Immediate": [], "Short": [], "Medium": [], "Long": []},
            "kpis_to_plot": kpis,
        }

    # Filter only relevant KPIs
    production_data = [
        {k: row.get(k) for k in ["Date", *kpis] if k in row} for row in rows
    ]

    # --- Build LLM prompt ---
    prompt = f"""
    You are an expert dairy production analyst.
    Analyze KPIs for farm '{farm_code}' focusing on:
    - Milk yield performance (305-day, lactation peaks)
    - Feed efficiency and body condition implications
    - Lactation consistency across parities
    - Key production bottlenecks and seasonality
    - Key risks or anomalies (e.g., low productions)
    - Practical recommendations by timeframe:
        Immediate (0–1 month)
        Short (1–3 months)
        Medium (3–6 months)
        Long (6+ months)

    Return JSON strictly as:
    {{
        "domain": "Production",
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
    {json.dumps(production_data[-10:], indent=2, ensure_ascii=False)}
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
    kpis = [
        "prod_a_305_del_1a_lact",
        "prod_a_305_del_2a_lact",
        "prod_a_305_del_3_lact",
        "pico_de_prod_1a_lact",
        "pico_de_prod_3_lact",
        "eficiencia_de_ganancia_de_peso",
    ]
    print(json.dumps(run_production_agent("GM", kpis), indent=2, ensure_ascii=False))
