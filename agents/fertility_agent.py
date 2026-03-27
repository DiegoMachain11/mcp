from __future__ import annotations

import json
import os
import requests
from openai import OpenAI

from agents.helpers import _extract_rows, normalize_kpi_list
from agents.domain_config import build_domain_kpi_list
from agents.kpi_signals import compute_kpi_signals, format_signals_for_prompt, build_rag_query_from_signals
from rag.retriever import get_rag_context

# === CONFIG ===
BRIDGE_URL = "http://localhost:8090"
OPENAI_MODEL = "gpt-4o-mini"
openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


def run_fertility_agent(
    farm_code: str, kpis: list[str], language: str = "es", months: int = 3,
    seasonal_context: str = "",
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

    # --- Pre-process signals and retrieve scientific context ---
    signals = compute_kpi_signals(rows, kpi_names)
    signals_text = format_signals_for_prompt(signals)
    rag_query = build_rag_query_from_signals(signals, "Fertility")
    rag_context = get_rag_context(rag_query, domain="Fertility")

    rag_section = (
        f"\n=== SCIENTIFIC CONTEXT (from research literature) ===\n{rag_context}\n"
        if rag_context
        else ""
    )

    # --- Build LLM prompt ---
    prompt = f"""
    You are an expert dairy reproduction analyst with 20 years of experience in Holstein herd management.

    Given fertility KPI data from farm '{farm_code}', analyze:
    - Conception, pregnancy and fertility trends
    - Estrus detection efficiency
    - Days open, abortions, service ages
    - Key risks or anomalies (e.g., high abortions, low heat detection)
    - Practical recommendations by timeframe:
        Immediate (0–1 month)
        Short (1–3 months)
        Medium (3–6 months)
        Long (6+ months)

    Use the `last_value` from the KPI SIGNAL ANALYSIS as the current value for each KPI.
    When reporting issues, always state the current value and the benchmark it violates.
    If a KPI shows `data_quality: poor` or `sparse`, note the limited data but still report the last known value.
    Do NOT invent values or say "not provided" — only report what the signal analysis shows.
    Ground your recommendations in the scientific context when available.
    Use seasonal/weather context to distinguish expected seasonal variation from true problems.

    Before writing your JSON, reason in this order:
    1. Which KPIs violate their benchmark? (compare last_value to reference ranges)
    2. For each violation: is the trend worsening, stable, or improving?
    3. Is this violation expected given the current season/weather, or is it abnormal?
    4. Are any flagged KPIs causally linked to each other?
    5. What is the single most urgent action?

    Return JSON strictly as:
    {{
        "domain": "Fertility",
        "summary": "...short paragraph overview...",
        "issues": [ "Each issue as a plain string, e.g.: '21-day pregnancy rate at 14% (benchmark ≥21%) — below_target, worsening trend'" ],
        "recommendations": {{
            "Immediate": [ "..." ],
            "Short": [ "..." ],
            "Medium": [ "..." ],
            "Long": [ "..." ]
        }},
        "kpis_to_plot": [ "list of the key KPI column names" ]
    }}

{seasonal_context}
=== BENCHMARK REFERENCE RANGES ===
  pct_partos_logrados       : target 85–95% (concern <75%)
  taza_prenez_21_dias       : target ≥21% (concern <15%)
  deteccion_de_celos_ult2   : target ≥65% (concern <50%)
  dias_abiertos_mx          : target <120 days (concern >150)
  pct_total_abortos         : target <3% (concern >5%)
  pct_fertilidad_en_vaquillas: target ≥65% (concern <50%)

=== KPI SIGNAL ANALYSIS (trend, anomaly, benchmark status) ===
{signals_text}
{rag_section}
=== RAW KPI DATA (last 10 periods) ===
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
