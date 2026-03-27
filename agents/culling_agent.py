from __future__ import annotations

import json, os, requests
from openai import OpenAI

from agents.helpers import _extract_rows, normalize_kpi_list
from agents.domain_config import build_domain_kpi_list
from agents.kpi_signals import compute_kpi_signals, format_signals_for_prompt, build_rag_query_from_signals
from rag.retriever import get_rag_context

BRIDGE_URL = "http://localhost:8090"
OPENAI_MODEL = "gpt-4o-mini"
openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


def run_culling_agent(farm_code, kpis, language="es", months=3, seasonal_context=""):
    normalized_kpis = normalize_kpi_list(kpis)
    kpi_names = build_domain_kpi_list("Culling", normalized_kpis)

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

    print("Culling rows fetched:", rows)

    data = [{k: r.get(k) for k in ["Date", *kpi_names]} for r in rows]

    # --- Pre-process signals and retrieve scientific context ---
    signals = compute_kpi_signals(rows, kpi_names)
    signals_text = format_signals_for_prompt(signals)
    rag_query = build_rag_query_from_signals(signals, "Culling")
    rag_context = get_rag_context(rag_query, domain="Culling")

    rag_section = (
        f"\n=== SCIENTIFIC CONTEXT (from research literature) ===\n{rag_context}\n"
        if rag_context
        else ""
    )

    prompt = f"""
    You are an expert in dairy herd structure, longevity, and replacement economics.

    Analyze culling and mortality KPIs for farm '{farm_code}'.
    Focus on:
    - Early-lactation culling and mortality (vs. target <5% before 60 DIM)
    - Culling causes and age/parity distribution
    - Long-term retention and replacement balance
    - Strategies to reduce involuntary culling
    - Return exact percentages and deviation from benchmarks where relevant.

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

    Return JSON:
    {{
      "domain":"Culling",
      "summary":"...",
      "issues":["Each issue as a plain string, e.g.: 'Early culling <60 DIM at 9% (benchmark <5%) — critical_high, worsening trend'"],
      "recommendations":{{"Immediate":[],"Short":[],"Medium":[],"Long":[]}},
      "kpis_to_plot":{json.dumps(kpi_names)}
    }}

=== BENCHMARK REFERENCE RANGES ===
  pct_desecho_vacas_lt_60_del_periodo  : target <5% early culling (concern >8%)
  pct_desecho_plus                     : target <25% total annual culling (concern >35%)
  vacas_muertas_frescas_lt_30_del      : target <2% fresh cow deaths (concern >4%)
  pct_vacas_muertas_frescas_lt_30_del  : target <2% fresh cow deaths (concern >4%)

{seasonal_context}
=== KPI SIGNAL ANALYSIS (trend, anomaly, benchmark status) ===
{signals_text}
{rag_section}
=== RAW KPI DATA (last 10 periods) ===
{json.dumps(data[-10:],indent=2,ensure_ascii=False)}
    """
    r = openai_client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=[
            {
                "role": "system",
                "content": "You are a dairy herd structure analyst focusing on culling trends.",
            },
            {"role": "user", "content": prompt},
        ],
        response_format={"type": "json_object"},
    )
    return json.loads(r.choices[0].message.content)


if __name__ == "__main__":
    kpis = [
        "pct_desecho_vacas_lt_60_del_periodo",
        "pct_desecho_plus",
        "vacas_muertas_frescas_lt_30_del",
        "dias_abiertos_mx",
    ]
    print(json.dumps(run_culling_agent("GM", kpis), indent=2, ensure_ascii=False))
