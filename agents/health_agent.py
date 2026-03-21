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


def run_health_agent(farm_code, kpis, language="es", months=3):
    normalized_kpis = normalize_kpi_list(kpis)
    kpi_names = build_domain_kpi_list("Health", normalized_kpis)

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

    print("Health rows fetched:", rows)

    data = [{k: r.get(k) for k in ["Date", *kpi_names]} for r in rows]

    # --- Pre-process signals and retrieve scientific context ---
    signals = compute_kpi_signals(rows, kpi_names)
    signals_text = format_signals_for_prompt(signals)
    rag_query = build_rag_query_from_signals(signals, "Health")
    rag_context = get_rag_context(rag_query, domain="Health")

    rag_section = (
        f"\n=== SCIENTIFIC CONTEXT (from research literature) ===\n{rag_context}\n"
        if rag_context
        else ""
    )

    prompt = f"""
    You are a dairy herd veterinarian specializing in transition cow health with 20 years of experience.
    Analyze health KPIs for farm '{farm_code}'.

    Focus areas:
    - Metabolic diseases (milk fever/hypocalcemia, ketosis/hyperketonemia)
    - Reproductive infections (metritis, retained placenta)
    - Lameness, digestive disorders, and overall morbidity
    - Risk patterns and intervention priorities
    - Return exact percentages and deviation from benchmarks where relevant.

    Use the `last_value` from the KPI SIGNAL ANALYSIS as the current value for each KPI.
    When reporting issues, always state the current value and the benchmark it violates.
    If a KPI shows `data_quality: poor` or `sparse`, note the limited data but still report the last known value.
    Do NOT invent values or say "not provided" — only report what the signal analysis shows.
    Ground your recommendations in the scientific context when available.

    Return JSON:
    {{
      "domain":"Health",
      "summary":"...",
      "issues":["Each issue as a plain string, e.g.: 'Ketosis at 18% (benchmark <10%) — critical_high, worsening trend'"],
      "recommendations":{{"Immediate":[],"Short":[],"Medium":[],"Long":[]}},
      "kpis_to_plot":{json.dumps(kpi_names)}
    }}

=== BENCHMARK REFERENCE RANGES ===
  pct_cetosis                  : target <10% (concern >15%)
  pct_fiebre_de_leche          : target <3% (concern >6%)
  pct_metritis_primaria        : target <15% (concern >25%)
  pct_retencion_de_placenta    : target <8% (concern >14%)
  pct_vacas_c_prob_digestivos  : target <5% (concern >10%)
  pct_vacas_c_prob_locomotores : target <10% (concern >20%)

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
                "content": "You are a dairy herd veterinarian analyzing herd health data.",
            },
            {"role": "user", "content": prompt},
        ],
        response_format={"type": "json_object"},
    )
    return json.loads(r.choices[0].message.content)


if __name__ == "__main__":
    kpis = [
        "pct_fiebre_de_leche",
        "pct_retencion_de_placenta",
        "pct_metritis_primaria",
        "pct_cetosis",
        "pct_vacas_c_prob_digestivos",
        "pct_vacas_c_prob_locomotores",
    ]
    print(json.dumps(run_health_agent("GM", kpis), indent=2, ensure_ascii=False))
