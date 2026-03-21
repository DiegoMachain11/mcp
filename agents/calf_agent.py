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


def run_calf_agent(farm_code, kpis, language="es", months=3):
    normalized_kpis = normalize_kpi_list(kpis)
    kpi_names = build_domain_kpi_list("Calf Raising", normalized_kpis)

    params = {"farm_code": farm_code, "language": language, "months": months}
    if kpi_names:
        params["selected_kpis"] = kpi_names

    resp = requests.get(f"{BRIDGE_URL}/get_farm_kpis", params=params)
    resp.raise_for_status()
    rows = _extract_rows(resp.json())
    if not rows:
        return {
            "domain": "Calf Raising",
            "summary": "No KPI data available for analysis.",
            "issues": [],
            "recommendations": {"Immediate": [], "Short": [], "Medium": [], "Long": []},
            "kpis_to_plot": kpi_names,
        }

    print("Calf Raising rows fetched:", rows)
    data = [{k: r.get(k) for k in ["Date", *kpi_names]} for r in rows]

    # --- Pre-process signals and retrieve scientific context ---
    signals = compute_kpi_signals(rows, kpi_names)
    signals_text = format_signals_for_prompt(signals)
    rag_query = build_rag_query_from_signals(signals, "Calf Raising")
    rag_context = get_rag_context(rag_query, domain="Calf Raising")

    rag_section = (
        f"\n=== SCIENTIFIC CONTEXT (from research literature) ===\n{rag_context}\n"
        if rag_context
        else ""
    )

    prompt = f"""
    You are an expert in dairy calf and heifer development with deep knowledge of preweaning nutrition.

    Analyze KPIs for farm '{farm_code}' focusing on:
    - Growth rates and weight gain efficiency (vs. target ≥650 g/day birth–weaning)
    - Mortality in hutches and pre/post-weaning phases
    - Heifer fertility and age at first service
    - Management or feeding issues affecting replacements
    - Return exact percentages and deviation from benchmarks where relevant.

    Use the `last_value` from the KPI SIGNAL ANALYSIS as the current value for each KPI.
    When reporting issues, always state the current value and the benchmark it violates.
    If a KPI shows `data_quality: poor` or `sparse`, note the limited data but still report the last known value.
    Do NOT invent values or say "not provided" — only report what the signal analysis shows.
    Ground your recommendations in the scientific context when available.

    Before writing your JSON, reason in this order:
    1. Which KPIs violate their benchmark? (compare last_value to reference ranges)
    2. For each violation: is the trend worsening, stable, or improving?
    3. Are any flagged KPIs causally linked to each other?
    4. What is the single most urgent action?

    Return JSON:
    {{
      "domain":"Calf Raising",
      "summary":"...",
      "issues":["Each issue as a plain string, e.g.: 'Daily weight gain at 420 g/day (benchmark ≥650) — below_target, worsening trend'"],
      "recommendations":{{"Immediate":[],"Short":[],"Medium":[],"Long":[]}},
      "kpis_to_plot":{json.dumps(kpi_names)}
    }}

=== BENCHMARK REFERENCE RANGES ===
  ganancia_peso_diaria_nac_vs_destete      : target ≥650 g/day (concern <500)
  eficiencia_de_ganancia_de_peso           : target ≥0.55 (concern <0.45)
  pct_becerras_jaulas_muertas_lt_1_meses   : target <3% (concern >6%)
  pct_becerras_muertas_2_13_meses          : target <2% (concern >5%)
  fertilidad_en_vaquillas                  : target ≥65% (concern <50%)
  edad_1er_servicio_gt_15                  : target <15% first service after 15 months (concern >30%)

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
                "content": "You are a calf and heifer development specialist.",
            },
            {"role": "user", "content": prompt},
        ],
        response_format={"type": "json_object"},
    )
    return json.loads(r.choices[0].message.content)


if __name__ == "__main__":
    kpis = [
        "ganancia_peso_diaria_nac_vs_destete",
        "eficiencia_de_ganancia_de_peso",
        "pct_becerras_jaulas_muertas_lt_1_meses",
        "pct_becerras_muertas_2_13_meses",
        "fertilidad_en_vaquillas",
        "edad_1er_servicio_gt_15",
    ]
    print(json.dumps(run_calf_agent("GM", kpis), indent=2, ensure_ascii=False))
