import json, os, requests
from openai import OpenAI

from helpers import _extract_rows, normalize_kpi_list
from domain_config import build_domain_kpi_list

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

    data = [{k: r.get(k) for k in ["Date", *kpi_names]} for r in rows]
    prompt = f"""
    You are a dairy herd health specialist.
    Analyze health KPIs for farm '{farm_code}'.

    Focus areas:
    - Metabolic diseases (milk fever, ketosis)
    - Reproductive infections (metritis, retained placenta)
    - Lameness, digestive disorders, and overall morbidity
    - Risk patterns and intervention priorities

    Return JSON:
    {{
      "domain":"Health",
      "summary":"...",
      "issues":["..."],
      "recommendations":{{"Immediate":[],"Short":[],"Medium":[],"Long":[]}},
      "kpis_to_plot":{json.dumps(kpi_names)}
    }}

    Data sample:
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
