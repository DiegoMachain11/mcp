import json, os, requests
from openai import OpenAI

from agents.helpers import _extract_rows, normalize_kpi_list
from agents.domain_config import build_domain_kpi_list

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

    prompt = f"""
    You are an expert in dairy calf and heifer management.

    Analyze KPIs for farm '{farm_code}' focusing on:
    - Growth rates and weight gain efficiency
    - Mortality in hutches and pre/post-weaning phases
    - Heifer fertility and age at first service
    - Management or feeding issues affecting replacements
    - Return exact percentages and numbers where relevant.

    Return JSON:
    {{
      "domain":"Calf Raising",
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
