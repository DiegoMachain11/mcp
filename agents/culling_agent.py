import json, os, requests
from openai import OpenAI

from helpers import _extract_rows

BRIDGE_URL = "http://localhost:8090"
OPENAI_MODEL = "gpt-4o-mini"
openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


def run_culling_agent(farm_code, kpis, language="es", months=3):
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

    data = [{k: r.get(k) for k in ["Date", *kpis]} for r in rows]

    prompt = f"""
    You are an expert in dairy herd structure and longevity analysis.

    Analyze culling and mortality KPIs for farm '{farm_code}'.
    Focus on:
    - Early-lactation culling and mortality
    - Culling causes and age distribution
    - Long-term retention and replacement balance
    - Strategies to reduce involuntary culling

    Return JSON:
    {{
      "domain":"Culling",
      "summary":"...",
      "issues":["..."],
      "recommendations":{{"Immediate":[],"Short":[],"Medium":[],"Long":[]}},
      "kpis_to_plot":{json.dumps(kpis)}
    }}

    Data sample:
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
