# pre_analyzer_agent.py
import json
import logging
from openai import OpenAI
import requests
import os

OPENAI_MODEL = "gpt-4o-mini"
openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
BRIDGE_URL = "http://localhost:8090"


def run_pre_analysis(farm_code="GM", language="es", months=4):
    resp = requests.get(
        f"{BRIDGE_URL}/summarize_kpis",
        params={"farm_code": farm_code, "language": language, "months": months},
    )
    logging.info(f"Summarize KPIs response: {resp.text}")
    resp.raise_for_status()
    data = resp.json()

    print("KPI Summary Data = ", data)
    data = data.get("result", {})
    summaries_dict = data.get("summaries", {})
    # Convert dict of dicts → list of {metric, mean, std, ...}
    summaries = [
        {"metric": metric, **stats} for metric, stats in summaries_dict.items()
    ]

    print("Summaries = ", summaries)

    prompt = f"""
    You are a dairy KPI analyst.
    Given the following KPI summaries (mean, trend, std, min, max):
    - Identify which KPIs show anomalies or significant worsening trends.
    - Group them by domain: Fertility, Production, Health, Calf Raising, Culling, Feeding.
    - For each group, explain key risks and urgency (High / Medium / Low).
    - Return JSON:
    {{
        "urgent_kpis": [list of kpi names],
        "domains_to_investigate": {{
            "Fertility": [...],
            "Production": [...],
            ...
        }},
        "summary": "plain text summary"
    }}
    KPI summaries:
    {json.dumps(summaries, indent=2, ensure_ascii=False)}
    """

    response = openai_client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=[
            {
                "role": "system",
                "content": "You are an expert dairy production KPI triage assistant.",
            },
            {"role": "user", "content": prompt},
        ],
        response_format={"type": "json_object"},
    )
    return json.loads(response.choices[0].message.content)


response = run_pre_analysis()

print("Pre-analysis result:")
print(json.dumps(response, indent=2, ensure_ascii=False))
