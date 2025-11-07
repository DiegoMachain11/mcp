# agents/master_summary_agent.py
import asyncio
import json
import os
from openai import OpenAI

from pre_analyzer_agent import run_pre_analysis
from fertility_agent import run_fertility_agent
from production_agent import run_production_agent
from health_agent import run_health_agent
from calf_agent import run_calf_agent
from culling_agent import run_culling_agent

OPENAI_MODEL = "gpt-4o-mini"
openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


async def run_master_summary(
    farm_code: str = "GM", language: str = "es", months: int = 4
):
    """
    Orchestrates all domain agents to produce a comprehensive farm-level analysis.
    """

    print("🔍 Step 1: Running PreAnalyzer...")
    pre_summary = run_pre_analysis(
        farm_code=farm_code, language=language, months=months
    )
    print("✅ PreAnalyzer completed")

    domains_to_investigate = pre_summary.get("domains_to_investigate", {})

    # --- Step 2: Run agents concurrently based on what PreAnalyzer found ---
    print("⚙️ Step 2: Launching domain-specific agents...")
    tasks = []

    if "Fertility" in domains_to_investigate:
        tasks.append(
            asyncio.to_thread(
                run_fertility_agent,
                farm_code,
                domains_to_investigate["Fertility"],
                language,
                months,
            )
        )

    if "Production" in domains_to_investigate:
        tasks.append(
            asyncio.to_thread(
                run_production_agent,
                farm_code,
                domains_to_investigate["Production"],
                language,
                months,
            )
        )

    if "Health" in domains_to_investigate:
        tasks.append(
            asyncio.to_thread(
                run_health_agent,
                farm_code,
                domains_to_investigate["Health"],
                language,
                months,
            )
        )

    if "Calf Raising" in domains_to_investigate:
        tasks.append(
            asyncio.to_thread(
                run_calf_agent,
                farm_code,
                domains_to_investigate["Calf Raising"],
                language,
                months,
            )
        )

    if "Culling" in domains_to_investigate:
        tasks.append(
            asyncio.to_thread(
                run_culling_agent,
                farm_code,
                domains_to_investigate["Culling"],
                language,
                months,
            )
        )

    results = await asyncio.gather(*tasks)
    print("✅ Domain agents completed")

    # --- Step 3: Combine all results ---
    combined = {
        "farm_code": farm_code,
        "overview": pre_summary.get("summary", ""),
        "domains": {r["domain"]: r for r in results if "domain" in r},
        "urgent_kpis": pre_summary.get("urgent_kpis", []),
    }

    # --- Step 4: Use LLM to produce one final narrative summary ---
    print("🧠 Step 4: Synthesizing overall summary...")
    domain_summaries = json.dumps(combined["domains"], indent=2, ensure_ascii=False)

    prompt = f"""
    You are a senior dairy management consultant.

    Combine the following domain analyses into one coherent report.
    Highlight:
    - Overall situation
    - Key performance risks
    - Strategic recommendations
    - Priority actions for the next 3 months
    - Confidence level (Low/Medium/High)

    Return JSON strictly as:
    {{
        "executive_summary": "...high-level insights...",
        "priority_actions": [ "...", "..." ],
        "overall_health": "High | Medium | Low",
        "domains_overview": {{ <short summary of each domain> }}
    }}

    Domain Analyses:
    {domain_summaries}
    """

    response = openai_client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=[
            {
                "role": "system",
                "content": "You are a senior dairy performance strategist.",
            },
            {"role": "user", "content": prompt},
        ],
        response_format={"type": "json_object"},
    )

    overall = json.loads(response.choices[0].message.content)

    combined["final_summary"] = overall

    print("✅ Master Summary ready")
    return combined


if __name__ == "__main__":
    import pprint

    result = asyncio.run(run_master_summary(farm_code="GM", language="es", months=4))
    pprint.pprint(result)
