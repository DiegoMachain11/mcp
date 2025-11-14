import asyncio
import json
from typing import Dict, Optional, TypedDict

from langgraph.graph import StateGraph, END
from openai import OpenAI
from dotenv import load_dotenv

from .calf_agent import run_calf_agent
from .culling_agent import run_culling_agent
from .fertility_agent import run_fertility_agent
from .domain_config import build_domain_kpi_list
from .health_agent import run_health_agent
from .master_summary_agent import OPENAI_MODEL
from .pre_analyzer_agent import run_pre_analysis
from .production_agent import run_production_agent

try:
    from pdf_reporter import generate_master_summary_pdf
except ImportError:  # pragma: no cover
    generate_master_summary_pdf = None

load_dotenv()
openai_client = OpenAI()


class AgentState(TypedDict, total=False):
    farm_code: str
    language: str
    months: int
    pre_analysis: dict
    domain_results: Dict[str, dict]
    urgent_kpis: list
    overview: str
    combined: dict


def pre_analysis_node(state: AgentState) -> AgentState:
    result = run_pre_analysis(
        farm_code=state["farm_code"],
        language=state.get("language", "es"),
        months=state.get("months", 4),
    )
    state["pre_analysis"] = result
    state["urgent_kpis"] = result.get("urgent_kpis", [])
    state["overview"] = result.get("summary", "")
    return state


def _prepare_domain_kpis(domain: str, suggested) -> list:
    return build_domain_kpi_list(domain, suggested or [])


async def _gather_domain_agents(
    farm_code: str,
    language: str,
    months: int,
    domains_to_investigate: dict,
) -> Dict[str, dict]:
    tasks = []

    if "Fertility" in domains_to_investigate:
        kpis = _prepare_domain_kpis("Fertility", domains_to_investigate["Fertility"])
        tasks.append(
            asyncio.to_thread(run_fertility_agent, farm_code, kpis, language, months)
        )

    if "Production" in domains_to_investigate:
        kpis = _prepare_domain_kpis("Production", domains_to_investigate["Production"])
        tasks.append(
            asyncio.to_thread(run_production_agent, farm_code, kpis, language, months)
        )

    if "Health" in domains_to_investigate:
        kpis = _prepare_domain_kpis("Health", domains_to_investigate["Health"])
        tasks.append(
            asyncio.to_thread(run_health_agent, farm_code, kpis, language, months)
        )

    if "Calf Raising" in domains_to_investigate:
        kpis = _prepare_domain_kpis(
            "Calf Raising", domains_to_investigate["Calf Raising"]
        )
        tasks.append(
            asyncio.to_thread(run_calf_agent, farm_code, kpis, language, months)
        )

    if "Culling" in domains_to_investigate:
        kpis = _prepare_domain_kpis("Culling", domains_to_investigate["Culling"])
        tasks.append(
            asyncio.to_thread(run_culling_agent, farm_code, kpis, language, months)
        )

    if not tasks:
        return {}

    results = await asyncio.gather(*tasks)
    return {item["domain"]: item for item in results if isinstance(item, dict)}


def domain_agents_node(state: AgentState) -> AgentState:
    pre_summary = state.get("pre_analysis") or {}
    domains = pre_summary.get("domains_to_investigate", {})
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        domain_results = loop.run_until_complete(
            _gather_domain_agents(
                state["farm_code"],
                state.get("language", "es"),
                state.get("months", 4),
                domains,
            )
        )
    finally:
        loop.close()
        asyncio.set_event_loop(None)

    state["domain_results"] = domain_results
    return state


def final_summary_node(state: AgentState) -> AgentState:
    pre_summary = state.get("pre_analysis") or {}
    domain_results = state.get("domain_results") or {}

    combined = {
        "farm_code": state["farm_code"],
        "overview": state.get("overview", ""),
        "domains": domain_results,
        "urgent_kpis": state.get("urgent_kpis", []),
    }

    domain_summaries = json.dumps(domain_results, indent=2, ensure_ascii=False)
    prompt = f"""
    You are a senior dairy management consultant.

    Combine the following domain analyses into one coherent report.
    Highlight:
    - Overall situation
    - Key performance risks
    - Return exact percentages and numbers where relevant.
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

    combined["final_summary"] = json.loads(response.choices[0].message.content)
    state["combined"] = combined
    return state


def build_agents_graph(pdf_output_dir: Optional[str] = None):
    graph = StateGraph(AgentState)
    graph.add_node("pre_analysis", pre_analysis_node)
    graph.add_node("domain_agents", domain_agents_node)
    graph.add_node("final_summary", final_summary_node)

    graph.set_entry_point("pre_analysis")
    graph.add_edge("pre_analysis", "domain_agents")
    graph.add_edge("domain_agents", "final_summary")
    graph.add_edge("final_summary", END)

    compiled = graph.compile()

    if pdf_output_dir and generate_master_summary_pdf:

        def run_with_pdf(config: AgentState):
            result = compiled.invoke(config)
            summary = result.get("combined")
            if summary:
                generate_master_summary_pdf(summary, output_dir=pdf_output_dir)
            return result

        compiled.invoke_with_pdf = run_with_pdf  # type: ignore[attr-defined]

    return compiled


if __name__ == "__main__":
    graph = build_agents_graph()
    output = graph.invoke({"farm_code": "GM", "language": "es", "months": 4})
    print(json.dumps(output.get("combined", {}), indent=2, ensure_ascii=False))
