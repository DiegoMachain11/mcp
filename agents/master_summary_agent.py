# agents/master_summary_agent.py
import asyncio
import json
import os
import sys
import time
from typing import Optional

from openai import OpenAI

from agents.pre_analyzer_agent import run_pre_analysis
from agents.fertility_agent import run_fertility_agent
from agents.production_agent import run_production_agent
from agents.health_agent import run_health_agent
from agents.calf_agent import run_calf_agent
from agents.culling_agent import run_culling_agent
from agents.helpers import normalize_kpi_list
from agents.domain_config import build_domain_kpi_list

try:
    from pdf_reporter import generate_master_summary_pdf
except ImportError:  # pragma: no cover - optional dependency
    generate_master_summary_pdf = None

OPENAI_MODEL = "gpt-4o-mini"
openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


class _ProgressBar:
    def __init__(self, label: str, width: int = 28):
        self.label = label
        self.width = width
        self.start_time = None
        self.current = 0.0

    def start(self, message: str = "Iniciando..."):
        self.start_time = time.perf_counter()
        self.update(0.0, message)

    def update(self, progress: float, message: str = ""):
        progress = min(max(progress, 0.0), 1.0)
        self.current = progress
        elapsed = time.perf_counter() - self.start_time if self.start_time else 0.0
        eta = (elapsed / progress - elapsed) if progress > 0 else None

        filled = int(self.width * progress)
        bar = "#" * filled + "-" * (self.width - filled)
        status = (
            f"\r{self.label}: [{bar}] {progress*100:5.1f}% | " f"tiempo {elapsed:5.1f}s"
        )
        if eta is not None and eta > 0:
            status += f" | ETA {eta:5.1f}s"
        if message:
            status += f" | {message}"

        sys.stdout.write(status)
        sys.stdout.flush()

    def finish(self, message: str = "Completado"):
        self.update(1.0, message)
        sys.stdout.write("\n")
        sys.stdout.flush()


async def run_master_summary(
    farm_code: str = "GM",
    language: str = "es",
    months: int = 4,
    pdf_output_dir: Optional[str] = None,
    pdf_filename: Optional[str] = None,
    triage_kpis: Optional[list[str]] = None,
):
    """
    Orchestrates all domain agents to produce a comprehensive farm-level analysis.
    """

    print("🔍 Step 1: Running PreAnalyzer...")
    progress = _ProgressBar("   PreAnalyzer")
    progress.start("Recopilando métricas")

    def _prepare_domain_kpis(domain_name: str, suggested):
        normalized = normalize_kpi_list(suggested or [])
        return build_domain_kpi_list(domain_name, normalized)

    def _progress_hook(value: float, msg: str):
        progress.update(value, msg)

    pre_summary = run_pre_analysis(
        farm_code=farm_code,
        language=language,
        months=months,
        progress_callback=_progress_hook,
        triage_kpis=triage_kpis,
    )
    progress.finish("Listo")
    print("✅ PreAnalyzer completed")

    domains_to_investigate = pre_summary.get("domains_to_investigate", {})
    domains_in_good_state = pre_summary.get("domains_in_good_state", {})

    # --- Step 2: Run agents concurrently based on what PreAnalyzer found ---
    print("⚙️ Step 2: Launching domain-specific agents...")
    tasks = []

    if "Fertility" in domains_to_investigate:
        fertility_kpis = _prepare_domain_kpis(
            "Fertility", domains_to_investigate["Fertility"]
        )
        tasks.append(
            asyncio.to_thread(
                run_fertility_agent,
                farm_code,
                fertility_kpis,
                language,
                months,
            )
        )

    if "Production" in domains_to_investigate:
        production_kpis = _prepare_domain_kpis(
            "Production", domains_to_investigate["Production"]
        )
        tasks.append(
            asyncio.to_thread(
                run_production_agent,
                farm_code,
                production_kpis,
                language,
                months,
            )
        )

    if "Health" in domains_to_investigate:
        health_kpis = _prepare_domain_kpis("Health", domains_to_investigate["Health"])
        tasks.append(
            asyncio.to_thread(
                run_health_agent,
                farm_code,
                health_kpis,
                language,
                months,
            )
        )

    if "Calf Raising" in domains_to_investigate:
        calf_kpis = _prepare_domain_kpis(
            "Calf Raising", domains_to_investigate["Calf Raising"]
        )
        tasks.append(
            asyncio.to_thread(
                run_calf_agent,
                farm_code,
                calf_kpis,
                language,
                months,
            )
        )

    if "Culling" in domains_to_investigate:
        culling_kpis = _prepare_domain_kpis(
            "Culling", domains_to_investigate["Culling"]
        )
        tasks.append(
            asyncio.to_thread(
                run_culling_agent,
                farm_code,
                culling_kpis,
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

    overall = json.loads(response.choices[0].message.content)

    combined["final_summary"] = overall

    if pdf_output_dir and generate_master_summary_pdf:
        try:
            pdf_path = generate_master_summary_pdf(
                combined,
                output_dir=pdf_output_dir,
                filename=pdf_filename,
            )
            combined["pdf_path"] = str(pdf_path)
            print(f"📄 PDF saved to {pdf_path}")
        except Exception as exc:
            print(f"⚠️ Unable to generate PDF: {exc}")
    elif pdf_output_dir:
        print("⚠️ PDF output requested but pdf_reporter is not available.")

    print("✅ Master Summary ready")
    return combined


if __name__ == "__main__":
    import pprint

    result = asyncio.run(run_master_summary(farm_code="GM", language="es", months=4))
    pprint.pprint(result)
