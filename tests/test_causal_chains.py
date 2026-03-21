"""
Test suite for causal chain functionality.

Tests three layers:
  1. _format_causal_chains()  — unit test, no API needed
  2. LLM synthesis step       — calls OpenAI with mock domain data, checks output structure
  3. Full pipeline (optional) — runs run_master_summary() on a real farm

Usage:
    conda run -n mcp python -m tests.test_causal_chains
    conda run -n mcp python -m tests.test_causal_chains --full   # includes live farm run
"""

from __future__ import annotations

import json
import os
import sys
from dotenv import load_dotenv

load_dotenv()

# ── helpers ─────────────────────────────────────────────────────────────────

def _section(title: str):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")

def _ok(msg: str):
    print(f"  ✅  {msg}")

def _fail(msg: str):
    print(f"  ❌  {msg}")
    sys.exit(1)

def _check(condition: bool, ok_msg: str, fail_msg: str):
    if condition:
        _ok(ok_msg)
    else:
        _fail(fail_msg)


# ── Layer 1: unit test _format_causal_chains ────────────────────────────────

def test_format_causal_chains():
    _section("Layer 1 — _format_causal_chains() unit tests")

    from agents.master_summary_agent import _format_causal_chains

    # Empty input
    result = _format_causal_chains({})
    _check("No significant" in result, "Empty input returns fallback message", f"Unexpected: {result}")

    # Single chain, high strength
    sample = {
        "pct_cetosis": [
            {"kpi": "pct_metritis_primaria", "risk": 0.055, "lag": 2, "cluster_strength": 0.55,
             "src_weight": 0.3, "tgt_weight": 0.25},
        ]
    }
    result = _format_causal_chains(sample)
    _check("pct_cetosis is ANOMALOUS" in result,   "Cause KPI appears in output", result)
    _check("pct_metritis_primaria" in result,       "Effect KPI appears in output", result)
    _check("~2 months" in result,                   "Lag label is correct", result)
    _check("causal strength: high" in result,       "Strength: 0.55 → 'high'", result)

    # Moderate strength
    sample2 = {
        "dias_abiertos_mx": [
            {"kpi": "taza_prenez_21_dias", "risk": 0.04, "lag": 1, "cluster_strength": 0.35,
             "src_weight": 0.2, "tgt_weight": 0.3},
        ]
    }
    result2 = _format_causal_chains(sample2)
    _check("causal strength: moderate" in result2,  "Strength: 0.35 → 'moderate'", result2)
    _check("~1 month" in result2,                   "Lag 1 → '~1 month'", result2)

    # Low strength
    sample3 = {
        "pct_fiebre_de_leche": [
            {"kpi": "pct_cetosis", "risk": 0.02, "lag": 3, "cluster_strength": 0.20,
             "src_weight": 0.1, "tgt_weight": 0.1},
        ]
    }
    result3 = _format_causal_chains(sample3)
    _check("causal strength: low" in result3,       "Strength: 0.20 → 'low'", result3)
    _check("~3 months" in result3,                  "Lag 3 → '~3 months'", result3)

    # Multiple causes
    multi = {
        "pct_cetosis": [
            {"kpi": "pct_metritis_primaria", "risk": 0.05, "lag": 2, "cluster_strength": 0.45, "src_weight": 0.3, "tgt_weight": 0.25},
            {"kpi": "pct_retencion_de_placenta", "risk": 0.03, "lag": 1, "cluster_strength": 0.28, "src_weight": 0.2, "tgt_weight": 0.2},
        ],
        "dias_abiertos_mx": [
            {"kpi": "taza_prenez_21_dias", "risk": 0.06, "lag": 2, "cluster_strength": 0.52, "src_weight": 0.4, "tgt_weight": 0.35},
        ],
    }
    result4 = _format_causal_chains(multi)
    _check(result4.count("ANOMALOUS") == 2,         "Two cause KPIs each get their own ANOMALOUS line", result4)
    _check(result4.count("•") == 3,                 "Three downstream effects total (2 + 1)", result4)

    print("\n  Full output sample:")
    for line in result4.splitlines():
        print(f"    {line}")


# ── Layer 2: LLM synthesis with mock data ───────────────────────────────────

MOCK_DOMAIN_SUMMARIES = {
    "Health": {
        "domain": "Health",
        "summary": "Ketosis is elevated at 18% (benchmark <10%), worsening trend over last 3 months.",
        "issues": [
            "pct_cetosis at 18% (benchmark <10%) — critical_high, worsening trend",
            "pct_fiebre_de_leche at 7% (benchmark <3%) — critical_high, stable trend",
        ],
        "recommendations": {
            "Immediate": ["Review dry-cow nutrition and DCAD balance immediately"],
            "Short": ["Implement systematic ketosis screening at 3, 7, 14 DIM"],
            "Medium": ["Audit transition cow pen stocking density"],
            "Long": ["Consider genetic selection for improved metabolic resilience"],
        },
        "kpis_to_plot": ["pct_cetosis", "pct_fiebre_de_leche"],
    },
    "Fertility": {
        "domain": "Fertility",
        "summary": "21-day pregnancy rate at 14%, below the 21% target. Days open averaging 148 days.",
        "issues": [
            "taza_prenez_21_dias at 14% (benchmark ≥21%) — below_target, worsening trend",
            "dias_abiertos_mx at 148 days (benchmark <120) — above_target, stable trend",
        ],
        "recommendations": {
            "Immediate": ["Review synchronization protocol compliance"],
            "Short": ["Audit heat detection rates and activity monitor calibration"],
            "Medium": ["Consider Presynch-Ovsynch protocol for anovular cows"],
            "Long": ["Genetic selection for improved fertility index"],
        },
        "kpis_to_plot": ["taza_prenez_21_dias", "dias_abiertos_mx"],
    },
}

MOCK_CAUSAL_CHAINS_TEXT = """pct_cetosis is ANOMALOUS → likely downstream effects:
    • pct_metritis_primaria in ~2 months (causal strength: moderate)
    • taza_prenez_21_dias in ~2 months (causal strength: moderate)

dias_abiertos_mx is ANOMALOUS → likely downstream effects:
    • taza_prenez_21_dias in ~2 months (causal strength: high)
    • prod_a_305_del_1a_lact in ~3 months (causal strength: moderate)"""


def test_llm_synthesis():
    _section("Layer 2 — LLM synthesis with mock domain data")

    from openai import OpenAI
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    domain_summaries = json.dumps(MOCK_DOMAIN_SUMMARIES, indent=2, ensure_ascii=False)

    prompt = (
        "You are a senior dairy management consultant and expert in causal farm performance analysis.\n\n"
        "Synthesize the domain analyses and causal chain predictions into a farm-level report.\n\n"
        "You MUST return JSON strictly as:\n"
        "{\n"
        '  "executive_summary": "2-3 sentence overview",\n'
        '  "priority_actions": ["action 1", "action 2"],\n'
        '  "overall_health": "High | Medium | Low",\n'
        '  "domains_overview": {"DomainName": "one sentence"},\n'
        '  "causal_chains": [\n'
        "    {\n"
        '      "cause": "kpi_alias",\n'
        '      "effect": "kpi_alias",\n'
        '      "timeline": "~2 months",\n'
        '      "severity": "high | moderate | low",\n'
        '      "reasoning": "one sentence",\n'
        '      "preventive_action": "one concrete action"\n'
        "    }\n"
        "  ]\n"
        "}\n\n"
        "Rules:\n"
        "- Only include chains where causal strength is moderate or high.\n"
        "- Use exact KPI aliases from the input.\n"
        "- Priority actions ordered by urgency.\n\n"
        "=== Domain Analyses ===\n"
        f"{domain_summaries}\n\n"
        "=== Causal Risk Chains ===\n"
        f"{MOCK_CAUSAL_CHAINS_TEXT}\n"
    )

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are a senior dairy performance strategist."},
            {"role": "user", "content": prompt},
        ],
        response_format={"type": "json_object"},
    )

    result = json.loads(response.choices[0].message.content)

    print("\n  Raw LLM output:")
    print(json.dumps(result, indent=2, ensure_ascii=False))

    # Structural checks
    _check("executive_summary" in result,   "Has executive_summary", str(result.keys()))
    _check("priority_actions" in result,    "Has priority_actions", str(result.keys()))
    _check("overall_health" in result,      "Has overall_health", str(result.keys()))
    _check("domains_overview" in result,    "Has domains_overview", str(result.keys()))
    _check("causal_chains" in result,       "Has causal_chains field", str(result.keys()))

    chains = result.get("causal_chains", [])
    _check(isinstance(chains, list),        "causal_chains is a list", type(chains))
    _check(len(chains) > 0,                 f"At least one chain returned ({len(chains)} total)", "Empty chains list")

    required_keys = {"cause", "effect", "timeline", "severity", "reasoning", "preventive_action"}
    for i, chain in enumerate(chains):
        missing = required_keys - set(chain.keys())
        _check(not missing, f"Chain {i+1} has all required keys", f"Missing: {missing} in {chain}")
        _check(chain.get("severity") in ("high", "moderate", "low"),
               f"Chain {i+1} severity is valid: {chain.get('severity')}",
               f"Invalid severity: {chain.get('severity')}")

    print(f"\n  Causal chains summary ({len(chains)} chains):")
    for c in chains:
        print(f"    {c['cause']} → {c['effect']} | {c['timeline']} | {c['severity']}")
        print(f"      Reason : {c.get('reasoning', '')}")
        print(f"      Action : {c.get('preventive_action', '')}")


# ── Layer 3: full pipeline (optional) ───────────────────────────────────────

def test_full_pipeline(farm_code: str = "GM", months: int = 3):
    _section(f"Layer 3 — Full pipeline on farm '{farm_code}' ({months} months)")

    import asyncio
    from agents.master_summary_agent import run_master_summary

    print("  Running run_master_summary()... (this may take 30-90 seconds)")
    result = asyncio.run(run_master_summary(farm_code=farm_code, language="es", months=months))

    final = result.get("final_summary", {})

    _check("causal_chains" in final,        "final_summary has causal_chains", str(final.keys()))

    chains = final.get("causal_chains", [])
    _check(isinstance(chains, list),        "causal_chains is a list", type(chains))

    if not chains:
        print("  ⚠  No chains returned — this may be expected if no urgent KPIs were found.")
    else:
        _check(len(chains) > 0,             f"{len(chains)} chain(s) returned", "Empty chains")
        required_keys = {"cause", "effect", "timeline", "severity", "reasoning", "preventive_action"}
        for i, chain in enumerate(chains):
            missing = required_keys - set(chain.keys())
            _check(not missing, f"Chain {i+1} has all required keys", f"Missing: {missing}")

        print(f"\n  Causal chains from live run:")
        for c in chains:
            print(f"    {c['cause']} → {c['effect']} | {c['timeline']} | {c['severity']}")
            print(f"      Action: {c.get('preventive_action', '')}")

    print(f"\n  Overall health : {final.get('overall_health', 'N/A')}")
    print(f"  Priority actions ({len(final.get('priority_actions', []))}):")
    for a in final.get("priority_actions", []):
        print(f"    - {a}")


# ── entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    run_full = "--full" in sys.argv

    test_format_causal_chains()
    test_llm_synthesis()

    if run_full:
        test_full_pipeline()
    else:
        print("\n  (Skipping Layer 3 live pipeline test. Run with --full to include it.)")

    print("\n✅ All tests passed.\n")
