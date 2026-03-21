"""
Farm run history — save and load past analysis results per farm.

Each run saves a compact record to:
    data/farm_history/{farm_code}/YYYY-MM-DD_HHMMSS.json

The record stores only the conclusions (not raw KPI data), making it
small enough to inject into future LLM prompts as historical context.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
HISTORY_DIR = REPO_ROOT / "data" / "farm_history"


# ── Save ─────────────────────────────────────────────────────────────────────

def save_run(combined: dict, months: int = 0) -> Path:
    """
    Extract the key conclusions from a master summary result and persist them.

    Args:
        combined: The dict returned by run_master_summary().
        months:   Analysis window in months (for context in future prompts).

    Returns:
        Path to the saved file.
    """
    farm_code = combined.get("farm_code", "unknown")
    final = combined.get("final_summary", {})

    record = {
        "run_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "run_timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "months_analyzed": months,
        "overall_health": final.get("overall_health", "Unknown"),
        "urgent_kpis": combined.get("urgent_kpis", []),
        "executive_summary": final.get("executive_summary", ""),
        "priority_actions": final.get("priority_actions", []),
        "causal_chains": [
            {
                "cause": c.get("cause", ""),
                "effect": c.get("effect", ""),
                "timeline": c.get("timeline", ""),
                "severity": c.get("severity", ""),
                "preventive_action": c.get("preventive_action", ""),
            }
            for c in final.get("causal_chains", [])
        ],
        "domain_issues": {
            domain: payload.get("issues", [])
            for domain, payload in combined.get("domains", {}).items()
        },
    }

    farm_dir = HISTORY_DIR / farm_code
    farm_dir.mkdir(parents=True, exist_ok=True)

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    existing = list(farm_dir.glob(f"{today}_*.json"))
    if existing:
        return existing[0]

    filename = today + datetime.now(timezone.utc).strftime("_%H%M%S") + ".json"
    path = farm_dir / filename

    with path.open("w", encoding="utf-8") as f:
        json.dump(record, f, indent=2, ensure_ascii=False)

    return path


# ── Load ─────────────────────────────────────────────────────────────────────

def load_recent_runs(farm_code: str, n: int = 2) -> list[dict]:
    """
    Load the N most recent history records for a farm, newest first.

    Returns an empty list if no history exists yet.
    """
    farm_dir = HISTORY_DIR / farm_code
    if not farm_dir.exists():
        return []

    files = sorted(farm_dir.glob("*.json"), reverse=True)
    records = []
    for f in files[:n]:
        try:
            with f.open("r", encoding="utf-8") as fh:
                records.append(json.load(fh))
        except Exception:
            continue

    return records


# ── Format for LLM prompt ─────────────────────────────────────────────────────

def format_history_for_prompt(records: list[dict]) -> str:
    """
    Format past run records as a readable block to inject into the synthesis prompt.

    The LLM uses this to:
    - Identify persistent problems (flagged across multiple runs)
    - Assess whether previous recommendations appear to have had effect
    - Avoid repeating identical advice without acknowledging prior context
    """
    if not records:
        return ""

    lines = ["=== HISTORICAL CONTEXT (previous analyses for this farm) ===",
             "Use this to identify whether previously flagged issues persist, improved, or worsened.\n"]

    for rec in records:
        date = rec.get("run_date", "unknown date")
        health = rec.get("overall_health", "?")
        months_analyzed = rec.get("months_analyzed")
        window_str = f" | Window: {months_analyzed} months" if months_analyzed else ""
        lines.append(f"[Run: {date}{window_str} | Overall health: {health}]")

        urgent = rec.get("urgent_kpis", [])
        if urgent:
            lines.append(f"  Urgent KPIs: {', '.join(urgent)}")

        actions = rec.get("priority_actions", [])
        if actions:
            lines.append("  Priority actions recommended:")
            for a in actions[:3]:  # cap at 3 to save tokens
                lines.append(f"    - {a}")

        chains = rec.get("causal_chains", [])
        if chains:
            lines.append("  Causal risks flagged:")
            for c in chains[:3]:
                lines.append(
                    f"    - {c['cause']} -> {c['effect']} "
                    f"in {c['timeline']} ({c['severity']})"
                )

        domain_issues = rec.get("domain_issues", {})
        if domain_issues:
            lines.append("  Issues by domain:")
            for domain, issues in domain_issues.items():
                if issues:
                    lines.append(f"    {domain}: {issues[0]}")  # first issue per domain

        lines.append("")

    return "\n".join(lines).strip()
