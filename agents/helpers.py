from __future__ import annotations

import json
import unicodedata
from functools import lru_cache
from pathlib import Path


def _slugify(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    alias = (
        normalized.lower()
        .replace("%", "pct")
        .replace("<", "lt")
        .replace(">", "gt")
        .replace("+", "plus")
        .replace("(", "")
        .replace(")", "")
        .replace(".", "")
        .replace(",", "")
        .replace("/", "_")
        .replace(" ", "_")
        .replace("-", "_")
    )
    while "__" in alias:
        alias = alias.replace("__", "_")
    return alias.strip("_")


def load_causal_graph():
    with open("graph_creation/causal_graph.json", "r") as f:
        causal_graph = json.load(f)
    return causal_graph


def load_causal_kpi_graph():
    with open("graph_creation/causal_kpi_graph.json", "r") as f:
        causal_kpi_graph = json.load(f)
    return causal_kpi_graph


def load_cluster_members():
    with open("graph_creation/cluster_members.json", "r") as f:
        cm = json.load(f)
    # Convert keys back to integers
    return {int(k): v for k, v in cm.items()}


def get_downstream_clusters(cluster_name, causal_graph):
    return causal_graph.get(cluster_name, {})


def find_cluster_of_kpi(kpi, cluster_members):
    for cl, kpis in cluster_members.items():
        if kpi in kpis:
            return cl
    return None


def get_kpi_level_risks(
    kpi_name: str, causal_kpi_graph: dict, min_risk: float = 0.05, max_results: int = 10
):
    """
    Return the top KPI-level causal risks for a given KPI.
    Entirely based on the precomputed causal_kpi_graph.
    Fast, no API calls.
    """
    if kpi_name not in causal_kpi_graph:
        return []

    risks = [r for r in causal_kpi_graph[kpi_name] if r["risk"] >= min_risk]

    # Sort highest risk first
    risks.sort(key=lambda x: x["risk"], reverse=True)

    return [r["kpi"] for r in risks[:max_results]]


def get_at_risk_kpis(
    kpi_name,
    cluster_members,
    causal_graph,
    min_strength: float = 0.15,
    max_kpis_per_cluster: int = 5,
):
    """
    Returns a filtered list of downstream at-risk KPIs based on
    causal impact strength, lag, and effect size thresholds.

    Much more selective than the naive version.
    """
    cl = find_cluster_of_kpi(kpi_name, cluster_members)
    if cl is None:
        return []

    cluster_name = f"Cluster_{cl}"
    if cluster_name not in causal_graph:
        return []

    risky_kpis = []
    for tgt_cluster, info in causal_graph[cluster_name].items():

        # 1. Filter by causal strength
        if abs(info["strength"]) < min_strength:
            continue

        # 2. Select only the top few KPIs in that downstream cluster
        target_kpis = info["target_kpis"][:max_kpis_per_cluster]

        risky_kpis.extend(target_kpis)

    return risky_kpis


@lru_cache(maxsize=1)
def alias_name_maps():
    map_path = Path(__file__).resolve().parents[1] / "data" / "kpi_alias_map.json"
    alias_to_name = {}
    name_to_alias = {}

    if map_path.exists():
        with map_path.open("r", encoding="utf-8") as fh:
            payload = json.load(fh)
            alias_to_name = payload.get("alias_to_name", {})
            name_to_alias = payload.get("name_to_alias", {})

    if not alias_to_name or not name_to_alias:
        members = load_cluster_members()
        for kpis in members.values():
            for original in kpis:
                alias = _slugify(original)
                alias_to_name.setdefault(alias, original)
                name_to_alias.setdefault(original, alias)

    # Ensure basic reverse mappings exist
    return alias_to_name, name_to_alias


def _extract_rows(resp_json):
    if isinstance(resp_json, dict):
        if "result" in resp_json:
            return resp_json["result"]
    if isinstance(resp_json, list):
        return resp_json
    return []


def normalize_kpi_list(kpis):
    """
    Ensure KPI payloads are a simple ordered list of unique string names.

    PreAnalyzer sometimes returns rich objects like
    {"metric": "...", "urgency": "..."} instead of bare strings. Downstream
    agents only need the column names, so strip extra metadata while keeping
    order and skipping duplicates.
    """
    if not kpis:
        return []

    normalized = []
    seen = set()
    preferred_keys = ("metric", "kpi", "name", "id")

    for item in kpis:
        candidate = None

        if isinstance(item, str):
            candidate = item
        elif isinstance(item, dict):
            for key in preferred_keys:
                value = item.get(key)
                if isinstance(value, str):
                    candidate = value
                    break
            if candidate is None:
                for value in item.values():
                    if isinstance(value, str):
                        candidate = value
                        break

        if candidate and candidate not in seen:
            normalized.append(candidate)
            seen.add(candidate)

    return normalized
