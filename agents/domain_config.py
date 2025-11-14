import json
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Optional, Sequence


def _slugify(text: str) -> str:
    slug = (
        text.strip()
        .lower()
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
    while "__" in slug:
        slug = slug.replace("__", "_")
    return slug.strip("_")


@lru_cache(maxsize=1)
def _load_domain_config() -> Dict[str, Dict]:
    config_path = Path(__file__).resolve().parents[1] / "data" / "domain_kpis.json"
    if not config_path.exists():
        return {}
    with config_path.open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    # Normalize structure
    for section, entry in data.items():
        entry.setdefault("description", section)
        entry.setdefault("kpis", [])
    return data


def get_domain_kpi_aliases(domain: str) -> List[str]:
    """Return alias list for a domain/section name."""
    config = _load_domain_config()
    if not config:
        return []
    if domain in config:
        entries = config[domain]["kpis"]
    else:
        target = _slugify(domain)
        entries = None
        for key, entry in config.items():
            if (
                _slugify(key) == target
                or _slugify(entry.get("description", "")) == target
            ):
                entries = entry["kpis"]
                break
        if entries is None:
            return []

    aliases: List[str] = []
    for item in entries:
        alias = item.get("alias") or _slugify(
            item.get("name", "") or item.get("code", "")
        )
        if alias and alias not in aliases:
            aliases.append(alias)
    return aliases


def build_domain_kpi_list(
    domain: str, prioritized: Optional[Sequence[str]]
) -> List[str]:
    """Merge prioritized KPIs with the domain defaults, preserving order."""
    merged: List[str] = []

    for bucket in (prioritized or [], get_domain_kpi_aliases(domain)):
        for alias in bucket:
            if alias and alias not in merged:
                merged.append(alias)
    return merged
