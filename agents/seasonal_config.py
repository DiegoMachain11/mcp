"""
Static seasonal context for Mexican dairy farms.

Provides domain-specific expectations by season so LLM agents can distinguish
normal seasonal variation from true management problems.

Mexican dairy seasons (northern/central regions):
  - Dry-cool:  Nov–Feb  (mild temps, good production, breeding season)
  - Dry-hot:   Mar–May  (rising heat, transition to stress)
  - Rainy-warm: Jun–Sep (heat stress peak, humidity, mud, flies)
  - Transition: Oct     (cooling, recovery, prep for dry season)
"""

from __future__ import annotations


def _month_to_season(month: int) -> str:
    if month in (11, 12, 1, 2):
        return "dry_cool"
    if month in (3, 4, 5):
        return "dry_hot"
    if month in (6, 7, 8, 9):
        return "rainy_warm"
    return "transition"


def _season_label(season: str) -> str:
    return {
        "dry_cool": "Dry-cool (Nov-Feb)",
        "dry_hot": "Dry-hot (Mar-May)",
        "rainy_warm": "Rainy-warm (Jun-Sep)",
        "transition": "Transition (Oct)",
    }[season]


# Domain × Season context blocks
# Each entry is a concise paragraph the LLM can use to calibrate its analysis.

_DOMAIN_SEASON: dict[tuple[str, str], str] = {
    # ── Fertility ──
    ("Fertility", "dry_cool"): (
        "Best fertility season. Cooler temperatures favor estrus expression and embryo survival. "
        "Conception rates should be at or above annual benchmarks. If fertility is low now, "
        "it signals a management or health problem, not a seasonal effect."
    ),
    ("Fertility", "dry_hot"): (
        "Fertility begins to decline as temperatures rise. Heat stress reduces estrus intensity "
        "and duration, lowers conception rates by 10-20%, and increases early embryonic loss. "
        "Farms without cooling systems will see sharper declines. A moderate drop is expected; "
        "a severe drop suggests inadequate heat abatement."
    ),
    ("Fertility", "rainy_warm"): (
        "Peak heat stress season. Conception rates typically drop 20-30% vs dry-cool season. "
        "Estrus detection is harder (shorter, weaker heats). Days open increase. This is the "
        "most challenging period for reproduction. Distinguish expected seasonal decline from "
        "additional management failures."
    ),
    ("Fertility", "transition"): (
        "Fertility should begin recovering as temperatures cool. Cows bred during heat stress "
        "may show higher early pregnancy loss. Monitor for delayed recovery — if conception "
        "rates don't improve by November, investigate persistent issues."
    ),

    # ── Production ──
    ("Production", "dry_cool"): (
        "Peak production season. Cooler temperatures maximize feed intake and milk yield. "
        "If production is below benchmark now, it's a true management/nutrition issue. "
        "Expect highest 305-day projections and best feed efficiency."
    ),
    ("Production", "dry_hot"): (
        "Production starts declining as heat builds. Feed intake drops 5-10% as THI rises. "
        "A 3-5% milk yield decline is expected. Monitor feed refusals and water intake. "
        "Persistent decline beyond 5% suggests inadequate cooling or ration adjustment."
    ),
    ("Production", "rainy_warm"): (
        "Lowest production period. Heat stress reduces DMI by 10-25%, milk yield drops 10-20%. "
        "Fat and protein content may decrease. Silage quality may suffer from humidity. "
        "A significant drop is EXPECTED — only flag as critical if decline exceeds regional norms "
        "or if recovery is absent after cooling events."
    ),
    ("Production", "transition"): (
        "Production should recover as temperatures moderate. Forage from rainy season becomes "
        "available. If production remains depressed, investigate carryover effects from heat "
        "stress (body condition loss, metabolic issues during summer)."
    ),

    # ── Health ──
    ("Health", "dry_cool"): (
        "Respiratory disease risk increases (cold nights, temperature swings). Metabolic disease "
        "(ketosis, milk fever) follows normal calving patterns. Mastitis should be at seasonal low. "
        "If metabolic disease is elevated, focus on transition cow management."
    ),
    ("Health", "dry_hot"): (
        "Heat stress is building. Watch for rising SCC and subclinical mastitis. Lameness may "
        "increase as cows stand more to dissipate heat. Metabolic disease risk rises for cows "
        "calving in heat. Ensure adequate shade, water, and ventilation."
    ),
    ("Health", "rainy_warm"): (
        "Highest disease pressure. Humidity and mud increase mastitis (environmental pathogens), "
        "digital dermatitis, and respiratory issues. Fly pressure peaks. Heat stress compounds all "
        "health risks. Elevated SCC and lameness are partly expected — but critical thresholds "
        "still indicate management failures that need intervention."
    ),
    ("Health", "transition"): (
        "Disease pressure eases. Monitor for lingering effects of summer stress — cows that lost "
        "body condition during heat may show delayed metabolic problems at calving. Good time to "
        "assess and repair housing, ventilation, and foot baths."
    ),

    # ── Calf Raising ──
    ("Calf Raising", "dry_cool"): (
        "Cold stress risk for newborn calves, especially at night. Ensure adequate bedding, "
        "nesting scores, and increased milk/replacer volume. Respiratory disease (pneumonia) "
        "risk peaks. Mortality should be low if cold management is adequate."
    ),
    ("Calf Raising", "dry_hot"): (
        "Rising temperatures improve calf comfort but increase fly pressure and scours risk. "
        "Hutch temperatures can become excessive — ensure ventilation and shade. Water intake "
        "becomes critical. Growth rates should be stable or improving."
    ),
    ("Calf Raising", "rainy_warm"): (
        "Highest calf mortality risk. Heat stress, humidity, and pathogen load combine. "
        "Hutch bedding stays wet, fly pressure is extreme. Scours and respiratory disease peak. "
        "Some mortality increase is expected but rates above 5% signal inadequate management. "
        "Ensure clean water, dry bedding, and fly control."
    ),
    ("Calf Raising", "transition"): (
        "Conditions improve for calves. Good time to assess summer losses and adjust protocols. "
        "Calves born in summer may show reduced growth rates — monitor catch-up growth."
    ),

    # ── Culling ──
    ("Culling", "dry_cool"): (
        "Culling rates should be at seasonal baseline. Voluntary culling (low production) is "
        "typically planned now. If involuntary culling is elevated, investigate health or "
        "reproductive causes rather than attributing to season."
    ),
    ("Culling", "dry_hot"): (
        "Heat-related culling begins. Cows with compromised health or low production are "
        "vulnerable. Watch for increased locomotor-related culling as lameness rises."
    ),
    ("Culling", "rainy_warm"): (
        "Highest involuntary culling pressure. Heat stress, mastitis, lameness, and reproductive "
        "failure drive exits. Some increase is expected — but rates significantly above annual "
        "average indicate compounding management issues beyond normal seasonal effects."
    ),
    ("Culling", "transition"): (
        "Culling decisions from summer problems finalize. Review culling reasons — if dominated "
        "by health or fertility, address root causes before next summer."
    ),
}

# Farm-level overview per season (injected into master summary)
_FARM_OVERVIEW: dict[str, str] = {
    "dry_cool": (
        "Dry-cool season (Nov-Feb): Best conditions for dairy production in Mexico. "
        "Cooler temperatures favor high feed intake, peak milk yield, and strong fertility. "
        "Any KPI below benchmark now is a true management issue, not seasonal."
    ),
    "dry_hot": (
        "Dry-hot season (Mar-May): Transition period. Heat is building, expect gradual "
        "declines in fertility and production. Farms should be activating heat abatement. "
        "Moderate declines are expected; sharp drops need investigation."
    ),
    "rainy_warm": (
        "Rainy-warm season (Jun-Sep): Most challenging period. Heat stress, humidity, mud, "
        "and flies compound to reduce production, fertility, and health. Distinguish normal "
        "seasonal decline from management failures. Focus on what's worse than expected."
    ),
    "transition": (
        "Transition season (Oct): Recovery period. Temperatures cooling, conditions improving. "
        "Monitor for carryover effects from summer stress. Good time for strategic interventions "
        "before the productive dry-cool season begins."
    ),
}


def get_domain_seasonal_context(domain: str, month: int) -> str:
    """Return seasonal context for a specific domain and month."""
    season = _month_to_season(month)
    key = (domain, season)
    context = _DOMAIN_SEASON.get(key, "")
    if not context:
        return ""
    return f"Season: {_season_label(season)} | {context}"


def get_farm_seasonal_overview(month: int) -> str:
    """Return farm-level seasonal overview for the master summary."""
    season = _month_to_season(month)
    return _FARM_OVERVIEW.get(season, "")
