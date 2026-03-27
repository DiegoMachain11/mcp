"""
Weather-driven seasonal context for dairy farms.

Instead of hardcoding month → season, we classify actual conditions from
Open-Meteo weather data into climate states and select the appropriate
domain-specific context. This ensures a desert farm in La Laguna gets
heat stress context in March if THI says so, while a highland farm in
Jalisco might still be in comfortable conditions in July.

Climate conditions (derived from weather data):
  - cold_stress:    Avg THI < 50 OR min temp < 5°C frequently
  - comfortable:    THI < 68, no cold stress
  - mild_heat:      THI 68–72 (heat building, not yet critical)
  - heat_stress:    THI 72–82 (significant production/fertility impact)
  - extreme_heat:   THI >= 82
  - wet:            Precipitation > 60mm in last 30 days (overlays with heat)
"""

from __future__ import annotations

from typing import Optional


def classify_conditions(weather: Optional[dict]) -> tuple[str, list[str]]:
    """
    Classify weather data into a primary condition + modifier flags.

    Args:
        weather: Dict from weather_context.get_weather_summary(), or None.

    Returns:
        (primary_condition, [modifier_flags])
        primary_condition: one of 'cold_stress', 'comfortable', 'mild_heat',
                          'heat_stress', 'extreme_heat', 'unknown'
        modifier_flags: e.g. ['wet'] if high precipitation
    """
    if not weather:
        return "unknown", []

    thi = weather.get("avg_thi")
    min_temp = weather.get("min_temp_c")
    precip = weather.get("total_precip_mm", 0)

    # Primary condition from THI
    if thi is None:
        primary = "unknown"
    elif thi >= 82:
        primary = "extreme_heat"
    elif thi >= 72:
        primary = "heat_stress"
    elif thi >= 68:
        primary = "mild_heat"
    elif min_temp is not None and min_temp < 5:
        primary = "cold_stress"
    else:
        primary = "comfortable"

    # Modifiers
    modifiers = []
    if precip > 60:
        modifiers.append("wet")

    return primary, modifiers


def _condition_label(condition: str, modifiers: list[str]) -> str:
    labels = {
        "cold_stress": "Cold stress conditions",
        "comfortable": "Comfortable conditions (no heat/cold stress)",
        "mild_heat": "Mild heat stress building",
        "heat_stress": "Heat stress conditions",
        "extreme_heat": "Extreme heat stress",
        "unknown": "Weather data unavailable",
    }
    label = labels.get(condition, condition)
    if "wet" in modifiers:
        label += " + wet/rainy conditions"
    return label


# ── Domain × Condition context blocks ────────────────────────────────────────
# Keyed by (domain, condition). Wet modifier adds extra context.

_DOMAIN_CONDITION: dict[tuple[str, str], str] = {
    # ── Fertility ──
    ("Fertility", "comfortable"): (
        "Comfortable conditions favor estrus expression and embryo survival. "
        "Conception rates should be at or above annual benchmarks. If fertility is low now, "
        "it signals a management or health problem, not a weather effect."
    ),
    ("Fertility", "mild_heat"): (
        "Heat stress is building. Fertility begins to decline — heat reduces estrus intensity "
        "and duration, lowers conception rates by 10-20%, and increases early embryonic loss. "
        "Farms without cooling systems will see sharper declines. A moderate drop is expected; "
        "a severe drop suggests inadequate heat abatement."
    ),
    ("Fertility", "heat_stress"): (
        "Active heat stress. Conception rates typically drop 20-30% vs comfortable conditions. "
        "Estrus detection is harder (shorter, weaker heats). Days open increase. This is a "
        "challenging period for reproduction. Distinguish expected heat-driven decline from "
        "additional management failures."
    ),
    ("Fertility", "extreme_heat"): (
        "Extreme heat stress. Expect severe fertility impact — conception rates may drop 30-50%. "
        "Embryonic mortality is high. Estrus behavior is heavily suppressed. Any breeding program "
        "should be evaluated for cost-effectiveness during this period. Heat abatement is critical."
    ),
    ("Fertility", "cold_stress"): (
        "Cold conditions. Fertility is generally not impacted by cold in dairy cattle, but "
        "sudden temperature swings can cause stress. Monitor body condition — cows burning "
        "extra energy for thermoregulation may have reduced fertility if underfed."
    ),

    # ── Production ──
    ("Production", "comfortable"): (
        "Comfortable conditions maximize feed intake and milk yield. "
        "If production is below benchmark now, it's a true management/nutrition issue. "
        "Expect best 305-day projections and feed efficiency."
    ),
    ("Production", "mild_heat"): (
        "Production starts declining as heat builds. Feed intake drops 5-10% as THI rises. "
        "A 3-5% milk yield decline is expected. Monitor feed refusals and water intake. "
        "Persistent decline beyond 5% suggests inadequate cooling or ration adjustment."
    ),
    ("Production", "heat_stress"): (
        "Heat stress reduces DMI by 10-25%, milk yield drops 10-20%. "
        "Fat and protein content may decrease. "
        "A significant drop is EXPECTED — only flag as critical if decline exceeds what THI predicts "
        "or if recovery is absent after cooling events."
    ),
    ("Production", "extreme_heat"): (
        "Extreme heat. Expect DMI reduction of 20-35% and milk yield drops of 20-30%+. "
        "Component quality declines sharply. Emergency cooling and ration reformulation are critical. "
        "Production losses at this level should not surprise — focus on preventing mortality and "
        "minimizing long-term body condition damage."
    ),
    ("Production", "cold_stress"): (
        "Cold conditions may increase feed requirements for maintenance (+10-20% energy for "
        "thermoregulation). If rations are not adjusted, production may decline. "
        "Ensure adequate dry matter availability and windbreaks."
    ),

    # ── Health ──
    ("Health", "comfortable"): (
        "Low weather-driven disease pressure. Metabolic disease (ketosis, milk fever) follows "
        "normal calving patterns. Mastitis should be at seasonal low. "
        "If health metrics are elevated, focus on transition cow management, not weather."
    ),
    ("Health", "mild_heat"): (
        "Heat stress is building. Watch for rising SCC and subclinical mastitis. Lameness may "
        "increase as cows stand more to dissipate heat. Metabolic disease risk rises for cows "
        "calving in heat. Ensure adequate shade, water, and ventilation."
    ),
    ("Health", "heat_stress"): (
        "Significant disease pressure from heat. SCC and clinical mastitis rise. Lameness increases. "
        "Rumination and lying time drop, reducing immune function. Heat stress compounds all "
        "health risks. Elevated SCC and lameness are partly expected — but critical thresholds "
        "still indicate management failures that need intervention."
    ),
    ("Health", "extreme_heat"): (
        "Extreme health risk. Heat stroke and mortality risk are real. Immune suppression is severe. "
        "Expect elevated disease across all categories. Prioritize emergency cooling, hydration, "
        "and monitoring of high-risk animals (fresh cows, high producers)."
    ),
    ("Health", "cold_stress"): (
        "Respiratory disease risk increases (cold air, temperature swings). "
        "Ensure ventilation without drafts. Calving area warmth is critical. "
        "Mastitis risk is generally lower in cold/dry conditions."
    ),

    # ── Calf Raising ──
    ("Calf Raising", "comfortable"): (
        "Good conditions for calf growth. Moderate temperatures favor feed intake and health. "
        "Mortality should be at baseline. If calf mortality is elevated, investigate "
        "management, colostrum, or sanitation — not weather."
    ),
    ("Calf Raising", "mild_heat"): (
        "Rising temperatures — hutch ventilation becomes important. Fly pressure increasing. "
        "Water intake critical. Growth rates should be stable. "
        "Ensure shade and airflow in calf housing."
    ),
    ("Calf Raising", "heat_stress"): (
        "Elevated calf mortality risk. Heat stress, fly pressure, and pathogen load combine. "
        "Hutch temperatures can be 5-10C above ambient. Scours risk increases. "
        "Some mortality increase is expected but rates above 5% signal inadequate management. "
        "Ensure clean water, shade, ventilation, and fly control."
    ),
    ("Calf Raising", "extreme_heat"): (
        "Critical calf welfare risk. Hutch temperatures may exceed lethal thresholds. "
        "Newborn calves are especially vulnerable. Ensure emergency shade, water, and consider "
        "alternative housing. Monitor closely for dehydration and respiratory distress."
    ),
    ("Calf Raising", "cold_stress"): (
        "Cold stress risk for newborn calves. Ensure adequate bedding (nesting score ≥3), "
        "increased milk/replacer volume (+20%), and calf jackets. Respiratory disease (pneumonia) "
        "risk peaks. Mortality should be low if cold management is adequate."
    ),

    # ── Culling ──
    ("Culling", "comfortable"): (
        "Culling rates should be at baseline. Voluntary culling (low production) is "
        "typically planned during comfortable periods. If involuntary culling is elevated, "
        "investigate health or reproductive causes — weather is not a factor."
    ),
    ("Culling", "mild_heat"): (
        "Heat-related culling begins. Cows with compromised health or low production are "
        "vulnerable. Watch for increased locomotor-related culling as lameness rises."
    ),
    ("Culling", "heat_stress"): (
        "Elevated involuntary culling pressure. Heat stress, mastitis, lameness, and reproductive "
        "failure drive exits. Some increase is expected — but rates significantly above annual "
        "average indicate compounding management issues beyond normal heat effects."
    ),
    ("Culling", "extreme_heat"): (
        "Peak culling and mortality pressure. Deaths from heat stroke possible. "
        "Emergency culling of compromised animals may be necessary. Focus on preventing "
        "mortality in high-value animals. Review culling reasons post-crisis."
    ),
    ("Culling", "cold_stress"): (
        "Cold is not a major direct driver of culling. If culling is elevated during cold, "
        "investigate respiratory disease, nutritional deficiencies from increased maintenance "
        "needs, or lameness from icy/muddy conditions."
    ),
}

# Extra context when 'wet' modifier is present
_WET_MODIFIER: dict[str, str] = {
    "Fertility": (
        "Wet conditions add mud stress, reducing cow comfort and increasing foot problems "
        "that compound fertility issues. Ensure dry, clean breeding areas."
    ),
    "Production": (
        "Wet conditions may reduce silage/forage quality (mold risk) and increase "
        "maintenance energy costs from mud. Monitor forage dry matter and adjust rations."
    ),
    "Health": (
        "Wet conditions strongly increase mastitis risk (environmental pathogens: E. coli, "
        "Klebsiella thrive in mud). Digital dermatitis and foot rot risk spike. "
        "Fly pressure intensifies. Bedding management is critical."
    ),
    "Calf Raising": (
        "Wet conditions are dangerous for calves. Hutch bedding stays wet, pathogen load rises. "
        "Scours and respiratory disease increase. Ensure dry bedding, drainage, and hutch placement."
    ),
    "Culling": (
        "Wet conditions drive up lameness and mastitis-related culling. "
        "Review locomotor and udder health as primary culling drivers."
    ),
}

# Farm-level overviews keyed by primary condition
_FARM_OVERVIEW: dict[str, str] = {
    "comfortable": (
        "Comfortable weather conditions — no significant heat or cold stress. "
        "Best conditions for dairy production. Any KPI below benchmark is a true "
        "management issue, not weather-driven."
    ),
    "mild_heat": (
        "Mild heat stress building. Expect gradual declines in fertility and production. "
        "Farms should be activating heat abatement. Moderate declines are expected; "
        "sharp drops need investigation."
    ),
    "heat_stress": (
        "Active heat stress conditions. Significant impact on production, fertility, and health "
        "is expected. Distinguish normal heat-driven decline from management failures. "
        "Focus on what's worse than the THI predicts."
    ),
    "extreme_heat": (
        "Extreme heat stress — emergency conditions. Major losses in production, fertility, "
        "and health are expected. Prioritize animal welfare and mortality prevention. "
        "Flag only issues beyond what extreme heat explains."
    ),
    "cold_stress": (
        "Cold stress conditions. Increased energy requirements and respiratory disease risk. "
        "Ensure adequate nutrition, shelter, and calf warmth. Production should be good "
        "if management compensates for cold."
    ),
    "unknown": (
        "Weather data unavailable for this farm. Unable to calibrate seasonal expectations. "
        "Interpret KPI deviations without weather context."
    ),
}


def get_domain_seasonal_context(
    domain: str, weather: Optional[dict] = None
) -> str:
    """
    Return condition-based context for a specific domain using actual weather.

    Args:
        domain: One of Fertility, Production, Health, Calf Raising, Culling.
        weather: Dict from weather_context.get_weather_summary(), or None.

    Returns:
        Context string for prompt injection.
    """
    condition, modifiers = classify_conditions(weather)
    label = _condition_label(condition, modifiers)

    context = _DOMAIN_CONDITION.get((domain, condition), "")
    if not context:
        return ""

    parts = [f"Conditions: {label} | {context}"]

    if "wet" in modifiers:
        wet_ctx = _WET_MODIFIER.get(domain, "")
        if wet_ctx:
            parts.append(f"Wet weather note: {wet_ctx}")

    return " ".join(parts)


def get_farm_seasonal_overview(weather: Optional[dict] = None) -> str:
    """
    Return farm-level overview based on actual weather conditions.
    """
    condition, modifiers = classify_conditions(weather)
    overview = _FARM_OVERVIEW.get(condition, _FARM_OVERVIEW["unknown"])

    if "wet" in modifiers:
        overview += (
            " Additionally, wet/rainy conditions increase disease pressure "
            "(mastitis, lameness, calf scours) — factor this into health assessments."
        )

    return overview
