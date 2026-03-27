"""
Fetches recent weather data from Open-Meteo and combines it with static
seasonal knowledge to build a context block for LLM agent prompts.

Open-Meteo is free, no API key required.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests

from agents.seasonal_config import get_domain_seasonal_context, get_farm_seasonal_overview

REPO_ROOT = Path(__file__).resolve().parents[1]
FARM_LOCATIONS_PATH = REPO_ROOT / "data" / "farm_locations.json"

# In-memory daily cache: {f"{farm_code}_{date}": weather_summary_dict}
_weather_cache: dict[str, dict] = {}


def _load_farm_location(farm_code: str) -> Optional[dict]:
    """Load a farm's location from farm_locations.json."""
    if not FARM_LOCATIONS_PATH.exists():
        return None
    with open(FARM_LOCATIONS_PATH, "r", encoding="utf-8") as f:
        locations = json.load(f)
    return locations.get(farm_code)


def save_farm_location(farm_code: str, name: str, municipality: str,
                       state: str, latitude: float, longitude: float):
    """Save or update a farm's location in farm_locations.json."""
    locations = {}
    if FARM_LOCATIONS_PATH.exists():
        with open(FARM_LOCATIONS_PATH, "r", encoding="utf-8") as f:
            locations = json.load(f)

    locations[farm_code] = {
        "name": name,
        "municipality": municipality,
        "state": state,
        "latitude": latitude,
        "longitude": longitude,
    }

    with open(FARM_LOCATIONS_PATH, "w", encoding="utf-8") as f:
        json.dump(locations, f, indent=2, ensure_ascii=False)


def _fetch_weather(latitude: float, longitude: float, past_days: int = 30) -> Optional[dict]:
    """
    Fetch recent daily weather from Open-Meteo.
    Returns raw API response dict or None on failure.
    """
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "daily": "temperature_2m_max,temperature_2m_min,relative_humidity_2m_mean,precipitation_sum",
        "past_days": past_days,
        "forecast_days": 0,
        "timezone": "America/Mexico_City",
    }
    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logging.warning(f"Open-Meteo API error: {e}")
        return None


def _compute_thi(temp_c: float, rh_pct: float) -> float:
    """
    Temperature-Humidity Index for dairy cattle.
    THI = 0.8*T + RH*(T - 14.4)/100 + 46.4
    Values >72 indicate heat stress onset.
    """
    return 0.8 * temp_c + rh_pct * (temp_c - 14.4) / 100 + 46.4


def _thi_classification(thi: float) -> str:
    if thi < 68:
        return "NO HEAT STRESS"
    if thi < 72:
        return "MILD STRESS"
    if thi < 78:
        return "MODERATE HEAT STRESS"
    if thi < 82:
        return "SEVERE HEAT STRESS"
    return "EXTREME HEAT STRESS"


def _summarize_weather(raw: dict) -> dict:
    """Extract summary stats from Open-Meteo daily response."""
    daily = raw.get("daily", {})
    t_max = daily.get("temperature_2m_max", [])
    t_min = daily.get("temperature_2m_min", [])
    rh = daily.get("relative_humidity_2m_mean", [])
    precip = daily.get("precipitation_sum", [])

    if not t_max or not t_min:
        return {}

    # Average temperatures
    avg_temps = [(mx + mn) / 2 for mx, mn in zip(t_max, t_min) if mx is not None and mn is not None]
    avg_rh = [h for h in rh if h is not None] if rh else []

    if not avg_temps:
        return {}

    avg_temp = sum(avg_temps) / len(avg_temps)
    max_temp = max(t_max) if t_max else None
    min_temp = min(t_min) if t_min else None
    avg_humidity = sum(avg_rh) / len(avg_rh) if avg_rh else None
    total_precip = sum(p for p in precip if p is not None) if precip else 0

    # Compute daily THI and count stress days
    thi_values = []
    for t, h in zip(avg_temps, avg_rh if avg_rh else [50] * len(avg_temps)):
        thi_values.append(_compute_thi(t, h))

    avg_thi = sum(thi_values) / len(thi_values) if thi_values else None
    stress_days = sum(1 for t in thi_values if t >= 72)

    return {
        "avg_temp_c": round(avg_temp, 1),
        "max_temp_c": round(max_temp, 1) if max_temp is not None else None,
        "min_temp_c": round(min_temp, 1) if min_temp is not None else None,
        "avg_humidity_pct": round(avg_humidity, 1) if avg_humidity is not None else None,
        "total_precip_mm": round(total_precip, 1),
        "avg_thi": round(avg_thi, 1) if avg_thi is not None else None,
        "thi_classification": _thi_classification(avg_thi) if avg_thi else "UNKNOWN",
        "heat_stress_days": stress_days,
        "total_days": len(avg_temps),
    }


def get_weather_summary(farm_code: str) -> Optional[dict]:
    """
    Get weather summary for a farm. Uses daily cache.
    Returns dict with weather stats or None if location unknown or API fails.
    """
    today = datetime.now().strftime("%Y-%m-%d")
    cache_key = f"{farm_code}_{today}"

    if cache_key in _weather_cache:
        return _weather_cache[cache_key]

    location = _load_farm_location(farm_code)
    if not location:
        return None

    raw = _fetch_weather(location["latitude"], location["longitude"])
    if not raw:
        return None

    summary = _summarize_weather(raw)
    if summary:
        summary["municipality"] = location.get("municipality", "")
        summary["state"] = location.get("state", "")
        _weather_cache[cache_key] = summary

    return summary if summary else None


def format_weather_for_prompt(weather: dict) -> str:
    """Format weather summary as a prompt-ready string."""
    if not weather:
        return ""

    location = f"{weather.get('municipality', '?')}, {weather.get('state', '?')}"
    thi = weather.get("avg_thi", "?")
    thi_class = weather.get("thi_classification", "?")
    stress_days = weather.get("heat_stress_days", 0)
    total_days = weather.get("total_days", 30)

    return (
        f"Location: {location}\n"
        f"Recent weather (last {total_days} days):\n"
        f"  Avg temp: {weather.get('avg_temp_c', '?')}C | "
        f"Max: {weather.get('max_temp_c', '?')}C | "
        f"Min: {weather.get('min_temp_c', '?')}C\n"
        f"  Avg humidity: {weather.get('avg_humidity_pct', '?')}%\n"
        f"  THI: {thi} ({thi_class} - {stress_days} of {total_days} days above THI 72)\n"
        f"  Precipitation: {weather.get('total_precip_mm', '?')}mm total"
    )


def build_seasonal_weather_context(farm_code: str, domain: str) -> str:
    """
    Build a combined seasonal + weather context block for a domain agent prompt.
    Returns empty string if no location is configured.
    """
    month = datetime.now().month

    seasonal = get_domain_seasonal_context(domain, month)
    weather = get_weather_summary(farm_code)
    weather_text = format_weather_for_prompt(weather) if weather else ""

    if not seasonal and not weather_text:
        return ""

    parts = ["=== SEASONAL & WEATHER CONTEXT ==="]
    parts.append(f"Month: {datetime.now().strftime('%B %Y')}")

    if weather_text:
        parts.append(weather_text)

    if seasonal:
        parts.append(f"\nDomain-specific seasonal notes:\n  {seasonal}")

    parts.append("")  # trailing newline
    return "\n".join(parts)


def build_master_seasonal_context(farm_code: str) -> str:
    """
    Build a farm-level seasonal + weather context for the master summary prompt.
    """
    month = datetime.now().month
    overview = get_farm_seasonal_overview(month)
    weather = get_weather_summary(farm_code)
    weather_text = format_weather_for_prompt(weather) if weather else ""

    if not overview and not weather_text:
        return ""

    parts = ["=== SEASONAL & WEATHER CONTEXT ==="]
    if weather_text:
        parts.append(weather_text)
    if overview:
        parts.append(f"\n{overview}")
    parts.append("")
    return "\n".join(parts)
