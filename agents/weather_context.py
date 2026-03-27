"""
Fetches recent and historical weather data from Open-Meteo and combines it
with condition-based seasonal knowledge to build context for LLM agent prompts.

Two layers of weather context:
  1. Current conditions (last 30 days) — used to classify the farm's current state
  2. Historical window (matching the analysis months) — monthly THI/precip breakdown
     so the LLM understands weather across the full KPI analysis period

Open-Meteo is free, no API key required.
  - Forecast API (last 30 days): api.open-meteo.com/v1/forecast
  - Archive API (historical):    archive-api.open-meteo.com/v1/archive
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import requests

from agents.seasonal_config import get_domain_seasonal_context, get_farm_seasonal_overview

REPO_ROOT = Path(__file__).resolve().parents[1]
FARM_LOCATIONS_PATH = REPO_ROOT / "data" / "farm_locations.json"

# In-memory daily cache
# Current weather: {f"{farm_code}_current_{date}": summary_dict}
# Historical:      {f"{farm_code}_hist_{months}_{date}": monthly_list}
_weather_cache: dict[str, object] = {}


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


# ── Weather API calls ────────────────────────────────────────────────────────

def _fetch_weather(latitude: float, longitude: float, past_days: int = 30) -> Optional[dict]:
    """
    Fetch recent daily weather from Open-Meteo forecast API.
    Used for current conditions (last 30 days).
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
        logging.warning(f"Open-Meteo forecast API error: {e}")
        return None


def _fetch_historical_weather(
    latitude: float, longitude: float, start_date: str, end_date: str
) -> Optional[dict]:
    """
    Fetch historical daily weather from Open-Meteo archive API.
    Used for the full analysis window (up to 24 months).

    Args:
        start_date: "YYYY-MM-DD"
        end_date: "YYYY-MM-DD"
    """
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "daily": "temperature_2m_max,temperature_2m_min,relative_humidity_2m_mean,precipitation_sum",
        "timezone": "America/Mexico_City",
    }
    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logging.warning(f"Open-Meteo archive API error: {e}")
        return None


# ── THI computation ──────────────────────────────────────────────────────────

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


# ── Summarization ────────────────────────────────────────────────────────────

def _summarize_weather(raw: dict) -> dict:
    """Extract summary stats from Open-Meteo daily response (any date range)."""
    daily = raw.get("daily", {})
    t_max = daily.get("temperature_2m_max", [])
    t_min = daily.get("temperature_2m_min", [])
    rh = daily.get("relative_humidity_2m_mean", [])
    precip = daily.get("precipitation_sum", [])

    if not t_max or not t_min:
        return {}

    avg_temps = [(mx + mn) / 2 for mx, mn in zip(t_max, t_min) if mx is not None and mn is not None]
    avg_rh = [h for h in rh if h is not None] if rh else []

    if not avg_temps:
        return {}

    avg_temp = sum(avg_temps) / len(avg_temps)
    max_temp = max(v for v in t_max if v is not None)
    min_temp = min(v for v in t_min if v is not None)
    avg_humidity = sum(avg_rh) / len(avg_rh) if avg_rh else None
    total_precip = sum(p for p in precip if p is not None) if precip else 0

    thi_values = []
    for t, h in zip(avg_temps, avg_rh if avg_rh else [50] * len(avg_temps)):
        thi_values.append(_compute_thi(t, h))

    avg_thi = sum(thi_values) / len(thi_values) if thi_values else None
    stress_days = sum(1 for t in thi_values if t >= 72)

    return {
        "avg_temp_c": round(avg_temp, 1),
        "max_temp_c": round(max_temp, 1),
        "min_temp_c": round(min_temp, 1),
        "avg_humidity_pct": round(avg_humidity, 1) if avg_humidity is not None else None,
        "total_precip_mm": round(total_precip, 1),
        "avg_thi": round(avg_thi, 1) if avg_thi is not None else None,
        "thi_classification": _thi_classification(avg_thi) if avg_thi else "UNKNOWN",
        "heat_stress_days": stress_days,
        "total_days": len(avg_temps),
    }


def _summarize_monthly(raw: dict) -> list[dict]:
    """
    Break raw Open-Meteo daily data into monthly summaries.
    Returns a list of dicts, one per month, sorted chronologically.
    """
    daily = raw.get("daily", {})
    dates = daily.get("time", [])
    t_max = daily.get("temperature_2m_max", [])
    t_min = daily.get("temperature_2m_min", [])
    rh = daily.get("relative_humidity_2m_mean", [])
    precip = daily.get("precipitation_sum", [])

    if not dates or not t_max:
        return []

    # Group by YYYY-MM
    months: dict[str, dict] = {}
    for i, date_str in enumerate(dates):
        ym = date_str[:7]  # "YYYY-MM"
        if ym not in months:
            months[ym] = {"temps": [], "rh": [], "precip": []}

        tmx = t_max[i] if i < len(t_max) else None
        tmn = t_min[i] if i < len(t_min) else None
        h = rh[i] if i < len(rh) else None
        p = precip[i] if i < len(precip) else None

        if tmx is not None and tmn is not None:
            months[ym]["temps"].append((tmx + tmn) / 2)
        if h is not None:
            months[ym]["rh"].append(h)
        if p is not None:
            months[ym]["precip"].append(p)

    result = []
    for ym in sorted(months.keys()):
        m = months[ym]
        if not m["temps"]:
            continue

        avg_temp = sum(m["temps"]) / len(m["temps"])
        avg_h = sum(m["rh"]) / len(m["rh"]) if m["rh"] else 50.0
        total_p = sum(m["precip"])

        thi_vals = [_compute_thi(t, h) for t, h in
                    zip(m["temps"], m["rh"] if m["rh"] else [50.0] * len(m["temps"]))]
        avg_thi = sum(thi_vals) / len(thi_vals)
        stress_days = sum(1 for t in thi_vals if t >= 72)

        result.append({
            "month": ym,
            "avg_temp_c": round(avg_temp, 1),
            "avg_humidity_pct": round(avg_h, 1),
            "total_precip_mm": round(total_p, 1),
            "avg_thi": round(avg_thi, 1),
            "thi_class": _thi_classification(avg_thi),
            "heat_stress_days": stress_days,
            "total_days": len(m["temps"]),
        })

    return result


# ── Public API ───────────────────────────────────────────────────────────────

def get_weather_summary(farm_code: str) -> Optional[dict]:
    """
    Get current weather summary (last 30 days) for a farm. Uses daily cache.
    Returns dict with weather stats or None if location unknown or API fails.
    """
    today = datetime.now().strftime("%Y-%m-%d")
    cache_key = f"{farm_code}_current_{today}"

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


def get_historical_weather(farm_code: str, months: int) -> Optional[list[dict]]:
    """
    Get monthly weather summaries for the analysis window.
    Returns list of monthly dicts or None.

    Args:
        farm_code: Farm identifier.
        months: Number of months of history to fetch (matches the KPI analysis window).
    """
    today = datetime.now().strftime("%Y-%m-%d")
    cache_key = f"{farm_code}_hist_{months}_{today}"

    if cache_key in _weather_cache:
        return _weather_cache[cache_key]

    location = _load_farm_location(farm_code)
    if not location:
        return None

    end_date = datetime.now()
    start_date = end_date - timedelta(days=int(months * 30.5))

    raw = _fetch_historical_weather(
        location["latitude"],
        location["longitude"],
        start_date.strftime("%Y-%m-%d"),
        end_date.strftime("%Y-%m-%d"),
    )
    if not raw:
        return None

    monthly = _summarize_monthly(raw)
    if monthly:
        _weather_cache[cache_key] = monthly

    return monthly if monthly else None


# ── Formatting ───────────────────────────────────────────────────────────────

def format_weather_for_prompt(weather: dict) -> str:
    """Format current weather summary as a prompt-ready string."""
    if not weather:
        return ""

    location = f"{weather.get('municipality', '?')}, {weather.get('state', '?')}"
    thi = weather.get("avg_thi", "?")
    thi_class = weather.get("thi_classification", "?")
    stress_days = weather.get("heat_stress_days", 0)
    total_days = weather.get("total_days", 30)

    return (
        f"Location: {location}\n"
        f"Current weather (last {total_days} days):\n"
        f"  Avg temp: {weather.get('avg_temp_c', '?')}C | "
        f"Max: {weather.get('max_temp_c', '?')}C | "
        f"Min: {weather.get('min_temp_c', '?')}C\n"
        f"  Avg humidity: {weather.get('avg_humidity_pct', '?')}%\n"
        f"  THI: {thi} ({thi_class} - {stress_days} of {total_days} days above THI 72)\n"
        f"  Precipitation: {weather.get('total_precip_mm', '?')}mm total"
    )


def format_historical_for_prompt(monthly: list[dict]) -> str:
    """
    Format monthly weather history as a compact table for the LLM prompt.
    Shows THI progression across the analysis window.
    """
    if not monthly:
        return ""

    lines = ["Weather history across analysis window (monthly):"]
    lines.append("  Month     | Avg Temp | THI  | Stress Days | Precip  | Classification")
    lines.append("  " + "-" * 75)

    total_stress_days = 0
    for m in monthly:
        total_stress_days += m["heat_stress_days"]
        lines.append(
            f"  {m['month']}  | {m['avg_temp_c']:5.1f}C   | {m['avg_thi']:4.1f} | "
            f"{m['heat_stress_days']:2d}/{m['total_days']:2d} days   | "
            f"{m['total_precip_mm']:5.1f}mm | {m['thi_class']}"
        )

    total_months = len(monthly)
    stress_months = sum(1 for m in monthly if m["avg_thi"] >= 72)
    lines.append(f"  Summary: {stress_months}/{total_months} months with heat stress, "
                 f"{total_stress_days} total heat stress days")

    return "\n".join(lines)


# ── Context builders ─────────────────────────────────────────────────────────

def build_seasonal_weather_context(
    farm_code: str, domain: str, months: int = 1
) -> str:
    """
    Build a combined seasonal + weather context block for a domain agent prompt.

    Includes:
      - Current conditions (last 30 days) with condition classification
      - Historical monthly breakdown if months > 1
      - Domain-specific notes based on current weather conditions

    Args:
        farm_code: Farm identifier.
        domain: Domain name (Fertility, Production, Health, Calf Raising, Culling).
        months: Analysis window in months (passed from dashboard slider).
    """
    weather = get_weather_summary(farm_code)
    weather_text = format_weather_for_prompt(weather) if weather else ""

    # Historical weather for the analysis window
    hist_text = ""
    if months > 1:
        monthly = get_historical_weather(farm_code, months)
        hist_text = format_historical_for_prompt(monthly) if monthly else ""

    # Condition classification from current weather
    seasonal = get_domain_seasonal_context(domain, weather)

    if not seasonal and not weather_text and not hist_text:
        return ""

    parts = ["=== SEASONAL & WEATHER CONTEXT ==="]
    parts.append(f"Month: {datetime.now().strftime('%B %Y')} | Analysis window: {months} months")

    if weather_text:
        parts.append(weather_text)

    if hist_text:
        parts.append(f"\n{hist_text}")

    if seasonal:
        parts.append(f"\nDomain-specific notes (based on current weather):\n  {seasonal}")

    parts.append("")  # trailing newline
    return "\n".join(parts)


def build_master_seasonal_context(farm_code: str, months: int = 1) -> str:
    """
    Build a farm-level seasonal + weather context for the master summary prompt.
    """
    weather = get_weather_summary(farm_code)
    weather_text = format_weather_for_prompt(weather) if weather else ""
    overview = get_farm_seasonal_overview(weather)

    hist_text = ""
    if months > 1:
        monthly = get_historical_weather(farm_code, months)
        hist_text = format_historical_for_prompt(monthly) if monthly else ""

    if not overview and not weather_text and not hist_text:
        return ""

    parts = ["=== SEASONAL & WEATHER CONTEXT ==="]
    if weather_text:
        parts.append(weather_text)
    if hist_text:
        parts.append(f"\n{hist_text}")
    if overview:
        parts.append(f"\n{overview}")
    parts.append("")
    return "\n".join(parts)
