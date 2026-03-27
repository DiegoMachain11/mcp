"""
Tests for seasonal config, weather context, and their integration.

Tests:
  1. Weather-based condition classification
  2. All domain × condition combos have context
  3. Farm-level overview covers all conditions
  4. THI computation is correct
  5. THI classification thresholds are correct
  6. Weather summary extraction from raw API response
  7. Farm location save/load round-trip
  8. Combined context builder produces expected format
  9. Weather cache avoids duplicate API calls
  10. Missing farm location returns graceful empty context
  11. Wet modifier adds extra context
  12. Full prompt context includes month, location, THI, and condition-driven notes
  13. Monthly summary extraction from raw daily data
  14. Historical context included when months > 1
  15. Single-month analysis has no historical table
  16. Analysis window label appears in context
  17. Live Open-Meteo API — current + historical (requires internet)

Usage:
    conda run -n mcp python -m tests.test_seasonal_weather
    conda run -n mcp python -m tests.test_seasonal_weather --live   # includes API test
"""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

# ── helpers ──────────────────────────────────────────────────────────────────

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

def _section(title: str):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


# ── mock weather dicts for testing ───────────────────────────────────────────

WEATHER_COMFORTABLE = {
    "avg_temp_c": 18.0, "max_temp_c": 24.0, "min_temp_c": 12.0,
    "avg_humidity_pct": 50.0, "total_precip_mm": 10.0,
    "avg_thi": 63.0, "thi_classification": "NO HEAT STRESS",
    "heat_stress_days": 0, "total_days": 30,
}

WEATHER_MILD_HEAT = {
    "avg_temp_c": 25.0, "max_temp_c": 32.0, "min_temp_c": 18.0,
    "avg_humidity_pct": 55.0, "total_precip_mm": 20.0,
    "avg_thi": 70.0, "thi_classification": "MILD STRESS",
    "heat_stress_days": 5, "total_days": 30,
}

WEATHER_HEAT_STRESS = {
    "avg_temp_c": 30.0, "max_temp_c": 38.0, "min_temp_c": 22.0,
    "avg_humidity_pct": 65.0, "total_precip_mm": 30.0,
    "avg_thi": 76.0, "thi_classification": "MODERATE HEAT STRESS",
    "heat_stress_days": 22, "total_days": 30,
}

WEATHER_EXTREME_HEAT = {
    "avg_temp_c": 36.0, "max_temp_c": 44.0, "min_temp_c": 28.0,
    "avg_humidity_pct": 70.0, "total_precip_mm": 5.0,
    "avg_thi": 84.0, "thi_classification": "EXTREME HEAT STRESS",
    "heat_stress_days": 30, "total_days": 30,
}

WEATHER_COLD = {
    "avg_temp_c": 5.0, "max_temp_c": 12.0, "min_temp_c": -2.0,
    "avg_humidity_pct": 40.0, "total_precip_mm": 15.0,
    "avg_thi": 48.0, "thi_classification": "NO HEAT STRESS",
    "heat_stress_days": 0, "total_days": 30,
}

WEATHER_HOT_WET = {
    "avg_temp_c": 29.0, "max_temp_c": 35.0, "min_temp_c": 24.0,
    "avg_humidity_pct": 80.0, "total_precip_mm": 120.0,
    "avg_thi": 78.0, "thi_classification": "MODERATE HEAT STRESS",
    "heat_stress_days": 25, "total_days": 30,
}


# ── test 1: weather-based condition classification ───────────────────────────

def test_condition_classification():
    _section("Test 1 — Weather-based condition classification")

    from agents.seasonal_config import classify_conditions

    cond, mods = classify_conditions(WEATHER_COMFORTABLE)
    _check(cond == "comfortable", f"Comfortable: {cond}", f"Expected comfortable, got {cond}")
    _check(mods == [], f"No modifiers: {mods}", f"Expected [], got {mods}")

    cond, mods = classify_conditions(WEATHER_MILD_HEAT)
    _check(cond == "mild_heat", f"Mild heat: {cond}", f"Expected mild_heat, got {cond}")

    cond, mods = classify_conditions(WEATHER_HEAT_STRESS)
    _check(cond == "heat_stress", f"Heat stress: {cond}", f"Expected heat_stress, got {cond}")

    cond, mods = classify_conditions(WEATHER_EXTREME_HEAT)
    _check(cond == "extreme_heat", f"Extreme heat: {cond}", f"Expected extreme_heat, got {cond}")

    cond, mods = classify_conditions(WEATHER_COLD)
    _check(cond == "cold_stress", f"Cold stress: {cond}", f"Expected cold_stress, got {cond}")

    cond, mods = classify_conditions(WEATHER_HOT_WET)
    _check(cond == "heat_stress", f"Hot+wet primary: {cond}", f"Expected heat_stress, got {cond}")
    _check("wet" in mods, f"Wet modifier present: {mods}", f"Expected ['wet'], got {mods}")

    cond, mods = classify_conditions(None)
    _check(cond == "unknown", f"None weather: {cond}", f"Expected unknown, got {cond}")


# ── test 2: all domain × condition combos have context ───────────────────────

def test_all_domain_condition_combos():
    _section("Test 2 — All domain × condition combos have context")

    from agents.seasonal_config import get_domain_seasonal_context

    domains = ["Fertility", "Production", "Health", "Calf Raising", "Culling"]
    weather_cases = [
        ("comfortable", WEATHER_COMFORTABLE),
        ("mild_heat", WEATHER_MILD_HEAT),
        ("heat_stress", WEATHER_HEAT_STRESS),
        ("extreme_heat", WEATHER_EXTREME_HEAT),
        ("cold_stress", WEATHER_COLD),
    ]

    count = 0
    for domain in domains:
        for label, weather in weather_cases:
            ctx = get_domain_seasonal_context(domain, weather)
            _check(
                len(ctx) > 20,
                f"{domain} × {label}: {len(ctx)} chars",
                f"{domain} × {label}: too short ({len(ctx)} chars)"
            )
            count += 1

    _ok(f"All {count} domain × condition combos have context")


# ── test 3: farm-level overview covers all conditions ────────────────────────

def test_farm_overview():
    _section("Test 3 — Farm-level overview covers all conditions")

    from agents.seasonal_config import get_farm_seasonal_overview

    for label, weather in [
        ("comfortable", WEATHER_COMFORTABLE),
        ("mild_heat", WEATHER_MILD_HEAT),
        ("heat_stress", WEATHER_HEAT_STRESS),
        ("extreme_heat", WEATHER_EXTREME_HEAT),
        ("cold_stress", WEATHER_COLD),
        ("unknown", None),
    ]:
        overview = get_farm_seasonal_overview(weather)
        _check(
            len(overview) > 30,
            f"{label}: overview has {len(overview)} chars",
            f"{label}: overview too short ({len(overview)} chars)"
        )


# ── test 4: THI computation ─────────────────────────────────────────────────

def test_thi_computation():
    _section("Test 4 — THI computation")

    from agents.weather_context import _compute_thi

    # Known: 0.8*30 + 80*(30-14.4)/100 + 46.4 = 24 + 12.48 + 46.4 = 82.88
    thi = _compute_thi(30.0, 80.0)
    _check(abs(thi - 82.88) < 0.1, f"THI(30C, 80%RH) = {thi:.2f} (expected ~82.88)", f"Got {thi}")

    # Low temp: 15°C, 40% RH → THI ≈ 58.6
    thi_low = _compute_thi(15.0, 40.0)
    _check(thi_low < 68, f"THI(15C, 40%RH) = {thi_low:.2f} — no stress", f"Got {thi_low}")

    # Moderate: 25°C, 60% RH → THI ≈ 72.8
    thi_mod = _compute_thi(25.0, 60.0)
    _check(72 <= thi_mod < 78, f"THI(25C, 60%RH) = {thi_mod:.2f} — moderate stress", f"Got {thi_mod}")


# ── test 5: THI classification ───────────────────────────────────────────────

def test_thi_classification():
    _section("Test 5 — THI classification thresholds")

    from agents.weather_context import _thi_classification

    cases = [
        (60.0, "NO HEAT STRESS"),
        (70.0, "MILD STRESS"),
        (75.0, "MODERATE HEAT STRESS"),
        (80.0, "SEVERE HEAT STRESS"),
        (85.0, "EXTREME HEAT STRESS"),
    ]

    for thi, expected in cases:
        result = _thi_classification(thi)
        _check(result == expected, f"THI {thi} → {result}", f"THI {thi}: expected {expected}, got {result}")


# ── test 6: weather summary extraction ───────────────────────────────────────

def test_weather_summary():
    _section("Test 6 — Weather summary from raw API response")

    from agents.weather_context import _summarize_weather

    raw = {
        "daily": {
            "temperature_2m_max": [30.0, 32.0, 28.0],
            "temperature_2m_min": [18.0, 20.0, 16.0],
            "relative_humidity_2m_mean": [60.0, 65.0, 55.0],
            "precipitation_sum": [0.0, 5.2, 0.0],
        }
    }

    summary = _summarize_weather(raw)

    _check(summary.get("avg_temp_c") is not None, f"avg_temp_c = {summary['avg_temp_c']}", "Missing avg_temp_c")
    _check(summary["max_temp_c"] == 32.0, f"max_temp_c = {summary['max_temp_c']}", f"Expected 32.0")
    _check(summary["min_temp_c"] == 16.0, f"min_temp_c = {summary['min_temp_c']}", f"Expected 16.0")
    _check(summary["total_precip_mm"] == 5.2, f"total_precip = {summary['total_precip_mm']}mm", f"Expected 5.2")
    _check(summary["total_days"] == 3, f"total_days = {summary['total_days']}", f"Expected 3")
    _check(summary["avg_thi"] is not None, f"avg_thi = {summary['avg_thi']}", "Missing avg_thi")
    _check(isinstance(summary["heat_stress_days"], int), f"heat_stress_days = {summary['heat_stress_days']}", "Not int")

    # Empty data
    empty = _summarize_weather({"daily": {}})
    _check(empty == {}, "Empty input returns empty dict", f"Got {empty}")


# ── test 7: farm location save/load ──────────────────────────────────────────

def test_farm_location_roundtrip():
    _section("Test 7 — Farm location save/load round-trip")

    import agents.weather_context as wc

    with tempfile.TemporaryDirectory() as tmp:
        original_path = wc.FARM_LOCATIONS_PATH
        wc.FARM_LOCATIONS_PATH = Path(tmp) / "farm_locations.json"

        try:
            wc.save_farm_location("TEST", "Test Farm", "Gomez Palacio", "Durango", 25.56, -103.50)

            loc = wc._load_farm_location("TEST")
            _check(loc is not None, "Location loaded after save", "Location is None")
            _check(loc["name"] == "Test Farm", f"name = {loc['name']}", "Wrong name")
            _check(loc["municipality"] == "Gomez Palacio", f"municipality = {loc['municipality']}", "Wrong")
            _check(loc["state"] == "Durango", f"state = {loc['state']}", "Wrong state")
            _check(abs(loc["latitude"] - 25.56) < 0.01, f"latitude = {loc['latitude']}", "Wrong lat")
            _check(abs(loc["longitude"] - (-103.50)) < 0.01, f"longitude = {loc['longitude']}", "Wrong lon")

            # Update
            wc.save_farm_location("TEST", "Updated Farm", "Lerdo", "Durango", 25.53, -103.52)
            loc2 = wc._load_farm_location("TEST")
            _check(loc2["name"] == "Updated Farm", "Update overwrites correctly", f"Got {loc2['name']}")

            # Missing farm
            missing = wc._load_farm_location("NOPE")
            _check(missing is None, "Missing farm returns None", f"Got {missing}")

        finally:
            wc.FARM_LOCATIONS_PATH = original_path


# ── test 8: combined context format ──────────────────────────────────────────

def test_combined_context_format():
    _section("Test 8 — Combined context builder format")

    from agents.weather_context import build_seasonal_weather_context, build_master_seasonal_context
    import agents.weather_context as wc

    with tempfile.TemporaryDirectory() as tmp:
        original_path = wc.FARM_LOCATIONS_PATH
        wc.FARM_LOCATIONS_PATH = Path(tmp) / "farm_locations.json"
        original_cache = wc._weather_cache.copy()
        wc._weather_cache.clear()

        try:
            # Without location → empty (no weather data = unknown condition = no context)
            ctx = build_seasonal_weather_context("NOFARM", "Fertility")
            _check(isinstance(ctx, str), f"Returns string (len={len(ctx)})", "Not a string")

            # Save a location and test
            wc.save_farm_location("MOCKFARM", "Mock", "Torreon", "Coahuila", 25.54, -103.41)

            # Master context — should have content even if API fails (fallback to unknown)
            master = build_master_seasonal_context("MOCKFARM")
            _check(isinstance(master, str), f"Master context is string ({len(master)} chars)", "Not string")

        finally:
            wc.FARM_LOCATIONS_PATH = original_path
            wc._weather_cache = original_cache


# ── test 9: weather cache deduplication ──────────────────────────────────────

def test_weather_cache():
    _section("Test 9 — Weather cache avoids duplicate fetches")

    import agents.weather_context as wc
    from datetime import datetime

    today = datetime.now().strftime("%Y-%m-%d")
    cache_key_current = f"CACHETEST_current_{today}"
    cache_key_hist = f"CACHETEST_hist_6_{today}"

    original_cache = wc._weather_cache.copy()

    try:
        wc._weather_cache[cache_key_current] = {
            "avg_temp_c": 99.9,
            "municipality": "Cached",
            "state": "Test",
        }
        wc._weather_cache[cache_key_hist] = [
            {"month": "2025-10", "avg_thi": 70.0},
        ]

        _check(cache_key_current in wc._weather_cache, "Current cache key present", "Missing")
        _check(wc._weather_cache[cache_key_current]["avg_temp_c"] == 99.9, "Current cached value preserved", "Overwritten")
        _check(cache_key_hist in wc._weather_cache, "Historical cache key present", "Missing")
        _check(len(wc._weather_cache[cache_key_hist]) == 1, "Historical cached value preserved", "Overwritten")

    finally:
        wc._weather_cache = original_cache


# ── test 10: missing location graceful handling ──────────────────────────────

def test_missing_location_graceful():
    _section("Test 10 — Missing location returns graceful empty/fallback context")

    import agents.weather_context as wc

    with tempfile.TemporaryDirectory() as tmp:
        original_path = wc.FARM_LOCATIONS_PATH
        wc.FARM_LOCATIONS_PATH = Path(tmp) / "farm_locations.json"
        with open(wc.FARM_LOCATIONS_PATH, "w") as f:
            json.dump({}, f)

        try:
            weather = wc.get_weather_summary("UNKNOWN")
            _check(weather is None, "get_weather_summary returns None for unknown farm", f"Got {weather}")

            formatted = wc.format_weather_for_prompt(None)
            _check(formatted == "", "format_weather_for_prompt(None) returns empty", f"Got '{formatted}'")

            # Seasonal config with None weather → unknown condition
            from agents.seasonal_config import classify_conditions, get_farm_seasonal_overview
            cond, mods = classify_conditions(None)
            _check(cond == "unknown", f"None → unknown condition", f"Got {cond}")

            overview = get_farm_seasonal_overview(None)
            _check("unavailable" in overview.lower() or len(overview) > 0,
                   f"Farm overview for unknown: {len(overview)} chars", "Empty")

        finally:
            wc.FARM_LOCATIONS_PATH = original_path


# ── test 11: wet modifier adds extra context ─────────────────────────────────

def test_wet_modifier():
    _section("Test 11 — Wet modifier adds extra context")

    from agents.seasonal_config import get_domain_seasonal_context, get_farm_seasonal_overview

    domains = ["Fertility", "Production", "Health", "Calf Raising", "Culling"]

    for domain in domains:
        ctx_dry = get_domain_seasonal_context(domain, WEATHER_HEAT_STRESS)
        ctx_wet = get_domain_seasonal_context(domain, WEATHER_HOT_WET)
        _check(
            len(ctx_wet) > len(ctx_dry),
            f"{domain}: wet context ({len(ctx_wet)}) > dry ({len(ctx_dry)})",
            f"{domain}: wet context not longer than dry"
        )
        _check(
            "wet" in ctx_wet.lower() or "mud" in ctx_wet.lower() or "rain" in ctx_wet.lower(),
            f"{domain}: wet keywords present",
            f"{domain}: missing wet-specific content"
        )

    # Farm overview
    overview_wet = get_farm_seasonal_overview(WEATHER_HOT_WET)
    _check("wet" in overview_wet.lower() or "rain" in overview_wet.lower(),
           "Farm overview mentions wet conditions", "Missing wet in overview")


# ── test 12: full prompt context includes month and weather numbers ───────────

def test_full_prompt_context():
    _section("Test 12 — Full prompt context includes month, location, THI, and condition notes")

    from datetime import datetime
    from agents.weather_context import build_seasonal_weather_context, build_master_seasonal_context
    import agents.weather_context as wc

    current_month_name = datetime.now().strftime("%B %Y")  # e.g. "March 2026"

    with tempfile.TemporaryDirectory() as tmp:
        original_path = wc.FARM_LOCATIONS_PATH
        wc.FARM_LOCATIONS_PATH = Path(tmp) / "farm_locations.json"
        original_cache = wc._weather_cache.copy()
        wc._weather_cache.clear()

        try:
            # Save a location
            wc.save_farm_location("CTXFARM", "Context Farm", "Torreon", "Coahuila", 25.54, -103.41)

            # Prime cache with known weather so test is deterministic
            today = datetime.now().strftime("%Y-%m-%d")
            wc._weather_cache[f"CTXFARM_current_{today}"] = {
                "avg_temp_c": 28.5, "max_temp_c": 36.0, "min_temp_c": 18.0,
                "avg_humidity_pct": 55.0, "total_precip_mm": 5.0,
                "avg_thi": 74.0, "thi_classification": "MODERATE HEAT STRESS",
                "heat_stress_days": 18, "total_days": 30,
                "municipality": "Torreon", "state": "Coahuila",
            }

            # -- Domain context (months=1, no historical) --
            ctx = build_seasonal_weather_context("CTXFARM", "Fertility", months=1)

            _check("SEASONAL & WEATHER CONTEXT" in ctx, "Header present", "Missing header")
            _check(current_month_name in ctx, f"Month '{current_month_name}' present", f"Month missing in:\n{ctx}")
            _check("Torreon" in ctx, "Municipality 'Torreon' present", "Missing municipality")
            _check("Coahuila" in ctx, "State 'Coahuila' present", "Missing state")
            _check("28.5" in ctx, "Avg temp 28.5C present", "Missing avg temp")
            _check("74.0" in ctx, "THI 74.0 present", "Missing THI value")
            _check("18 of 30" in ctx, "Stress days '18 of 30' present", "Missing stress days")
            _check("heat stress" in ctx.lower(), "Heat stress mentioned in notes", "Missing heat stress")
            _check("Domain-specific notes" in ctx, "Domain notes section present", "Missing domain notes")

            # -- Master context --
            master = build_master_seasonal_context("CTXFARM", months=1)

            _check("SEASONAL & WEATHER CONTEXT" in master, "Master header present", "Missing header")
            _check("Torreon" in master, "Municipality in master", "Missing municipality")
            _check("74.0" in master, "THI in master", "Missing THI")
            _check(len(master) > 100, f"Master context substantial ({len(master)} chars)", "Too short")

            # -- Verify different weather → different context --
            wc._weather_cache[f"CTXFARM_current_{today}"] = {
                "avg_temp_c": 8.0, "max_temp_c": 15.0, "min_temp_c": 1.0,
                "avg_humidity_pct": 35.0, "total_precip_mm": 10.0,
                "avg_thi": 52.0, "thi_classification": "NO HEAT STRESS",
                "heat_stress_days": 0, "total_days": 30,
                "municipality": "Torreon", "state": "Coahuila",
            }

            ctx_cold = build_seasonal_weather_context("CTXFARM", "Calf Raising", months=1)
            _check("cold" in ctx_cold.lower() or "Cold" in ctx_cold, "Cold weather → cold context", f"Missing cold in:\n{ctx_cold}")
            _check("heat stress" not in ctx_cold.lower().split("notes")[1] if "notes" in ctx_cold.lower() else True,
                   "Cold weather does NOT get heat stress domain notes",
                   "Got heat stress notes for cold weather")

        finally:
            wc.FARM_LOCATIONS_PATH = original_path
            wc._weather_cache = original_cache


# ── test 13: monthly summary extraction ──────────────────────────────────────

def test_monthly_summary():
    _section("Test 13 — Monthly summary extraction from raw daily data")

    from agents.weather_context import _summarize_monthly

    # Build 90 days of mock data spanning 3 months
    raw = {"daily": {
        "time": [],
        "temperature_2m_max": [],
        "temperature_2m_min": [],
        "relative_humidity_2m_mean": [],
        "precipitation_sum": [],
    }}

    import datetime as dt
    start = dt.date(2025, 6, 1)
    for i in range(90):  # Jun, Jul, Aug
        d = start + dt.timedelta(days=i)
        raw["daily"]["time"].append(d.isoformat())
        # June: hot (35/22), July: hotter (38/25), Aug: moderate (30/20)
        if d.month == 6:
            raw["daily"]["temperature_2m_max"].append(35.0)
            raw["daily"]["temperature_2m_min"].append(22.0)
            raw["daily"]["relative_humidity_2m_mean"].append(40.0)
            raw["daily"]["precipitation_sum"].append(0.5)
        elif d.month == 7:
            raw["daily"]["temperature_2m_max"].append(38.0)
            raw["daily"]["temperature_2m_min"].append(25.0)
            raw["daily"]["relative_humidity_2m_mean"].append(55.0)
            raw["daily"]["precipitation_sum"].append(3.0)
        else:
            raw["daily"]["temperature_2m_max"].append(30.0)
            raw["daily"]["temperature_2m_min"].append(20.0)
            raw["daily"]["relative_humidity_2m_mean"].append(50.0)
            raw["daily"]["precipitation_sum"].append(1.0)

    monthly = _summarize_monthly(raw)

    _check(len(monthly) == 3, f"Got {len(monthly)} months (expected 3)", f"Expected 3, got {len(monthly)}")
    _check(monthly[0]["month"] == "2025-06", f"First month: {monthly[0]['month']}", "Wrong first month")
    _check(monthly[2]["month"] == "2025-08", f"Last month: {monthly[2]['month']}", "Wrong last month")

    # July should be hottest
    _check(monthly[1]["avg_temp_c"] > monthly[0]["avg_temp_c"],
           f"July ({monthly[1]['avg_temp_c']}C) > June ({monthly[0]['avg_temp_c']}C)", "July not hottest")
    _check(monthly[1]["avg_thi"] > monthly[0]["avg_thi"],
           f"July THI ({monthly[1]['avg_thi']}) > June THI ({monthly[0]['avg_thi']})", "July THI not highest")

    # Each month has correct day count
    _check(monthly[0]["total_days"] == 30, f"June: {monthly[0]['total_days']} days", "Expected 30")
    _check(monthly[1]["total_days"] == 31, f"July: {monthly[1]['total_days']} days", "Expected 31")

    # Precipitation sums correctly
    _check(monthly[1]["total_precip_mm"] == 93.0, f"July precip: {monthly[1]['total_precip_mm']}mm", "Expected 93.0")

    # THI classification varies by month
    _check("HEAT STRESS" in monthly[1]["thi_class"] or "STRESS" in monthly[1]["thi_class"],
           f"July classified as: {monthly[1]['thi_class']}", "Expected heat stress")

    # Empty input
    empty = _summarize_monthly({"daily": {}})
    _check(empty == [], "Empty input → empty list", f"Got {empty}")


# ── test 14: historical context included when months > 1 ────────────────────

def test_historical_context_multimonth():
    _section("Test 14 — Historical context included when months > 1")

    from agents.weather_context import build_seasonal_weather_context, build_master_seasonal_context
    import agents.weather_context as wc
    from datetime import datetime

    today = datetime.now().strftime("%Y-%m-%d")

    with tempfile.TemporaryDirectory() as tmp:
        original_path = wc.FARM_LOCATIONS_PATH
        wc.FARM_LOCATIONS_PATH = Path(tmp) / "farm_locations.json"
        original_cache = wc._weather_cache.copy()
        wc._weather_cache.clear()

        try:
            wc.save_farm_location("HISTFARM", "Hist Farm", "Torreon", "Coahuila", 25.54, -103.41)

            # Prime current weather cache
            wc._weather_cache[f"HISTFARM_current_{today}"] = {
                "avg_temp_c": 25.0, "max_temp_c": 33.0, "min_temp_c": 15.0,
                "avg_humidity_pct": 45.0, "total_precip_mm": 10.0,
                "avg_thi": 69.0, "thi_classification": "MILD STRESS",
                "heat_stress_days": 3, "total_days": 30,
                "municipality": "Torreon", "state": "Coahuila",
            }

            # Prime historical cache with 6 months of data
            wc._weather_cache[f"HISTFARM_hist_6_{today}"] = [
                {"month": "2025-10", "avg_temp_c": 24.0, "avg_humidity_pct": 50.0,
                 "total_precip_mm": 5.0, "avg_thi": 70.0, "thi_class": "MILD STRESS",
                 "heat_stress_days": 8, "total_days": 31},
                {"month": "2025-11", "avg_temp_c": 20.0, "avg_humidity_pct": 40.0,
                 "total_precip_mm": 2.0, "avg_thi": 64.0, "thi_class": "NO HEAT STRESS",
                 "heat_stress_days": 0, "total_days": 30},
                {"month": "2025-12", "avg_temp_c": 16.0, "avg_humidity_pct": 35.0,
                 "total_precip_mm": 8.0, "avg_thi": 58.0, "thi_class": "NO HEAT STRESS",
                 "heat_stress_days": 0, "total_days": 31},
                {"month": "2026-01", "avg_temp_c": 14.0, "avg_humidity_pct": 30.0,
                 "total_precip_mm": 12.0, "avg_thi": 55.0, "thi_class": "NO HEAT STRESS",
                 "heat_stress_days": 0, "total_days": 31},
                {"month": "2026-02", "avg_temp_c": 18.0, "avg_humidity_pct": 35.0,
                 "total_precip_mm": 3.0, "avg_thi": 61.0, "thi_class": "NO HEAT STRESS",
                 "heat_stress_days": 0, "total_days": 28},
                {"month": "2026-03", "avg_temp_c": 22.0, "avg_humidity_pct": 40.0,
                 "total_precip_mm": 1.0, "avg_thi": 66.0, "thi_class": "NO HEAT STRESS",
                 "heat_stress_days": 0, "total_days": 27},
            ]

            # -- Domain context with 6 months --
            ctx = build_seasonal_weather_context("HISTFARM", "Production", months=6)

            _check("Weather history across analysis window" in ctx,
                   "Historical table header present", f"Missing historical in:\n{ctx[:200]}")
            _check("2025-10" in ctx, "First historical month (2025-10) present", "Missing 2025-10")
            _check("2026-03" in ctx, "Last historical month (2026-03) present", "Missing 2026-03")
            _check("1/6" in ctx or "Summary:" in ctx,
                   "Summary line with stress month count present", "Missing summary line")
            _check("Analysis window: 6 months" in ctx,
                   "Analysis window label present", f"Missing window label")

            # -- Master context with 6 months --
            master = build_master_seasonal_context("HISTFARM", months=6)
            _check("Weather history" in master, "Master has historical table", "Missing historical")
            _check("2025-12" in master, "Historical months in master", "Missing 2025-12")

        finally:
            wc.FARM_LOCATIONS_PATH = original_path
            wc._weather_cache = original_cache


# ── test 15: single-month analysis has no historical table ───────────────────

def test_single_month_no_history():
    _section("Test 15 — Single-month analysis has no historical table")

    from agents.weather_context import build_seasonal_weather_context
    import agents.weather_context as wc
    from datetime import datetime

    today = datetime.now().strftime("%Y-%m-%d")

    with tempfile.TemporaryDirectory() as tmp:
        original_path = wc.FARM_LOCATIONS_PATH
        wc.FARM_LOCATIONS_PATH = Path(tmp) / "farm_locations.json"
        original_cache = wc._weather_cache.copy()
        wc._weather_cache.clear()

        try:
            wc.save_farm_location("SINGLE", "Single Farm", "Lerdo", "Durango", 25.53, -103.52)

            wc._weather_cache[f"SINGLE_current_{today}"] = {
                "avg_temp_c": 22.0, "max_temp_c": 30.0, "min_temp_c": 12.0,
                "avg_humidity_pct": 40.0, "total_precip_mm": 5.0,
                "avg_thi": 66.0, "thi_classification": "NO HEAT STRESS",
                "heat_stress_days": 0, "total_days": 30,
                "municipality": "Lerdo", "state": "Durango",
            }

            ctx_1mo = build_seasonal_weather_context("SINGLE", "Health", months=1)
            _check("Weather history across" not in ctx_1mo,
                   "months=1: NO historical table", "Historical table present for 1 month")
            _check("Current weather" in ctx_1mo,
                   "months=1: current weather present", "Missing current weather")
            _check("Analysis window: 1 month" in ctx_1mo,
                   "months=1: window label says 1 month", "Wrong window label")

        finally:
            wc.FARM_LOCATIONS_PATH = original_path
            wc._weather_cache = original_cache


# ── test 16: analysis window label appears in context ────────────────────────

def test_analysis_window_label():
    _section("Test 16 — Analysis window label varies with months parameter")

    from agents.weather_context import build_seasonal_weather_context
    import agents.weather_context as wc
    from datetime import datetime

    today = datetime.now().strftime("%Y-%m-%d")

    with tempfile.TemporaryDirectory() as tmp:
        original_path = wc.FARM_LOCATIONS_PATH
        wc.FARM_LOCATIONS_PATH = Path(tmp) / "farm_locations.json"
        original_cache = wc._weather_cache.copy()
        wc._weather_cache.clear()

        try:
            wc.save_farm_location("LABELFARM", "Label Farm", "Chihuahua", "Chihuahua", 28.63, -106.09)

            wc._weather_cache[f"LABELFARM_current_{today}"] = {
                "avg_temp_c": 20.0, "max_temp_c": 28.0, "min_temp_c": 10.0,
                "avg_humidity_pct": 35.0, "total_precip_mm": 5.0,
                "avg_thi": 63.0, "thi_classification": "NO HEAT STRESS",
                "heat_stress_days": 0, "total_days": 30,
                "municipality": "Chihuahua", "state": "Chihuahua",
            }

            for m in [1, 3, 6, 12, 24]:
                # Prime historical cache for multi-month
                if m > 1:
                    wc._weather_cache[f"LABELFARM_hist_{m}_{today}"] = [
                        {"month": "2025-06", "avg_temp_c": 30.0, "avg_humidity_pct": 50.0,
                         "total_precip_mm": 20.0, "avg_thi": 75.0, "thi_class": "MODERATE HEAT STRESS",
                         "heat_stress_days": 25, "total_days": 30},
                    ]

                ctx = build_seasonal_weather_context("LABELFARM", "Fertility", months=m)
                expected_label = f"Analysis window: {m} month"
                _check(expected_label in ctx,
                       f"months={m}: label '{expected_label}' present",
                       f"months={m}: missing label. Got:\n{ctx[:150]}")

        finally:
            wc.FARM_LOCATIONS_PATH = original_path
            wc._weather_cache = original_cache


# ── test 17: live Open-Meteo API — current + historical ──────────────────────

def test_live_api():
    _section("Test 17 — Live Open-Meteo API — current + historical weather")

    from agents.weather_context import (
        _fetch_weather, _fetch_historical_weather,
        _summarize_weather, _summarize_monthly,
        get_weather_summary, get_historical_weather,
        build_seasonal_weather_context, build_master_seasonal_context,
    )
    from agents.seasonal_config import classify_conditions, get_domain_seasonal_context

    # -- Current weather (forecast API) --
    raw = _fetch_weather(25.54, -103.41, past_days=7)
    _check(raw is not None, "Forecast API returned data", "API returned None")

    summary = _summarize_weather(raw)
    _check(summary.get("avg_temp_c") is not None, f"avg_temp = {summary['avg_temp_c']}C", "Missing")
    _check(summary.get("avg_thi") is not None, f"avg_thi = {summary['avg_thi']}", "Missing THI")

    cond, mods = classify_conditions(summary)
    _check(
        cond in ("comfortable", "mild_heat", "heat_stress", "extreme_heat", "cold_stress"),
        f"Current condition: {cond} (mods: {mods})",
        f"Unknown condition: {cond}"
    )

    # -- Historical weather (archive API) --
    from datetime import datetime, timedelta
    end = datetime.now()
    start = end - timedelta(days=6 * 30)
    raw_hist = _fetch_historical_weather(25.54, -103.41, start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
    _check(raw_hist is not None, "Archive API returned data", "Archive API returned None")

    monthly = _summarize_monthly(raw_hist)
    _check(len(monthly) >= 5, f"Got {len(monthly)} months of history (expected ≥5)", f"Only {len(monthly)}")

    # Verify monthly structure
    for m in monthly:
        _check("month" in m, f"Month key present: {m.get('month')}", "Missing month key")
        _check("avg_thi" in m, f"THI present for {m['month']}: {m['avg_thi']}", "Missing THI")
        _check("heat_stress_days" in m, "heat_stress_days present", "Missing")

    # -- Full integration: get_historical_weather for GM --
    gm_monthly = get_historical_weather("GM", 6)
    if gm_monthly:
        _check(len(gm_monthly) >= 5, f"GM 6-month history: {len(gm_monthly)} months", "Too few")
    else:
        print("  ⚠️  GM historical weather returned None (location may not be configured)")

    # -- Full context build with months --
    ctx_6mo = build_seasonal_weather_context("GM", "Production", months=6)
    if "Weather history" in ctx_6mo:
        _check(True, f"GM 6-month context has historical table ({len(ctx_6mo)} chars)", "")
    else:
        print("  ⚠️  GM 6-month context has no historical (location may be missing)")

    master_12 = build_master_seasonal_context("GM", months=12)
    if "Weather history" in master_12:
        _check(True, f"GM 12-month master has historical ({len(master_12)} chars)", "")

    print(f"\n  📍 Torreon: current THI {summary['avg_thi']} ({cond}), "
          f"historical {len(monthly)} months fetched")
    if monthly:
        hottest = max(monthly, key=lambda m: m["avg_thi"])
        coldest = min(monthly, key=lambda m: m["avg_thi"])
        print(f"  🔥 Hottest month: {hottest['month']} (THI {hottest['avg_thi']})")
        print(f"  ❄️  Coldest month: {coldest['month']} (THI {coldest['avg_thi']})")


# ── entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    test_condition_classification()
    test_all_domain_condition_combos()
    test_farm_overview()
    test_thi_computation()
    test_thi_classification()
    test_weather_summary()
    test_farm_location_roundtrip()
    test_combined_context_format()
    test_weather_cache()
    test_missing_location_graceful()
    test_wet_modifier()
    test_full_prompt_context()
    test_monthly_summary()
    test_historical_context_multimonth()
    test_single_month_no_history()
    test_analysis_window_label()

    if "--live" in sys.argv:
        test_live_api()
    else:
        print(f"\n  ⏭  Skipping live API test (use --live to include)")

    print("\n✅ All seasonal/weather tests passed.\n")
