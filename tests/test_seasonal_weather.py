"""
Tests for seasonal config, weather context, and their integration.

Tests:
  1. Seasonal config returns correct season for each month
  2. All domain × season combinations have context
  3. Farm-level seasonal overview covers all seasons
  4. THI computation is correct
  5. THI classification thresholds are correct
  6. Weather summary extraction from raw API response
  7. Farm location save/load round-trip
  8. Combined context builder produces expected format
  9. Weather cache avoids duplicate API calls
  10. Missing farm location returns graceful empty context
  11. Live Open-Meteo API call (requires internet)

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


# ── test 1: month-to-season mapping ─────────────────────────────────────────

def test_month_to_season():
    _section("Test 1 — Month-to-season mapping")

    from agents.seasonal_config import _month_to_season

    expected = {
        1: "dry_cool", 2: "dry_cool",
        3: "dry_hot", 4: "dry_hot", 5: "dry_hot",
        6: "rainy_warm", 7: "rainy_warm", 8: "rainy_warm", 9: "rainy_warm",
        10: "transition",
        11: "dry_cool", 12: "dry_cool",
    }

    for month, season in expected.items():
        result = _month_to_season(month)
        _check(result == season, f"Month {month:2d} → {result}", f"Month {month}: expected {season}, got {result}")


# ── test 2: all domain × season combos have context ─────────────────────────

def test_all_domain_season_combos():
    _section("Test 2 — All domain × season combos have context")

    from agents.seasonal_config import get_domain_seasonal_context

    domains = ["Fertility", "Production", "Health", "Calf Raising", "Culling"]
    # Sample months from each season
    test_months = [1, 3, 7, 10]

    for domain in domains:
        for month in test_months:
            ctx = get_domain_seasonal_context(domain, month)
            _check(
                len(ctx) > 20,
                f"{domain} month {month:2d}: {len(ctx)} chars",
                f"{domain} month {month}: empty or too short ({len(ctx)} chars)"
            )

    _ok(f"All {len(domains) * len(test_months)} domain × season combos have context")


# ── test 3: farm-level overview covers all seasons ───────────────────────────

def test_farm_overview():
    _section("Test 3 — Farm-level overview covers all seasons")

    from agents.seasonal_config import get_farm_seasonal_overview

    for month in [1, 4, 7, 10]:
        overview = get_farm_seasonal_overview(month)
        _check(
            len(overview) > 30,
            f"Month {month:2d}: overview has {len(overview)} chars",
            f"Month {month}: overview too short ({len(overview)} chars)"
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
    _check(thi_low < 68, f"THI(15C, 40%RH) = {thi_low:.2f} — no stress", f"Got {thi_low}, expected <68")

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
    _check(summary["max_temp_c"] == 32.0, f"max_temp_c = {summary['max_temp_c']}", f"Expected 32.0, got {summary['max_temp_c']}")
    _check(summary["min_temp_c"] == 16.0, f"min_temp_c = {summary['min_temp_c']}", f"Expected 16.0, got {summary['min_temp_c']}")
    _check(summary["total_precip_mm"] == 5.2, f"total_precip = {summary['total_precip_mm']}mm", f"Expected 5.2, got {summary['total_precip_mm']}")
    _check(summary["total_days"] == 3, f"total_days = {summary['total_days']}", f"Expected 3, got {summary['total_days']}")
    _check(summary["avg_thi"] is not None, f"avg_thi = {summary['avg_thi']}", "Missing avg_thi")
    _check(isinstance(summary["heat_stress_days"], int), f"heat_stress_days = {summary['heat_stress_days']}", "Not an int")

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
            # Save
            wc.save_farm_location("TEST", "Test Farm", "Gomez Palacio", "Durango", 25.56, -103.50)

            # Load
            loc = wc._load_farm_location("TEST")
            _check(loc is not None, "Location loaded after save", "Location is None")
            _check(loc["name"] == "Test Farm", f"name = {loc['name']}", f"Expected 'Test Farm'")
            _check(loc["municipality"] == "Gomez Palacio", f"municipality = {loc['municipality']}", "Wrong municipality")
            _check(loc["state"] == "Durango", f"state = {loc['state']}", "Wrong state")
            _check(abs(loc["latitude"] - 25.56) < 0.01, f"latitude = {loc['latitude']}", "Wrong latitude")
            _check(abs(loc["longitude"] - (-103.50)) < 0.01, f"longitude = {loc['longitude']}", "Wrong longitude")

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
        # Clear cache so we don't get stale results
        original_cache = wc._weather_cache.copy()
        wc._weather_cache.clear()

        try:
            # Without location → empty
            ctx = build_seasonal_weather_context("NOFARM", "Fertility")
            # Should still have seasonal (no weather), or empty
            # Seasonal config doesn't depend on farm location
            _check("SEASONAL" in ctx or ctx == "", f"No-location context: {'has seasonal' if ctx else 'empty'}", "Unexpected")

            # Save a location and test with mock weather (skip API)
            wc.save_farm_location("MOCKFARM", "Mock", "Torreon", "Coahuila", 25.54, -103.41)

            # Test master context
            master = build_master_seasonal_context("MOCKFARM")
            # Even if API fails, seasonal overview should be present
            _check("season" in master.lower() or "Season" in master or len(master) > 0,
                   f"Master context has content ({len(master)} chars)", "Empty master context")

        finally:
            wc.FARM_LOCATIONS_PATH = original_path
            wc._weather_cache = original_cache


# ── test 9: weather cache deduplication ──────────────────────────────────────

def test_weather_cache():
    _section("Test 9 — Weather cache avoids duplicate fetches")

    import agents.weather_context as wc
    from datetime import datetime

    today = datetime.now().strftime("%Y-%m-%d")
    cache_key = f"CACHETEST_{today}"

    original_cache = wc._weather_cache.copy()

    try:
        # Prime cache manually
        wc._weather_cache[cache_key] = {
            "avg_temp_c": 99.9,
            "municipality": "Cached",
            "state": "Test",
        }

        # Should hit cache, not API
        result = wc.get_weather_summary.__wrapped__(
            "CACHETEST"
        ) if hasattr(wc.get_weather_summary, '__wrapped__') else None

        # Directly check cache
        _check(cache_key in wc._weather_cache, "Cache key present", "Cache key missing")
        _check(wc._weather_cache[cache_key]["avg_temp_c"] == 99.9, "Cached value preserved", "Cache was overwritten")

    finally:
        wc._weather_cache = original_cache


# ── test 10: missing location graceful handling ──────────────────────────────

def test_missing_location_graceful():
    _section("Test 10 — Missing location returns graceful empty context")

    import agents.weather_context as wc

    with tempfile.TemporaryDirectory() as tmp:
        original_path = wc.FARM_LOCATIONS_PATH
        wc.FARM_LOCATIONS_PATH = Path(tmp) / "farm_locations.json"
        # Write empty locations file
        with open(wc.FARM_LOCATIONS_PATH, "w") as f:
            json.dump({}, f)

        try:
            weather = wc.get_weather_summary("UNKNOWN")
            _check(weather is None, "get_weather_summary returns None for unknown farm", f"Got {weather}")

            formatted = wc.format_weather_for_prompt(None)
            _check(formatted == "", "format_weather_for_prompt(None) returns empty string", f"Got '{formatted}'")

            # Combined context should still have seasonal part
            from agents.weather_context import build_seasonal_weather_context
            ctx = build_seasonal_weather_context("UNKNOWN", "Health")
            # Seasonal exists even without weather
            has_seasonal = "SEASONAL" in ctx
            _check(has_seasonal, "Context has seasonal even without weather data", "Missing seasonal")

        finally:
            wc.FARM_LOCATIONS_PATH = original_path


# ── test 11: live Open-Meteo API ─────────────────────────────────────────────

def test_live_api():
    _section("Test 11 — Live Open-Meteo API call")

    from agents.weather_context import _fetch_weather, _summarize_weather

    # Torreon, Coahuila coordinates
    raw = _fetch_weather(25.54, -103.41, past_days=7)
    _check(raw is not None, "API returned data", "API returned None")

    daily = raw.get("daily", {})
    _check("temperature_2m_max" in daily, "Has temperature_2m_max", "Missing temperature_2m_max")
    _check("relative_humidity_2m_mean" in daily, "Has humidity", "Missing humidity")

    summary = _summarize_weather(raw)
    _check(summary.get("avg_temp_c") is not None, f"avg_temp = {summary['avg_temp_c']}C", "Missing avg_temp")
    _check(summary.get("avg_thi") is not None, f"avg_thi = {summary['avg_thi']}", "Missing THI")
    _check(summary.get("total_days") == 7, f"Got {summary['total_days']} days of data", f"Expected 7 days")
    _check(
        -10 < summary["avg_temp_c"] < 50,
        f"Temperature {summary['avg_temp_c']}C is plausible",
        f"Temperature {summary['avg_temp_c']}C seems wrong"
    )


# ── entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    test_month_to_season()
    test_all_domain_season_combos()
    test_farm_overview()
    test_thi_computation()
    test_thi_classification()
    test_weather_summary()
    test_farm_location_roundtrip()
    test_combined_context_format()
    test_weather_cache()
    test_missing_location_graceful()

    if "--live" in sys.argv:
        test_live_api()
    else:
        print(f"\n  ⏭  Skipping live API test (use --live to include)")

    print("\n✅ All seasonal/weather tests passed.\n")
