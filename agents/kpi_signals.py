"""
Pre-processes raw KPI time series into structured signals before the LLM sees them.

Instead of dumping 10 rows of raw numbers, this produces per-KPI:
  - last_value     : Most recent observation
  - trend          : 'improving' | 'stable' | 'worsening' (based on last 4 points)
  - anomaly        : True if last value is >1.5 std from the series mean
  - vs_benchmark   : 'within_target' | 'below_target' | 'above_target' |
                     'critical_low' | 'critical_high' | 'unknown'
  - data_quality   : 'good' | 'sparse' | 'poor'
  - unit           : Measurement unit from benchmarks
  - target_range   : Human-readable target from benchmarks
"""

from __future__ import annotations

import pandas as pd

from agents.kpi_benchmarks import KPI_BENCHMARKS


def compute_kpi_signals(rows: list[dict], kpi_names: list[str]) -> dict[str, dict]:
    """
    Compute structured signals for each KPI from raw time-series rows.

    Args:
        rows: List of dicts with 'Date' and KPI columns (from /get_farm_kpis).
        kpi_names: KPI aliases to process.

    Returns:
        Dict mapping kpi_alias → signal dict.
    """
    if not rows:
        return {}

    df = pd.DataFrame(rows)

    signals: dict[str, dict] = {}

    for kpi in kpi_names:
        if kpi not in df.columns:
            continue

        series = df[kpi].dropna()

        if len(series) == 0:
            signals[kpi] = {"data_quality": "poor", "note": "No data available"}
            continue

        total = len(df)
        valid = len(series)
        null_pct = (total - valid) / total if total > 0 else 1.0
        # Even sparse/poor quality — still surface the last known value
        data_quality = "good" if null_pct < 0.2 else ("sparse" if null_pct < 0.5 else "poor (limited data — use with caution)")

        last_val = float(series.iloc[-1])
        mean_val = float(series.mean())
        std_val = float(series.std()) if len(series) > 1 else 0.0

        # Trend from last 4 data points
        trend = "unknown"
        if len(series) >= 2:
            trend_series = series.tail(4)
            slope = float(trend_series.diff().mean())
            bench = KPI_BENCHMARKS.get(kpi, {})
            higher_is_better = bench.get("higher_is_better", True)

            # Stable = slope < 1% of mean magnitude
            threshold = abs(mean_val) * 0.01 if mean_val != 0 else 0.001
            if abs(slope) < threshold:
                trend = "stable"
            elif (slope > 0 and higher_is_better) or (slope < 0 and not higher_is_better):
                trend = "improving"
            else:
                trend = "worsening"

        # Anomaly: last value is >1.5 std from mean
        anomaly = False
        if std_val > 0:
            anomaly = abs(last_val - mean_val) / std_val > 1.5

        # Benchmark comparison
        bench = KPI_BENCHMARKS.get(kpi, {})
        vs_benchmark = "unknown"
        if bench:
            higher_is_better = bench.get("higher_is_better", True)
            w_min = bench.get("warning_min")
            w_max = bench.get("warning_max")
            t_min = bench.get("target_min")
            t_max = bench.get("target_max")

            if w_min is not None and last_val < w_min:
                vs_benchmark = "critical_low"
            elif w_max is not None and last_val > w_max:
                vs_benchmark = "critical_high"
            elif t_min is not None and last_val < t_min:
                vs_benchmark = "below_target"
            elif t_max is not None and last_val > t_max:
                vs_benchmark = "above_target"
            else:
                vs_benchmark = "within_target"

        signals[kpi] = {
            "last_value": round(last_val, 3),
            "period_mean": round(mean_val, 3),
            "trend": trend,
            "anomaly": anomaly,
            "vs_benchmark": vs_benchmark,
            "data_quality": data_quality,
            "unit": bench.get("unit", ""),
            "target_range": bench.get("target_range", ""),
        }

    return signals


def format_signals_for_prompt(signals: dict[str, dict]) -> str:
    """
    Returns a human-readable signals block to inject into LLM prompts.
    """
    if not signals:
        return "No signal data available."

    lines = []
    for kpi, sig in signals.items():
        if sig.get("data_quality") == "poor":
            lines.append(f"  {kpi}: ⚠ NO DATA")
            continue

        flags = []
        if sig.get("anomaly"):
            flags.append("ANOMALY")
        vs = sig.get("vs_benchmark", "unknown")
        if vs in ("critical_low", "critical_high"):
            flags.append(f"⚠ {vs.upper()}")
        elif vs in ("below_target", "above_target"):
            flags.append(vs)

        flag_str = f"  [{', '.join(flags)}]" if flags else ""
        trend_arrow = {"improving": "↑", "worsening": "↓", "stable": "→", "unknown": "?"}.get(
            sig.get("trend", "unknown"), "?"
        )

        lines.append(
            f"  {kpi}: {sig['last_value']} {sig.get('unit','')} | "
            f"trend {trend_arrow} | benchmark: {sig.get('vs_benchmark','?')} "
            f"(target: {sig.get('target_range','N/A')}) | "
            f"quality: {sig.get('data_quality','?')}"
            f"{flag_str}"
        )

    return "\n".join(lines)


def build_rag_query_from_signals(signals: dict[str, dict], domain: str) -> str:
    """
    Build a focused RAG search query from anomalous or off-target KPIs.
    Falls back to a generic domain query if nothing stands out.
    """
    from agents.kpi_benchmarks import KPI_BENCHMARKS

    problem_queries = []
    for kpi, sig in signals.items():
        vs = sig.get("vs_benchmark", "unknown")
        trend = sig.get("trend", "unknown")
        if vs in ("critical_low", "critical_high", "below_target", "above_target") or sig.get("anomaly"):
            bench = KPI_BENCHMARKS.get(kpi, {})
            q = bench.get("rag_query", "")
            if q:
                problem_queries.append(q)

    if problem_queries:
        # Use the first 2 most relevant queries combined
        return " | ".join(problem_queries[:2])

    # Domain-level fallback queries
    domain_fallbacks = {
        "Fertility": "dairy reproductive management conception pregnancy rate",
        "Production": "dairy milk yield lactation performance management",
        "Health": "transition cow health metabolic disease prevention",
        "Calf Raising": "calf growth mortality management preweaning",
        "Culling": "dairy herd culling longevity replacement management",
    }
    return domain_fallbacks.get(domain, "dairy farm management best practices")
