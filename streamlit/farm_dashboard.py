import asyncio
import base64
import logging
import sys
from pathlib import Path

import pandas as pd
import requests
import streamlit as st

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from agents.master_summary_agent import run_master_summary
from agents.farm_history import save_run, load_recent_runs
from agents.weather_context import (
    _load_farm_location,
    save_farm_location,
    get_weather_summary,
    format_weather_for_prompt,
    FARM_LOCATIONS_PATH,
)
import json

# ================= CONFIG ====================
BRIDGE_URL = "http://localhost:8090"
# =============================================

st.set_page_config(page_title="🐄 Dairy Farm AI Advisor", layout="wide")

st.markdown(
    """
# 🐄 Dairy Farm AI Advisor
Your data, your insights — powered by Dairy Farm Intelligence Unit.
"""
)

farm_code = st.text_input("Farm Code", "GM")
language = "es"
months = st.slider("Months to analyze", 3, 24, 3)

# --- Farm Location ---
existing_location = _load_farm_location(farm_code)

with st.expander("Farm Location (for weather context)", expanded=existing_location is None):
    loc_col1, loc_col2 = st.columns(2)
    with loc_col1:
        farm_name = st.text_input("Farm Name", value=existing_location.get("name", "") if existing_location else "")
        municipality = st.text_input("Municipality", value=existing_location.get("municipality", "") if existing_location else "")
        state = st.text_input("State", value=existing_location.get("state", "") if existing_location else "")
    with loc_col2:
        latitude = st.number_input("Latitude", value=existing_location.get("latitude", 25.0) if existing_location else 25.0, format="%.4f", min_value=-90.0, max_value=90.0)
        longitude = st.number_input("Longitude", value=existing_location.get("longitude", -103.0) if existing_location else -103.0, format="%.4f", min_value=-180.0, max_value=180.0)

    if st.button("Save Location"):
        save_farm_location(farm_code, farm_name, municipality, state, latitude, longitude)
        st.success(f"Location saved for {farm_code}: {municipality}, {state} ({latitude}, {longitude})")
        st.rerun()

    if existing_location:
        weather = get_weather_summary(farm_code)
        if weather:
            thi = weather.get("avg_thi", "?")
            thi_class = weather.get("thi_classification", "?")
            stress_days = weather.get("heat_stress_days", 0)
            st.info(
                f"Current weather: {weather.get('avg_temp_c')}C avg | "
                f"THI {thi} ({thi_class}) | "
                f"{stress_days} heat stress days | "
                f"{weather.get('total_precip_mm')}mm rain"
            )

analyze_button = st.button("🔍 Analyze Farm Performance")


def get_farm_kpis(farm_code, language, months=13):
    """Call the FastAPI MCP bridge to fetch KPI data."""
    url = f"{BRIDGE_URL}/get_farm_kpis"
    resp = requests.get(
        url, params={"farm_code": farm_code, "language": language, "months": months}
    )
    resp.raise_for_status()
    data = resp.json()
    if isinstance(data, dict):
        data = [data]
    df = pd.DataFrame(data)
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df = df.sort_values("Date")
    return df


def analyze_kpis(farm_code, metric, days, months):
    """Call the FastAPI MCP bridge to analyze the selected metric."""
    url = f"{BRIDGE_URL}/analyze_kpis"
    resp = requests.get(
        url,
        params={
            "farm_code": farm_code,
            "metric": metric,
            "days": days,
            "months": months,
        },
    )
    resp.raise_for_status()
    return resp.json()


def get_critical_plot(farm_code, language, days=90, top_n=5):
    """Fetch a combined critical KPI plot as base64 PNG."""
    url = f"{BRIDGE_URL}/plot_critical_kpis"
    resp = requests.get(
        url,
        params={
            "farm_code": farm_code,
            "language": language,
            "days": days,
            "top_n": top_n,
        },
    )
    resp.raise_for_status()
    data = resp.json()
    return data


def generate_master_summary(farm_code: str, language: str, months: int):
    """Run the async master summary agent."""
    return asyncio.run(
        run_master_summary(farm_code=farm_code, language=language, months=months)
    )


# --------------- UI FLOW ---------------
if analyze_button:
    with st.spinner("Fetching data and analyzing farm performance..."):
        try:
            df = get_farm_kpis(farm_code, language, months)

            st.markdown("---")

            master_report = generate_master_summary(farm_code, language, months)
            try:
                saved_path = save_run(master_report, months=months)
                st.toast(f"Run saved to history: {saved_path.name}", icon="💾")
            except Exception as e:
                st.warning(f"Could not save run history: {e}")
            final_summary = master_report.get("final_summary", {})
            overview = master_report.get("overview", "")

            st.markdown("### 🧠 Executive Summary")
            if overview:
                st.markdown(f"**Overview:** {overview}")
            st.markdown(final_summary.get("executive_summary", ""))

            priority_actions = final_summary.get("priority_actions", [])
            if priority_actions:
                st.markdown("### 🎯 Priority Actions")
                for action in priority_actions:
                    st.markdown(f"- {action}")

            causal_chains = final_summary.get("causal_chains", [])
            if causal_chains:
                st.markdown("### 🔗 Causal Risk Chains")
                st.caption(
                    "KPIs that are anomalous today and their predicted downstream effects."
                )
                severity_colors = {"high": "🔴", "moderate": "🟡", "low": "🟢"}
                for chain in causal_chains:
                    severity = chain.get("severity", "low")
                    icon = severity_colors.get(severity, "⚪")
                    cause = chain.get("cause", "?")
                    effect = chain.get("effect", "?")
                    timeline = chain.get("timeline", "?")
                    reasoning = chain.get("reasoning", "")
                    action = chain.get("preventive_action", "")
                    with st.container(border=True):
                        st.markdown(
                            f"{icon} **{cause}** → **{effect}** &nbsp;&nbsp; `{timeline}` &nbsp;&nbsp; severity: *{severity}*"
                        )
                        if reasoning:
                            st.markdown(f"_{reasoning}_")
                        if action:
                            st.markdown(f"**Preventive action:** {action}")

            domains_overview = final_summary.get("domains_overview", {})
            if domains_overview:
                st.markdown("### 📂 Domain Snapshots")
                for domain_name, note in domains_overview.items():
                    st.markdown(f"**{domain_name}:** {note}")

            if master_report.get("domains"):
                st.markdown("### 🔍 Domain Deep Dives")
                for domain, payload in master_report["domains"].items():
                    st.markdown(f"#### {domain}")
                    st.markdown(payload.get("summary", ""))
                    issues = payload.get("issues") or []
                    if issues:
                        st.markdown("**Issues:**")
                        for issue in issues:
                            st.markdown(f"- {issue}")

                    recommendations = payload.get("recommendations", {})
                    for horizon, items in recommendations.items():
                        if items:
                            st.markdown(f"**{horizon} actions:**")
                            for item in items:
                                st.markdown(f"- {item}")

            selected_kpis = master_report.get("urgent_kpis") or []
            if not selected_kpis and master_report.get("domains"):
                collected = []
                for payload in master_report["domains"].values():
                    for alias in payload.get("kpis_to_plot", []):
                        if alias not in collected:
                            collected.append(alias)
                selected_kpis = collected

            if selected_kpis:
                st.markdown(f"**KPI focus:** {', '.join(selected_kpis)}")
            else:
                st.markdown("**KPI focus:** No KPIs highlighted.")

            if selected_kpis:
                try:
                    payload = {
                        "farm_code": farm_code,
                        "selected_kpis": selected_kpis,
                        "language": language,
                        "days": months * 30,
                    }
                    plot_resp = requests.post(
                        f"{BRIDGE_URL}/plot_selected_kpis", json=payload
                    )
                    plot_resp.raise_for_status()
                    plot_json = plot_resp.json()

                    logging.info("Received plot JSON:", plot_json)
                    st.markdown("### 📊 AI-Selected KPI Trends")
                    image_64 = plot_json.get("result").get("image_base64")
                    if image_64:

                        img_bytes = base64.b64decode(image_64)

                        logging.info("Decoded image bytes:", img_bytes)
                        st.markdown("#### AI-Selected KPI Trends")
                        st.image(
                            img_bytes,
                            caption="AI-selected KPI trends",
                            use_container_width=True,
                        )
                except Exception as e:
                    st.error(f"Error plotting AI-selected KPIs: {e}")
            else:
                st.info("No KPIs suggested by AI for plotting.")
            # --- History panel ---
            past_runs = load_recent_runs(farm_code, n=5)
            if past_runs:
                st.markdown("---")
                st.markdown("### 📋 Past Analyses")
                for rec in past_runs:
                    run_date = rec.get("run_date", "unknown")
                    health = rec.get("overall_health", "?")
                    health_icon = {"High": "🟢", "Medium": "🟡", "Low": "🔴"}.get(
                        health, "⚪"
                    )
                    months_label = rec.get("months_analyzed")
                    months_str = f" · {months_label}mo window" if months_label else ""
                    with st.expander(
                        f"{health_icon} {run_date}{months_str} — Overall health: {health}"
                    ):
                        urgent = rec.get("urgent_kpis", [])
                        if urgent:
                            st.markdown(f"**Urgent KPIs:** {', '.join(urgent)}")

                        summary = rec.get("executive_summary", "")
                        if summary:
                            st.markdown(f"_{summary}_")

                        actions = rec.get("priority_actions", [])
                        if actions:
                            st.markdown("**Priority actions recommended:**")
                            for a in actions:
                                st.markdown(f"- {a}")

                        chains = rec.get("causal_chains", [])
                        if chains:
                            st.markdown("**Causal risks flagged:**")
                            severity_icons = {
                                "high": "🔴",
                                "moderate": "🟡",
                                "low": "🟢",
                            }
                            for c in chains:
                                icon = severity_icons.get(c.get("severity", ""), "⚪")
                                st.markdown(
                                    f"{icon} `{c['cause']}` → `{c['effect']}` "
                                    f"in {c.get('timeline','?')} — _{c.get('preventive_action','')}_"
                                )

                        domain_issues = rec.get("domain_issues", {})
                        if domain_issues:
                            st.markdown("**Issues detected:**")
                            for domain, issues in domain_issues.items():
                                if issues:
                                    for issue in issues:
                                        st.markdown(f"- **{domain}:** {issue}")

        except requests.exceptions.RequestException as e:
            st.error(f"Error calling MCP bridge: {e}")
        except Exception as e:
            st.error(f"Unexpected error: {e}")
