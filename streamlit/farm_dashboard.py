import streamlit as st
import pandas as pd
import requests
import json
import os
from openai import OpenAI
import logging
import base64

# ================= CONFIG ====================
BRIDGE_URL = "http://localhost:8090"
OPENAI_MODEL = "gpt-4o-mini"
openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
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
months = st.slider("Months to analyze", 1, 24, 3)

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


def generate_ai_insight(df, farm_code):
    """Use OpenAI to create natural-language recommendations."""
    sample_data = json.dumps(
        df.tail(10).to_dict(orient="records"), indent=2, ensure_ascii=False
    )
    prompt = f"""
    You are a dairy farm advisor.
    1. Given the KPI data sample and the analysis, analyze:
    - Fertility trends
    - Production trends
    - Health trends
    - Culling levels trends
    - Calf raising trends
    - Key anomalies
    - Actionable recommendations in:
        Immediate (0-1 months)
        Short (1-3 months)
        Medium (3-6 months)
        Long (6+ months)
    - Each analysis should be divided into clear sections and bullet points must be used.
    - The analysis is the most important part, so do not make it short try to explain every detail!

    2. Then, identify up to 5 KPI variable names from the data that are most critical for visual monitoring.
    3. Return your answer in strict JSON with two keys:
    - "insights": a string of your narrative explanation
    - "kpis_to_plot": a JSON list of the exact column names (e.g. ["pct_partos_logrados","prod_a_305_del_1a_lact"])

    Farm: {farm_code}

    KPI Data (sample):
    {sample_data}
    """

    response = openai_client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=[
            {
                "role": "system",
                "content": "You are a dairy farm analyst skilled in data-driven reasoning.",
            },
            {"role": "user", "content": prompt},
        ],
        response_format={"type": "json_object"},  # ensures valid JSON response
    )

    ai_response = json.loads(response.choices[0].message.content)
    return ai_response


# --------------- UI FLOW ---------------
if analyze_button:
    with st.spinner("Fetching data and analyzing farm performance..."):
        try:
            df = get_farm_kpis(farm_code, language, months)

            st.markdown("---")

            ai_response = generate_ai_insight(df, farm_code)
            st.markdown(f"### 🧠 Insights\n{ai_response['insights']}")

            selected_kpis = ai_response.get("kpis_to_plot", [])
            st.markdown(f"**AI-selected KPIs:** {', '.join(selected_kpis)}")

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
        except requests.exceptions.RequestException as e:
            st.error(f"Error calling MCP bridge: {e}")
        except Exception as e:
            st.error(f"Unexpected error: {e}")
