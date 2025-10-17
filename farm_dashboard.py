import streamlit as st
import pandas as pd
import requests
import json
import os
from openai import OpenAI

# ================= CONFIG ====================
BRIDGE_URL = "http://localhost:8090"
OPENAI_MODEL = "gpt-4o-mini"
openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
# =============================================

st.set_page_config(page_title="🐄 Dairy Farm AI Advisor", layout="wide")

st.markdown(
    """
# 🐄 Dairy Farm AI Advisor
Your data, your insights — powered by MCP & AI.
"""
)

farm_code = st.text_input("Farm Code", "GM")
language = "es"
metric = st.text_input("Metric to analyze", "pct_partos_logrados")
days = st.slider("Days to analyze", 30, 365, 90)

analyze_button = st.button("🔍 Analyze Farm Performance")


def get_farm_kpis(farm_code, language):
    """Call the FastAPI MCP bridge to fetch KPI data."""
    url = f"{BRIDGE_URL}/get_farm_kpis"
    resp = requests.get(url, params={"farm_code": farm_code, "language": language})
    resp.raise_for_status()
    data = resp.json()
    if isinstance(data, dict):
        data = [data]
    df = pd.DataFrame(data)
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df = df.sort_values("Date")
    return df


def analyze_kpis(farm_code, metric, days):
    """Call the FastAPI MCP bridge to analyze the selected metric."""
    url = f"{BRIDGE_URL}/analyze_kpis"
    resp = requests.get(
        url, params={"farm_code": farm_code, "metric": metric, "days": days}
    )
    resp.raise_for_status()
    return resp.json()


def generate_ai_insight(df, analysis_data, farm_code, metric):
    """Use OpenAI to create natural-language recommendations."""
    sample_data = json.dumps(
        df.tail(10).to_dict(orient="records"), indent=2, ensure_ascii=False
    )
    analysis_json = json.dumps(analysis_data, indent=2, ensure_ascii=False)
    prompt = f"""
You are a dairy farm advisor.
Given the KPI data sample and the analysis, summarize:
- Fertility, production, and health trends
- Key anomalies
- Actionable recommendations (Immediate / Short / Medium / Long term)

Farm: {farm_code}
Metric: {metric}

KPI Data (sample):
{sample_data}

Analysis:
{analysis_json}
"""
    response = openai_client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=[
            {
                "role": "system",
                "content": "You are an expert in dairy farm management and analytics.",
            },
            {"role": "user", "content": prompt},
        ],
    )
    return response.choices[0].message.content


# --------------- UI FLOW ---------------
if analyze_button:
    with st.spinner("Fetching data and analyzing farm performance..."):
        try:
            df = get_farm_kpis(farm_code, language)
            analysis = analyze_kpis(farm_code, metric, days)

            col1, col2 = st.columns([2, 1])

            with col1:
                st.subheader("📈 KPI Trend")
                if metric in df.columns:
                    st.line_chart(df.set_index("Date")[metric])
                else:
                    st.warning(f"Metric '{metric}' not found in data.")

            with col2:
                st.subheader("📊 Analysis Summary")
                st.json(analysis)

            st.markdown("---")
            st.subheader("💬 AI Recommendations")

            ai_text = generate_ai_insight(df, analysis, farm_code, metric)
            st.markdown(f"### 🧠 Insights\n{ai_text}")

        except requests.exceptions.RequestException as e:
            st.error(f"Error calling MCP bridge: {e}")
        except Exception as e:
            st.error(f"Unexpected error: {e}")
