import asyncio
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client
from openai import OpenAI
import os
import json

# --- CONFIG ---
SERVER_URL = "http://localhost:8080/mcp"
OPENAI_MODEL = "gpt-5-nano"

# Initialize OpenAI client
openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


async def main():
    print(f"🔗 Connecting to MCP server at {SERVER_URL} ...")

    async with streamablehttp_client(SERVER_URL) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()
            print("Connected and initialized MCP session")
            tools = await session.list_tools()
            print(" Available tools:", [t.name for t in tools.tools])

            # Call get_farm_kpis
            print("\n Fetching KPI data for farm GM ...")
            result = await session.call_tool(
                "get_farm_kpis", {"farm_code": "GM", "language": "es"}
            )
            kpi_data = result.structuredContent or result.content

            #  Normalize to list
            if isinstance(kpi_data, dict):
                kpi_data_list = [kpi_data]
            elif isinstance(kpi_data, list):
                kpi_data_list = kpi_data
            else:
                kpi_data_list = []

            print(f" Received {len(kpi_data_list)} rows")

            # Call analyze_kpis
            print("\n Analyzing KPI 'pct_partos_logrados' ...")
            analysis = await session.call_tool(
                "analyze_kpis",
                {"farm_code": "GM", "metric": "pct_partos_logrados", "days": 90},
            )

            analysis_data = analysis.structuredContent or analysis.content
            print(" Analysis result:", analysis_data)

            sample_data = json.dumps(kpi_data_list[:10], indent=2, ensure_ascii=False)
            analysis_json = json.dumps(analysis_data, indent=2, ensure_ascii=False)

            prompt = f"""
            You are a dairy farm analyst.
            Given the following KPI data (sample) and KPI analysis, explain:
            - Fertility trends
            - Production trends
            - Health trends
            - Possible issues or anomalies
            - Recommendations to improve performance

            The recommendations should be practical and actionable. Divide them into:
                1. Immediate (next 1-2 months)
                2. Short term (0-3 months)
                3. Medium term (3-6 months)
                4. Long term (6-12 months)

            KPI Data Sample:
            {sample_data}

            KPI Analysis:
            {analysis_json}
            """

            print("\n Generating interpretation...")
            response = openai_client.chat.completions.create(
                model=OPENAI_MODEL,
                messages=[
                    {
                        "role": "system",
                        "content": "You are an expert in dairy farm analytics.",
                    },
                    {"role": "user", "content": prompt},
                ],
            )

            print("\n GPT Analysis:")
            print(response.choices[0].message.content)


if __name__ == "__main__":
    asyncio.run(main())
