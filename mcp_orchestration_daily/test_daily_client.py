"""
Quick CLI helper to explore the daily MCP server.
- Lists available tools.
- Probes one or more group codes with `list_daily_fields`.
- Optionally fetches sample rows and a metric summary.

Usage examples:
  python mcp_orchestration_daily/test_daily_client.py --farm-code GM --group-codes 1,2
  python mcp_orchestration_daily/test_daily_client.py --farm-code GM --group-range 1:8 --sample-rows 5
  python mcp_orchestration_daily/test_daily_client.py --farm-code GM --group-codes F1 --summary-metric MilkKg

By default, connects to DAILY_MCP_URL (env) or http://localhost:8081/mcp.
"""

import argparse
import asyncio
import json
import os
from typing import Iterable, List, Optional, Tuple

from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client


def parse_group_codes(
    group_codes: Optional[str], group_range: Optional[str]
) -> List[str]:
    if group_codes:
        return [code.strip() for code in group_codes.split(",") if code.strip()]
    if group_range:
        try:
            start_s, end_s = group_range.split(":", 1)
            start, end = int(start_s), int(end_s)
            return [str(i) for i in range(start, end + 1)]
        except ValueError:
            raise SystemExit("Invalid --group-range. Use start:end, e.g. 1:5")
    return []


async def call_tool(session: ClientSession, name: str, args: dict):
    res = await session.call_tool(name, args)
    return res.structuredContent or res.content


async def run(
    server_url: str,
    farm_code: Optional[str],
    group_codes: Iterable[str],
    days: int,
    sample_rows: int,
    summary_metric: Optional[str],
):
    print(f"🔗 Connecting to {server_url} ...")
    async with streamablehttp_client(server_url) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()
            tools = await session.list_tools()
            print("Available tools:", [t.name for t in tools.tools])

            if not farm_code:
                print("No --farm-code provided; skipping data calls.")
                return

            if not group_codes:
                print("No group codes provided; use --group-codes or --group-range.")
                return

            for group in group_codes:
                print(f"\n=== Group {group} ===")
                try:
                    fields = await call_tool(
                        session,
                        "list_daily_fields",
                        {"farm_code": farm_code, "group_code": group, "days": days},
                    )
                    print("Fields:", json.dumps(fields, indent=2, ensure_ascii=False))
                except Exception as exc:
                    print(f"list_daily_fields failed: {exc}")
                    continue

                try:
                    rows = await call_tool(
                        session,
                        "get_milk_feed_data_group",
                        {
                            "farm_code": farm_code,
                            "group_code": group,
                            "days": days,
                        },
                    )
                    preview = rows[:sample_rows] if isinstance(rows, list) else rows
                    print(
                        f"Sample rows (first {sample_rows}):",
                        json.dumps(preview, indent=2, ensure_ascii=False),
                    )
                except Exception as exc:
                    print(f"get_milk_feed_data_group failed: {exc}")

                if summary_metric:
                    try:
                        summary = await call_tool(
                            session,
                            "summarize_daily_metric",
                            {
                                "farm_code": farm_code,
                                "group_code": group,
                                "metric": summary_metric,
                                "days": days,
                            },
                        )
                        print(
                            "Summary:",
                            json.dumps(summary, indent=2, ensure_ascii=False),
                        )
                    except Exception as exc:
                        print(f"summarize_daily_metric failed: {exc}")


def main():
    parser = argparse.ArgumentParser(description="Probe the daily MCP server.")
    parser.add_argument(
        "--server-url", default=os.getenv("DAILY_MCP_URL", "http://localhost:8081/mcp")
    )
    parser.add_argument("--farm-code", help="Farm code to query")
    parser.add_argument(
        "--group-codes",
        help="Comma-separated list of group codes to probe (e.g. 1,2,A)",
    )
    parser.add_argument(
        "--group-range",
        help="Range of numeric group codes start:end (inclusive); ignored if --group-codes is set",
    )
    parser.add_argument(
        "--days", type=int, default=7, help="How many trailing days to query"
    )
    parser.add_argument(
        "--sample-rows",
        type=int,
        default=5,
        help="How many rows to show from each dataset",
    )
    parser.add_argument(
        "--summary-metric",
        help="If set, summarize this metric for each group (must be a returned column)",
    )

    args = parser.parse_args()
    groups = parse_group_codes(args.group_codes, args.group_range)
    asyncio.run(
        run(
            server_url=args.server_url,
            farm_code=args.farm_code,
            group_codes=groups,
            days=args.days,
            sample_rows=args.sample_rows,
            summary_metric=args.summary_metric,
        )
    )


if __name__ == "__main__":
    main()
