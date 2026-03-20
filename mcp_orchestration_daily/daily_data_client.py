import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, Sequence

import pandas as pd
import requests


class DailyDataClient:
    """
    Lightweight client to pull daily metrics for a farm.
    Defaults to the Milk/Feed grouped endpoint but can be extended with more.
    """

    def __init__(self, api_base_url: str):
        self.api_base_url = api_base_url.rstrip("/")

    def _make_api_call(self, url: str) -> list:
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.RequestException as exc:
            raise RuntimeError(f"API call failed: {exc}") from exc

    def _normalize_df(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        # Try to coerce any date-like column to a datetime type
        date_cols = [c for c in df.columns if "date" in c.lower()]
        if date_cols:
            primary_date_col = date_cols[0]
            if primary_date_col != "Date":
                df = df.rename(columns={primary_date_col: "Date"})
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce", utc=True)
        return df

    def _resolve_dates(
        self,
        from_date: Optional[str],
        to_date: Optional[str],
        days: int,
    ) -> tuple[str, str]:
        to_dt = (
            datetime.fromisoformat(to_date).date()
            if to_date
            else datetime.now(timezone.utc).date()
        )
        from_dt = (
            datetime.fromisoformat(from_date).date()
            if from_date
            else to_dt - timedelta(days=days - 1)
        )
        return from_dt.strftime("%Y-%m-%d"), to_dt.strftime("%Y-%m-%d")

    def fetch_milk_feed_data_group(
        self,
        farm_code: str,
        group_code: str,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        days: int = 30,
        columns: Optional[Sequence[str]] = None,
    ) -> pd.DataFrame:
        """
        Pull daily Milk/Feed data for a group.
        Dates can be explicitly provided (YYYY-MM-DD) or inferred from `days`.
        """
        start, end = self._resolve_dates(from_date, to_date, days)
        url = (
            f"{self.api_base_url}/MaderoService.svc/rest/"
            f"GetMilkFeedDataGroup/{start}/{end}/{group_code}/{farm_code}"
        )

        logging.info(
            "Fetching milk/feed data for farm=%s group=%s from %s to %s",
            farm_code,
            group_code,
            start,
            end,
        )

        payload = self._make_api_call(url)
        if not payload:
            logging.warning("No daily data returned for farm %s group %s", farm_code, group_code)
            return pd.DataFrame()

        df = pd.DataFrame(payload)
        df = self._normalize_df(df)

        if columns:
            available = set(df.columns)
            wanted = {col for col in columns if col in available}
            if wanted:
                df = df[[*(["Date"] if "Date" in df.columns else []), *sorted(wanted)]]
        return df
