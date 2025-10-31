import requests
import pandas as pd
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta


class DairyKPIClient:
    def __init__(self, api_base_url: str):
        self.API_BASE_URL = api_base_url
        self._alias_map = {}

    def _get_kpi_list(self) -> pd.DataFrame:
        """Return static KPI definitions."""
        kpi_data = {
            "Description": [
                "% Partos Logrados",
                "% Fiebre de leche",
                "% Retencion de Placenta",
                "% Metritis Primaria",
                "% Cetosis",
                "% Vacas Muertas Frescas < 30 DEL",
                "% Desecho Vacas < 60 DEL (Periodo)",
                "% Desecho +",
                "Pico de Prod. 1a Lact",
                "Pico de Prod. 2a Lact",
                "Pico de Prod. 3+ Lact",
                "Ganancia Peso Diaria (Nac. vs Destete)",
                "Eficiencia de Ganancia de Peso",
                "% Becerras Jaulas Muertas < 1 Meses",
                "% Becerras Jaulas Muertas < 2 Meses",
                "% Becerras Muertas (2-13 Meses)",
                "% Fertilidad en Vaquillas",
                "Edad 1er Servicio < 13",
                "Edad 1er Servicio 13 < 14",
                "Edad 1er Servicio > 15",
                "Prod a 305 DEL 1a Lact",
                "Prod a 305 DEL 2a Lact",
                "Prod a 305 DEL 3+ Lact",
                "% Total Abortos Vaquillas (M)",
                "Daily Rest Time (Min) 1a Lact",
                "Daily Rest Time (Min) 2a Lact",
                "Daily Rest Time (Min) 3+ Lact",
                "% Vacas c/Prob. Digestivos",
                "% Vacas c/Prob. Locomotores",
                "% Total Abortos Vacas (M)",
                "Deteccion de Celos (Ult2)",
                "Taza Prenez (21 Dias)",
                "Dias Abiertos (MX)",
            ],
            "Code": [
                "255d",
                "224d",
                "226d",
                "227d",
                "228d",
                "309d",
                "311a",
                "251f",
                "79",
                "79a",
                "79b",
                "43",
                "44",
                "298a",
                "299a",
                "301a",
                "45b",
                "53",
                "54",
                "56",
                "78",
                "78a",
                "78b",
                "328h",
                "259",
                "266",
                "273",
                "291d",
                "293d",
                "329l",
                "125",
                "134",
                "24a",
            ],
            "Context": [
                "Percentage of successful calvings out of total calvings.",
                "Percentage of cows with milk fever (hypocalcemia) after calving.",
                "Percentage of cows retaining placenta after calving.",
                "Percentage of cows with primary metritis postpartum.",
                "Percentage of cows diagnosed with ketosis.",
                "Mortality rate of fresh cows < 30 days in milk.",
                "Culling percentage of cows < 60 days in milk.",
                "Overall culling rate across all lactations.",
                "Peak milk production for cows in 1st lactation.",
                "Peak milk production for cows in 2nd lactation.",
                "Peak milk production for cows in 3rd+ lactations.",
                "Average daily weight gain from birth to weaning.",
                "Efficiency of weight gain relative to feed intake.",
                "Mortality % of calves in hutches < 1 month old.",
                "Mortality % of calves in hutches < 2 months old.",
                "Mortality % of heifers 2–13 months old.",
                "Fertility percentage in heifers.",
                "Percentage of heifers first bred < 13 months old.",
                "Percentage of heifers first bred 13 14 months old.",
                "Percentage of heifers first bred > 15 months old.",
                "Milk yield standardized to 305 days for 1st lactation.",
                "Milk yield standardized to 305 days for 2nd lactation.",
                "Milk yield standardized to 305 days for 3rd+ lactations.",
                "Abortion rate in heifers (measured).",
                "Average daily lying/rest time for 1st lactation cows.",
                "Average daily lying/rest time for 2nd lactation cows.",
                "Average daily lying/rest time for 3rd+ lactation cows.",
                "Percentage of cows with digestive disorders.",
                "Percentage of cows with lameness/locomotion problems.",
                "Abortion rate in mature cows (measured).",
                "Heat detection efficiency (last 2 cycles).",
                "Pregnancy rate every 21 days.",
                "Average days open (from calving to conception).",
            ],
        }
        return pd.DataFrame(kpi_data)

    def _make_alias(self, description: str) -> str:
        alias = (
            description.lower()
            .replace("%", "pct")
            .replace("<", "lt")
            .replace(">", "gt")
            .replace("+", "plus")
            .replace("(", "")
            .replace(")", "")
            .replace(".", "")
            .replace(",", "")
            .replace("/", "_")
            .replace(" ", "_")
            .replace("-", "_")
        )
        while "__" in alias:
            alias = alias.replace("__", "_")
        return alias.strip("_")

    def get_kpi_schema(self) -> dict:
        """Build JSON schema for all KPIs."""
        kpis = self._get_kpi_list()
        properties = {"Date": {"type": "string", "format": "date-time"}}
        for _, row in kpis.iterrows():
            alias = self._make_alias(row["Description"])
            properties[alias] = {"type": "number", "description": row["Context"]}
        return {
            "type": "object",
            "properties": properties,
            "description": "Dairy farm KPIs over time.",
        }

    def _make_api_call(self, url: str) -> dict:
        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.RequestException as e:
            raise Exception(f"API call failed: {e}")

    def fetch_farm_kpis(
        self, farm_code: str, language: str = "es", months: int = 13
    ) -> pd.DataFrame:
        today = datetime.now(timezone.utc)
        date_13_months_ago = today - relativedelta(months=months)
        today_unix = int(today.timestamp())
        date_13_months_ago_unix = int(date_13_months_ago.timestamp())
        language_code = "0" if language.lower() == "es" else "1"

        print("Fetching months = ", months)
        kpis = self._get_kpi_list()
        all_data = []
        alias_map = {}

        for _, row in kpis.iterrows():
            alias = self._make_alias(row["Description"])
            alias_map[alias] = row["Description"]
            url = (
                f"{self.API_BASE_URL}/GetIndicatorAnalysis/"
                f"{farm_code}/{row['Code']}/{date_13_months_ago_unix}/"
                f"{today_unix}/{language_code}/2"
            )

            api_data = self._make_api_call(url)
            if api_data:
                df = pd.DataFrame(api_data)
                if "Date" in df.columns and "Value" in df.columns:
                    df["Date"] = pd.to_datetime(df["Date"], unit="s", origin="unix")
                    df["Value"] = pd.to_numeric(df["Value"], errors="coerce")
                    df = df[["Date", "Value"]].rename(columns={"Value": alias})
                    all_data.append(df)

        if not all_data:
            raise Exception(f"No data returned for farm {farm_code}")

        final_df = all_data[0]
        for i in range(1, len(all_data)):
            final_df = pd.merge(final_df, all_data[i], on="Date", how="outer")

        final_df = final_df.sort_values("Date").reset_index(drop=True)
        self._alias_map = alias_map
        return final_df
