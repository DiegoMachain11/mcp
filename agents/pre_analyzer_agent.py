# pre_analyzer_agent.py
import json
import logging
import math
import unicodedata
from typing import Callable, List, Optional, Sequence, Tuple

from openai import OpenAI
import requests
import os

OPENAI_MODEL = "gpt-4o-mini"
openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
BRIDGE_URL = "http://localhost:8090"

CORE_TRIAGE_KPIS: List[dict] = [
    {"alias": "pct_partos_logrados", "code": "255d"},
    {"alias": "pct_fiebre_de_leche", "code": "224d"},
    {"alias": "pct_retencion_de_placenta", "code": "226d"},
    {"alias": "pct_metritis_primaria", "code": "227d"},
    {"alias": "pct_cetosis", "code": "228d"},
    {"alias": "pct_vacas_muertas_frescas_lt_30_del", "code": "309d"},
    {"alias": "pct_desecho_vacas_lt_60_del_periodo", "code": "311a"},
    {"alias": "pct_desecho_plus", "code": "251f"},
    {"alias": "pico_de_prod_1a_lact", "code": "79"},
    {"alias": "pico_de_prod_2a_lact", "code": "79a"},
    {"alias": "pico_de_prod_3plus_lact", "code": "79b"},
    {"alias": "ganancia_peso_diaria_nac_vs_destete", "code": "43"},
    {"alias": "eficiencia_de_ganancia_de_peso", "code": "44"},
    {"alias": "pct_becerras_jaulas_muertas_lt_1_meses", "code": "298a"},
    {"alias": "pct_becerras_jaulas_muertas_lt_2_meses", "code": "299a"},
    {"alias": "pct_becerras_muertas_2_13_meses", "code": "301a"},
    {"alias": "pct_fertilidad_en_vaquillas", "code": "45b"},
    {"alias": "edad_1er_servicio_lt_13", "code": "53"},
    {"alias": "edad_1er_servicio_13_lt_14", "code": "54"},
    {"alias": "edad_1er_servicio_gt_15", "code": "56"},
    {"alias": "prod_a_305_del_1a_lact", "code": "78"},
    {"alias": "prod_a_305_del_2a_lact", "code": "78a"},
    {"alias": "prod_a_305_del_3plus_lact", "code": "78b"},
    {"alias": "pct_total_abortos_vaquillas_m", "code": "328h"},
    {"alias": "daily_rest_time_min_1a_lact", "code": "259"},
    {"alias": "daily_rest_time_min_2a_lact", "code": "266"},
    {"alias": "daily_rest_time_min_3plus_lact", "code": "273"},
    {"alias": "pct_vacas_c_prob_digestivos", "code": "291d"},
    {"alias": "pct_vacas_c_prob_locomotores", "code": "293d"},
    {"alias": "pct_total_abortos_vacas_m", "code": "329l"},
    {"alias": "deteccion_de_celos_ult2", "code": "125"},
    {"alias": "taza_prenez_21_dias", "code": "134"},
    {"alias": "dias_abiertos_mx", "code": "24a"},
]


def _slugify(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    alias = (
        normalized.lower()
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


def _resolve_kpi_selection(
    requested: Optional[Sequence[str]],
) -> Tuple[List[str], Optional[set]]:
    alias_to_code = {entry["alias"]: entry["code"] for entry in CORE_TRIAGE_KPIS}
    code_to_alias = {entry["code"]: entry["alias"] for entry in CORE_TRIAGE_KPIS}

    if not requested:
        return (
            [entry["code"] for entry in CORE_TRIAGE_KPIS],
            set(alias_to_code.keys()),
        )

    codes: List[str] = []
    aliases: set = set()
    unknown_alias = False

    for item in requested:
        if not item:
            continue
        normalized = str(item).strip()

        if normalized in alias_to_code:
            codes.append(alias_to_code[normalized])
            aliases.add(normalized)
        elif normalized in code_to_alias:
            codes.append(normalized)
            aliases.add(code_to_alias[normalized])
        else:
            slug = _slugify(normalized)
            if slug in alias_to_code:
                codes.append(alias_to_code[slug])
                aliases.add(slug)
            else:
                codes.append(normalized)
                unknown_alias = True

    return codes, (None if unknown_alias else aliases or set())


def run_pre_analysis(
    farm_code="GM",
    language="es",
    months=4,
    progress_callback: Optional[Callable[[float, str], None]] = None,
    triage_kpis: Optional[List[str]] = None,
):
    def _notify(progress: float, message: str):
        if progress_callback:
            try:
                progress_callback(progress, message)
            except Exception as exc:  # don't let UI callbacks break logic
                logging.debug(f"Progress callback error: {exc}")

    _notify(0.02, "Iniciando conexión MCP")

    base_params = {"farm_code": farm_code, "language": language, "months": months}
    kpi_codes, alias_whitelist = _resolve_kpi_selection(triage_kpis)

    def _call_summary(params):
        resp = requests.get(f"{BRIDGE_URL}/summarize_kpis", params=params)
        logging.info(f"Summarize KPIs response: {resp.text[:500]}")
        resp.raise_for_status()
        payload = resp.json()
        print("Summary payload:", payload)
        return payload.get("result", payload)

    print("KPI codes to fetch:", kpi_codes)
    if kpi_codes:
        combined = {}
        template = None
        batch_size = 4
        total_batches = math.ceil(len(kpi_codes) / batch_size)
        for idx in range(total_batches):
            chunk = kpi_codes[idx * batch_size : (idx + 1) * batch_size]
            params = dict(base_params)
            params["selected_kpis"] = chunk
            print(params)
            payload = _call_summary(params)
            combined.update(payload.get("summaries", {}))
            if template is None:
                template = {k: v for k, v in payload.items() if k != "summaries"}
            progress_position = 0.02 + (0.20 * (idx + 1) / total_batches)
            _notify(progress_position, f"Lote {idx + 1}/{total_batches} obtenido")

        data = template or {}
        data["summaries"] = combined
    else:
        data = _call_summary(base_params)

    print("KPI summary data:", data)

    _notify(0.25, "Datos de KPIs recibidos")

    # data = data.get("result", {})
    summaries_dict = data.get("summaries", {})
    if alias_whitelist:
        summaries_dict = {
            metric: stats
            for metric, stats in summaries_dict.items()
            if metric in alias_whitelist
        }

    logging.info(f"KPI summaries received: {list(summaries_dict.keys())}")
    print("KPI summaries received:", list(summaries_dict.keys()))
    # Convert dict of dicts → list of {metric, mean, std, ...}
    summaries = [
        {"metric": metric, **stats} for metric, stats in summaries_dict.items()
    ]

    print("KPI summaries prepared for LLM:", summaries)

    _notify(0.4, "Preparando análisis de riesgos")

    prompt = f"""
    You are a dairy KPI analyst.
    Given the following KPI summaries (mean, trend, std, min, max):
    - Identify which KPIs show anomalies or significant worsening trends.
    - Identify which KPIs are in a good state or show improvement.
    - Group them by domain: Fertility, Production, Health, Calf Raising, Culling, Feeding.
    - For each group, explain key risks and urgency (High / Medium / Low).
    - Return JSON:
    {{
        "urgent_kpis": [list of kpi names],
        "domains_to_investigate": {{
            "Fertility": [...],
            "Production": [...],
            ...
        ""
        }},
        "domains_in_good_state": {{
            "Fertility": [...],
            "Production": [...],
            ...
        }},
        "summary": "plain text summary"
    }}
    KPI summaries:
    {json.dumps(summaries, indent=2, ensure_ascii=False)}
    """

    _notify(0.6, "Solicitando evaluación al LLM")
    response = openai_client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=[
            {
                "role": "system",
                "content": "You are an expert dairy production KPI triage assistant.",
            },
            {"role": "user", "content": prompt},
        ],
        response_format={"type": "json_object"},
    )
    result = json.loads(response.choices[0].message.content)
    _notify(1.0, "PreAnalyzer completado")
    return result


if __name__ == "__main__":
    response = run_pre_analysis()

    print("Pre-analysis result:")
    print(json.dumps(response, indent=2, ensure_ascii=False))
