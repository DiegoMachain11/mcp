
def _extract_rows(resp_json):
    if isinstance(resp_json, dict):
        if "result" in resp_json:
            return resp_json["result"]
    if isinstance(resp_json, list):
        return resp_json
    return []


def normalize_kpi_list(kpis):
    """
    Ensure KPI payloads are a simple ordered list of unique string names.

    PreAnalyzer sometimes returns rich objects like
    {"metric": "...", "urgency": "..."} instead of bare strings. Downstream
    agents only need the column names, so strip extra metadata while keeping
    order and skipping duplicates.
    """
    if not kpis:
        return []

    normalized = []
    seen = set()
    preferred_keys = ("metric", "kpi", "name", "id")

    for item in kpis:
        candidate = None

        if isinstance(item, str):
            candidate = item
        elif isinstance(item, dict):
            for key in preferred_keys:
                value = item.get(key)
                if isinstance(value, str):
                    candidate = value
                    break
            if candidate is None:
                for value in item.values():
                    if isinstance(value, str):
                        candidate = value
                        break

        if candidate and candidate not in seen:
            normalized.append(candidate)
            seen.add(candidate)

    return normalized
