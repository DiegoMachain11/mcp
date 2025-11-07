
def _extract_rows(resp_json):
    if isinstance(resp_json, dict):
        if "result" in resp_json:
            return resp_json["result"]
    if isinstance(resp_json, list):
        return resp_json
    return []
