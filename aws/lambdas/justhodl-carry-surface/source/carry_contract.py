"""Public carry publication and missing-data contracts; no provider or AWS calls."""
import copy
import json
import math
from datetime import datetime, timezone

REVIEW_VERSION = "20260910.v1"
DIAGNOSTICS = {
    "body", "raw_sample", "raw_status", "request_id", "request_url", "response",
    "response_body", "raw_response", "headers", "exception", "traceback", "stack_trace",
    "error_message", "telegram_info", "wss_broadcast_info",
}
ERROR_FIELDS = {"error", "err", "report_error", "error_code", "fetch_err"}


def number(value):
    if value is None or isinstance(value, bool):
        return None
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (TypeError, ValueError, OverflowError):
        return None


def quality(observation_date, publication_date, *, status="fresh", missing=(), frequency="daily"):
    return {"observation_date": observation_date, "publication_date": publication_date,
            "frequency": frequency, "freshness_basis": "observation", "status": status,
            "missing": sorted(set(missing))}


def public_payload(document):
    """Preserve metric fields while replacing provider/transport diagnostics before marking reviewed."""
    def clean(value):
        if isinstance(value, list):
            return [clean(item) for item in value]
        if not isinstance(value, dict):
            if isinstance(value, float) and not math.isfinite(value):
                return None
            return value
        out = {}
        for key, child in value.items():
            if key in DIAGNOSTICS:
                out[key] = "DIAGNOSTIC_REDACTED" if child else child
            elif key in ERROR_FIELDS:
                out[key] = "PROVIDER_REQUEST_FAILED" if child else child
            elif key == "errors":
                out[key] = ["PROVIDER_REQUEST_FAILED" for item in child if item] if isinstance(child, list) else (
                    child if child is None or isinstance(child, (int, float)) else "PROVIDER_REQUEST_FAILED")
            elif key == "webhook_results":
                out[key] = []
            else:
                out[key] = clean(child)
        return out
    out = clean(document)
    out["public_history_review"] = REVIEW_VERSION
    json.dumps(out, allow_nan=False)
    return out


def stale_snapshot(previous, attempted_at):
    """Keep the original observation/publication dates; a failed refresh is not a new print."""
    if not isinstance(previous, dict) or not previous.get("generated_at") or "by_class" not in previous:
        return None
    out = copy.deepcopy(previous)
    old = out.get("quality") or {}
    out["quality"] = quality(old.get("observation_date"), old.get("publication_date") or out["generated_at"],
                             status="stale", missing=old.get("missing", []))
    out["quality"].update(last_refresh_attempt=attempted_at, note="Last successful snapshot; refresh failed")
    out["ok"] = False
    out["call"] = None
    out["used_last_good"] = True
    for key in ("cross_asset_top", "cross_asset_bottom", "risk_adjusted_leaders", "dislocation_leaders"):
        out[key] = []
    if isinstance(out.get("unwind_overlay"), dict):
        out["unwind_overlay"].update(verdict="UNKNOWN — snapshot stale", cohort_fragility=None, fragile_assets=[])
    return public_payload(out)


def encode_public(document):
    return json.dumps(public_payload(document), allow_nan=False, separators=(",", ":")).encode("utf-8")
