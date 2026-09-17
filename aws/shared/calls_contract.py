"""Calls v2: explicit abstention, observation-only briefs and durable history.

An LLM's wording is not a validated allocation model. Legacy and narrative-only
rows remain inspectable but cannot authorize sizing or enter an execution replay.
No AWS clients, credentials, model calls or notifications are created here.
"""
import hashlib
import json
import math
import re
from datetime import datetime, timedelta, timezone

SCHEMA = "calls.v2"
LEDGER_KEY = "data/decisive-call-history.json"
EVENT_PREFIX = "data/decisive-call-events/"
CADENCE = {"interval_hours": 4, "minutes_past_hour": 5, "timezone": "UTC"}
VERBS = {"LONG", "TRIM", "EXIT", "EXIT_ALL_RISK", "LEVER", "HEDGE", "LOAD", "WAIT", "HOLD"}


def finite(value):
    if value is None or isinstance(value, bool):
        return None
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (TypeError, ValueError, OverflowError):
        return None


def timestamp(value):
    try:
        if not isinstance(value, str) or "T" not in value:
            return None
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.astimezone(timezone.utc) if parsed.tzinfo else None
    except ValueError:
        return None


def khalid_value(intel):
    intel = intel if isinstance(intel, dict) else {}
    scores = intel.get("scores") if isinstance(intel.get("scores"), dict) else {}
    index = intel.get("khalid_index")
    values = [intel.get("khalid_score"), scores.get("khalid_score"), scores.get("khalid_index"),
              scores.get("khalid"), scores.get("ka"), intel.get("ka_score"), index]
    for value in values:
        if isinstance(value, dict):
            value = value.get("score")
        number = finite(value)
        if number is not None and 0 <= number <= 100:
            return number
    return None


def candidate_verb(markdown):
    """Read an advisory verb only from its labeled section, never watch triggers."""
    if not isinstance(markdown, str) or len(markdown.strip()) < 120:
        return None
    section = re.search(r"(?:DECISIVE CALL|DECISION STATUS)\s*\*{0,2}\s*[:\-—]?([^\n]*(?:\n[^\n]+){0,3})",
                        markdown.upper())
    if not section:
        return None
    match = re.search(r"\b(EXIT[ _]ALL[ _]RISK|LONG|TRIM|EXIT|LEVER|HEDGE|LOAD|WAIT|HOLD)\b", section.group(1))
    return match.group(1).replace(" ", "_") if match else None


def make_snapshot(snapshot, output):
    at = timestamp(snapshot.get("as_of"))
    if at is None:
        raise ValueError("Calls snapshot requires a timezone-aware as_of")
    intel = snapshot.get("intelligence") or {}
    cal = snapshot.get("calibration_v2") or {}
    md = output.get("brief_md") or ""
    proposed = candidate_verb(md)
    if output.get("error"):
        status, reason, generation = "ERROR", "generation_failed", "error"
    elif not isinstance(md, str) or len(md.strip()) < 120:
        status, reason, generation = "ERROR", "empty_or_stub_brief", "invalid"
    else:
        status, reason, generation = "ABSTAIN", "decision_model_not_validated", "ok"
    # A narrative can describe evidence; it cannot confer eligibility on itself.
    row = {
        "schema_version": SCHEMA, "timestamp": at.isoformat(),
        "expires_at": (at + timedelta(hours=4)).isoformat(),
        "decision_status": status, "decision_reason": reason,
        "generation_status": generation, "decision_eligible": False,
        "sizing_eligible": False, "validation_status": "monitor_only",
        "call_verb": "UNKNOWN" if status == "ERROR" else "WAIT",
        "candidate_verb": proposed, "evidence_ids": [], "target_exposure": None,
        "asset": None, "forecast_horizon_days": None,
        "regime": intel.get("regime"), "phase": intel.get("phase"),
        "khalid_score": khalid_value(intel), "iso_week": cal.get("iso_week"),
        "weighted_mean_accuracy": finite(cal.get("weighted_mean_accuracy")),
        "accuracy_basis": "legacy_signal_weighted_accuracy_not_calls_accuracy",
        "highest_weight_signal": (cal.get("highest_weight") or {}).get("signal"),
        "duration_s": finite(output.get("duration_s")), "brief_chars": len(md) if isinstance(md, str) else 0,
    }
    digest = hashlib.sha256(json.dumps(row, sort_keys=True, allow_nan=False).encode()).hexdigest()
    row["snapshot_id"] = "call2-" + digest[:32]
    return row


def decision_eligibility(row, now=None):
    """Strict contract for future validated decisions; legacy rows never qualify."""
    now = now or datetime.now(timezone.utc)
    if not isinstance(row, dict) or row.get("schema_version") != SCHEMA:
        return False, "legacy_or_missing_contract"
    if row.get("decision_status") != "VALID":
        return False, row.get("decision_reason") or "no_valid_decision"
    if row.get("decision_eligible") is not True or row.get("sizing_eligible") is not True:
        return False, "decision_not_eligible"
    if row.get("validation_status") != "validated" or not row.get("model_version"):
        return False, "unvalidated_model"
    if row.get("call_verb") not in VERBS - {"WAIT", "HOLD"}:
        return False, "no_allocation_instruction"
    at, expiry = timestamp(row.get("timestamp")), timestamp(row.get("expires_at"))
    if at is None or expiry is None or expiry <= at:
        return False, "invalid_decision_times"
    if at > now:
        return False, "future_decision"
    if now >= expiry:
        return False, "expired_decision"
    evidence = row.get("evidence_ids")
    if not isinstance(evidence, list) or not evidence or any(not isinstance(x, str) or not x for x in evidence):
        return False, "missing_evidence"
    return True, "eligible"


def latest_snapshot(rows):
    valid = [row for row in rows if isinstance(row, dict) and timestamp(row.get("timestamp"))]
    return max(valid, key=lambda row: timestamp(row["timestamp"])) if valid else {}


def _error_code(exc):
    return str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))


def append_snapshot(s3, bucket, row):
    """Immutable event + optimistic recent view; never erase history on read failure."""
    body = json.dumps(row, sort_keys=True, allow_nan=False).encode()
    event_key = EVENT_PREFIX + row["snapshot_id"] + ".json"
    try:
        s3.put_object(Bucket=bucket, Key=event_key, Body=body,
                      ContentType="application/json", CacheControl="no-cache", IfNoneMatch="*")
    except Exception as exc:
        if _error_code(exc) not in {"PreconditionFailed", "412"}:
            raise
        existing = s3.get_object(Bucket=bucket, Key=event_key)["Body"].read()
        if json.loads(existing) != row:
            raise ValueError("immutable Calls event conflict") from exc
    for _ in range(5):
        try:
            result = s3.get_object(Bucket=bucket, Key=LEDGER_KEY)
            document = json.loads(result["Body"].read())
            condition = {"IfMatch": result["ETag"]}
        except Exception as exc:
            if _error_code(exc) not in {"NoSuchKey", "404"}:
                raise
            document, condition = {"snapshots": []}, {"IfNoneMatch": "*"}
        rows = document.get("snapshots")
        if not isinstance(rows, list):
            raise ValueError("Invalid Calls ledger; refusing to replace it")
        # Preserve all existing rows; the immutable stream also supports replay.
        if not any(r.get("snapshot_id") == row["snapshot_id"] for r in rows if isinstance(r, dict)):
            rows.append(row)
        rows.sort(key=lambda r: r.get("timestamp", ""))
        latest = latest_snapshot(rows)
        document.update(v="2.0", schema_version=SCHEMA, snapshots=rows, n_snapshots=len(rows),
                        last_updated=latest.get("timestamp"), generated_at=latest.get("timestamp"),
                        schedule=CADENCE, decision_status=latest.get("decision_status", "LEGACY"),
                        event_archive_prefix=EVENT_PREFIX)
        try:
            s3.put_object(Bucket=bucket, Key=LEDGER_KEY,
                          Body=json.dumps(document, allow_nan=False).encode(), ContentType="application/json",
                          CacheControl="public, max-age=60", **condition)
            return document
        except Exception as exc:
            if _error_code(exc) not in {"PreconditionFailed", "412", "ConditionalRequestConflict", "409"}:
                raise
    raise RuntimeError("Concurrent Calls writers: recent view not updated; immutable event retained")
