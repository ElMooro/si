"""Immutable, replayable public Calls research runs.

Only typed, explicitly selected market fields may enter this public archive.
This is a frozen compiler-input projection, not original-provider evidence or
an archive of the private account snapshot. Stored code is never executed.
"""
import copy
import hashlib
import json
import math
import re
from datetime import datetime, timezone
from pathlib import Path

from calls_free_brief import build, METHOD

CONTRACT = "calls-research-run.v1"
PREFIX = "data/calls-research-runs/"
CODE_FILES = ("calls_research_replay.py", "calls_free_brief.py", "pd_fails_context.py")
COMMON = {"generated_at": "date", "quality.status": "quality", "quality.observation_date": "date"}
FIELDS = {
    "data/liquidity-flow.json": {**COMMON, "current.net_liquidity_b": "number", "as_of": "date"},
    "data/settlement-fails.json": {**COMMON, **{
        scope + "." + field: kind for scope in ("treasury", "headline") for field, kind in {
            "as_of": "date", "ftd_bn": "number", "ftr_bn": "number", "gross_bn": "number", "combined_bn": "number",
            "scope_id": "scope", "scope": "scope", "complete": "boolean", "quality.status": "quality",
            "quality.publication_date": "date"}.items()}},
    "data/ciss-stress.json": {**COMMON, "ea_composite": "number", "ea_composite_date": "date"},
    "data/capital-inflows.json": {**COMMON, "headline.foreign_net_into_us_lt_12mo_b": "number", "data_asof": "date"},
    "data/auction-crisis.json": {**COMMON, "composite_score": "number", "freshness.latest_auction_date": "date"},
    "data/risk-regime.json": {**COMMON, "risk_regime_score": "number"},
    "data/market-extremes.json": {**COMMON, "scores.top_risk": "number"},
}


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def compiler_identity():
    return {"method": METHOD, "source_hash_basis": "UTF-8 source with LF newlines",
            "files": {name: hashlib.sha256(Path(__file__).with_name(name).read_text(encoding="utf-8").encode("utf-8")).hexdigest()
                      for name in CODE_FILES}}


def _get(doc, path):
    for part in path.split("."):
        if not isinstance(doc, dict): return None
        doc = doc.get(part)
    return doc


def _typed(value, kind):
    if kind == "number":
        return value if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) else None
    if kind == "boolean": return value if isinstance(value, bool) else None
    if kind == "quality": return value if value in ("fresh", "stale", "partial", "unavailable", "unverified") else None
    if kind == "scope": return value if value in ("treasury_incl_tips", "ust_ex_tips") else "__invalid_scope__" if value is not None else None
    if kind == "date":
        if not isinstance(value, str) or len(value) > 40 or not re.match(r"^\d{4}-\d{2}-\d{2}(T|$)", value): return None
        try: datetime.fromisoformat(value.replace("Z", "+00:00")); return value
        except ValueError: return None
    return None


def project(key, document):
    if key not in FIELDS: raise ValueError("unapproved public compiler source")
    out = {}
    for path, kind in FIELDS[key].items():
        value = _typed(_get(document, path), kind)
        if value is None: continue
        dest = out
        parts = path.split(".")
        for part in parts[:-1]: dest = dest.setdefault(part, {})
        dest[parts[-1]] = value
    return out


def evidence_inventory(output, inputs):
    roots, unmapped, permissions = {}, [], []
    for row in output["evidence"]:
        row["input_projection_sha256"] = inputs[row["source"]]["sha256"]
        row["evidence_id"] = "observation-" + digest(row)
        mapped = bool(row["root_ids"]) and all(not root.startswith("COMPOSITE:") for root in row["root_ids"])
        if not mapped: unmapped.append(row["series_id"])
        for root in row["root_ids"]:
            if not root.startswith("COMPOSITE:"): roots.setdefault(root, []).append(row["series_id"])
        reasons = ["monitor_only", "decision_model_not_validated"]
        if not mapped: reasons.append("ancestry_not_mapped")
        if row["quality_status"] != "fresh": reasons.append("observation_not_fresh")
        permissions.append({"series_id": row["series_id"], "evidence_id": row["evidence_id"],
                            "may_inform_research": row["value"] is not None and row["quality_status"] == "fresh",
                            "may_vote": False, "may_size": False, "reasons": reasons})
    return {"schema_version": "calls-evidence-map.v1", "scope": "displayed_public_fields_only",
            "root_groups": [{"root_id": key, "fields": sorted(value)} for key, value in sorted(roots.items())],
            "unmapped_fields": unmapped, "permission_mask": permissions,
            "eligible_votes": 0, "independent_evidence_count": None,
            "note": "Shared roots are grouped; root count is not a statistical independence estimate. Composite ancestry remains incomplete."}


def compile_frozen(inputs, at):
    output = build(lambda key: copy.deepcopy(inputs[key]["projection"]), at)
    output["evidence_inventory"] = evidence_inventory(output, inputs)
    return output


def prepare(load, generated_at=None):
    inputs = {}
    for key in FIELDS:
        try:
            doc = load(key)
            status = "captured_projection" if isinstance(doc, dict) and doc else "source_unavailable"
        except Exception:
            doc, status = {}, "source_read_failed"
        selected = project(key, doc)
        inputs[key] = {"source": key, "status": status, "projection": selected, "sha256": digest(selected),
                       "basis": "typed_public_compiler_input_projection; not_complete_source_packet"}
    # In production the cutoff follows collection, so subsequently read inputs
    # are not assigned to an earlier decision clock. Explicit clocks are for replay fixtures.
    generated_at = generated_at or datetime.now(timezone.utc).isoformat()
    at = datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
    if at.tzinfo is None: raise ValueError("replay requires timezone-aware decision time")
    payload = {"generated_at": generated_at, "compiler": compiler_identity(), "inputs": inputs,
               "output": compile_frozen(inputs, generated_at),
               "scope": "public_market_research_brief; private_account_snapshot_excluded",
               "authority": "monitor_only", "sizing_eligible": False}
    sha = digest(payload)
    return {"contract_version": CONTRACT, "run_id": "calls-research-"+sha, "payload_sha256": sha, "payload": payload}


def replay(bundle):
    if not isinstance(bundle, dict) or bundle.get("contract_version") != CONTRACT:
        raise ValueError("unsupported public Calls replay contract")
    payload = bundle.get("payload")
    if not isinstance(payload, dict): raise ValueError("research run payload absent")
    if digest(payload) != bundle.get("payload_sha256") or bundle.get("run_id") != "calls-research-"+bundle["payload_sha256"]:
        raise ValueError("research run content hash mismatch")
    if payload.get("compiler") != compiler_identity():
        raise ValueError("compiler version differs; use the source hashes recorded in this run")
    inputs = payload.get("inputs")
    if not isinstance(inputs, dict) or set(inputs) != set(FIELDS): raise ValueError("incomplete input manifest")
    for key, row in inputs.items():
        selected = row.get("projection")
        if row.get("source") != key or project(key, selected) != selected or digest(selected) != row.get("sha256"):
            raise ValueError("input projection identity, whitelist or hash mismatch")
    output = compile_frozen(inputs, payload["generated_at"])
    if canonical(output) != canonical(payload.get("output")): raise ValueError("reproduced brief differs")
    return {"status": "reproduced", "run_id": bundle["run_id"], "output_sha256": digest(output),
            "inputs": len(inputs), "evidence_fields": len(output["evidence"]), "call_verb": output["call_verb"],
            "sizing_eligible": False, "scope": payload["scope"]}


def persist(client, bucket, bundle):
    replay(bundle)
    key = PREFIX + bundle["payload_sha256"] + ".json"
    body = canonical(bundle)
    try:
        client.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json",
                          CacheControl="public, max-age=31536000, immutable", IfNoneMatch="*")
    except Exception as exc:
        code = str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))
        if code not in ("PreconditionFailed", "412", "ConditionalRequestConflict", "409"): raise
        old = client.get_object(Bucket=bucket, Key=key)["Body"].read()
        if old != body: raise ValueError("immutable public research-run conflict") from exc
    return {"contract_version": CONTRACT, "run_id": bundle["run_id"], "bundle_key": key,
            "bundle_sha256": hashlib.sha256(body).hexdigest(),
            "payload_sha256": bundle["payload_sha256"], "output_sha256": digest(bundle["payload"]["output"]),
            "status": "retained", "scope": bundle["payload"]["scope"], "runner_verified": False}


def publish_current(client, bucket, key, document):
    """A slower earlier run cannot replace a newer current brief."""
    from calls_contract import timestamp
    at = timestamp(document.get("generated_at"))
    if at is None: raise ValueError("current brief requires a dated timestamp")
    for _ in range(5):
        try:
            obj = client.get_object(Bucket=bucket, Key=key)
            previous = json.loads(obj["Body"].read())
            old_at = timestamp(previous.get("generated_at"))
            if old_at and old_at > at: return {"published": False, "reason": "newer_current_present"}
            condition = {"IfMatch": obj["ETag"]}
        except Exception as exc:
            code = str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))
            if code not in ("NoSuchKey", "404"): raise
            condition = {"IfNoneMatch": "*"}
        try:
            client.put_object(Bucket=bucket, Key=key, Body=canonical(document),
                              ContentType="application/json", CacheControl="public, max-age=60", **condition)
            return {"published": True}
        except Exception as exc:
            code = str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))
            if code not in ("PreconditionFailed", "412", "ConditionalRequestConflict", "409"): raise
    raise RuntimeError("current brief update conflicted; immutable research run retained")
