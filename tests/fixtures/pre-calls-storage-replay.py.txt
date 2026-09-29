"""Immutable, replayable public Calls research runs.

Only typed, explicitly selected market fields may enter this public archive.
Most inputs remain typed compiler projections. Liquidity, settlement fails, CISS and TIC
may additionally bind complete retained originals. Private account snapshots are excluded; stored
code is never executed.
"""
import copy
import hashlib
import json
import math
import re
from datetime import datetime, timezone
from pathlib import Path

from calls_free_brief import build, METHOD
import calls_liquidity_binding as liquidity_binding
import calls_fails_binding as fails_binding
import calls_ciss_binding as ciss_binding
import calls_tic_binding as tic_binding
from calls_original_reader import ImmutableReader

CONTRACT = "calls-research-run.v1"
PREFIX = "data/calls-research-runs/"
CODE_FILES = ("calls_research_replay.py", "calls_free_brief.py", "pd_fails_context.py",
    "calls_liquidity_binding.py", "calls_liquidity_originals.py", "liquidity_flow_store.py",
    "liquidity_flow_model.py", "liquidity_flow_arithmetic.py", "verify_liquidity_arithmetic.py",
    "canonical_fred_replay.py", "report_observations.py", "research_brief_model.py", "evidence_store.py",
    "calls_fails_binding.py", "calls_fails_originals.py", "calls_original_reader.py", "fails_store.py",
    "fails_research.py", "fails_native.py", "verify_fails_arithmetic.py",
    "calls_ciss_binding.py", "calls_ciss_originals.py", "ciss_original_replay.py", "ciss_source_model.py", "verify_ciss_arithmetic.py",
    "calls_tic_binding.py", "calls_tic_originals.py", "tic_original_replay.py", "tic_original.py", "tic_research.py", "verify_tic_arithmetic.py")
COMMON = {"generated_at": "date", "quality.status": "quality", "quality.observation_date": "date"}
FIELDS = {
    "data/liquidity-flow.json": {**COMMON, "current.net_liquidity_b": "number", "as_of": "date",
        "contract": "liquidity_contract", **{"current.observation_dates."+sid: "calendar_date"
        for sid in ("WALCL", "WTREGEN", "RRPONTSYD")}},
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


# Exact reviewed source sets retained in tests/fixtures/pre-liquidity-transport-*,
# pre-calls-publication-* and pre-calls-cache-*. Only transport, publication and
# invocation-local cache concurrency changed; economic calculations are unchanged.
# All other compiler bytes, frozen
# inputs and reproduced output must still match. No archived code is executed.
REVIEWED_TRANSPORT_REVISIONS = ({
    "calls_research_replay.py": "dfec155b8a6be0e6c7064e5d5bb5e926059d91770923166b1096a2d2bee40c7d",
    "calls_original_reader.py": "7470b5bb9890f2438cee3a7cee2658595347770df79835e15415c7db9f543bae",
    "liquidity_flow_store.py": "4e7c99fe5eabdae3380cbdbbb7ecf6d65d25928c21b64c36b373402924336533",
},{
    "calls_research_replay.py": "00ee43f23fbe2e2649f1323a18ae7d177570e4fdf7fe27bd1b984a56846521fd",
    "calls_original_reader.py": "7470b5bb9890f2438cee3a7cee2658595347770df79835e15415c7db9f543bae",
    "liquidity_flow_store.py": "1961dde33e0e44a73bf5e26fdb011dd0a98494ef92f0aa7d0a738fa7a56ce42e",
},{
    "calls_research_replay.py": "96140866488a49ae90b444b97a569b57e1e63fead53941f3b4339d33a1dc7406",
    "calls_original_reader.py": "7470b5bb9890f2438cee3a7cee2658595347770df79835e15415c7db9f543bae",
    "liquidity_flow_store.py": "1961dde33e0e44a73bf5e26fdb011dd0a98494ef92f0aa7d0a738fa7a56ce42e",
},)


def compiler_match(recorded):
    current=compiler_identity()
    if recorded==current:return "exact"
    for revision in REVIEWED_TRANSPORT_REVISIONS:
        expected={**current,"files":{**current["files"],**revision}}
        if recorded==expected:return "reviewed_storage_transport_revision"
    raise ValueError("compiler version differs; no reviewed compatibility for these source hashes")


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
    if kind == "liquidity_contract": return value if value == "liquidity-flow-research.v1" else None
    if kind == "calendar_date":
        if not isinstance(value,str) or len(value)!=10: return None
        try: return datetime.strptime(value,'%Y-%m-%d').date().isoformat() if re.fullmatch(r'\d{4}-\d{2}-\d{2}',value) else None
        except ValueError: return None
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


def compile_frozen(inputs, at, read_original=None):
    binding=inputs[liquidity_binding.KEY].get('original_binding')
    originals=None
    if binding is not None:
        originals,packet=liquidity_binding.resolve(binding,read_original,at)
        if packet is not None and canonical(project(liquidity_binding.KEY,packet))!=canonical(inputs[liquidity_binding.KEY]['projection']):
            raise ValueError('Original liquidity packet and frozen projection differ')
    fails_originals=None
    fails=inputs[fails_binding.KEY].get('original_binding')
    if fails is not None:
        fails_originals,packet=fails_binding.resolve(fails,read_original,at)
        if packet is not None and canonical(project(fails_binding.KEY,packet))!=canonical(inputs[fails_binding.KEY]['projection']):
            raise ValueError('Original FR2004 packet and frozen projection differ')
    ciss_originals=None
    ciss=inputs[ciss_binding.KEY].get('original_binding')
    if ciss is not None:
        ciss_originals,packet=ciss_binding.resolve(ciss,read_original,at)
        if packet is not None and canonical(project(ciss_binding.KEY,packet))!=canonical(inputs[ciss_binding.KEY]['projection']):
            raise ValueError('Original CISS packet and frozen projection differ')
    tic_originals=None
    tic=inputs[tic_binding.KEY].get('original_binding')
    if tic is not None:
        tic_originals,packet=tic_binding.resolve(tic,read_original,at)
        if packet is not None and canonical(project(tic_binding.KEY,packet))!=canonical(inputs[tic_binding.KEY]['projection']):
            raise ValueError('Original TIC packet and frozen projection differ')
    output = build(lambda key: copy.deepcopy(inputs[key]["projection"]), at,liquidity_originals=originals,
                   fails_originals=fails_originals,ciss_originals=ciss_originals,tic_originals=tic_originals)
    if originals is not None:output['original_source_lineage']={'liquidity_flow':originals}
    if fails_originals is not None:output.setdefault('original_source_lineage',{})['settlement_fails']=fails_originals
    if ciss_originals is not None:output.setdefault('original_source_lineage',{})['ciss']=ciss_originals
    if tic_originals is not None:output.setdefault('original_source_lineage',{})['tic']=tic_originals
    output["evidence_inventory"] = evidence_inventory(output, inputs)
    return output


def prepare(load, generated_at=None, read_original=None):
    inputs = {}
    liquidity_document=None;fails_document=None;ciss_document=None;tic_document=None
    for key in FIELDS:
        try:
            doc = load(key)
            status = "captured_projection" if isinstance(doc, dict) and doc else "source_unavailable"
        except Exception:
            doc, status = {}, "source_read_failed"
        selected = project(key, doc)
        if key==liquidity_binding.KEY:liquidity_document=doc
        if key==fails_binding.KEY:fails_document=doc
        if key==ciss_binding.KEY:ciss_document=doc
        if key==tic_binding.KEY:tic_document=doc
        inputs[key] = {"source": key, "status": status, "projection": selected, "sha256": digest(selected),
                       "basis": "typed_public_compiler_input_projection; not_complete_source_packet"}
    # In production the cutoff follows collection, so subsequently read inputs
    # are not assigned to an earlier decision clock. Explicit clocks are for replay fixtures.
    if read_original is not None:
        if not isinstance(read_original,ImmutableReader):read_original=ImmutableReader(read_original)
        inputs[liquidity_binding.KEY]['original_binding']=liquidity_binding.capture(
            liquidity_document,read_original,generated_at or datetime.now(timezone.utc).isoformat())
        inputs[fails_binding.KEY]['original_binding']=fails_binding.capture(
            fails_document,read_original,generated_at or datetime.now(timezone.utc).isoformat())
        inputs[ciss_binding.KEY]['original_binding']=ciss_binding.capture(
            ciss_document,read_original,generated_at or datetime.now(timezone.utc).isoformat())
        inputs[tic_binding.KEY]['original_binding']=tic_binding.capture(
            tic_document,read_original,generated_at or datetime.now(timezone.utc).isoformat())
    generated_at = generated_at or datetime.now(timezone.utc).isoformat()
    at = datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
    if at.tzinfo is None: raise ValueError("replay requires timezone-aware decision time")
    payload = {"generated_at": generated_at, "compiler": compiler_identity(), "inputs": inputs,
               "output": compile_frozen(inputs, generated_at,read_original),
               "scope": "public_market_research_brief; private_account_snapshot_excluded",
               "authority": "monitor_only", "sizing_eligible": False}
    sha = digest(payload)
    return {"contract_version": CONTRACT, "run_id": "calls-research-"+sha, "payload_sha256": sha, "payload": payload}


def replay(bundle,read_original=None):
    if not isinstance(bundle, dict) or bundle.get("contract_version") != CONTRACT:
        raise ValueError("unsupported public Calls replay contract")
    payload = bundle.get("payload")
    if not isinstance(payload, dict): raise ValueError("research run payload absent")
    if digest(payload) != bundle.get("payload_sha256") or bundle.get("run_id") != "calls-research-"+bundle["payload_sha256"]:
        raise ValueError("research run content hash mismatch")
    match = compiler_match(payload.get("compiler"))
    inputs = payload.get("inputs")
    if not isinstance(inputs, dict) or set(inputs) != set(FIELDS): raise ValueError("incomplete input manifest")
    for key, row in inputs.items():
        if 'original_binding' in row and key not in (liquidity_binding.KEY,fails_binding.KEY,ciss_binding.KEY,tic_binding.KEY):raise ValueError('Original binding belongs to a reviewed source only')
        selected = row.get("projection")
        if row.get("source") != key or project(key, selected) != selected or digest(selected) != row.get("sha256"):
            raise ValueError("input projection identity, whitelist or hash mismatch")
    output = compile_frozen(inputs, payload["generated_at"],read_original)
    if canonical(output) != canonical(payload.get("output")): raise ValueError("reproduced brief differs")
    return {"status": "reproduced", "compiler_match": match, "run_id": bundle["run_id"], "output_sha256": digest(output),
            "inputs": len(inputs), "evidence_fields": len(output["evidence"]), "call_verb": output["call_verb"],
            "sizing_eligible": False, "scope": payload["scope"]}


def persist(client, bucket, bundle,read_original=None):
    replay(bundle,read_original)
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
    """Preserve newer publications and reject conflicting same-clock content."""
    from calls_contract import timestamp
    if not isinstance(document, dict): raise ValueError("current brief requires a JSON object")
    at = timestamp(document.get("generated_at"))
    if at is None: raise ValueError("current brief requires a dated timestamp")
    body = canonical(document)
    for _ in range(5):
        try:
            obj = client.get_object(Bucket=bucket, Key=key)
            response = obj["Body"]
            try: previous = liquidity_binding.lineage.store.strict(response.read())
            finally: response.close()
            if not isinstance(previous, dict): raise ValueError("stored current brief requires a JSON object")
            old_at = timestamp(previous.get("generated_at"))
            if old_at and old_at > at: return {"published": False, "reason": "newer_current_present"}
            if old_at == at:
                if canonical(previous) != body: raise ValueError("Conflicting same-clock publication")
                return {"published": True, "reason": "already_current"}
            condition = {"IfMatch": obj["ETag"]}
        except Exception as exc:
            code = str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))
            if code not in ("NoSuchKey", "404"): raise
            condition = {"IfNoneMatch": "*"}
        try:
            client.put_object(Bucket=bucket, Key=key, Body=body,
                              ContentType="application/json", CacheControl="public, max-age=60", **condition)
            return {"published": True}
        except Exception as exc:
            code = str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))
            if code not in ("PreconditionFailed", "412", "ConditionalRequestConflict", "409"): raise
    raise RuntimeError("current brief update conflicted; immutable research run retained")
