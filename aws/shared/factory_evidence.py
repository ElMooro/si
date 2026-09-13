"""Evidence contract for every lesson the factory may keep (2026-09-13 doctrine).

A lesson exists only as an add-only ``factory-evidence.v1`` object: what was
claimed, which warehouse keys were used, what was held out, what would falsify
it, when it is graded. The grader writes grades; authors never do. No evidence
means nothing was learned. Reading the outside world produces a
``factory-reading.v1`` receipt: citable, never trainable, never a lesson.

Pure module: no cloud clients, no model calls, no code execution.
"""
from __future__ import annotations

import re

from factory_core import Invalid, canonical, digest, identifier, timestamp

SCHEMA_EVIDENCE = "factory-evidence.v1"
SCHEMA_READING = "factory-reading.v1"
DOMAINS = ("market", "code", "method")
CLAIM_TYPES = ("weekly_forecast", "regime_rule", "code_patch", "method_card")
AUTHOR_KINDS = ("student", "guest", "lane")
CHECKERS = ("grader:market_v1", "grader:code_v1", "grader:decimal_v1")
HORIZONS = (5, 21, 63)
# Warehouse and factory prefixes an evidence row may cite. Nothing else exists to the student.
DATA_PREFIXES = ("data/", "factory/")
# Minimum graded instances before a lesson may be called proved, per horizon (trading days).
PROVED_MIN = {5: 12, 21: 6, 63: 4}
RETIRE_MIN = 6
_HEX64 = re.compile(r"^[0-9a-f]{64}$")


def evidence_id(claim, data_keys, alias):
    """Deterministic id: the same claim on the same keys by the same author is one lesson."""
    return digest({"claim": claim, "keys": sorted(str(k) for k in data_keys), "alias": alias})[:16]


def _text(value, field, maximum):
    if not isinstance(value, str) or not value.strip() or len(value) > maximum:
        raise Invalid("evidence_%s_invalid" % field)
    return value.strip()


def validate_evidence(doc, *, received_at, holdout_hash=None):
    """Return the normalized evidence object or raise Invalid. Never mutates ``doc``."""
    if not isinstance(doc, dict) or doc.get("schema_version") != SCHEMA_EVIDENCE:
        raise Invalid("evidence_schema_required")
    allowed = {"schema_version", "id", "domain", "author", "claim", "citation", "data", "holdout",
               "grade_after", "checker", "evidence_hash"}
    if set(doc) - allowed:
        raise Invalid("evidence_unknown_fields")
    if doc.get("domain") not in DOMAINS:
        raise Invalid("evidence_domain_invalid")
    author = doc.get("author") or {}
    if not isinstance(author, dict) or author.get("kind") not in AUTHOR_KINDS:
        raise Invalid("evidence_author_invalid")
    alias = identifier(author.get("alias"))
    if alias == "owner" or alias.startswith("teacher-"):
        raise Invalid("evidence_author_reserved")
    claim = doc.get("claim") or {}
    if not isinstance(claim, dict) or claim.get("type") not in CLAIM_TYPES:
        raise Invalid("evidence_claim_type_invalid")
    claim_text = _text(claim.get("text"), "claim_text", 500)
    falsifier = _text(claim.get("falsifier"), "falsifier", 300)
    horizon = claim.get("horizon_days")
    if horizon not in HORIZONS:
        raise Invalid("evidence_horizon_invalid")
    data = doc.get("data") or {}
    keys = data.get("keys") if isinstance(data, dict) else None
    if not isinstance(keys, list) or not keys or len(keys) > 64:
        raise Invalid("evidence_data_keys_required")
    normalized_keys = []
    for row in keys:
        if not isinstance(row, dict) or row.get("bucket") not in ("public", "private"):
            raise Invalid("evidence_data_key_invalid")
        key = row.get("key")
        if not isinstance(key, str) or not key.startswith(DATA_PREFIXES) or ".." in key or len(key) > 400:
            raise Invalid("evidence_data_key_outside_warehouse")
        sha = row.get("sha256")
        if not isinstance(sha, str) or not _HEX64.match(sha):
            raise Invalid("evidence_data_sha256_required")
        normalized_keys.append({"bucket": row["bucket"], "key": key, "sha256": sha,
                                "last_modified": row.get("last_modified")})
    cutoff = data.get("data_cutoff")
    if timestamp(cutoff) > timestamp(received_at):
        raise Invalid("evidence_future_data_cutoff")
    holdout = doc.get("holdout") or {}
    if not isinstance(holdout, dict) or holdout.get("touched") is not False:
        raise Invalid("evidence_holdout_touched")
    if holdout_hash is not None and holdout.get("manifest_hash") != holdout_hash:
        raise Invalid("evidence_holdout_manifest_mismatch")
    if doc.get("checker") not in CHECKERS:
        raise Invalid("evidence_checker_invalid")
    grade_after = doc.get("grade_after") or {}
    if not isinstance(grade_after, dict) or str(horizon) not in grade_after or not grade_after[str(horizon)]:
        raise Invalid("evidence_grade_after_required")
    if timestamp(grade_after[str(horizon)]) < timestamp(received_at):
        raise Invalid("evidence_grade_after_in_past")
    citation = doc.get("citation")
    if citation is not None:
        if not isinstance(citation, dict) or citation.get("kind") not in ("paper", "oss", "official"):
            raise Invalid("evidence_citation_invalid")
        _text(citation.get("ref"), "citation_ref", 300)
        _text(citation.get("license"), "citation_license", 60)
    expected = evidence_id(claim_text, [k["key"] for k in normalized_keys], alias)
    if doc.get("id") != expected:
        raise Invalid("evidence_id_mismatch")
    out = {"schema_version": SCHEMA_EVIDENCE, "id": expected, "domain": doc["domain"],
           "author": {"kind": author["kind"], "alias": alias},
           "received_at": received_at if isinstance(received_at, str) else received_at.isoformat(),
           "claim": {"type": claim["type"], "text": claim_text, "horizon_days": horizon, "falsifier": falsifier},
           "citation": citation,
           "data": {"keys": normalized_keys, "data_cutoff": cutoff},
           "holdout": {"manifest_hash": holdout.get("manifest_hash"), "touched": False},
           "grade_after": {str(horizon): grade_after[str(horizon)]},
           "checker": doc["checker"]}
    out["evidence_hash"] = digest(out)
    return out


def reading_receipt(track, title, hits, at, *, stage=None, why=None):
    """A citable record of something read outside. It is not a lesson and is never trainable."""
    rows = []
    for hit in (hits or [])[:12]:
        if not isinstance(hit, dict):
            continue
        rows.append({"source": str(hit.get("source") or "")[:40], "title": str(hit.get("title") or "")[:200],
                     "url": str(hit.get("url") or "")[:300]})
    return {"schema_version": SCHEMA_READING, "kind": "reading_receipt", "trainable": False, "evidence": False,
            "track": str(track or "")[:40], "stage": stage, "title": str(title or "")[:200],
            "why": str(why or "")[:500], "at": at, "hits": rows, "n": len(rows),
            "license_note": "citation only; text outside US-gov/permissive-OSS/own-repo/warehouse never enters a training lake"}


def proved_status(instances, horizon_days):
    """Lesson status from graded instances: proved | provisional | retired.

    ``instances``: dicts with status graded|void|pending, score, baseline_score, brier,
    baseline_brier, crisis_stratum. Voids are neither passes nor fails.
    """
    graded = [i for i in instances if i.get("status") == "graded"]
    passes = [i for i in graded if float(i.get("score", 0)) > float(i.get("baseline_score", 0))]
    fails = [i for i in graded if i not in passes]
    summary = {"graded": len(graded), "passes": len(passes), "fails": len(fails),
               "voids": sum(1 for i in instances if i.get("status") == "void"),
               "crisis_instances": sum(1 for i in graded if i.get("crisis_stratum"))}
    if len(graded) >= RETIRE_MIN and len(fails) > len(passes):
        return {"status": "retired", "reason": "fails_exceed_passes", **summary}
    need = PROVED_MIN.get(int(horizon_days), PROVED_MIN[5])
    if len(graded) < need:
        return {"status": "provisional", "reason": "graded_%d_of_%d" % (len(graded), need), **summary}
    mean = lambda rows, key: sum(float(r.get(key, 0)) for r in rows) / len(rows)  # noqa: E731
    if mean(graded, "score") <= mean(graded, "baseline_score"):
        return {"status": "provisional", "reason": "not_above_naive_baseline", **summary}
    if mean(graded, "brier") > mean(graded, "baseline_brier"):
        return {"status": "provisional", "reason": "brier_not_better_than_baseline", **summary}
    if summary["crisis_instances"] == 0:
        return {"status": "provisional", "reason": "no_crisis_stratum_instance", **summary}
    return {"status": "proved", "reason": "beats_baseline_on_score_and_brier_with_crisis_instance", **summary}
