"""justhodl-fabrication-weekly — F9 (ops 4446). Weekly data-quality report:
detector totals (F2 sites by kind, top offenders), guard adoption (engines
running guard_output), and LIVE provenance coverage measured on the flagship
feeds via provenance.coverage() -> data/audit/fabrication-weekly.json.
Trend: each run appends to history so coverage-over-time is visible."""
import json
import os
from datetime import datetime, timezone

import boto3

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
s3 = boto3.client("s3", region_name="us-east-1")
from provenance import coverage

FLAGSHIPS = ["liquidity-data.json", "data/plumbing-stress.json",
             "data/crisis-plumbing.json", "data/breadth-thrust.json",
             "data/risk-gate.json", "data/llm-cost.json",
             "data/signal-board.json", "data/canary-macro.json", "data/soma-holdings.json",
             "data/bls-macro.json", "data/bea-gdp.json"]
GUARDED = ["justhodl-signal-board", "justhodl-prepump-alerts-router",
           "justhodl-stock-screener"]


def _get(k):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=k)["Body"].read())
    except Exception:
        return None


def lambda_handler(event, context):
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    sites = _get("data/audit/fabrication-sites.json") or {}
    feeds_cov = {}
    for f in FLAGSHIPS:
        d = _get(f)
        if d is None:
            feeds_cov[f] = {"data_unavailable": True,
                            "reason": "feed unreadable"}
        elif coverage:
            feeds_cov[f] = coverage(d)
        else:
            feeds_cov[f] = {"data_unavailable": True,
                            "reason": "provenance module missing"}
    covs = [v.get("coverage_pct") for v in feeds_cov.values()
            if v.get("coverage_pct") is not None]
    prev = _get("data/audit/fabrication-weekly.json") or {}
    hist = (prev.get("history") or [])[-25:]
    numeric = sum(v.get("numeric_leaves", 0) for v in feeds_cov.values())
    verified = sum(v.get("replay_verified", 0) for v in feeds_cov.values())
    entry = {"as_of": now, "coverage_contract": "provenance-coverage.v2",
             "coverage_meaning": "structural_envelope_only_not_source_verification",
             "replay_verified_pct": round(100 * verified / numeric, 1) if numeric else None,
             "sites_by_kind": sites.get("by_kind"),
             "engines_flagged": sites.get("n_engines_flagged"),
             "guarded_engines": GUARDED,
             "flagship_coverage_avg_pct": (round(sum(covs) / len(covs), 1)
                                           if covs else None)}
    hist.append(entry)
    doc = {"as_of": now, "generated_at": now, "schema_version": "2.0", "spec": "F9 weekly data-quality report",
           "detector": {"by_kind": sites.get("by_kind"),
                        "engines_flagged": sites.get("n_engines_flagged"),
                        "top": (sites.get("top_engines") or [])[:10]},
           "guard_adoption": {"engines": GUARDED, "n": len(GUARDED),
                              "of_flagged": sites.get("n_engines_flagged"),
                              "mode": "warn (migration phase)"},
           "flagship_provenance": feeds_cov,
           "flagship_coverage_avg_pct": entry[
               "flagship_coverage_avg_pct"],
           "coverage_meaning": entry["coverage_meaning"],
           "replay_verified_pct": entry["replay_verified_pct"],
           "coverage_contract": entry["coverage_contract"],
           "sizing_eligible": False,
           "history": hist,
           "note": "Envelope coverage measures structure, not verified sources or model accuracy. "
                   "Replay verification is reported separately. Historical rows without coverage_contract "
                   "use the legacy counting method and are not directly comparable. Guard adoption is a "
                   "configured list, not proof of runtime enforcement. Missing feeds are explicit."}
    s3.put_object(Bucket=BUCKET, Key="data/audit/fabrication-weekly.json",
                  Body=json.dumps(doc, default=str).encode(),
                  ContentType="application/json", CacheControl="no-cache")
    res = {"ok": True, "avg_cov": entry["flagship_coverage_avg_pct"],
           "guarded": len(GUARDED),
           "flagged": sites.get("n_engines_flagged")}
    print(json.dumps(res))
    return {"statusCode": 200, "body": json.dumps(res)}
