"""
ops 1010 - PEAD + 13F + squeeze-pretrigger health audit.

Pure read-only audit of 11 deployed Lambdas spanning three institutional stacks
that were assumed-buildable but turned out to be pre-existing this session:

PEAD stack:
  justhodl-earnings-pead
  justhodl-pead-detector
  justhodl-post-earnings-mean-rev

13F / SEC filings stack:
  justhodl-sec-13f
  justhodl-13f-positions
  justhodl-13f-price-divergence
  justhodl-activist-13d
  justhodl-activist-filings-scanner

Diagnostic:
  justhodl-squeeze-pretrigger — investigate why n_evaluated=0 with all feeds
  available (separate from ops 1008's pre-build gate)

Confirms each Lambda exists + S3 output is present + freshness, and surfaces
the squeeze-pretrigger candidate-generation issue for fix later.
"""
import json
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

REPO_ROOT = Path(__file__).resolve().parents[3]
REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

cfg = Config(read_timeout=60, connect_timeout=10, retries={"max_attempts": 2})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION)

PEAD = [
    {"fn": "justhodl-earnings-pead",
     "primary_keys": ["data/earnings-pead.json", "data/pead.json"],
     "max_age_hours": 28},
    {"fn": "justhodl-pead-detector",
     "primary_keys": ["data/pead-detector.json", "data/pead-signals.json"],
     "max_age_hours": 28},
    {"fn": "justhodl-post-earnings-mean-rev",
     "primary_keys": ["data/post-earnings-mean-rev.json"],
     "max_age_hours": 48},
]

THIRTEEN_F = [
    {"fn": "justhodl-sec-13f",
     "primary_keys": ["data/sec-13f.json", "data/13f-filings.json",
                       "data/13f.json"],
     "max_age_hours": 168},  # weekly
    {"fn": "justhodl-13f-positions",
     "primary_keys": ["data/13f-positions.json",
                       "data/institutional-positions.json"],
     "max_age_hours": 168},
    {"fn": "justhodl-13f-price-divergence",
     "primary_keys": ["data/13f-price-divergence.json",
                       "data/13f-aggregate.json"],
     "max_age_hours": 168},
    {"fn": "justhodl-activist-13d",
     "primary_keys": ["data/activist-13d.json"],
     "max_age_hours": 36},
    {"fn": "justhodl-activist-filings-scanner",
     "primary_keys": ["data/activist-filings.json"],
     "max_age_hours": 36},
]

DIAGNOSTIC = [
    {"fn": "justhodl-squeeze-pretrigger",
     "primary_keys": ["data/squeeze-pretrigger.json"],
     "max_age_hours": 30,
     "deep_peek": True},
]


def s3_head_any(keys):
    """Try multiple S3 keys; return first one that exists with metadata."""
    for k in keys:
        try:
            h = s3.head_object(Bucket=BUCKET, Key=k)
            return {
                "found_key": k,
                "exists": True,
                "size_bytes": h.get("ContentLength"),
                "last_modified": h["LastModified"].isoformat(),
                "age_hours": round(
                    (datetime.now(timezone.utc) - h["LastModified"]
                     ).total_seconds() / 3600, 1),
            }
        except Exception:
            continue
    return {"exists": False, "tried_keys": keys}


def s3_peek_top_keys(key):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        d = json.loads(obj["Body"].read().decode("utf-8"))
        if not isinstance(d, dict):
            return {"top_type": type(d).__name__}
        return {
            "top_keys": list(d.keys())[:20],
            "version": d.get("version"),
            "generated_at": d.get("generated_at") or d.get("as_of"),
            "n_items": (len(d.get("setups") or d.get("signals") or
                          d.get("names") or d.get("positions") or
                          d.get("results") or []) or None),
        }
    except Exception as e:
        return {"error": str(e)[:200]}


def squeeze_pretrigger_diagnostic(key):
    """Why is n_evaluated=0 despite all feeds_available=true?
    Peek at the upstream feeds it reads to find the bottleneck."""
    out = {"target_key": key}
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        d = json.loads(obj["Body"].read().decode("utf-8"))
        summary = d.get("summary") or {}
        out["state"] = d.get("state")
        out["n_evaluated"] = summary.get("n_candidates_evaluated")
        out["feeds_available"] = summary.get("feeds_available")
        out["why_now"] = (d.get("why_now_explainer") or "")[:160]
        # Now probe each upstream feed for candidate-list health
        upstream = {}
        # FINRA short
        try:
            f = json.loads(s3.get_object(
                Bucket=BUCKET, Key="data/finra-short.json")[
                "Body"].read().decode("utf-8"))
            cands = f.get("squeeze_candidates") or []
            top_z = f.get("top_zscore") or []
            upstream["finra"] = {
                "ok": True,
                "n_squeeze_candidates": len(cands),
                "first_3_candidates": cands[:3] if cands else [],
                "n_top_zscore": len(top_z),
                "first_3_top_zscore": [
                    (t.get("ticker") or t.get("symbol"))
                    if isinstance(t, dict) else t
                    for t in top_z[:3]],
            }
        except Exception as e:
            upstream["finra"] = {"ok": False, "err": str(e)[:120]}
        # short-interest
        try:
            si = json.loads(s3.get_object(
                Bucket=BUCKET, Key="data/short-interest.json")[
                "Body"].read().decode("utf-8"))
            crowded = si.get("top_crowded_shorts") or []
            risk = si.get("top_squeeze_risk") or []
            upstream["short_interest"] = {
                "ok": True,
                "n_top_crowded": len(crowded),
                "first_3_crowded": [r.get("ticker") for r in crowded[:3]],
                "n_top_squeeze_risk": len(risk),
                "first_3_risk": [r.get("ticker") for r in risk[:3]],
            }
        except Exception as e:
            upstream["short_interest"] = {"ok": False, "err": str(e)[:120]}
        # catalyst-calendar
        try:
            cc = json.loads(s3.get_object(
                Bucket=BUCKET, Key="data/catalyst-calendar.json")[
                "Body"].read().decode("utf-8"))
            cats = cc.get("catalysts") or cc.get("upcoming") or []
            upstream["catalyst_calendar"] = {
                "ok": True,
                "n_upcoming": len(cats),
                "first_3": cats[:3] if cats else [],
            }
        except Exception as e:
            upstream["catalyst_calendar"] = {"ok": False,
                                              "err": str(e)[:120]}
        out["upstream_feeds"] = upstream
        # Hypothesis check
        hypothesis = []
        if upstream.get("finra", {}).get("n_squeeze_candidates", 0) == 0:
            hypothesis.append(
                "FINRA squeeze_candidates list is empty — upstream filter "
                "rejecting everything (likely si_pct/util thresholds too tight "
                "for current market regime)")
        if upstream.get("short_interest", {}).get(
                "n_top_crowded", 0) > 0 and not upstream.get(
                    "finra", {}).get("n_squeeze_candidates"):
            hypothesis.append(
                "short-interest has crowded names but FINRA candidate list "
                "is empty — engine may not be merging short-interest fallback")
        out["likely_root_cause"] = hypothesis or [
            "Need deeper code inspection — feeds present, candidates listed, "
            "yet pre-trigger evaluates 0"]
    except Exception as e:
        out["error"] = str(e)[:300]
    return out


def lambda_exists(fn):
    try:
        c = lam.get_function(FunctionName=fn)["Configuration"]
        return {
            "exists": True,
            "state": c.get("State"),
            "last_update_status": c.get("LastUpdateStatus"),
            "code_size": c.get("CodeSize"),
            "last_modified": c.get("LastModified"),
            "timeout_s": c.get("Timeout"),
            "memory_mb": c.get("MemorySize"),
        }
    except lam.exceptions.ResourceNotFoundException:
        return {"exists": False}
    except Exception as e:
        return {"exists": False, "error": str(e)[:200]}


def audit_stack(name, lambdas):
    out = {"name": name, "n_total": len(lambdas), "feeds": []}
    n_lambdas_ok = 0
    n_feeds_fresh = 0
    for f in lambdas:
        cfg_info = lambda_exists(f["fn"])
        head = s3_head_any(f["primary_keys"])
        peek = (s3_peek_top_keys(head["found_key"])
                if head.get("exists") else {})
        diag = (squeeze_pretrigger_diagnostic(head["found_key"])
                if (f.get("deep_peek") and head.get("exists")) else None)
        fresh = (head.get("exists") and
                 isinstance(head.get("age_hours"), (int, float)) and
                 head["age_hours"] <= f["max_age_hours"])
        if cfg_info.get("exists"):
            n_lambdas_ok += 1
        if fresh:
            n_feeds_fresh += 1
        rec = {
            "fn": f["fn"],
            "lambda_exists": cfg_info.get("exists"),
            "lambda_state": cfg_info.get("state"),
            "code_size": cfg_info.get("code_size"),
            "last_modified": cfg_info.get("last_modified"),
            "s3": head,
            "fresh": fresh,
            "max_age_hours": f["max_age_hours"],
            "peek": peek,
        }
        if diag is not None:
            rec["diagnostic"] = diag
        out["feeds"].append(rec)
    out["n_lambdas_ok"] = n_lambdas_ok
    out["n_feeds_fresh"] = n_feeds_fresh
    return out


def main():
    report = {"started_at": datetime.now(timezone.utc).isoformat()}
    report["pead_stack"] = audit_stack("PEAD", PEAD)
    report["thirteen_f_stack"] = audit_stack("13F", THIRTEEN_F)
    report["diagnostic_stack"] = audit_stack("Squeeze diagnostic", DIAGNOSTIC)

    p = report["pead_stack"]
    f13 = report["thirteen_f_stack"]
    diag = report["diagnostic_stack"]
    report["scorecard"] = {
        "pead_n_lambdas_total": p["n_total"],
        "pead_n_lambdas_ok": p["n_lambdas_ok"],
        "pead_n_feeds_fresh": p["n_feeds_fresh"],
        "pead_all_fresh": p["n_feeds_fresh"] == p["n_total"],
        "13f_n_lambdas_total": f13["n_total"],
        "13f_n_lambdas_ok": f13["n_lambdas_ok"],
        "13f_n_feeds_fresh": f13["n_feeds_fresh"],
        "13f_all_fresh": f13["n_feeds_fresh"] == f13["n_total"],
        "squeeze_pretrigger_root_cause_identified": bool(
            diag["feeds"] and diag["feeds"][0].get(
                "diagnostic", {}).get("likely_root_cause")),
    }
    report["ended_at"] = datetime.now(timezone.utc).isoformat()

    out_path = REPO_ROOT / "aws" / "ops" / "reports" / "1010.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2, default=str))
    print(f"[ops 1010] report written {out_path.relative_to(REPO_ROOT)}")
    print(json.dumps(report["scorecard"], indent=2))


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print(traceback.format_exc())
        sys.exit(1)
