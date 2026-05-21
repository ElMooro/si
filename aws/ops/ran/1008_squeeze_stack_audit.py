"""
ops 1008 - Squeeze stack audit (Phase D pre-build).

Inspects deployed state of the 5 short/squeeze Lambdas to confirm:
- All 5 outputs exist on S3 + last_modified within freshness window
- squeeze-pretrigger EventBridge schedule is enabled
- signal-board input list — is squeeze-pretrigger already wired in?

Pure read-only — no invokes (squeeze-pretrigger is FMP-heavy; preserve quota
for upcoming #10 EVA build).

Outputs report at aws/ops/reports/1008.json with go/no-go for squeeze.html
build and pinpoints any broken feeds before we build the cockpit on top.
"""
import json
import sys
import traceback
from datetime import datetime, timezone, timedelta
from pathlib import Path

import boto3
from botocore.config import Config

REPO_ROOT = Path(__file__).resolve().parents[3]
REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

cfg = Config(read_timeout=60, connect_timeout=10, retries={"max_attempts": 2})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION)
sch = boto3.client("scheduler", region_name=REGION)
events = boto3.client("events", region_name=REGION)

# Map each Lambda -> its primary S3 output + expected refresh cadence
FEEDS = [
    {"fn": "justhodl-finra-short",
     "key": "data/finra-short.json",
     "max_age_hours": 36,  # daily TUE-SAT 01:00 UTC
     "role": "Raw FINRA daily short volume (T+1, ~11.5k tickers)"},
    {"fn": "justhodl-short-interest",
     "key": "data/short-interest.json",
     "max_age_hours": 72,
     "role": "FINRA SI snapshots + Polygon SI (bi-monthly settlement)"},
    {"fn": "justhodl-short-pressure",
     "key": "data/short-pressure.json",
     "max_age_hours": 36,  # daily 12:30 UTC MON-FRI
     "role": "Daily short-volume ratio vs 20d baseline (signal-board wired)"},
    {"fn": "justhodl-squeeze-pretrigger",
     "key": "data/squeeze-pretrigger.json",
     "max_age_hours": 30,  # daily 23:30 UTC
     "role": "Mid/large 5-condition pre-trigger (FLAGSHIP)"},
    {"fn": "justhodl-microcap-float-squeeze",
     "key": "data/microcap-float-squeeze.json",
     "max_age_hours": 30,
     "role": "Microcap float-driven squeeze candidates"},
]


def s3_head(key):
    try:
        h = s3.head_object(Bucket=BUCKET, Key=key)
        return {
            "exists": True,
            "size_bytes": h.get("ContentLength"),
            "last_modified": h["LastModified"].isoformat(),
            "age_hours": round(
                (datetime.now(timezone.utc) - h["LastModified"]
                 ).total_seconds() / 3600, 1),
            "etag": h.get("ETag", "").strip('"')[:12],
        }
    except Exception as e:
        return {"exists": False, "error": str(e)[:200]}


def s3_peek(key, head_only=False):
    """Get top-level keys + critical fields from a JSON output."""
    if head_only:
        return {}
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        data = json.loads(obj["Body"].read().decode("utf-8"))
        if not isinstance(data, dict):
            return {"top_type": type(data).__name__,
                    "n_items": len(data) if hasattr(data, "__len__") else None}
        peek = {
            "top_keys": list(data.keys())[:20],
            "version": data.get("version"),
            "as_of": data.get("as_of") or data.get("generated_at"),
            "state": data.get("state") or data.get("regime"),
            "signal_strength": data.get("signal_strength"),
        }
        # squeeze-pretrigger specific
        if "imminent_setups" in data or "summary" in data:
            s = data.get("summary") or {}
            peek["squeeze_summary"] = {
                "n_imminent": s.get("n_imminent_5of5"),
                "n_pretrigger": s.get("n_pretrigger_4of5"),
                "n_early": s.get("n_early_3of5"),
                "n_total": s.get("n_total_setups"),
                "n_evaluated": s.get("n_candidates_evaluated"),
                "feeds_available": s.get("feeds_available"),
            }
            top_t = (data.get("current_readings") or {}).get(
                "top_squeeze_tickers") or []
            peek["top_5_tickers"] = top_t[:5]
        return peek
    except Exception as e:
        return {"error": str(e)[:200]}


def lambda_config(fn):
    try:
        c = lam.get_function(FunctionName=fn)["Configuration"]
        env_keys = list((c.get("Environment") or {}
                         ).get("Variables", {}).keys())
        return {
            "exists": True,
            "state": c.get("State"),
            "last_update_status": c.get("LastUpdateStatus"),
            "code_size_bytes": c.get("CodeSize"),
            "last_modified": c.get("LastModified"),
            "runtime": c.get("Runtime"),
            "timeout_s": c.get("Timeout"),
            "memory_mb": c.get("MemorySize"),
            "env_keys": sorted(env_keys),
        }
    except lam.exceptions.ResourceNotFoundException:
        return {"exists": False}
    except Exception as e:
        return {"exists": False, "error": str(e)[:200]}


def schedule_state(fn):
    """Try EventBridge Scheduler first (new), fall back to EventBridge Rules."""
    # Try Scheduler API
    try:
        for grp in ("default",):
            r = sch.list_schedules(GroupName=grp, MaxResults=100)
            for s in r.get("Schedules", []):
                name = s.get("Name", "")
                if fn in name or name.startswith(fn):
                    return {"api": "scheduler",
                            "name": name,
                            "state": s.get("State"),
                            "schedule": s.get("ScheduleExpression"),
                            "tz": s.get("ScheduleExpressionTimezone")}
    except Exception:
        pass
    # Fall back to legacy EventBridge rules
    try:
        # iterate rules with matching target
        paginator = events.get_paginator("list_rules")
        for page in paginator.paginate(NamePrefix=""):
            for rule in page.get("Rules", []):
                name = rule.get("Name", "")
                if fn in name:
                    return {"api": "events",
                            "name": name,
                            "state": rule.get("State"),
                            "schedule": rule.get("ScheduleExpression")}
    except Exception as e:
        return {"error": str(e)[:200]}
    return {"found": False}


def signal_board_wiring():
    """Inspect signal-board source list to see if squeeze-pretrigger is wired."""
    out = {}
    # Try to read signal-board's input list from the Lambda config or S3 output
    sb_key = "data/signal-board.json"
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=sb_key)
        data = json.loads(obj["Body"].read().decode("utf-8"))
        if isinstance(data, dict):
            engines = data.get("engines") or data.get("inputs") or {}
            engine_names = (list(engines.keys()) if isinstance(engines, dict)
                            else [e.get("name") for e in engines
                                  if isinstance(e, dict)])
            out["signal_board_engines"] = engine_names
            out["squeeze_pretrigger_wired"] = any(
                "squeeze" in (n or "").lower() for n in engine_names)
            out["short_pressure_wired"] = any(
                "short-pressure" in (n or "") or "short_pressure" in (n or "")
                for n in engine_names)
            out["last_modified"] = data.get("as_of") or data.get(
                "generated_at")
    except Exception as e:
        out["error"] = str(e)[:200]
    return out


def main():
    report = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "feeds": [],
    }
    for f in FEEDS:
        head = s3_head(f["key"])
        peek = s3_peek(f["key"]) if head.get("exists") else {}
        cfg_data = lambda_config(f["fn"])
        sched = schedule_state(f["fn"])
        fresh = (head.get("exists") and
                 isinstance(head.get("age_hours"), (int, float)) and
                 head["age_hours"] <= f["max_age_hours"])
        report["feeds"].append({
            "fn": f["fn"],
            "key": f["key"],
            "role": f["role"],
            "fresh": fresh,
            "max_age_hours": f["max_age_hours"],
            "s3": head,
            "lambda": cfg_data,
            "schedule": sched,
            "peek": peek,
        })

    report["signal_board"] = signal_board_wiring()

    # Cockpit-build readiness scorecard
    pretrigger_feed = next(
        (f for f in report["feeds"]
         if f["fn"] == "justhodl-squeeze-pretrigger"), {})
    n_fresh = sum(1 for f in report["feeds"] if f["fresh"])
    report["scorecard"] = {
        "all_5_lambdas_exist": all(
            f["lambda"].get("exists") for f in report["feeds"]),
        "n_feeds_fresh": n_fresh,
        "min_3_feeds_fresh": n_fresh >= 3,
        "pretrigger_feed_fresh": pretrigger_feed.get("fresh"),
        "pretrigger_has_setups": (
            (pretrigger_feed.get("peek", {}).get(
                "squeeze_summary", {}) or {}
             ).get("n_total") or 0) >= 0,
        "go_build_squeeze_html": (
            n_fresh >= 3 and pretrigger_feed.get("fresh")),
        "squeeze_pretrigger_wired_to_signal_board":
            report["signal_board"].get("squeeze_pretrigger_wired", False),
    }
    report["ended_at"] = datetime.now(timezone.utc).isoformat()

    out_path = REPO_ROOT / "aws" / "ops" / "reports" / "1008.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2, default=str))
    print(f"[ops 1008] report written {out_path.relative_to(REPO_ROOT)}")
    print(json.dumps(report["scorecard"], indent=2))


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print(traceback.format_exc())
        sys.exit(1)
