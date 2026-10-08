#!/usr/bin/env python3
"""1058 — verify ARK + patent-velocity state after 1057 cancellation.

The 1057 ops script was cancelled by GitHub Actions (likely a runner
timeout or preempt). The Deploy Lambdas workflow succeeded at the same
time, which auto-creates Lambdas from config.json. This script verifies
whether the deploy worked and runs both engines to populate their S3
outputs.

Phases:
  1. Confirm both Lambdas exist
  2. Sync-invoke each (allow 5 min each)
  3. Verify S3 outputs
  4. Confirm event coordinator has new routes
"""
import io, json, os, pathlib, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1058_ark_patent_verify.json"
REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

lam = boto3.client("lambda", region_name=REGION,
                    config=Config(read_timeout=300))
long_lam = boto3.client("lambda", region_name=REGION,
                          config=Config(read_timeout=700))
events = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def lambda_exists(name):
    try:
        info = lam.get_function(FunctionName=name)
        return {
            "exists":       True,
            "runtime":      info["Configuration"].get("Runtime"),
            "memory":       info["Configuration"].get("MemorySize"),
            "timeout":      info["Configuration"].get("Timeout"),
            "last_modified": info["Configuration"].get("LastModified"),
            "version":      info["Configuration"].get("Version"),
        }
    except lam.exceptions.ResourceNotFoundException:
        return {"exists": False}


def invoke_sync(name):
    r = long_lam.invoke(FunctionName=name,
                         InvocationType="RequestResponse", Payload=b"{}")
    body = r["Payload"].read().decode("utf-8", errors="replace")
    try:
        p = json.loads(body)
        return json.loads(p["body"]) if isinstance(p.get("body"), str) else p
    except Exception:
        return {"_raw": body[:400]}


def schedule_exists(rule_name):
    try:
        r = events.describe_rule(Name=rule_name)
        return {
            "exists": True,
            "schedule": r.get("ScheduleExpression"),
            "state":  r.get("State"),
        }
    except events.exceptions.ResourceNotFoundException:
        return {"exists": False}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    # Phase 1: Confirm Lambdas exist
    print("[1058] phase 1: check Lambdas exist…")
    out["ark_state"] = lambda_exists("justhodl-ark-holdings")
    out["patent_state"] = lambda_exists("justhodl-patent-velocity")
    out["coordinator_state"] = lambda_exists("justhodl-event-coordinator")
    
    # Phase 2: Schedule state
    print("[1058] phase 2: check schedules…")
    out["ark_schedule"] = schedule_exists("ark-holdings-daily")
    out["patent_schedule"] = schedule_exists("patent-velocity-daily")
    
    # Phase 3: sync-invoke ARK (fast — 6 CSV downloads)
    print("[1058] phase 3: sync-invoke ark-holdings…")
    if out["ark_state"].get("exists"):
        t0 = time.time()
        try:
            r = invoke_sync("justhodl-ark-holdings")
            out["ark_invoke"] = {
                "elapsed_s":   round(time.time() - t0, 1),
                "ok":          r.get("ok"),
                "n_funds":     r.get("n_funds"),
                "n_positions": r.get("n_positions"),
                "n_unique_tickers": r.get("n_unique_tickers"),
                "n_new":       r.get("n_new_positions"),
                "n_adds":      r.get("n_adds"),
                "n_trims":     r.get("n_trims"),
                "n_closed":    r.get("n_closed"),
                "duration_s":  r.get("duration_s"),
                "err":         r.get("_raw"),
            }
        except Exception as e:
            out["ark_invoke_err"] = str(e)[:300]
    
    time.sleep(2)
    
    # Phase 4: sync-invoke patent-velocity (slower — USPTO pacing)
    print("[1058] phase 4: sync-invoke patent-velocity (≤9min budget)…")
    if out["patent_state"].get("exists"):
        t0 = time.time()
        try:
            r = invoke_sync("justhodl-patent-velocity")
            out["patent_invoke"] = {
                "elapsed_s":   round(time.time() - t0, 1),
                "ok":          r.get("ok"),
                "n_results":   r.get("n_results"),
                "n_velocity_spikes": r.get("n_velocity_spikes"),
                "n_new_tech_focus": r.get("n_new_tech_focus"),
                "duration_s":  r.get("duration_s"),
                "err":         r.get("_raw"),
            }
        except Exception as e:
            out["patent_invoke_err"] = str(e)[:300]
    
    time.sleep(2)
    
    # Phase 5: S3 snapshots
    print("[1058] phase 5: verify S3 outputs…")
    for key, label in [
        ("data/ark-holdings.json",  "ark_snapshot"),
        ("data/patent-velocity.json", "patent_snapshot"),
    ]:
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=key)
            body = obj["Body"].read()
            d = json.loads(body)
            snapshot = {
                "size_kb":      round(len(body) / 1024, 1),
                "schema":       d.get("schema_version"),
                "method":       d.get("method"),
                "generated_at": d.get("generated_at"),
                "duration_s":   d.get("duration_s"),
            }
            if label == "ark_snapshot":
                snapshot["n_funds"]    = d.get("n_funds_fetched")
                snapshot["n_positions"] = d.get("n_positions_total")
                snapshot["n_unique"]   = d.get("n_unique_tickers")
                snapshot["diff"]       = d.get("diff_vs_prev", {}).get(
                    "n_new_positions"), d.get("diff_vs_prev", {}).get("n_position_adds")
                snapshot["top_5_cross_fund"] = [
                    {"ticker": r["ticker"], "n_funds": r["n_funds"],
                     "total_value": r["total_value"]}
                    for r in (d.get("cross_fund_top") or [])[:5]
                ]
            elif label == "patent_snapshot":
                snapshot["universe_size"]      = d.get("universe_size")
                snapshot["n_results"]          = d.get("n_results")
                snapshot["n_velocity_spikes"]  = d.get("n_velocity_spikes")
                snapshot["n_new_tech_focus"]   = d.get("n_new_tech_focus")
                hl = d.get("highlights", {})
                snapshot["top_5_spikes"] = [
                    {"ticker": r["ticker"], "score": r["score"],
                     "velocity": r["velocity_ratio"],
                     "n_recent": r["n_recent_patents"],
                     "n_baseline": r["n_baseline_patents"],
                     "new_cpcs": r.get("new_cpcs") or [],
                     "thesis": r.get("thesis", "")[:100]}
                    for r in (hl.get("velocity_spikes") or [])[:5]
                ]
            out[label] = snapshot
        except Exception as e:
            out[label + "_err"] = str(e)[:200]
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print(f"[1058] DONE → {REPORT}")


if __name__ == "__main__":
    main()
