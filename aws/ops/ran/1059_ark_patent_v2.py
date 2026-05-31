#!/usr/bin/env python3
"""1059 — ARK + patent FAST verify (async patent to avoid 15-min runner timeout).

Strategy:
  - ARK invoke sync (fast, <30s)
  - Patent invoke ASYNC (fire & forget — InvocationType=Event)
  - Wait 8 minutes for patent to complete (Lambda timeout is 600s)
  - Read patent S3 output to verify
  - Total runtime ≤10 min (safe margin under 15-min cap)
"""
import json, os, pathlib, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1059_ark_patent_v2.json"
REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

lam = boto3.client("lambda", region_name=REGION,
                    config=Config(read_timeout=120))
long_lam = boto3.client("lambda", region_name=REGION,
                          config=Config(read_timeout=120))
events = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def lambda_exists(name):
    try:
        info = lam.get_function(FunctionName=name)
        return {
            "exists":   True,
            "memory":   info["Configuration"].get("MemorySize"),
            "timeout":  info["Configuration"].get("Timeout"),
            "last_modified": info["Configuration"].get("LastModified"),
        }
    except lam.exceptions.ResourceNotFoundException:
        return {"exists": False}


def schedule_exists(rule_name):
    try:
        r = events.describe_rule(Name=rule_name)
        return {"exists": True, "schedule": r.get("ScheduleExpression"),
                 "state": r.get("State")}
    except events.exceptions.ResourceNotFoundException:
        return {"exists": False}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    # Lambdas exist
    print("[1059] phase 1: check Lambdas + schedules…")
    out["ark"]       = lambda_exists("justhodl-ark-holdings")
    out["patent"]    = lambda_exists("justhodl-patent-velocity")
    out["coord"]     = lambda_exists("justhodl-event-coordinator")
    out["ark_sched"] = schedule_exists("ark-holdings-daily")
    out["patent_sched"] = schedule_exists("patent-velocity-daily")
    
    # ARK invoke (sync, fast)
    print("[1059] phase 2: sync-invoke ARK (fast)…")
    if out["ark"].get("exists"):
        t0 = time.time()
        try:
            r = lam.invoke(FunctionName="justhodl-ark-holdings",
                            InvocationType="RequestResponse", Payload=b"{}")
            body = r["Payload"].read().decode("utf-8", errors="replace")
            try:
                p = json.loads(body)
                result = json.loads(p["body"]) if isinstance(p.get("body"), str) else p
                out["ark_invoke"] = {
                    "elapsed_s":   round(time.time() - t0, 1),
                    "ok":          result.get("ok"),
                    "n_funds":     result.get("n_funds"),
                    "n_positions": result.get("n_positions"),
                    "n_unique_tickers": result.get("n_unique_tickers"),
                    "n_new":       result.get("n_new_positions"),
                    "n_adds":      result.get("n_adds"),
                    "n_trims":     result.get("n_trims"),
                    "n_closed":    result.get("n_closed"),
                }
            except Exception:
                out["ark_invoke"] = {"elapsed_s": round(time.time() - t0, 1),
                                       "raw": body[:400]}
        except Exception as e:
            out["ark_invoke_err"] = str(e)[:300]
    
    # Patent invoke ASYNC (fire & forget)
    print("[1059] phase 3: ASYNC invoke patent-velocity…")
    if out["patent"].get("exists"):
        try:
            r = long_lam.invoke(FunctionName="justhodl-patent-velocity",
                                  InvocationType="Event",  # async!
                                  Payload=b"{}")
            out["patent_async"] = {
                "status_code":     r["StatusCode"],
                "executed_version": r.get("ExecutedVersion"),
            }
        except Exception as e:
            out["patent_async_err"] = str(e)[:300]
    
    # Wait for patent to complete (Lambda timeout is 600s)
    # Poll S3 every 30s for up to 8 minutes
    print("[1059] phase 4: wait + poll for patent S3 output…")
    patent_landed = False
    for i in range(16):  # 16 × 30s = 8 min max
        time.sleep(30)
        try:
            obj = s3.get_object(Bucket=BUCKET, Key="data/patent-velocity.json")
            generated = json.loads(obj["Body"].read().decode("utf-8")).get("generated_at", "")
            # Check if generated within last 10 minutes (fresh from our invoke)
            try:
                gen_dt = datetime.fromisoformat(generated.replace("Z", "+00:00"))
                age_min = (datetime.now(timezone.utc) - gen_dt).total_seconds() / 60
                if age_min < 12:
                    patent_landed = True
                    print(f"[1059]   ✓ patent landed (age {age_min:.1f}min)")
                    break
                else:
                    print(f"[1059]   poll {i+1}/16: file exists but stale ({age_min:.0f}min old)")
            except Exception:
                print(f"[1059]   poll {i+1}/16: parse err")
        except Exception:
            print(f"[1059]   poll {i+1}/16: not yet")
    
    out["patent_completed"] = patent_landed
    
    # Read S3 snapshots
    print("[1059] phase 5: read S3 outputs…")
    for key, label in [
        ("data/ark-holdings.json",    "ark_snapshot"),
        ("data/patent-velocity.json", "patent_snapshot"),
    ]:
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=key)
            body = obj["Body"].read()
            d = json.loads(body)
            snap = {
                "size_kb":      round(len(body) / 1024, 1),
                "schema":       d.get("schema_version"),
                "method":       d.get("method"),
                "generated_at": d.get("generated_at"),
                "duration_s":   d.get("duration_s"),
            }
            if label == "ark_snapshot":
                snap["n_funds"]     = d.get("n_funds_fetched")
                snap["n_positions"] = d.get("n_positions_total")
                snap["n_unique"]    = d.get("n_unique_tickers")
                diff = d.get("diff_vs_prev", {})
                snap["diff"] = {
                    "new":    diff.get("n_new_positions"),
                    "adds":   diff.get("n_position_adds"),
                    "trims":  diff.get("n_position_trims"),
                    "closed": diff.get("n_closed_positions"),
                }
                snap["top_5_cross_fund"] = [
                    {"ticker": r["ticker"], "n_funds": r["n_funds"],
                     "total_value": r["total_value"]}
                    for r in (d.get("cross_fund_top") or [])[:5]
                ]
            elif label == "patent_snapshot":
                snap["universe_size"]      = d.get("universe_size")
                snap["n_results"]          = d.get("n_results")
                snap["n_velocity_spikes"]  = d.get("n_velocity_spikes")
                snap["n_new_tech_focus"]   = d.get("n_new_tech_focus")
                hl = d.get("highlights", {})
                snap["top_5_spikes"] = [
                    {"ticker": r["ticker"], "score": r["score"],
                     "velocity": r["velocity_ratio"],
                     "n_recent": r["n_recent_patents"],
                     "n_baseline": r["n_baseline_patents"],
                     "new_cpcs": r.get("new_cpcs") or [],
                     "thesis": r.get("thesis", "")[:90]}
                    for r in (hl.get("velocity_spikes") or [])[:5]
                ]
            out[label] = snap
        except Exception as e:
            out[label + "_err"] = str(e)[:200]
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    out["total_runtime_s"] = round(
        (datetime.now(timezone.utc) - datetime.fromisoformat(out["started"].replace("Z", "+00:00"))).total_seconds(), 1
    )
    pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print(f"[1059] DONE in {out['total_runtime_s']}s → {REPORT}")


if __name__ == "__main__":
    main()
