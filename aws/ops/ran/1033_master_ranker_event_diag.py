#!/usr/bin/env python3
"""Step 1033 — Diagnose why master-ranker convergence events didn't land
in the audit log even though n_tier_5_plus=3.

Possibilities:
  1. .prev file already existed → no NEW tier crossings detected (most likely)
  2. publish_many silently failed (event publish is fire-and-forget)
  3. EventBridge dropped events (would show in coordinator logs)
  4. Coordinator's ROUTES didn't have convergence.tier_up when events arrived

This script:
  a. Checks if data/master-ranker.json.prev exists (and its content)
  b. Reads master-ranker CloudWatch logs for the latest run, looking for
     'emitted N tier-up events' or any publish errors
  c. Re-invokes master-ranker after deleting .prev (forces first-run state)
     to confirm events DO fire when expected
  d. Reads audit log 60s later to see if they landed
"""
import json, os, pathlib, time
from datetime import datetime, timezone, timedelta
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1033_master_ranker_event_diag.json"
REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

lam = boto3.client("lambda", region_name=REGION,
                    config=Config(read_timeout=180, connect_timeout=10))
s3 = boto3.client("s3", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    # ─── 1. Check .prev file ─────────────────────────────────────────────
    print("[1033] checking for master-ranker.json.prev…")
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="data/master-ranker.json.prev")
        prev_data = json.loads(obj["Body"].read().decode())
        prev_tickers = prev_data.get("top_tickers") or []
        out["prev_file"] = {
            "exists":       True,
            "as_of":        prev_data.get("as_of"),
            "n_tickers":    len(prev_tickers),
            "tier_5_plus":  [t for t in prev_tickers if (t.get("n_systems") or 0) >= 5],
            "tier_3_plus_count": sum(1 for t in prev_tickers if (t.get("n_systems") or 0) >= 3),
            "sample":       prev_tickers[:5],
        }
        print(f"[1033]   .prev exists from {prev_data.get('as_of','?')}")
        print(f"[1033]   {len(prev_tickers)} tickers tracked, "
              f"{out['prev_file']['tier_3_plus_count']} were tier-3+ at that time")
    except s3.exceptions.NoSuchKey:
        out["prev_file"] = {"exists": False}
        print("[1033]   .prev does NOT exist — first run should fire all events")
    
    # ─── 2. Read master-ranker recent logs ───────────────────────────────
    print("[1033] reading master-ranker recent logs…")
    out["recent_logs"] = []
    try:
        lg = "/aws/lambda/justhodl-master-ranker"
        streams = logs.describe_log_streams(
            logGroupName=lg, orderBy="LastEventTime", descending=True, limit=2,
        ).get("logStreams", [])
        for stream in streams[:2]:
            events_resp = logs.get_log_events(
                logGroupName=lg, logStreamName=stream["logStreamName"],
                limit=100, startFromHead=False,
            )
            for ev in events_resp.get("events") or []:
                msg = ev.get("message", "")
                if any(k in msg for k in ("tier-up", "event publish",
                                            "publish_many", "n_tier",
                                            "regime", "ERROR", "Exception",
                                            "[master-ranker]")):
                    out["recent_logs"].append({
                        "ts": datetime.fromtimestamp(ev["timestamp"]/1000,
                                                       tz=timezone.utc).isoformat(),
                        "msg": msg.strip()[:300],
                    })
        out["recent_logs"] = out["recent_logs"][-15:]
    except Exception as e:
        out["recent_logs_err"] = str(e)[:200]
    
    # ─── 3. Force-fire: delete .prev + invoke ────────────────────────────
    if out.get("prev_file", {}).get("exists"):
        print("[1033] deleting .prev to force first-run state → all tier-3+ become 'new'…")
        try:
            s3.delete_object(Bucket=BUCKET, Key="data/master-ranker.json.prev")
            out["prev_deleted"] = True
        except Exception as e:
            out["prev_deleted_err"] = str(e)[:200]
    
    print("[1033] re-invoking master-ranker…")
    try:
        r = lam.invoke(FunctionName="justhodl-master-ranker",
                         InvocationType="RequestResponse", Payload=b"{}")
        body = r["Payload"].read().decode("utf-8", errors="replace")
        try:
            p = json.loads(body)
            result = json.loads(p.get("body", "{}")) if isinstance(p.get("body"), str) else p
            out["invoke_result"] = result
            print(f"[1033]   n_tier_3_plus={result.get('n_tier_3_plus','?')} "
                  f"n_tier_5_plus={result.get('n_tier_5_plus','?')}")
        except Exception:
            out["invoke_raw"] = body[:400]
    except Exception as e:
        out["invoke_err"] = str(e)[:200]
    
    # ─── 4. Wait then read audit log ────────────────────────────────────
    print("[1033] waiting 60s for pipeline…")
    time.sleep(60)
    
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    print("[1033] reading audit log…")
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=f"system-events/audit/{today}.jsonl")
        lines = [l for l in obj["Body"].read().decode().split("\n") if l.strip()]
        entries = [json.loads(l) for l in lines]
        out["audit_after_invoke"] = {
            "n_total":  len(entries),
            "convergence_events": [
                e for e in entries
                if e.get("event") == "convergence.tier_up"
            ][:20],
        }
        print(f"[1033]   audit log now has {len(entries)} events, "
              f"{len(out['audit_after_invoke']['convergence_events'])} convergence")
    except Exception as e:
        out["audit_err"] = str(e)[:200]
    
    # ─── 5. Read fresh .prev (verify it was created by latest invoke) ───
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="data/master-ranker.json.prev")
        prev = json.loads(obj["Body"].read().decode())
        out["new_prev_file"] = {
            "exists":      True,
            "as_of":       prev.get("as_of"),
            "n_tickers":   len(prev.get("top_tickers") or []),
        }
    except Exception as e:
        out["new_prev_err"] = str(e)[:200]
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
