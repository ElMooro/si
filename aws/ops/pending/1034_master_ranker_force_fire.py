#!/usr/bin/env python3
"""Step 1034 — Redeploy master-ranker with the systems-extraction bugfix +
force-fire to validate events actually land this time.

Bug was: my code did [s.get('system') for s in t.get('systems')] but the
'systems' field is a list of strings, not dicts. AttributeError caused
publish_many to fail silently inside try/except, and the resulting
[master-ranker] event publish failed: 'str' object has no attribute 'get'
log line was the only visible symptom.

Fix: just use the list as-is — list(t.get('systems') or [])[:10]

This script:
  1. Redeploys master-ranker source
  2. Deletes master-ranker.json.prev (forces all tier-3+ to be 'new')
  3. Invokes master-ranker
  4. Waits 75s for EventBridge → coordinator → audit log pipeline
  5. Reads audit log → expects ≥38 convergence.tier_up events
     (the 38 tier-3+ tickers from the previous run)
  6. Reads coordinator's CloudWatch logs to verify it routed them
"""
import io, json, os, pathlib, time, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1034_master_ranker_force_fire.json"
REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

lam = boto3.client("lambda", region_name=REGION,
                    config=Config(read_timeout=180, connect_timeout=10))
s3 = boto3.client("s3", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)


def redeploy(fn):
    src_dir = pathlib.Path(f"aws/lambdas/{fn}/source")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for f in src_dir.glob("*.py"):
            zf.writestr(f.name, f.read_bytes())
    zb = buf.getvalue()
    for attempt in range(4):
        try:
            lam.update_function_code(FunctionName=fn, ZipFile=zb, Publish=False)
            lam.get_waiter("function_updated").wait(FunctionName=fn)
            return {"action": "updated", "zip_size": len(zb)}
        except Exception as e:
            if "ResourceConflict" in str(e) and attempt < 3:
                time.sleep(5 * (attempt + 1))
                continue
            return {"err": f"{type(e).__name__}: {str(e)[:200]}"}


def invoke_sync(fn):
    try:
        r = lam.invoke(FunctionName=fn, InvocationType="RequestResponse", Payload=b"{}")
        body = r["Payload"].read().decode("utf-8", errors="replace")
        out = {"status": r.get("StatusCode"), "fn_err": r.get("FunctionError")}
        try:
            p = json.loads(body)
            out["result"] = json.loads(p["body"]) if isinstance(p.get("body"), str) else p
        except Exception:
            out["raw"] = body[:400]
        return out
    except Exception as e:
        return {"fail": f"{type(e).__name__}: {str(e)[:200]}"}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    # 1. Redeploy
    print("[1034] redeploying master-ranker…")
    out["redeploy"] = redeploy("justhodl-master-ranker")
    time.sleep(3)
    
    # 2. Delete .prev (force all tier-3+ to be 'new')
    print("[1034] deleting master-ranker.json.prev…")
    try:
        s3.delete_object(Bucket=BUCKET, Key="data/master-ranker.json.prev")
        out["prev_deleted"] = True
    except Exception as e:
        out["prev_deleted_err"] = str(e)[:200]
    
    # 3. Invoke
    print("[1034] invoking master-ranker…")
    out["invoke"] = invoke_sync("justhodl-master-ranker")
    
    # 4. Wait
    print("[1034] waiting 75s for pipeline…")
    time.sleep(75)
    
    # 5. Read audit log
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=f"system-events/audit/{today}.jsonl")
        lines = [l for l in obj["Body"].read().decode().split("\n") if l.strip()]
        entries = [json.loads(l) for l in lines]
        conv_events = [e for e in entries if e.get("event") == "convergence.tier_up"]
        out["audit"] = {
            "n_total":                len(entries),
            "n_convergence":          len(conv_events),
            "tier_5_count":           sum(1 for e in conv_events
                                            if (e.get("detail") or {}).get("new_tier") == 5),
            "tier_3_count":           sum(1 for e in conv_events
                                            if (e.get("detail") or {}).get("new_tier") == 3),
            "sample_convergence":     [
                {
                    "ts":         e.get("ts", "")[:19],
                    "ticker":     (e.get("detail") or {}).get("ticker"),
                    "new_tier":   (e.get("detail") or {}).get("new_tier"),
                    "n_systems":  (e.get("detail") or {}).get("n_systems"),
                    "score":      (e.get("detail") or {}).get("score"),
                    "systems":    (e.get("detail") or {}).get("systems", [])[:4],
                    "alpha_compass_invoked": next(
                        (i.get("ok") for i in
                         ((e.get("route") or {}).get("invokes") or [])
                         if i.get("fn") == "justhodl-alpha-compass"),
                        None,
                    ),
                    "telegram_sent": (e.get("route") or {}).get("notify"),
                }
                for e in conv_events[:8]
            ],
        }
    except Exception as e:
        out["audit_err"] = str(e)[:200]
    
    # 6. Check coordinator logs for routing confirmation
    print("[1034] reading coordinator logs…")
    out["coordinator_logs"] = []
    try:
        lg = "/aws/lambda/justhodl-event-coordinator"
        streams = logs.describe_log_streams(
            logGroupName=lg, orderBy="LastEventTime", descending=True, limit=2,
        ).get("logStreams", [])
        for stream in streams[:2]:
            ev = logs.get_log_events(
                logGroupName=lg, logStreamName=stream["logStreamName"],
                limit=50, startFromHead=False,
            )
            for e in ev.get("events") or []:
                msg = e.get("message", "")
                if "convergence" in msg.lower() or "[coordinator]" in msg:
                    out["coordinator_logs"].append({
                        "ts": datetime.fromtimestamp(e["timestamp"]/1000,
                                                       tz=timezone.utc).isoformat(),
                        "msg": msg.strip()[:200],
                    })
        out["coordinator_logs"] = out["coordinator_logs"][-10:]
    except Exception as e:
        out["coordinator_logs_err"] = str(e)[:200]
    
    # 7. Verify .prev was written by latest run
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="data/master-ranker.json.prev")
        prev = json.loads(obj["Body"].read().decode())
        out["new_prev"] = {
            "as_of":      prev.get("as_of"),
            "n_tickers":  len(prev.get("top_tickers") or []),
        }
    except Exception as e:
        out["new_prev_err"] = str(e)[:200]
    
    # 8. Read master-ranker recent log
    try:
        lg = "/aws/lambda/justhodl-master-ranker"
        streams = logs.describe_log_streams(
            logGroupName=lg, orderBy="LastEventTime", descending=True, limit=1,
        ).get("logStreams", [])
        if streams:
            ev = logs.get_log_events(
                logGroupName=lg, logStreamName=streams[0]["logStreamName"],
                limit=30, startFromHead=False,
            )
            out["master_ranker_log"] = [
                e["message"].strip()[:200]
                for e in (ev.get("events") or [])
                if "master-ranker" in e["message"] or "tier" in e["message"].lower()
            ][-15:]
    except Exception as e:
        out["master_ranker_log_err"] = str(e)[:200]
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
