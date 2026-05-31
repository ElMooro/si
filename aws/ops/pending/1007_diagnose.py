#!/usr/bin/env python3
"""Step 1007 — Diagnose magdist + verify miss-detector fix.

1006 didn't fire (workflow trigger collision). This script runs the same
diagnostic + invokes miss-detector to verify the prev_date bug fix landed.
"""
import io, json, os, time, zipfile, pathlib
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/1007_diagnose.json"
REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
dynamodb = boto3.resource("dynamodb", region_name=REGION)


def sample_ddb():
    from boto3.dynamodb.conditions import Attr
    table = dynamodb.Table("justhodl-signals")
    out = {}
    # Sample 20 items
    resp = table.scan(Limit=20)
    items = resp.get("Items", [])
    out["scan_20_returned"] = len(items)
    out["scanned_count"]    = resp.get("ScannedCount", 0)
    out["samples"] = []
    statuses = {}
    for it in items:
        s = it.get("status", "?")
        statuses[s] = statuses.get(s, 0) + 1
        if len(out["samples"]) < 5:
            out["samples"].append({
                "signal_id":  str(it.get("signal_id", ""))[:30],
                "signal_type": str(it.get("signal_type", ""))[:60],
                "status":      it.get("status"),
                "horizon_days_primary": it.get("horizon_days_primary"),
                "check_windows":         it.get("check_windows"),
                "predicted_direction":   it.get("predicted_direction"),
                "supporting_signals":    it.get("supporting_signals"),
                "outcomes_keys":         list((it.get("outcomes") or {}).keys()),
                "outcomes_value":        it.get("outcomes"),
                "schema_version":        it.get("schema_version"),
            })
    out["status_counts"] = statuses
    # Filter for checked
    try:
        r2 = table.scan(FilterExpression=Attr("status").eq("checked"), Limit=20)
        checked = r2.get("Items", [])
        out["checked_found"] = len(checked)
        out["checked_scanned"] = r2.get("ScannedCount", 0)
        if checked:
            ci = checked[0]
            out["checked_first"] = {
                "signal_type": ci.get("signal_type"),
                "horizon_days_primary": ci.get("horizon_days_primary"),
                "check_windows": ci.get("check_windows"),
                "outcomes": ci.get("outcomes"),
                "supporting_signals": ci.get("supporting_signals"),
            }
    except Exception as e:
        out["checked_err"] = str(e)[:200]
    return out


def reinvoke_miss():
    """Just invoke — code should have been updated by deploy-lambdas workflow."""
    r = lam.invoke(FunctionName="justhodl-miss-detector",
                   InvocationType="RequestResponse", Payload=b"{}")
    body = r["Payload"].read().decode("utf-8", errors="replace")
    out = {"fn_err": r.get("FunctionError")}
    try:
        p = json.loads(body)
        out["result"] = json.loads(p["body"]) if isinstance(p.get("body"), str) else p
    except Exception:
        out["raw"] = body[:600]
    
    # Also pull recent CloudWatch logs
    try:
        logs = boto3.client("logs", region_name=REGION)
        lg = "/aws/lambda/justhodl-miss-detector"
        streams = logs.describe_log_streams(
            logGroupName=lg, orderBy="LastEventTime",
            descending=True, limit=1).get("logStreams", [])
        if streams:
            evs = logs.get_log_events(
                logGroupName=lg, logStreamName=streams[0]["logStreamName"],
                limit=30, startFromHead=False).get("events", [])
            out["logs"] = [e["message"].strip() for e in evs[-15:]]
    except Exception as e:
        out["log_err"] = str(e)[:200]
    return out


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    print("[1007] sampling DDB…")
    out["ddb"] = sample_ddb()
    print("[1007] invoking miss-detector (post-fix)…")
    out["miss_invoke"] = reinvoke_miss()
    
    # S3 state
    out["s3"] = {}
    for k in ("data/magnitude-distributions.json", "data/miss-summary.json",
              "data/alpha-compass.json"):
        try:
            obj = s3.head_object(Bucket=BUCKET, Key=k)
            out["s3"][k] = {"size": obj["ContentLength"],
                            "modified": str(obj["LastModified"])}
        except Exception as e:
            out["s3"][k] = {"missing": str(e)[:80]}
    
    # Also read miss-summary content
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="data/miss-summary.json")
        out["miss_summary_content"] = json.loads(obj["Body"].read().decode())
    except Exception as e:
        out["miss_summary_err"] = str(e)[:200]
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(f"[1007] wrote {REPORT}")


if __name__ == "__main__":
    main()
