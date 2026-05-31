#!/usr/bin/env python3
"""Step 1006 — Sample real DDB signals + rebuild miss-detector after bug fix.

Two goals:
1. Look at 10 actual signals from DDB justhodl-signals to confirm schema —
   why did magnitude-distributions find 0 checked outcomes?
2. Re-deploy miss-detector with the prev_date bug fix and re-invoke.
"""
import io, json, os, time, zipfile, pathlib
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/1006_diagnose.json"
REGION = "us-east-1"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
ACCOUNT_ID = "857687956942"
BUCKET = "justhodl-dashboard-live"

lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
dynamodb = boto3.resource("dynamodb", region_name=REGION)


def sample_ddb():
    """Scan a few items from each status group + look at outcomes structure."""
    table = dynamodb.Table("justhodl-signals")
    out = {}
    
    # Get total count rough
    try:
        meta = table.scan(Select="COUNT", Limit=1)
        out["scan_one_returned"] = meta.get("Count")
        out["scanned_count"] = meta.get("ScannedCount")
    except Exception as e:
        out["count_err"] = str(e)[:200]
    
    # Sample 5 items from latest scan
    try:
        from boto3.dynamodb.conditions import Attr
        resp = table.scan(Limit=20)
        items = resp.get("Items", [])
        out["scan_20_returned"] = len(items)
        # Show structure of first 5
        out["samples"] = []
        for it in items[:5]:
            sample = {
                "signal_id": str(it.get("signal_id", ""))[:30],
                "signal_type": str(it.get("signal_type", ""))[:60],
                "status": it.get("status"),
                "horizon_days_primary": it.get("horizon_days_primary"),
                "check_windows": it.get("check_windows"),
                "predicted_direction": it.get("predicted_direction"),
                "supporting_signals": it.get("supporting_signals"),
                "outcomes_keys": list((it.get("outcomes") or {}).keys()),
                "outcomes_sample": it.get("outcomes"),
                "measure_against": it.get("measure_against"),
                "schema_version": it.get("schema_version"),
            }
            out["samples"].append(sample)
        
        # Count statuses
        statuses = {}
        for it in items:
            s = it.get("status", "?")
            statuses[s] = statuses.get(s, 0) + 1
        out["status_in_sample_20"] = statuses
        
        # Try filtering for checked
        resp2 = table.scan(
            FilterExpression=Attr("status").eq("checked"),
            Limit=10
        )
        checked = resp2.get("Items", [])
        out["checked_returned"] = len(checked)
        out["checked_scanned"] = resp2.get("ScannedCount")
        if checked:
            ci = checked[0]
            out["checked_sample"] = {
                "signal_id":  str(ci.get("signal_id",""))[:30],
                "signal_type": ci.get("signal_type"),
                "horizon_days_primary": ci.get("horizon_days_primary"),
                "outcomes": ci.get("outcomes"),
                "supporting_signals": ci.get("supporting_signals"),
            }
    except Exception as e:
        out["scan_err"] = f"{type(e).__name__}: {str(e)[:300]}"
    
    return out


def redeploy_miss_detector():
    """Read latest miss-detector source from disk + push to existing Lambda."""
    name = "justhodl-miss-detector"
    src_dir = pathlib.Path(f"aws/lambdas/{name}/source")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for f in src_dir.glob("*.py"):
            zf.writestr(f.name, f.read_bytes())
    zb = buf.getvalue()
    lam.update_function_code(FunctionName=name, ZipFile=zb, Publish=False)
    lam.get_waiter("function_updated").wait(FunctionName=name)
    time.sleep(2)
    # Invoke
    r = lam.invoke(FunctionName=name, InvocationType="RequestResponse", Payload=b"{}")
    body = r["Payload"].read().decode("utf-8", errors="replace")
    out = {"zip_size": len(zb), "fn_err": r.get("FunctionError")}
    try:
        p = json.loads(body)
        out["result"] = json.loads(p["body"]) if isinstance(p.get("body"), str) else p
    except Exception:
        out["raw"] = body[:1500]
    
    # Also check CloudWatch logs for prints
    try:
        logs = boto3.client("logs", region_name=REGION)
        lg_name = f"/aws/lambda/{name}"
        streams = logs.describe_log_streams(
            logGroupName=lg_name,
            orderBy="LastEventTime",
            descending=True,
            limit=1,
        ).get("logStreams", [])
        if streams:
            stream = streams[0]["logStreamName"]
            ev = logs.get_log_events(
                logGroupName=lg_name,
                logStreamName=stream,
                limit=30,
                startFromHead=False,
            ).get("events", [])
            out["recent_logs"] = [e["message"].strip() for e in ev[-20:]
                                    if "miss" in e["message"].lower() or "[" in e["message"]]
    except Exception as e:
        out["log_err"] = str(e)[:200]
    return out


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    print("[1006] sampling DDB signals…")
    out["ddb"] = sample_ddb()
    print("[1006] redeploying miss-detector…")
    out["miss_detector_redeploy"] = redeploy_miss_detector()
    
    # Re-check S3
    out["s3_after"] = {}
    for k in ("data/magnitude-distributions.json", "data/miss-summary.json",
              "data/alpha-compass.json"):
        try:
            obj = s3.head_object(Bucket=BUCKET, Key=k)
            out["s3_after"][k] = {"size": obj["ContentLength"],
                                  "modified": str(obj["LastModified"])}
        except Exception as e:
            out["s3_after"][k] = {"missing": str(e)[:80]}
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(f"[1006] wrote {REPORT}")


if __name__ == "__main__":
    main()
