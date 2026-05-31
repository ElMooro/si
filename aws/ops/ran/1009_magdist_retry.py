#!/usr/bin/env python3
"""Step 1009 — Retry magdist deploy (1008 hit ResourceConflictException)."""
import io, json, os, time, zipfile, pathlib
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/1009_magdist_retry.json"
REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    name = "justhodl-magnitude-distributions"
    
    # Build zip from latest source
    src_dir = pathlib.Path(f"aws/lambdas/{name}/source")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for f in src_dir.glob("*.py"):
            zf.writestr(f.name, f.read_bytes())
    zb = buf.getvalue()
    out["zip_size"] = len(zb)
    
    # Retry deploy with backoff
    for attempt in range(5):
        try:
            lam.update_function_code(FunctionName=name, ZipFile=zb, Publish=False)
            lam.get_waiter("function_updated").wait(FunctionName=name)
            out["deploy_attempt"] = attempt + 1
            out["deploy_ok"] = True
            break
        except Exception as e:
            err = str(e)
            if "ResourceConflictException" in err and attempt < 4:
                time.sleep(5 * (attempt + 1))
                continue
            out["deploy_err"] = f"{type(e).__name__}: {err[:300]}"
            break
    
    if not out.get("deploy_ok"):
        with open(REPORT, "w") as f:
            json.dump(out, f, indent=2, default=str)
        return
    
    # Invoke
    time.sleep(3)
    try:
        r = lam.invoke(FunctionName=name, InvocationType="RequestResponse", Payload=b"{}")
        body = r["Payload"].read().decode("utf-8", errors="replace")
        out["fn_err"] = r.get("FunctionError")
        try:
            p = json.loads(body)
            out["result"] = json.loads(p["body"]) if isinstance(p.get("body"), str) else p
        except Exception:
            out["raw"] = body[:1500]
    except Exception as e:
        out["invoke_err"] = str(e)[:300]
    
    # Pull CloudWatch logs to see what was scanned
    try:
        logs = boto3.client("logs", region_name=REGION)
        lg = f"/aws/lambda/{name}"
        streams = logs.describe_log_streams(
            logGroupName=lg, orderBy="LastEventTime",
            descending=True, limit=1).get("logStreams", [])
        if streams:
            evs = logs.get_log_events(
                logGroupName=lg, logStreamName=streams[0]["logStreamName"],
                limit=30, startFromHead=False).get("events", [])
            out["logs"] = [e["message"].strip()[:200] for e in evs[-15:]
                           if "magdist" in e["message"] or "scan" in e["message"].lower()
                              or "publish" in e["message"].lower()]
    except Exception as e:
        out["log_err"] = str(e)[:200]
    
    # Read output payload
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="data/magnitude-distributions.json")
        d = json.loads(obj["Body"].read().decode())
        out["payload"] = {
            "totals": d.get("totals"),
            "stacks_count": len(d.get("stacks", [])),
            "by_signal_count": len(d.get("by_signal", {})),
            "top_5_stacks": d.get("stacks", [])[:5],
        }
    except Exception as e:
        out["payload_err"] = str(e)[:200]
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
