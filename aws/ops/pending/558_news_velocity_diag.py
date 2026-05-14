#!/usr/bin/env python3
"""558 — Diagnose news-velocity returning UNKNOWN/no-data. Inspect sidecar,
fetch CloudWatch logs from last few runs, capture errors."""
import io, json, os, time as _time
from datetime import datetime, timezone, timedelta
import boto3

REPORT = "aws/ops/reports/558_news_velocity_diag.json"

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")
logs = boto3.client("logs", region_name="us-east-1")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # 1. Sidecar contents
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/news-velocity.json")
        body = obj["Body"].read()
        p = json.loads(body)
        out["sidecar"] = {
            "size_kb": round(len(body) / 1024, 2),
            "modified": obj["LastModified"].isoformat()[:19],
            "top_level": list(p.keys()),
            "full": p,
        }
    except Exception as e:
        out["sidecar_err"] = str(e)[:200]

    # 2. Lambda config
    try:
        l = lam.get_function(FunctionName="justhodl-news-velocity")
        cfg = l["Configuration"]
        out["lambda"] = {
            "state": cfg.get("State"),
            "memory": cfg.get("MemorySize"),
            "timeout": cfg.get("Timeout"),
            "last_modified": cfg.get("LastModified"),
            "runtime": cfg.get("Runtime"),
            "handler": cfg.get("Handler"),
            "env_keys": list(((cfg.get("Environment") or {}).get("Variables") or {}).keys()),
        }
    except Exception as e:
        out["lambda_err"] = str(e)[:200]

    # 3. Recent CloudWatch logs
    try:
        log_group = "/aws/lambda/justhodl-news-velocity"
        # Last 30 minutes
        start_ms = int((datetime.now(timezone.utc) - timedelta(minutes=30)).timestamp() * 1000)
        events = logs.filter_log_events(
            logGroupName=log_group,
            startTime=start_ms,
            limit=200,
        )
        log_lines = []
        for e in events.get("events", []):
            msg = e.get("message", "").rstrip()
            if msg and not msg.startswith("START Request") and not msg.startswith("END Request") and not msg.startswith("REPORT"):
                log_lines.append(msg[:300])
        out["recent_logs_n"] = len(log_lines)
        out["recent_logs"] = log_lines[-30:]
    except Exception as e:
        out["logs_err"] = str(e)[:200]

    # 4. Force-invoke to see fresh error
    try:
        resp = lam.invoke(FunctionName="justhodl-news-velocity",
                           InvocationType="RequestResponse", LogType="Tail",
                           Payload=b"{}")
        out["force_invoke_status"] = resp.get("StatusCode")
        out["force_invoke_fn_error"] = resp.get("FunctionError")
        body = resp["Payload"].read().decode("utf-8")
        out["force_invoke_response"] = body[:1500]
        if resp.get("LogResult"):
            import base64
            out["force_invoke_log_tail"] = base64.b64decode(resp["LogResult"]).decode("utf-8", "replace")[-3500:]
    except Exception as e:
        out["force_invoke_err"] = str(e)[:200]

    # 5. Check sidecar AFTER force invoke
    _time.sleep(2)
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/news-velocity.json")
        body = obj["Body"].read()
        p = json.loads(body)
        out["sidecar_after"] = {
            "size_kb": round(len(body) / 1024, 2),
            "modified": obj["LastModified"].isoformat()[:19],
            "composite_regime": p.get("composite_regime"),
            "composite_signal": p.get("composite_signal"),
            "n_with_data": p.get("n_with_data"),
            "n_surge": p.get("n_surge"),
            "n_elevated": p.get("n_elevated"),
        }
    except Exception as e:
        out["sidecar_after_err"] = str(e)[:200]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
