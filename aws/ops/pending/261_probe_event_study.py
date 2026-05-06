#!/usr/bin/env python3
"""Step 261 — Probe justhodl-event-study to see if it's running and producing data."""
import json, os, boto3
from datetime import datetime, timezone

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
KEY = "data/event-study.json"
LAMBDA_NAME = "justhodl-event-study"
REPORT_PATH = "aws/ops/reports/261_event_study_probe.json"

def main():
    s3 = boto3.client("s3", region_name=REGION)
    lam = boto3.client("lambda", region_name=REGION)
    eb = boto3.client("events", region_name=REGION)

    out = {"probed_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}

    # 1. Lambda config
    try:
        cfg = lam.get_function(FunctionName=LAMBDA_NAME)
        out["lambda"] = {
            "exists": True,
            "last_modified": cfg["Configuration"].get("LastModified"),
            "runtime": cfg["Configuration"].get("Runtime"),
            "memory_mb": cfg["Configuration"].get("MemorySize"),
            "timeout_s": cfg["Configuration"].get("Timeout"),
        }
    except Exception as e:
        out["lambda"] = {"exists": False, "err": str(e)[:200]}

    # 2. Is there an EB rule pointing at it?
    try:
        rules = eb.list_rule_names_by_target(TargetArn=f"arn:aws:lambda:{REGION}:857687956942:function:{LAMBDA_NAME}")
        out["eb_rules"] = rules.get("RuleNames", [])
    except Exception as e:
        out["eb_rules_err"] = str(e)[:200]

    # 3. Output state
    try:
        head = s3.head_object(Bucket=BUCKET, Key=KEY)
        out["output"] = {
            "exists": True,
            "size_bytes": head.get("ContentLength"),
            "last_modified": head["LastModified"].isoformat(),
        }
        # Read content shape
        body = json.loads(s3.get_object(Bucket=BUCKET, Key=KEY)["Body"].read())
        out["output"]["v"] = body.get("v")
        out["output"]["generated_at"] = body.get("generated_at")
        out["output"]["n_event_types"] = len(body.get("events") or {})
        # Sample: first event type's stats
        events = body.get("events") or {}
        out["output"]["event_keys"] = sorted(events.keys())
        sample = {}
        for k, v in list(events.items())[:3]:
            sample[k] = {
                "n_occurrences": len(v.get("occurrences") or []),
                "currently_active": v.get("currently_active"),
                "summary": v.get("summary"),
            }
        out["output"]["sample"] = sample
    except Exception as e:
        out["output"] = {"exists": False, "err": str(e)[:200]}

    # 4. If output is missing/stale, invoke Lambda
    invoke_needed = False
    if "output" not in out or not out["output"].get("exists"):
        invoke_needed = True
    else:
        last = out["output"]["last_modified"]
        from datetime import datetime as _dt
        try:
            age_h = (_dt.now(timezone.utc) - _dt.fromisoformat(last.replace("Z","+00:00"))).total_seconds() / 3600
            out["output"]["age_hours"] = round(age_h, 1)
            if age_h > 48:
                invoke_needed = True
        except Exception:
            invoke_needed = True

    if invoke_needed and out["lambda"].get("exists"):
        print(f"[261] invoking {LAMBDA_NAME} to produce fresh output…")
        try:
            r = lam.invoke(FunctionName=LAMBDA_NAME, InvocationType="RequestResponse", Payload=b"{}")
            payload = json.loads(r["Payload"].read())
            out["fresh_invoke"] = {
                "status": r.get("StatusCode"),
                "func_err": r.get("FunctionError"),
                "payload": payload,
            }
        except Exception as e:
            out["fresh_invoke"] = {"err": str(e)[:200]}

    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    with open(REPORT_PATH, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str)[:3000])
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
