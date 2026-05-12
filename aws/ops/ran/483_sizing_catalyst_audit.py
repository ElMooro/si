#!/usr/bin/env python3
"""Step 483 — Audit which #11/#12 candidate Lambdas are actually deployed in AWS
and which output files exist in S3."""
import io, json, os, time as _time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/483_sizing_catalyst_audit.json"
NAME = "justhodl-tmp-483"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, boto3
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")
events = boto3.client("events", region_name="us-east-1")

def lambda_handler(event, context):
    out = {"lambdas": {}, "sidecars": {}, "schedules": {}}
    candidates = [
        "justhodl-risk-sizer", "justhodl-position-sizer-v2", "justhodl-catalyst-calendar",
        "justhodl-earnings-tracker", "justhodl-earnings-pead", "justhodl-earnings-sentiment",
        "justhodl-earnings-whisper", "justhodl-event-study", "justhodl-sector-earnings-diffusion",
    ]
    for name in candidates:
        try:
            cfg = lam.get_function_configuration(FunctionName=name)
            out["lambdas"][name] = {
                "deployed": True,
                "last_modified": cfg["LastModified"][:19],
                "size": cfg["CodeSize"],
                "memory": cfg["MemorySize"],
                "timeout": cfg["Timeout"],
                "env_keys": list((cfg.get("Environment") or {}).get("Variables", {}).keys()),
            }
        except Exception:
            out["lambdas"][name] = {"deployed": False}
    
    sidecar_keys = [
        "portfolio/sizer-v2.json", "portfolio/risk-sizer.json", "portfolio/risk-sized.json",
        "data/catalyst-calendar.json", "data/calendar.json",
        "data/earnings-tracker.json", "data/earnings-sentiment.json",
        "data/earnings-pead.json", "data/earnings-whisper.json",
        "signals/event-study.json", "data/earnings-diffusion.json",
    ]
    for k in sidecar_keys:
        try:
            obj = s3.head_object(Bucket="justhodl-dashboard-live", Key=k)
            out["sidecars"][k] = {
                "exists": True,
                "size_kb": round(obj["ContentLength"] / 1024, 1),
                "last_modified": obj["LastModified"].isoformat()[:19],
            }
        except Exception:
            out["sidecars"][k] = {"exists": False}
    
    # Sample content from deployed sidecars
    out["samples"] = {}
    for k in ["data/catalyst-calendar.json", "data/earnings-tracker.json",
              "portfolio/sizer-v2.json", "portfolio/risk-sizer.json"]:
        if out["sidecars"].get(k, {}).get("exists"):
            try:
                obj = s3.get_object(Bucket="justhodl-dashboard-live", Key=k)
                p = json.loads(obj["Body"].read())
                if isinstance(p, dict):
                    out["samples"][k] = {
                        "top_keys": list(p.keys())[:15],
                        "generated_at": p.get("generated_at") or p.get("as_of"),
                    }
                    # Type-specific extracts
                    if "events" in p:
                        out["samples"][k]["n_events"] = len(p["events"])
                        out["samples"][k]["by_type"] = p.get("by_type")
                        out["samples"][k]["sample_events"] = p["events"][:3]
                    if "earnings" in p:
                        out["samples"][k]["n_earnings"] = len(p["earnings"]) if isinstance(p["earnings"], list) else None
                    if "recommendations" in p:
                        out["samples"][k]["n_recs"] = len(p["recommendations"])
            except Exception as e:
                out["samples"][k] = {"err": str(e)[:200]}
    
    # Check EventBridge rules
    for name in ["justhodl-risk-sizer-daily", "justhodl-position-sizer-v2-daily",
                  "justhodl-catalyst-calendar-daily", "justhodl-earnings-tracker-hourly"]:
        try:
            rule = events.describe_rule(Name=name)
            out["schedules"][name] = {
                "exists": True,
                "schedule": rule.get("ScheduleExpression"),
                "state": rule.get("State"),
            }
        except Exception:
            out["schedules"][name] = {"exists": False}
    
    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", DIAG)
    zb = buf.getvalue()
    try:
        lam.create_function(FunctionName=NAME, Runtime="python3.12",
                            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
                            MemorySize=512, Timeout=300, Code={"ZipFile": zb})
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except Exception:
        lam.update_function_code(FunctionName=NAME, ZipFile=zb)
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
    _time.sleep(2)
    resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    body = resp["Payload"].read().decode("utf-8")
    try:
        parsed = json.loads(body)
        out["test"] = json.loads(parsed["body"]) if "body" in parsed and parsed["body"] else parsed
    except Exception:
        out["raw"] = body[:30000]
    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
