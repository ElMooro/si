#!/usr/bin/env python3
"""Step 477 — Verify justhodl-anomaly-detector deployed + invoke + show
all 5 detector results so we can see what fires today."""
import io, json, os, time as _time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/477_anomaly_detector_verify.json"
NAME = "justhodl-tmp-477"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json
import boto3
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")

def lambda_handler(event, context):
    out = {}
    try:
        cfg = lam.get_function_configuration(FunctionName="justhodl-anomaly-detector")
        env_keys = list((cfg.get("Environment") or {}).get("Variables", {}).keys())
        out["lambda"] = {
            "exists": True,
            "last_modified": cfg["LastModified"][:19],
            "env_keys": env_keys,
            "memory": cfg["MemorySize"],
            "timeout": cfg["Timeout"],
            "code_size": cfg["CodeSize"],
        }
    except Exception as e:
        out["lambda_err"] = str(e)[:300]
        return {"statusCode": 200, "body": json.dumps(out, default=str)}

    # Force-invoke
    try:
        resp = lam.invoke(FunctionName="justhodl-anomaly-detector",
                            InvocationType="RequestResponse", Payload=b"{}")
        body = resp["Payload"].read().decode("utf-8")
        parsed = json.loads(body)
        out["invoke"] = json.loads(parsed["body"]) if parsed.get("body") else parsed
    except Exception as e:
        out["invoke_err"] = str(e)[:400]

    # Read sidecar
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="signals/anomalies.json")
        body = obj["Body"].read()
        p = json.loads(body)
        out["sidecar"] = {
            "size_kb": round(len(body)/1024, 1),
            "generated_at": p.get("generated_at"),
            "elapsed_seconds": p.get("elapsed_seconds"),
            "anomalies_count": p.get("anomalies_count"),
            "high_or_extreme_count": p.get("high_or_extreme_count"),
            "by_severity": p.get("by_severity"),
            "categories_checked": p.get("categories_checked"),
            "categories_with_anomalies": p.get("categories_with_anomalies"),
            "detector_timings_s": p.get("detector_timings_s"),
            "alerts_sent": p.get("alerts_sent"),
            "alerts_skipped_dedupe": p.get("alerts_skipped_dedupe"),
            "actions": p.get("actions"),
            "anomalies": p.get("anomalies"),
        }
    except Exception as e:
        out["sidecar_err"] = str(e)[:300]

    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    print("Waiting 130s for deploy...")
    _time.sleep(130)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", DIAG)
    zb = buf.getvalue()
    try:
        lam.create_function(FunctionName=NAME, Runtime="python3.12",
                            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
                            MemorySize=512, Timeout=400, Code={"ZipFile": zb})
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
