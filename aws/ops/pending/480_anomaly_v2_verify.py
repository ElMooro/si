#!/usr/bin/env python3
"""Step 480 — Verify anomaly-detector v2.0 deployed + invoke + show comprehensive
output across all 22 detectors and macro stress score composite."""
import io, json, os, time as _time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/480_anomaly_v2_verify.json"
NAME = "justhodl-tmp-480"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, boto3
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")

def lambda_handler(event, context):
    out = {}
    try:
        cfg = lam.get_function_configuration(FunctionName="justhodl-anomaly-detector")
        out["lambda"] = {
            "exists": True,
            "last_modified": cfg["LastModified"][:19],
            "env_keys": list((cfg.get("Environment") or {}).get("Variables", {}).keys()),
            "memory": cfg["MemorySize"], "timeout": cfg["Timeout"],
            "code_size": cfg["CodeSize"],
        }
    except Exception as e:
        out["lambda_err"] = str(e)[:300]
        return {"statusCode": 200, "body": json.dumps(out, default=str)}

    # Force-invoke (300s timeout in the Lambda, but use 600 for safety from this caller)
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
            "version": p.get("version"),
            "generated_at": p.get("generated_at"),
            "elapsed_seconds": p.get("elapsed_seconds"),
            "macro_stress_score": p.get("macro_stress_score"),
            "stress_interpretation": p.get("stress_interpretation"),
            "stress_contributions": p.get("stress_contributions"),
            "anomalies_count": p.get("anomalies_count"),
            "high_or_extreme_count": p.get("high_or_extreme_count"),
            "by_severity": p.get("by_severity"),
            "detectors_count": p.get("detectors_count"),
            "categories": p.get("categories"),
            "detector_timings_s": p.get("detector_timings_s"),
            "alerts_sent": p.get("alerts_sent"),
            "anomalies": (p.get("anomalies") or [])[:15],
            "metrics_by_category_keys": {c: list(m.keys()) for c, m in (p.get("metrics_by_category") or {}).items()},
            "metric_count": len(p.get("metrics") or {}),
            "sample_metrics": dict(list((p.get("metrics") or {}).items())[:20]),
        }
    except Exception as e:
        out["sidecar_err"] = str(e)[:300]

    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    print("Waiting 150s for v2 deploy...")
    _time.sleep(150)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", DIAG)
    zb = buf.getvalue()
    try:
        lam.create_function(FunctionName=NAME, Runtime="python3.12",
                            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
                            MemorySize=512, Timeout=600, Code={"ZipFile": zb})
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
