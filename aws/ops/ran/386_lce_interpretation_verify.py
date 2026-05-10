#!/usr/bin/env python3
"""Step 386 — Fetch + show the full LCE interpretation block."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/386_lce_interpretation_verify.json"
NAME = "justhodl-tmp-lce-interp"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG_CODE = '''
import json
import boto3
s3 = boto3.client("s3", region_name="us-east-1")

def lambda_handler(event, context):
    obj = s3.get_object(Bucket="justhodl-dashboard-live",
                          Key="data/liquidity-credit-engine.json")
    d = json.loads(obj["Body"].read())
    interp = d.get("interpretation", {})
    out = {
        "regime": d.get("regime"),
        "composite": d.get("composite"),
        "n_series": len(d.get("series", {})),
        "interp_present": bool(interp),
        "interp_error": interp.get("error"),
        "posture": interp.get("overall_posture"),
        "confidence": interp.get("confidence"),
        "decisive_call": interp.get("decisive_call"),
        "pillars": interp.get("pillars"),
        "cross_asset": interp.get("cross_asset"),
        "target_allocation": interp.get("target_allocation"),
        "avoid": interp.get("avoid"),
        "hedges": interp.get("hedges"),
        "key_risks": interp.get("key_risks"),
    }
    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", DIAG_CODE)
    zb = buf.getvalue()
    try:
        lam.create_function(FunctionName=NAME, Runtime="python3.12",
                            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
                            MemorySize=256, Timeout=180, Code={"ZipFile": zb})
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except Exception:
        lam.update_function_code(FunctionName=NAME, ZipFile=zb)
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
    time.sleep(2)
    resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    body = resp["Payload"].read().decode("utf-8")
    try:
        parsed = json.loads(body)
        out["test"] = json.loads(parsed["body"]) if "body" in parsed else parsed
    except Exception:
        out["raw"] = body[:5000]
    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
