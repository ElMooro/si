#!/usr/bin/env python3
"""Step 380b — Inspect liquidity-credit-engine output."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/380b_lce_inspect.json"
NAME = "justhodl-tmp-lce-inspect"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG_CODE = '''
import json
import boto3
s3 = boto3.client("s3", region_name="us-east-1")
def lambda_handler(event, context):
    obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/liquidity-credit-engine.json")
    d = json.loads(obj["Body"].read())
    out = {
        "regime": d.get("regime"),
        "composite": d.get("composite"),
        "by_category": d.get("by_category"),
        "n_transitions": len(d.get("transitions", [])),
        "transitions": d.get("transitions", []),
        "series_summary": {},
    }
    for sid, info in d.get("series", {}).items():
        if info.get("available"):
            out["series_summary"][sid] = {
                "label": info.get("_label"),
                "category": info.get("_category"),
                "latest": info.get("latest_value"),
                "wow_pct": info.get("wow_pct"),
                "mom_pct": info.get("mom_pct"),
                "yoy_pct": info.get("yoy_pct"),
                "z_1y": info.get("z_1y"),
                "signal": info.get("signal"),
                "reason": info.get("signal_reason"),
            }
        else:
            out["series_summary"][sid] = {"available": False, "error": info.get("error")}
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
                            MemorySize=256, Timeout=60, Code={"ZipFile": zb})
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
    try:
        lam.delete_function(FunctionName=NAME)
    except Exception:
        pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
