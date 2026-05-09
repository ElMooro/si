#!/usr/bin/env python3
"""Step 378b — Inspect actual report.json fred sub-structure."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/378b_fred_struct.json"
NAME = "justhodl-tmp-fred-struct"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG_CODE = '''
import json
import boto3
s3 = boto3.client("s3", region_name="us-east-1")

def lambda_handler(event, context):
    out = {}
    obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/report.json")
    body = obj["Body"].read()
    d = json.loads(body)

    out["top_level"] = list(d.keys())
    fred = d.get("fred") or {}
    out["fred_keys"] = list(fred.keys())
    out["fred_data_present"] = "fred_data" in d

    # Sample a few subgroups to see shape
    if "treasury" in fred:
        t = fred["treasury"]
        out["fred.treasury.keys"] = list(t.keys())[:20]
        # Pick one ticker to inspect
        if "DGS10" in t:
            out["fred.treasury.DGS10"] = t["DGS10"]
        if "DGS2" in t:
            out["fred.treasury.DGS2"] = t["DGS2"]
        if "T10Y2Y" in t:
            out["fred.treasury.T10Y2Y"] = t["T10Y2Y"]

    # Check other commonly-needed groups
    for g in ["credit", "real_rates", "breakevens", "vol", "fx", "stress"]:
        if g in fred:
            out[f"fred.{g}.keys"] = list(fred[g].keys())[:15]

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
