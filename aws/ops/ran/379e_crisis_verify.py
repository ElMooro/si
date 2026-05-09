#!/usr/bin/env python3
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3
REPORT = "aws/ops/reports/379e_crisis_verify.json"
NAME = "justhodl-tmp-crisis-verify"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")
DIAG = '''
import json, urllib.request
def lambda_handler(event, context):
    req = urllib.request.Request("https://justhodl.ai/crisis.html", headers={"User-Agent":"v"})
    with urllib.request.urlopen(req, timeout=15) as r:
        body = r.read().decode("utf-8")
    out = {
        "status": r.status, "size": len(body),
        "has_renderYieldCurve_def": "function renderYieldCurve" in body,
        "has_renderYieldCurve_call": "renderYieldCurve(data.yield_curve)" in body,
        "has_yc_grid_id": 'id="yc-grid"' in body,
    }
    return {"statusCode": 200, "body": json.dumps(out)}
'''
buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
    zf.writestr("lambda_function.py", DIAG)
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
out = {"started": datetime.now(timezone.utc).isoformat()}
try:
    parsed = json.loads(body)
    out["test"] = json.loads(parsed["body"]) if "body" in parsed else parsed
except Exception:
    out["raw"] = body[:5000]
try: lam.delete_function(FunctionName=NAME)
except Exception: pass
out["finished"] = datetime.now(timezone.utc).isoformat()
os.makedirs(os.path.dirname(REPORT), exist_ok=True)
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
