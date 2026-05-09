#!/usr/bin/env python3
"""Step 379 — Verify bonds.html now has correct paths."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/379_bonds_fix_verify.json"
NAME = "justhodl-tmp-bonds-fix-verify"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG_CODE = '''
import json, urllib.request

def fetch(url):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "verify"})
        with urllib.request.urlopen(req, timeout=15) as r:
            return r.status, r.read().decode("utf-8", errors="ignore")
    except Exception as e:
        return None, f"ERROR: {type(e).__name__}: {e}"

def lambda_handler(event, context):
    s, body = fetch("https://justhodl.ai/bonds.html?nocache=" + str(__import__("time").time()))
    out = {"status": s, "size": len(body) if isinstance(body, str) else None}
    if isinstance(body, str):
        out["fred_data_count"] = body.count("fred_data")
        out["fred_correct_count"] = body.count("?.fred?.")
        out["has_correct_gv"] = "report?.fred?.[group]?.[sid]?.current" in body
        out["has_history_fallback"] = "Array.isArray(hist)" in body
        # Did the OLD bug get cached?
        out["still_has_old_bug"] = "fred_data?.[group]?.[sid]?.current" in body
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
