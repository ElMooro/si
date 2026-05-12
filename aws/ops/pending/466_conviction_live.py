#!/usr/bin/env python3
"""Step 466 — Rebuild sidecar with issuer_name + verify /conviction/ live.
"""
import io, json, os, time as _time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/466_conviction_live.json"
NAME = "justhodl-tmp-466"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, urllib.request
import boto3
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")

def lambda_handler(event, context):
    out = {}
    # Force rebuild sidecar with issuer_name
    resp = lam.invoke(FunctionName="justhodl-smart-money-holdings",
                        InvocationType="RequestResponse", Payload=b"{}")
    body = resp["Payload"].read().decode("utf-8")
    parsed = json.loads(body)
    out["invoke"] = json.loads(parsed["body"]) if parsed.get("body") else parsed

    # Read sidecar — confirm issuer_name is present
    obj = s3.get_object(Bucket="justhodl-dashboard-live",
                          Key="screener/smart-money-holdings.json")
    body = obj["Body"].read()
    p = json.loads(body)
    out["sidecar"] = {
        "size_mb": round(len(body)/1024/1024, 2),
        "n_symbols": p.get("n_symbols"),
        "n_funds_scanned": p.get("n_funds_scanned"),
        "generated_at": p.get("generated_at"),
    }
    # Sample 5 well-known stocks — check issuer_name populated
    holdings = p.get("holdings") or {}
    samples = {}
    for sym in ["AAPL", "MSFT", "NVDA", "JOE", "IEP", "BAC", "TSLA", "OXY", "AXP", "META"]:
        entry = holdings.get(sym)
        if isinstance(entry, dict):
            samples[sym] = {
                "issuer_name": entry.get("issuer_name"),
                "max_pct": entry.get("max_pct_of_fund"),
                "n_high": entry.get("n_high_conviction"),
                "n_flag": entry.get("n_flagship"),
                "n_holders": len(entry.get("holders") or []),
            }
    out["samples"] = samples

    # Check /conviction/ page reachable
    try:
        r = urllib.request.urlopen("https://justhodl.ai/conviction/", timeout=10)
        out["conviction_page"] = {"status": r.status, "size_kb": round(len(r.read())/1024, 1)}
    except Exception as e:
        out["conviction_page_err"] = str(e)[:200]

    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    _time.sleep(90)  # wait for Lambda code deploy + page deploy
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
        out["raw"] = body[:10000]
    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
