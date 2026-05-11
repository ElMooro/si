#!/usr/bin/env python3
"""Step 441 — Focused COT endpoint probe. Test:
  - commitment-of-traders-report?symbol=ES (raw weekly reports)
  - commitment-of-traders-list (directory of symbols)
  - commitment-of-traders-analysis?symbol=ES (re-verify dates)
"""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/441_cot_probe_v2.json"
NAME = "justhodl-tmp-441"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, urllib.request
FMP = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
BASE = "https://financialmodelingprep.com"

CALLS = [
    "/stable/commitment-of-traders-list",
    "/stable/commitment-of-traders-report?symbol=ES",
    "/stable/commitment-of-traders-report?symbol=GC",
    "/stable/commitment-of-traders-report?symbol=ZC",
    "/stable/commitment-of-traders-report?symbol=CL",
    "/stable/commitment-of-traders-analysis?symbol=ES",
    "/stable/commitment-of-traders-report?from=2026-04-01&to=2026-05-15",
]

def fetch(path):
    url = BASE + path + ("&" if "?" in path else "?") + "apikey=" + FMP
    try:
        r = urllib.request.urlopen(
            urllib.request.Request(url, headers={"User-Agent":"JH/1.0"}),
            timeout=15)
        body = r.read().decode("utf-8")
        parsed = json.loads(body)
        return r.status, parsed
    except urllib.error.HTTPError as e:
        return e.code, None
    except Exception as e:
        return None, str(e)[:100]

def lambda_handler(event, context):
    out = {}
    for path in CALLS:
        status, data = fetch(path)
        rec = {"status": status}
        if isinstance(data, list):
            rec["n"] = len(data)
            if data:
                if isinstance(data[0], dict):
                    rec["keys"] = list(data[0].keys())[:20]
                    rec["sample"] = data[0]
                    # All dates
                    dates = sorted(set(d.get("date","")[:10] for d in data if isinstance(d, dict) and d.get("date")), reverse=True)[:8]
                    rec["recent_dates"] = dates
                    # All distinct symbols
                    syms = sorted(set(d.get("symbol","") for d in data if isinstance(d, dict) and d.get("symbol")))[:20]
                    rec["symbols"] = syms
                else:
                    rec["sample"] = data[0]
        elif isinstance(data, dict):
            rec["keys"] = list(data.keys())[:15]
            rec["data"] = data
        elif isinstance(data, str):
            rec["err"] = data
        out[path] = rec
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
                            MemorySize=256, Timeout=120, Code={"ZipFile": zb})
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except Exception:
        lam.update_function_code(FunctionName=NAME, ZipFile=zb)
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
    time.sleep(2)
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
