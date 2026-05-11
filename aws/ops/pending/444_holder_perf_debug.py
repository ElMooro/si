#!/usr/bin/env python3
"""Step 444 — Debug why holder-performance-summary returns empty data."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/444_holder_perf_debug.json"
NAME = "justhodl-tmp-444"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, urllib.request
FMP = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
BASE = "https://financialmodelingprep.com"

CALLS = [
    "/stable/institutional-ownership/holder-performance-summary?year=2025&quarter=4",
    "/stable/institutional-ownership/holder-performance-summary?year=2025&quarter=4&page=0",
    "/stable/institutional-ownership/holder-performance-summary?cik=0001067983",  # Berkshire
    "/stable/institutional-ownership/latest?page=0&limit=10",
    "/stable/institutional-ownership/symbol-positions-summary?symbol=AAPL&year=2025&quarter=4",
    "/stable/institutional-ownership/list",
    "/stable/institutional-ownership/portfolio-holdings-summary?cik=0001067983&year=2025&quarter=4",
    "/stable/institutional-ownership/extract-analytics-by-holder?cik=0001067983&year=2025&quarter=4&page=0",
]

def fetch(path):
    url = BASE + path + ("&" if "?" in path else "?") + "apikey=" + FMP
    try:
        r = urllib.request.urlopen(
            urllib.request.Request(url, headers={"User-Agent":"JH/1.0"}),
            timeout=20)
        body = r.read().decode("utf-8")
        try:
            parsed = json.loads(body)
            return r.status, parsed
        except:
            return r.status, body[:500]
    except urllib.error.HTTPError as e:
        return e.code, None
    except Exception as e:
        return None, str(e)[:100]

def lambda_handler(event, context):
    out = {}
    for path in CALLS:
        st, data = fetch(path)
        rec = {"status": st}
        if isinstance(data, list):
            rec["n"] = len(data)
            if data and isinstance(data[0], dict):
                rec["keys"] = list(data[0].keys())[:25]
                rec["sample"] = data[0]
                # Look for relevant fields
                for k in ("marketValue", "previousMarketValue", "securitiesAdded", "securitiesRemoved", "portfolioSize", "performance"):
                    if k in data[0]:
                        rec[f"has_{k}"] = data[0][k]
        elif isinstance(data, dict):
            rec["keys"] = list(data.keys())[:15]
        elif isinstance(data, str):
            rec["body_preview"] = data[:300]
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
