#!/usr/bin/env python3
"""Step 447 — Probe FMP for the endpoint that returns a fund's full
13F holdings (list of positions per fund) so we can invert it to:
  stock → [list of famous funds holding it]
"""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/447_holdings_probe.json"
NAME = "justhodl-tmp-447"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, urllib.request
FMP = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
BASE = "https://financialmodelingprep.com"

# Use Berkshire Hathaway CIK as test
BRK_CIK = "0001067983"

# Try variants of holdings extraction endpoints
CALLS = [
    # Direct fund-holdings endpoints
    f"/stable/institutional-ownership/extract?cik={BRK_CIK}&year=2025&quarter=4",
    f"/stable/institutional-ownership/extract?cik={BRK_CIK}&year=2025&quarter=4&page=0",
    f"/stable/institutional-ownership/positions?cik={BRK_CIK}&year=2025&quarter=4",
    f"/stable/institutional-ownership/holdings?cik={BRK_CIK}",
    f"/stable/institutional-ownership/portfolio?cik={BRK_CIK}&year=2025&quarter=4",
    f"/stable/institutional-ownership/portfolio-holdings?cik={BRK_CIK}&year=2025&quarter=4",
    f"/stable/institutional-ownership/13f-filings-extract?cik={BRK_CIK}&year=2025&quarter=4",
    f"/stable/institutional-ownership/13f-filings?cik={BRK_CIK}&year=2025&quarter=4",
    f"/stable/institutional-ownership/13f-filings-dates?cik={BRK_CIK}",
    # Symbol-side: who holds a given stock
    f"/stable/institutional-ownership/symbol-ownership?symbol=AAPL&year=2025&quarter=4",
    f"/stable/institutional-ownership/symbol-ownership-by-shares?symbol=AAPL&year=2025&quarter=4",
    f"/stable/institutional-ownership/symbol-positions-summary?symbol=AAPL&year=2025&quarter=4",
    # API v4 fallbacks
    f"/api/v4/institutional-ownership/portfolio-holdings?cik={BRK_CIK}&date=2025-12-31",
    f"/api/v3/institutional-holder/AAPL",
]


def fetch(path):
    url = BASE + path + ("&" if "?" in path else "?") + "apikey=" + FMP
    try:
        r = urllib.request.urlopen(
            urllib.request.Request(url, headers={"User-Agent":"JH/1.0"}),
            timeout=15)
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
                rec["keys"] = list(data[0].keys())[:20]
                # Look for symbol/ticker fields
                for k in ("symbol", "ticker", "securityCusip", "securityName",
                              "weight", "marketValue", "sharesNumber", "value",
                              "ownership", "name", "investorName", "shares"):
                    if k in data[0]:
                        rec[f"has_{k}"] = data[0][k]
                # Show 2 records
                rec["sample"] = data[:2]
                # Distinct symbols if present
                syms = sorted(set(d.get("symbol") or d.get("ticker") for d in data if isinstance(d, dict))) [:15]
                rec["symbols"] = [s for s in syms if s]
        elif isinstance(data, dict):
            rec["keys"] = list(data.keys())[:15]
            rec["body"] = {k: data[k] for k in list(data.keys())[:8]}
        elif isinstance(data, str):
            rec["body_preview"] = data[:200]
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
