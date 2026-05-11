#!/usr/bin/env python3
"""Step 447 — Probe for endpoints that return a hedge fund's actual stock
holdings. Need this for Smart Money × Screener integration: given Berkshire's
CIK, list every stock they own."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/447_fund_holdings_probe.json"
NAME = "justhodl-tmp-447"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, urllib.request
FMP = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
BASE = "https://financialmodelingprep.com"

# Berkshire CIK = 0001067983 — use as the test target
BRK = "0001067983"

CALLS = [
    # /stable/ paths
    f"/stable/institutional-ownership/extract?cik={BRK}&year=2025&quarter=4",
    f"/stable/institutional-ownership/extract?cik={BRK}&year=2025&quarter=4&page=0",
    f"/stable/institutional-ownership/holder-industry-breakdown?cik={BRK}",
    f"/stable/institutional-ownership/holder-portfolio?cik={BRK}",
    f"/stable/institutional-ownership/holder-holdings?cik={BRK}",
    f"/stable/institutional-ownership/by-holder?cik={BRK}",
    f"/stable/institutional-ownership/by-shares-held?cik={BRK}",
    f"/stable/institutional-ownership/portfolio-date?cik={BRK}",
    f"/stable/institutional-ownership/portfolio-dates?cik={BRK}",
    f"/stable/institutional-ownership/portfolio-summary?cik={BRK}",
    f"/stable/institutional-ownership/positions-summary-by-holder?cik={BRK}&year=2025&quarter=4",
    f"/stable/institutional-ownership/positions-summary-by-cik?cik={BRK}&year=2025&quarter=4",
    f"/stable/institutional-ownership/holdings-by-holder?cik={BRK}&year=2025&quarter=4",
    f"/stable/institutional-ownership/13F-filings?cik={BRK}",
    f"/stable/institutional-ownership/13f?cik={BRK}",
    f"/stable/institutional-ownership/holder-13f?cik={BRK}",
    # /api/v4/ paths (older FMP routes)
    f"/api/v4/institutional-ownership/portfolio-holdings?cik={BRK}&date=2025-12-31",
    f"/api/v4/institutional-ownership/portfolio-holdings-summary?cik={BRK}",
    f"/api/v4/institutional-ownership/portfolio-date?cik={BRK}",
    # Generic FMP v3/v4 holdings endpoints
    f"/api/v3/13F/{BRK}/2025-12-31",
    f"/api/v3/form-thirteen/{BRK}?date=2025-12-31",
    f"/api/v3/13f-filings/{BRK}",
    # SEC EDGAR shape paths (sometimes mirrored)
    f"/stable/13F/portfolio?cik={BRK}&date=2025-12-31",
    f"/stable/13F?cik={BRK}",
]

def fetch(path):
    url = BASE + path + ("&" if "?" in path else "?") + "apikey=" + FMP
    try:
        r = urllib.request.urlopen(
            urllib.request.Request(url, headers={"User-Agent":"JH/1.0"}),
            timeout=15)
        body = r.read().decode("utf-8", errors="replace")
        try:
            parsed = json.loads(body)
            return r.status, parsed
        except Exception:
            return r.status, body[:500]
    except urllib.error.HTTPError as e:
        try: err_body = e.read().decode("utf-8")[:200]
        except: err_body = ""
        return e.code, err_body
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
                # Look for tradeable symbol field
                for k in ("symbol", "ticker", "tickerCusip", "cusip", "securityName", "nameOfIssuer", "shares", "value"):
                    if k in data[0]:
                        rec[f"has_{k}"] = str(data[0][k])[:60]
                rec["sample"] = {k: v for k, v in list(data[0].items())[:8]}
        elif isinstance(data, dict):
            rec["keys"] = list(data.keys())[:15]
        elif isinstance(data, str):
            rec["body"] = data[:200]
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
                            MemorySize=256, Timeout=300, Code={"ZipFile": zb})
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
