#!/usr/bin/env python3
"""Step 424 — Deep-probe the political + analyst endpoints to confirm field
names and date ranges. We need to know exact shapes before writing parsers."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/424_endpoint_shapes.json"
NAME = "justhodl-tmp-shapes"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = r'''
import json, urllib.request
FMP = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
BASE = "https://financialmodelingprep.com"

# Confirmed working endpoints — get full sample for each
CONFIRMED = [
    ("senate-trades-AAPL",            "/stable/senate-trades?symbol=AAPL"),
    ("house-trades-AAPL",             "/stable/house-trades?symbol=AAPL"),
    ("senate-trades-NVDA",            "/stable/senate-trades?symbol=NVDA"),
    ("price-target-consensus-AAPL",   "/stable/price-target-consensus?symbol=AAPL"),
    ("price-target-summary-AAPL",     "/stable/price-target-summary?symbol=AAPL"),
    ("grades-consensus-AAPL",         "/stable/grades-consensus?symbol=AAPL"),
    ("grades-AAPL",                   "/stable/grades?symbol=AAPL"),
    ("grades-historical-AAPL",        "/stable/grades-historical?symbol=AAPL&limit=5"),
    ("dcf-AAPL",                      "/stable/discounted-cash-flow?symbol=AAPL"),
    ("dcf-levered-AAPL",              "/stable/levered-discounted-cash-flow?symbol=AAPL"),
    ("esg-ratings-AAPL",              "/stable/esg-ratings?symbol=AAPL"),
    ("news-stock-AAPL",               "/stable/news/stock?symbols=AAPL&limit=20"),
    ("transcript-dates-AAPL",         "/stable/earning-call-transcript-dates?symbol=AAPL"),
    # Try a few stocks for senate/house since not every stock will have records
    ("senate-trades-MSFT",            "/stable/senate-trades?symbol=MSFT"),
    ("house-trades-MSFT",             "/stable/house-trades?symbol=MSFT"),
    ("senate-trades-PLTR",            "/stable/senate-trades?symbol=PLTR"),
    ("house-trades-NVDA",             "/stable/house-trades?symbol=NVDA"),
    # Bulk-style probes
    ("price-target-consensus-multi",  "/stable/price-target-consensus?symbol=AAPL,MSFT,NVDA"),
    ("grades-consensus-multi",        "/stable/grades-consensus?symbol=AAPL,MSFT,NVDA"),
    # Try once-per-day bulk-endpoints if they exist
    ("price-targets-by-date",         "/stable/price-target-news?page=0"),
    ("grades-by-date",                "/stable/grades-news?page=0"),
    # Institutional ownership — what we CAN get on Premium
    ("inst-ownership-list-test",      "/stable/institutional-ownership/latest?page=0&limit=20"),
]


def lambda_handler(event, context):
    out = {}
    for label, path in CONFIRMED:
        url = BASE + path + ("&apikey=" if "?" in path else "?apikey=") + FMP
        try:
            r = urllib.request.urlopen(
                urllib.request.Request(url, headers={"User-Agent":"JH/1.0"}),
                timeout=15)
            body = r.read().decode("utf-8", errors="replace")
            try:
                parsed = json.loads(body)
                rec = {"status": r.status}
                if isinstance(parsed, list):
                    rec["n"] = len(parsed)
                    rec["sample_0"] = parsed[0] if parsed else None
                    rec["sample_1"] = parsed[1] if len(parsed) > 1 else None
                elif isinstance(parsed, dict):
                    rec["n"] = 1
                    rec["data"] = parsed
                else:
                    rec["raw"] = str(parsed)[:300]
                out[label] = rec
            except Exception as e:
                out[label] = {"status": r.status, "parse_err": str(e)[:100], "raw": body[:300]}
        except urllib.error.HTTPError as e:
            out[label] = {"status": e.code, "err": str(e)[:120]}
        except Exception as e:
            out[label] = {"err": str(e)[:120]}
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
