#!/usr/bin/env python3
"""Step 411 — Probe FMP /stable/ namespace for the correct insider trading,
earnings surprise, and institutional ownership endpoint paths."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/411_fmp_endpoint_probe.json"
NAME = "justhodl-tmp-fmp-probe"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = r'''
import json, urllib.request
FMP = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
BASE = "https://financialmodelingprep.com"

# Endpoint candidates to test. Format: (label, path_template_with_apikey_appended_separately)
# We try /stable/, /v3/, and /v4/ variants
CANDIDATES = [
    # Insider trading
    ("insider:stable:insider-trades",     "/stable/insider-trades?symbol=AAPL&limit=5"),
    ("insider:stable:insider-trading",    "/stable/insider-trading?symbol=AAPL&limit=5"),
    ("insider:stable:insider-trading-latest", "/stable/insider-trading/latest?limit=5"),
    ("insider:stable:insider-trading-search", "/stable/insider-trading/search?symbol=AAPL"),
    ("insider:v4:insider-trading",        "/api/v4/insider-trading?symbol=AAPL&limit=5"),
    ("insider:v4:insider-trading-rss",    "/api/v4/insider-trading-rss-feed?symbol=AAPL"),

    # Earnings surprises
    ("eps:stable:earnings-surprises",       "/stable/earnings-surprises?symbol=AAPL&limit=5"),
    ("eps:stable:earnings",                  "/stable/earnings?symbol=AAPL&limit=5"),
    ("eps:stable:earnings-historical",       "/stable/earnings-calendar?symbol=AAPL&limit=5"),
    ("eps:v3:earnings-surprises",            "/api/v3/earnings-surprises/AAPL?limit=5"),
    ("eps:v3:historical-earning",            "/api/v3/historical/earning_calendar/AAPL?limit=5"),
    ("eps:stable:earnings-surprises-bulk",  "/stable/earnings-surprises-bulk?year=2024&period=Q4"),

    # Institutional ownership
    ("inst:stable:insti-ownership-summary",  "/stable/institutional-ownership/symbol-positions-summary?symbol=AAPL&limit=2"),
    ("inst:stable:insti-ownership-list",     "/stable/institutional-ownership/symbol-ownership?symbol=AAPL&limit=2"),
    ("inst:stable:institutional-holder",     "/stable/institutional-holder?symbol=AAPL"),
    ("inst:v3:institutional-holder",         "/api/v3/institutional-holder/AAPL"),
    ("inst:v4:institutional-ownership",      "/api/v4/institutional-ownership/symbol-ownership?symbol=AAPL"),
    ("inst:v3:13f-list",                     "/api/v3/form-thirteen-date/0001067983"),  # BRK 13F dates

    # Just to confirm endpoints that DO work
    ("ctl:stable:profile",                   "/stable/profile?symbol=AAPL"),
]

def lambda_handler(event, context):
    out = {"probes": []}
    for label, path in CANDIDATES:
        url = BASE + path + ("&apikey=" if "?" in path else "?apikey=") + FMP
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/1.0"})
            r = urllib.request.urlopen(req, timeout=12)
            body = r.read().decode("utf-8", errors="replace")
            try:
                parsed = json.loads(body)
                rec = {"label": label, "status": r.status, "ok": True,
                        "n": len(parsed) if isinstance(parsed, list) else 1,
                        "keys": list(parsed[0].keys())[:15] if isinstance(parsed, list) and parsed else (list(parsed.keys())[:15] if isinstance(parsed, dict) else None),
                        "sample": (json.dumps(parsed)[:400] if parsed else None)}
            except Exception:
                rec = {"label": label, "status": r.status, "ok": False, "raw": body[:400]}
            out["probes"].append(rec)
        except urllib.error.HTTPError as e:
            out["probes"].append({"label": label, "status": e.code,
                                    "ok": False, "err": str(e)[:120]})
        except Exception as e:
            out["probes"].append({"label": label, "ok": False,
                                    "err": str(e)[:120]})
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
        out["raw"] = body[:8000]
    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
