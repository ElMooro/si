#!/usr/bin/env python3
"""Step 423 — Probe ALL FMP Ultimate endpoints we want to use.
Returns: endpoint URL, status, sample response shape. Even on Premium,
many will 200; the rest will 402 until Ultimate upgrade kicks in."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/423_fmp_ultimate_probe.json"
NAME = "justhodl-tmp-fmp-ult-probe"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = r'''
import json, urllib.request
FMP = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
BASE = "https://financialmodelingprep.com"

# ENDPOINTS TO PROBE — organized by feature area
# Format: (category, label, path)
CANDIDATES = [
    # ─ INSTITUTIONAL (13F) ─
    ("inst", "stable:inst-positions-summary",       "/stable/institutional-ownership/symbol-positions-summary?symbol=AAPL&limit=2"),
    ("inst", "stable:inst-positions-latest",         "/stable/institutional-ownership/latest?page=0&limit=10"),
    ("inst", "stable:inst-symbol-positions",         "/stable/institutional-ownership/symbol-positions?symbol=AAPL"),
    ("inst", "stable:inst-ownership-extract",        "/stable/institutional-ownership/extract?symbol=AAPL&year=2024&quarter=4"),
    ("inst", "stable:inst-holders",                  "/stable/institutional-ownership/symbol-holders?symbol=AAPL&page=0"),
    ("inst", "stable:holders-13f",                  "/stable/institutional-ownership/13f?symbol=AAPL"),
    ("inst", "stable:institutional-ownership",      "/stable/institutional-ownership?symbol=AAPL"),

    # ─ POLITICAL (Senate/House) ─
    ("senate", "stable:senate-trading",              "/stable/senate-trading?symbol=AAPL"),
    ("senate", "stable:senate-trading-rss",          "/stable/senate-trading/rss?page=0"),
    ("senate", "stable:senate-trades-latest",        "/stable/senate-trades?symbol=AAPL"),
    ("senate", "v4:senate-trading",                  "/api/v4/senate-trading?symbol=AAPL"),
    ("senate", "v4:senate-disclosure",               "/api/v4/senate-disclosure?symbol=AAPL"),

    ("house", "stable:house-trading",                "/stable/house-trading?symbol=AAPL"),
    ("house", "stable:house-trades",                 "/stable/house-trades?symbol=AAPL"),
    ("house", "v4:senate-trading-house",             "/api/v4/senate-trading-house?symbol=AAPL"),
    ("house", "v4:house-disclosure",                 "/api/v4/house-disclosure?symbol=AAPL"),

    # ─ ANALYST INTELLIGENCE ─
    ("price_target", "stable:price-target-consensus", "/stable/price-target-consensus?symbol=AAPL"),
    ("price_target", "stable:price-target",          "/stable/price-target?symbol=AAPL"),
    ("price_target", "stable:price-target-summary", "/stable/price-target-summary?symbol=AAPL"),
    ("price_target", "stable:price-target-latest",  "/stable/price-target-latest-news?symbol=AAPL&limit=10"),
    ("price_target", "v4:price-target-consensus",   "/api/v4/price-target-consensus?symbol=AAPL"),
    ("price_target", "v3:price-target",             "/api/v3/price-target?symbol=AAPL"),

    ("grades", "stable:grades",                       "/stable/grades?symbol=AAPL"),
    ("grades", "stable:grades-historical",           "/stable/grades-historical?symbol=AAPL"),
    ("grades", "stable:upgrades-downgrades",         "/stable/upgrades-downgrades?symbol=AAPL"),
    ("grades", "stable:grades-consensus",            "/stable/grades-consensus?symbol=AAPL"),
    ("grades", "v4:upgrades-downgrades-consensus",   "/api/v4/upgrades-downgrades-consensus?symbol=AAPL"),
    ("grades", "v3:upgrades-downgrades",             "/api/v3/upgrades-downgrades?symbol=AAPL"),

    ("estimates", "stable:analyst-estimates",        "/stable/analyst-estimates?symbol=AAPL&limit=4"),
    ("estimates", "stable:earnings-estimates",       "/stable/earnings-estimates?symbol=AAPL"),

    # ─ NEWS + SENTIMENT ─
    ("news", "stable:news-stock",                    "/stable/news/stock?symbols=AAPL&limit=10"),
    ("news", "stable:stock-news",                    "/stable/stock-news?symbols=AAPL&limit=10"),
    ("news", "stable:news-stock-latest",            "/stable/news/stock-latest?limit=10"),
    ("news", "stable:stock-news-sentiments-rss",    "/stable/stock-news-sentiments-rss-feed?page=0&limit=10"),
    ("news", "v4:stock-news",                       "/api/v4/stock-news?tickers=AAPL&limit=10"),

    # ─ VALUATION (DCF) ─
    ("dcf", "stable:dcf",                            "/stable/discounted-cash-flow?symbol=AAPL"),
    ("dcf", "stable:dcf-levered",                   "/stable/levered-discounted-cash-flow?symbol=AAPL"),
    ("dcf", "stable:dcf-advanced",                  "/stable/advanced_discounted_cash_flow?symbol=AAPL"),
    ("dcf", "stable:dcf-historical",                "/stable/historical-discounted-cash-flow-statement?symbol=AAPL&limit=4"),
    ("dcf", "v3:dcf",                                "/api/v3/discounted-cash-flow/AAPL"),

    # ─ ESG ─
    ("esg", "stable:esg-data",                       "/stable/esg-data?symbol=AAPL"),
    ("esg", "stable:esg-ratings",                   "/stable/esg-ratings?symbol=AAPL"),
    ("esg", "stable:esg-environmental",             "/stable/esg-environmental-social-governance-data?symbol=AAPL"),
    ("esg", "stable:esg-rating",                    "/stable/esg-rating?symbol=AAPL"),
    ("esg", "v4:esg-environmental",                  "/api/v4/esg-environmental-social-governance-data?symbol=AAPL"),

    # ─ TRANSCRIPTS ─
    ("transcript", "stable:earnings-transcript",     "/stable/earning-call-transcript?symbol=AAPL&year=2024&quarter=4"),
    ("transcript", "stable:transcript-dates",        "/stable/earning-call-transcript-dates?symbol=AAPL"),
    ("transcript", "stable:transcript-batch",        "/stable/batch-earning-call-transcript?symbol=AAPL"),

    # ─ ETF HOLDINGS (which ETFs hold this stock) ─
    ("etf", "stable:etf-holder",                     "/stable/etf-holder?symbol=AAPL"),
    ("etf", "stable:etf-stock-exposure",             "/stable/etf-stock-exposure?symbol=AAPL"),
    ("etf", "v4:etf-holder",                         "/api/v4/etf-holder?symbol=AAPL"),

    # ─ GOVERNMENT CONTRACTS ─
    ("contracts", "stable:gov-contracts",            "/stable/government-contracts?symbol=AAPL"),
    ("contracts", "v4:gov-contracts",                "/api/v4/government-contracts?symbol=AAPL"),

    # ─ KEY METRICS / FINANCIAL HEALTH (sanity check our existing endpoint still works) ─
    ("control", "stable:key-metrics-ttm",            "/stable/key-metrics-ttm?symbol=AAPL"),
]

def lambda_handler(event, context):
    out = {"probes": []}
    for cat, label, path in CANDIDATES:
        url = BASE + path + ("&apikey=" if "?" in path else "?apikey=") + FMP
        rec = {"category": cat, "label": label, "path": path}
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/1.0"})
            r = urllib.request.urlopen(req, timeout=12)
            body = r.read().decode("utf-8", errors="replace")
            try:
                parsed = json.loads(body)
                rec["status"] = r.status
                rec["ok"] = True
                if isinstance(parsed, list):
                    rec["n"] = len(parsed)
                    if parsed:
                        rec["keys"] = list(parsed[0].keys())[:18] if isinstance(parsed[0], dict) else None
                        rec["sample"] = json.dumps(parsed[0], default=str)[:300]
                elif isinstance(parsed, dict):
                    rec["n"] = 1
                    rec["keys"] = list(parsed.keys())[:18]
                    rec["sample"] = json.dumps(parsed, default=str)[:300]
            except Exception:
                rec["status"] = r.status
                rec["ok"] = False
                rec["raw"] = body[:300]
        except urllib.error.HTTPError as e:
            rec["status"] = e.code
            rec["ok"] = False
            rec["err"] = str(e)[:120]
        except Exception as e:
            rec["ok"] = False
            rec["err"] = str(e)[:120]
        out["probes"].append(rec)
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
                            MemorySize=256, Timeout=600, Code={"ZipFile": zb})
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
