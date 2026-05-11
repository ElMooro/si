#!/usr/bin/env python3
"""Step 427 — Re-probe all previously-failed endpoints now that Ultimate
is active. Also probe bulk endpoints + ETF + government contracts +
historical 13F + holder details + earnings transcripts content."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/427_ultimate_unlock_probe.json"
NAME = "justhodl-tmp-ult-unlock"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = r'''
import json, urllib.request
FMP = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
BASE = "https://financialmodelingprep.com"

# Categorized list of endpoints to test
CANDIDATES = [
    # ─ INSTITUTIONAL OWNERSHIP (was gated) ─
    ("inst:positions-summary",       "/stable/institutional-ownership/symbol-positions-summary?symbol=AAPL&year=2024&quarter=4"),
    ("inst:positions-summary-no-q",  "/stable/institutional-ownership/symbol-positions-summary?symbol=AAPL"),
    ("inst:extract",                  "/stable/institutional-ownership/extract?symbol=AAPL&year=2024&quarter=4"),
    ("inst:holder-performance",      "/stable/institutional-ownership/holder-performance-summary?cik=0001067983"),
    ("inst:holder-industry",         "/stable/institutional-ownership/holder-industry-breakdown?cik=0001067983&date=2024-12-31"),
    ("inst:industry-summary",        "/stable/institutional-ownership/industry-summary?year=2024&quarter=4"),
    ("inst:by-shares-held",          "/stable/institutional-ownership/by-shares-held?symbol=AAPL"),
    ("inst:portfolio-holdings",      "/stable/institutional-ownership/portfolio-holdings?cik=0001067983&date=2024-12-31"),
    ("inst:13f",                      "/stable/13f?cik=0001067983&date=2024-12-31"),

    # ─ ETF HOLDINGS (was 404/403) ─
    ("etf:holder-stock",             "/stable/etf-holdings?symbol=SPY"),
    ("etf:info",                     "/stable/etf-info?symbol=SPY"),
    ("etf:asset-exposure",           "/stable/etf-asset-exposure?symbol=AAPL"),
    ("etf:country-weights",          "/stable/etf-country-weightings?symbol=SPY"),
    ("etf:sector-weights",           "/stable/etf-sector-weightings?symbol=SPY"),
    ("etf:stock-exposure-AAPL",      "/stable/etf-stock-exposure?symbol=AAPL"),

    # ─ GOVERNMENT CONTRACTS (was 404/403) ─
    ("gov:contracts",                "/stable/government-contracts?symbol=AAPL"),
    ("gov:contracts-LMT",            "/stable/government-contracts?symbol=LMT"),
    ("gov:senate-pelosi",            "/stable/senate-trades?symbol=AAPL&page=0"),

    # ─ BULK ENDPOINTS (Ultimate-only feature) ─
    ("bulk:profile",                 "/stable/profile-bulk?part=0"),
    ("bulk:ratios-ttm",              "/stable/ratios-ttm-bulk?part=0"),
    ("bulk:key-metrics-ttm",         "/stable/key-metrics-ttm-bulk?part=0"),
    ("bulk:income-statement",        "/stable/income-statement-bulk?year=2024&period=annual"),
    ("bulk:financial-growth",        "/stable/financial-growth-bulk?year=2024&period=annual"),
    ("bulk:dcf",                     "/stable/dcf-bulk?part=0"),
    ("bulk:price-targets",           "/stable/price-target-consensus-bulk?part=0"),
    ("bulk:earnings-surprise",       "/stable/earnings-surprise-bulk?year=2024"),
    ("bulk:grades-consensus",        "/stable/grades-consensus-bulk?part=0"),

    # ─ EARNINGS TRANSCRIPTS — try fetching content (not just dates) ─
    ("transcript:content-AAPL",      "/stable/earning-call-transcript?symbol=AAPL&year=2026&quarter=2"),

    # ─ FORM 13F BY DATE / CIK ─
    ("13f:cik-list",                  "/stable/13f-cik-list?date=2024-12-31"),
    ("13f:filing-dates",             "/stable/13f-filing-dates?cik=0001067983"),

    # ─ ANALYST ESTIMATES (was 400 before) ─
    ("estimates:analyst-AAPL",       "/stable/analyst-estimates?symbol=AAPL&period=annual&limit=4"),
    ("estimates:analyst-quart",      "/stable/analyst-estimates?symbol=AAPL&period=quarter&limit=4"),
    ("estimates:financial-AAPL",     "/stable/financial-estimates?symbol=AAPL&limit=4"),

    # ─ NEWS WITH SENTIMENT ─
    ("news:fmp-articles",            "/stable/fmp-articles?page=0"),
    ("news:press-releases",          "/stable/press-releases?symbol=AAPL&limit=5"),
    ("news:general-news",            "/stable/general-news?page=0"),

    # ─ MUTUAL FUND HOLDINGS (Ultimate) ─
    ("mf:holder",                    "/stable/mutual-fund-holdings?symbol=AAPL"),
    ("mf:by-name",                   "/stable/mutual-fund-holder?symbol=AAPL"),

    # ─ FOREX / CRYPTO REALTIME (Ultimate) ─
    ("crypto:realtime",              "/stable/quote?symbol=BTCUSD"),
    ("forex:eurusd",                 "/stable/quote?symbol=EURUSD"),

    # ─ COT REPORTS (commitment of traders, was Ultimate per docs) ─
    ("cot:reports",                  "/stable/commitment-of-traders?symbol=ZC"),
    ("cot:analysis",                 "/stable/commitment-of-traders-analysis?symbol=ZC"),

    # ─ MERGERS & ACQUISITIONS ─
    ("ma:latest",                    "/stable/mergers-acquisitions-latest?page=0"),
    ("ma:search",                    "/stable/mergers-acquisitions-search?name=Apple"),

    # ─ KEY EXECUTIVES + COMPENSATION ─
    ("exec:key-executives",          "/stable/key-executives?symbol=AAPL"),
    ("exec:compensation",            "/stable/governance-executive-compensation?symbol=AAPL"),

    # ─ SHARE FLOAT + SHORT INTEREST ─
    ("share:float",                  "/stable/shares-float?symbol=AAPL"),
    ("share:short-interest",         "/stable/short-interest?symbol=AAPL"),
    ("share:short-volume",           "/stable/short-volume?symbol=AAPL"),

    # ─ EARNINGS CALENDAR + IPO CALENDAR ─
    ("cal:earnings",                 "/stable/earnings-calendar?from=2026-05-11&to=2026-05-18"),
    ("cal:ipo",                       "/stable/ipo-calendar?from=2026-05-11&to=2026-06-11"),

    # ─ TECHNICAL INDICATORS (Ultimate gets more) ─
    ("ta:rsi",                        "/stable/technical_indicators/rsi?symbol=AAPL&periodLength=14&timeframe=1day"),
    ("ta:macd",                       "/stable/technical_indicators/macd?symbol=AAPL&periodLength=12&timeframe=1day"),
]

def lambda_handler(event, context):
    out = {"probes": []}
    for label, path in CANDIDATES:
        url = BASE + path + ("&apikey=" if "?" in path else "?apikey=") + FMP
        rec = {"label": label, "path": path}
        try:
            r = urllib.request.urlopen(
                urllib.request.Request(url, headers={"User-Agent":"JH/1.0"}),
                timeout=15)
            body = r.read().decode("utf-8", errors="replace")
            try:
                parsed = json.loads(body)
                rec["status"] = r.status
                rec["ok"] = True
                if isinstance(parsed, list):
                    rec["n"] = len(parsed)
                    if parsed and isinstance(parsed[0], dict):
                        rec["keys"] = list(parsed[0].keys())[:18]
                        rec["sample"] = json.dumps(parsed[0], default=str)[:400]
                elif isinstance(parsed, dict):
                    rec["n"] = 1
                    rec["keys"] = list(parsed.keys())[:18]
                    rec["sample"] = json.dumps(parsed, default=str)[:400]
            except Exception:
                rec["status"] = r.status
                rec["ok"] = False
                rec["raw"] = body[:200]
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
                            MemorySize=256, Timeout=900, Code={"ZipFile": zb})
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
