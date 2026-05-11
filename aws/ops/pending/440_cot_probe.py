#!/usr/bin/env python3
"""Step 440 — Probe COT endpoint variants to find one returning FRESH data.
Stage 13 found commitment-of-traders-analysis stuck at 2024-02-27. Try:
  - commitment-of-traders          (raw weekly reports)
  - commitment-of-traders-report
  - commitment-of-traders-list
  - Different symbol formats: ES vs ES_F vs CME:ES vs S&P 500
"""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/440_cot_probe.json"
NAME = "justhodl-tmp-440"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = r'''
import json, urllib.request
FMP = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
BASE = "https://financialmodelingprep.com"

# Probe a matrix of endpoint × symbol combinations
ENDPOINTS = [
    "/stable/commitment-of-traders",
    "/stable/commitment-of-traders-analysis",
    "/stable/commitment-of-traders-report",
    "/stable/commitment-of-traders-list",
    "/stable/commitment-of-traders-analysis-list",
    "/api/v4/commitment_of_traders_report",
    "/api/v4/commitment_of_traders_report_analysis",
    "/stable/cot-report",
    "/stable/cot-analysis",
]

# Try different symbol formats
SYMBOLS = ["ES", "GC", "ZC", "CL", "BTC", "DX"]


def lambda_handler(event, context):
    out = {"probes": []}
    # 1. List-endpoints — see if any return a directory of symbols
    for ep in ENDPOINTS:
        rec = {"endpoint": ep, "no_symbol": None, "with_symbol": {}}
        # Without symbol
        url = BASE + ep + "?apikey=" + FMP
        try:
            r = urllib.request.urlopen(
                urllib.request.Request(url, headers={"User-Agent":"JH/1.0"}),
                timeout=15)
            body = r.read().decode("utf-8", errors="replace")[:2000]
            try:
                parsed = json.loads(body)
                rec["no_symbol"] = {"status": r.status,
                                     "n": len(parsed) if isinstance(parsed, list) else 1}
                if isinstance(parsed, list) and parsed:
                    rec["no_symbol"]["sample"] = parsed[0] if isinstance(parsed[0], dict) else str(parsed[0])[:300]
                    # Extract dates if present
                    dates = sorted(set(p.get("date","")[:10] for p in parsed if isinstance(p, dict) and p.get("date")), reverse=True)[:5]
                    rec["no_symbol"]["recent_dates"] = dates
                elif isinstance(parsed, dict):
                    rec["no_symbol"]["keys"] = list(parsed.keys())[:10]
            except Exception:
                rec["no_symbol"] = {"status": r.status, "raw": body[:200]}
        except urllib.error.HTTPError as e:
            rec["no_symbol"] = {"status": e.code}
        except Exception as e:
            rec["no_symbol"] = {"err": str(e)[:100]}

        # With each symbol (only if no_symbol worked at least)
        if rec["no_symbol"] and rec["no_symbol"].get("status") == 200:
            for sym in SYMBOLS[:3]:  # ES, GC, ZC
                sep = "&" if "?" in ep else "?"
                url2 = BASE + ep + sep + f"symbol={sym}&apikey=" + FMP
                if "?" not in ep: url2 = BASE + ep + f"?symbol={sym}&apikey=" + FMP
                else: url2 = BASE + ep + f"&symbol={sym}&apikey=" + FMP
                # Recompute carefully
                url2 = BASE + ep + ("&" if "?" in ep else "?") + f"symbol={sym}&apikey=" + FMP
                try:
                    r2 = urllib.request.urlopen(
                        urllib.request.Request(url2, headers={"User-Agent":"JH/1.0"}),
                        timeout=12)
                    body2 = r2.read().decode("utf-8")[:2500]
                    parsed2 = json.loads(body2)
                    rs = {"status": r2.status,
                           "n": len(parsed2) if isinstance(parsed2, list) else 1}
                    if isinstance(parsed2, list) and parsed2:
                        dates2 = sorted(set(p.get("date","")[:10] for p in parsed2 if isinstance(p, dict) and p.get("date")), reverse=True)[:3]
                        rs["recent_dates"] = dates2
                        if isinstance(parsed2[0], dict):
                            rs["keys"] = list(parsed2[0].keys())[:15]
                    rec["with_symbol"][sym] = rs
                except urllib.error.HTTPError as e:
                    rec["with_symbol"][sym] = {"status": e.code}
                except Exception as e:
                    rec["with_symbol"][sym] = {"err": str(e)[:80]}
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
