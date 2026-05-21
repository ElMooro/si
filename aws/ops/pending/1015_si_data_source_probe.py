"""
ops 1015 - Probe candidate replacement short-interest data sources.

Polygon /stocks/v1/short-interest confirmed dead post-2018 (ops 1014).
Need a working data feed for current SI/days-to-cover/SI-as-pct-float.

Candidates to test (read-only HTTP probes against AAPL as canonical ticker):

FMP /stable/:
  - /stable/equity-short-interest?symbol=AAPL
  - /stable/short-interest-by-symbol?symbol=AAPL
  - /stable/short-volume?symbol=AAPL  (may differ from SI)
  - /stable/historical-short-interest?symbol=AAPL

Polygon alternatives:
  - /v2/reference/short-interest/AAPL
  - /vX/reference/short-interest/AAPL
  - /stocks/v2/short-interest?ticker=AAPL  (next-gen path)

FINRA bi-monthly SI archive:
  - https://cdn.finra.org/equity/shortsale/...
  - https://www.finra.org/finra-data/browse-catalog/short-sale-volume-data
  - Direct file: https://cdn.finra.org/equity/regsho/shortinterest/...

For each candidate: HTTP status + content-type + first 500 bytes (or settlement
date if JSON) so we can pick a winner.
"""
import json
import os
import sys
import time
import traceback
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3

REPO_ROOT = Path(__file__).resolve().parents[3]
REGION = "us-east-1"
PROBE_FN = "justhodl-ops-1015-si-probe-tmp"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"

lam = boto3.client("lambda", region_name=REGION)

# Lambda body executed in AWS environment with FMP+Polygon keys from env donor
LAMBDA_CODE = '''
import json, os, urllib.request, urllib.error
import urllib.parse

FMP = os.environ.get("FMP_KEY", "")
POLY = os.environ.get("POLY_KEY") or os.environ.get("POLYGON_KEY", "")

def probe(url, headers=None, timeout=12):
    try:
        req = urllib.request.Request(
            url, headers=headers or {"User-Agent": "JustHodl-SI-Probe/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read()
            ct = r.headers.get("Content-Type", "")
            sample = body[:600].decode("utf-8", errors="replace")
            j = None
            settlement = None
            n_results = None
            try:
                j = json.loads(sample if len(body) < 600 else body.decode("utf-8"))
                if isinstance(j, list) and j:
                    settlement = (j[0].get("settlement_date")
                                  or j[0].get("date")
                                  or j[0].get("reportDate"))
                    n_results = len(j)
                elif isinstance(j, dict):
                    if "results" in j and isinstance(j["results"], list):
                        n_results = len(j["results"])
                        if j["results"]:
                            r0 = j["results"][0]
                            settlement = (r0.get("settlement_date")
                                          or r0.get("date"))
            except Exception:
                pass
            return {
                "status": r.status,
                "content_type": ct,
                "n_bytes": len(body),
                "n_results": n_results,
                "latest_settlement_date": settlement,
                "sample_first_600b": sample,
            }
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read()[:300].decode("utf-8", errors="replace")
        except Exception:
            pass
        return {"status": e.code, "error": str(e)[:200], "body_first_300b": body}
    except Exception as e:
        return {"status": None, "error": str(e)[:300]}

def handler(event, ctx):
    out = {"fmp_key_present": bool(FMP), "poly_key_present": bool(POLY)}
    today = "20260521"  # YYYYMMDD for FINRA file
    
    # FMP /stable/ candidates
    fmp_paths = [
        ("fmp_equity_short_interest",
         f"https://financialmodelingprep.com/stable/equity-short-interest?symbol=AAPL&apikey={FMP}"),
        ("fmp_short_interest_by_symbol",
         f"https://financialmodelingprep.com/stable/short-interest-by-symbol?symbol=AAPL&apikey={FMP}"),
        ("fmp_short_interest",
         f"https://financialmodelingprep.com/stable/short-interest?symbol=AAPL&apikey={FMP}"),
        ("fmp_short_volume",
         f"https://financialmodelingprep.com/stable/short-volume?symbol=AAPL&apikey={FMP}"),
        ("fmp_historical_short_interest",
         f"https://financialmodelingprep.com/stable/historical-short-interest?symbol=AAPL&apikey={FMP}"),
        ("fmp_v3_short_interest",  # legacy v3 sanity check
         f"https://financialmodelingprep.com/api/v3/short-interest/AAPL?apikey={FMP}"),
    ]
    
    # Polygon alternatives
    poly_paths = [
        ("poly_v2_short_interest",
         f"https://api.polygon.io/v2/reference/short-interest/AAPL?apiKey={POLY}"),
        ("poly_v3_short_interest",
         f"https://api.polygon.io/v3/reference/short-interest/AAPL?apiKey={POLY}"),
        ("poly_stocks_v2",
         f"https://api.polygon.io/stocks/v2/short-interest?ticker=AAPL&apiKey={POLY}"),
        ("poly_short_volume_aggregates",  # daily aggregates, fresh
         f"https://api.polygon.io/stocks/v1/short-volume?ticker=AAPL&limit=2&order=desc&apiKey={POLY}"),
    ]
    
    # FINRA bi-monthly SI archive - settlement dates are 15th + last day of month
    # 2026-05-15 + 2026-04-30 are likely most recent bi-monthly settlements
    finra_paths = [
        ("finra_shortint_20260515",
         "https://cdn.finra.org/equity/regsho/monthly/shrt20260515.txt"),
        ("finra_shortint_20260430",
         "https://cdn.finra.org/equity/regsho/monthly/shrt20260430.txt"),
        ("finra_shortint_alt_path",
         "https://cdn.finra.org/equity/shortsale/20260515.txt"),
        ("finra_data_catalog",
         "https://api.finra.org/data/group/otcMarket/name/regShoDaily"),
    ]
    
    for name, url in fmp_paths + poly_paths + finra_paths:
        out[name] = probe(url)
    
    return {"statusCode": 200, "body": json.dumps(out)}
'''


def deploy_probe():
    import io
    import zipfile
    # zipfile build
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("lambda_function.py", LAMBDA_CODE)
    buf.seek(0)
    zip_bytes = buf.getvalue()

    # Get FMP_KEY from justhodl-starmine and POLY_KEY from justhodl-finra-short
    try:
        c = lam.get_function(FunctionName="justhodl-starmine")[
            "Configuration"]
        fmp_key = (c.get("Environment") or {}).get(
            "Variables", {}).get("FMP_KEY", "")
    except Exception:
        fmp_key = ""
    try:
        c = lam.get_function(FunctionName="justhodl-finra-short")[
            "Configuration"]
        poly_key = (c.get("Environment") or {}).get(
            "Variables", {}).get("POLY_KEY", "")
    except Exception:
        poly_key = ""

    try:
        lam.delete_function(FunctionName=PROBE_FN)
        time.sleep(2)
    except Exception:
        pass

    lam.create_function(
        FunctionName=PROBE_FN,
        Runtime="python3.12",
        Role=ROLE_ARN,
        Handler="lambda_function.handler",
        Code={"ZipFile": zip_bytes},
        Timeout=120,
        MemorySize=256,
        Environment={"Variables": {"FMP_KEY": fmp_key,
                                     "POLY_KEY": poly_key}},
    )
    # Wait active
    for _ in range(20):
        try:
            c = lam.get_function(FunctionName=PROBE_FN)["Configuration"]
            if (c.get("State") == "Active" and
                    c.get("LastUpdateStatus") == "Successful"):
                return True
        except Exception:
            pass
        time.sleep(3)
    return False


def main():
    report = {"started_at": datetime.now(timezone.utc).isoformat()}

    ready = deploy_probe()
    report["probe_deployed"] = ready
    if not ready:
        report["error"] = "probe deploy timeout"
        _write(report)
        return

    try:
        r = lam.invoke(FunctionName=PROBE_FN,
                       InvocationType="RequestResponse",
                       Payload=json.dumps({}).encode("utf-8"))
        raw = r["Payload"].read()
        body = json.loads(raw.decode("utf-8"))
        if isinstance(body.get("body"), str):
            body["body"] = json.loads(body["body"])
        report["probe_result"] = body.get("body") or body
        report["function_error"] = r.get("FunctionError")
    except Exception as e:
        report["error"] = str(e)[:400]
    finally:
        try:
            lam.delete_function(FunctionName=PROBE_FN)
        except Exception:
            pass

    # Score each candidate
    candidates = report.get("probe_result", {})
    winners = []
    for name, res in candidates.items():
        if not isinstance(res, dict):
            continue
        if res.get("status") == 200 and (
                res.get("n_results") or 0) > 0:
            sd = res.get("latest_settlement_date") or ""
            is_fresh = sd[:4] >= "2024" if sd else False
            winners.append({
                "candidate": name,
                "n_results": res.get("n_results"),
                "latest_settlement": sd,
                "fresh_2024_plus": is_fresh,
            })
    # Sort: fresh first, then by n_results
    winners.sort(key=lambda x: (not x["fresh_2024_plus"],
                                  -(x["n_results"] or 0)))
    report["winners"] = winners
    report["recommended"] = winners[0] if winners else None

    report["ended_at"] = datetime.now(timezone.utc).isoformat()
    _write(report)


def _write(report):
    out_path = REPO_ROOT / "aws" / "ops" / "reports" / "1015.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2, default=str))
    print(f"[ops 1015] report written {out_path.relative_to(REPO_ROOT)}")
    if report.get("recommended"):
        print(f"  RECOMMENDED: {report['recommended']}")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print(traceback.format_exc())
        sys.exit(1)
