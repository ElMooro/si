"""
ops 1019 - FINRA SI probe with CORRECTED operator names.

ops 1018 got "Unable to parse request body" because compareType
GREATER_THAN_EQUAL is invalid. FINRA Query API documented operators are:
  EQUAL, NOT_EQUAL, GREATER, LESSER, IN, NOT_IN

Also FINRA has a separate `dateRangeFilters` block for date range queries
that's the documented pattern. Trying both approaches.

Goal: settle whether equityShortInterestStandardized has 2026 data for
exchange-listed names (AAPL/MSFT), unlocking the data-source switch.
"""
import json
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

import boto3

REPO_ROOT = Path(__file__).resolve().parents[3]
REGION = "us-east-1"
PROBE_FN = "justhodl-ops-1019-finra-correct-ops-tmp"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"

lam = boto3.client("lambda", region_name=REGION)

LAMBDA_CODE = '''
import json, urllib.request, urllib.error

URL = "https://api.finra.org/data/group/otcMarket/name/equityShortInterestStandardized"

def call(method, url, payload=None, timeout=30):
    try:
        data = json.dumps(payload).encode("utf-8") if payload is not None else None
        headers = {"Accept": "application/json",
                   "User-Agent": "JustHodl-FINRA-V3/1.0"}
        if payload is not None:
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(url, data=data, method=method,
                                      headers=headers)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read()
            j = None
            try:
                j = json.loads(body.decode("utf-8"))
            except Exception:
                pass
            settlements = []
            symbols = []
            first = None
            n = None
            if isinstance(j, list):
                n = len(j)
                if j:
                    first = j[0] if isinstance(j[0], dict) else None
                    for r0 in j[:10]:
                        if isinstance(r0, dict):
                            settlements.append(r0.get("settlementDate"))
                            symbols.append(r0.get(
                                "securitiesInformationProcessorSymbolIdentifier"))
            return {"status": r.status, "n_items": n,
                    "first_record": first,
                    "settlement_dates_sample": settlements,
                    "symbols_sample": symbols}
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read()[:600].decode("utf-8", errors="replace")
        except Exception:
            pass
        return {"status": e.code, "error": str(e)[:200],
                "body_first_600b": body}
    except Exception as e:
        return {"status": None, "error": str(e)[:300]}

def handler(event, ctx):
    out = {}
    
    # Test 1: dateRangeFilters (documented pattern for date ranges)
    p1 = {
        "limit": 10,
        "dateRangeFilters": [
            {"fieldName": "settlementDate",
             "startDate": "2026-01-01",
             "endDate": "2026-12-31"}
        ],
        "sortFields": ["-settlementDate"]
    }
    out["dateRange_2026"] = call("POST", URL, payload=p1)
    
    # Test 2: GREATER (correct operator name, not GREATER_THAN_EQUAL)
    p2 = {
        "limit": 10,
        "compareFilters": [
            {"compareType": "GREATER",
             "fieldName": "settlementDate",
             "fieldValue": "2026-01-01"}
        ],
        "sortFields": ["-settlementDate"]
    }
    out["compare_GREATER"] = call("POST", URL, payload=p2)
    
    # Test 3: dateRangeFilters + AAPL filter (the institutional query)
    p3 = {
        "limit": 10,
        "dateRangeFilters": [
            {"fieldName": "settlementDate",
             "startDate": "2025-01-01",
             "endDate": "2026-12-31"}
        ],
        "compareFilters": [
            {"compareType": "EQUAL",
             "fieldName": "securitiesInformationProcessorSymbolIdentifier",
             "fieldValue": "AAPL"}
        ],
        "sortFields": ["-settlementDate"]
    }
    out["dateRange_2025plus_AAPL"] = call("POST", URL, payload=p3)
    
    # Test 4: Same but MSFT
    p4 = dict(p3)
    p4["compareFilters"] = [{"compareType": "EQUAL",
                              "fieldName": "securitiesInformationProcessorSymbolIdentifier",
                              "fieldValue": "MSFT"}]
    out["dateRange_2025plus_MSFT"] = call("POST", URL, payload=p4)
    
    # Test 5: Wider 2020+ date range to see what is actually available
    p5 = {
        "limit": 5,
        "dateRangeFilters": [
            {"fieldName": "settlementDate",
             "startDate": "2020-01-01",
             "endDate": "2026-12-31"}
        ],
        "sortFields": ["-settlementDate"]
    }
    out["dateRange_2020plus_latest"] = call("POST", URL, payload=p5)
    
    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def deploy_probe():
    import io, zipfile
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("lambda_function.py", LAMBDA_CODE)
    buf.seek(0)
    zip_bytes = buf.getvalue()
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
        Timeout=180,
        MemorySize=256,
    )
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
    if not deploy_probe():
        report["error"] = "deploy timeout"
        _write(report)
        return
    try:
        r = lam.invoke(FunctionName=PROBE_FN,
                       InvocationType="RequestResponse",
                       Payload=json.dumps({}).encode("utf-8"))
        body = json.loads(r["Payload"].read().decode("utf-8"))
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

    pr = report.get("probe_result") or {}
    latest_all = pr.get("dateRange_2020plus_latest") or {}
    aapl = pr.get("dateRange_2025plus_AAPL") or {}
    msft = pr.get("dateRange_2025plus_MSFT") or {}
    max_sd = (max(latest_all.get("settlement_dates_sample") or [""])
              if latest_all.get("settlement_dates_sample") else None)

    report["verdict"] = {
        "endpoint_works_with_dateRange": (latest_all.get("status") == 200),
        "max_settlement_date_seen": max_sd,
        "is_fresh_2025_plus": (max_sd or "")[:4] >= "2025" if max_sd else False,
        "is_fresh_2026_plus": (max_sd or "")[:4] >= "2026" if max_sd else False,
        "aapl_n_records": aapl.get("n_items") or 0,
        "aapl_latest_settlement": ((aapl.get("settlement_dates_sample") or
                                     [None])[0]),
        "msft_n_records": msft.get("n_items") or 0,
        "msft_latest_settlement": ((msft.get("settlement_dates_sample") or
                                     [None])[0]),
    }
    v = report["verdict"]
    if v["aapl_n_records"] > 0 and v["is_fresh_2025_plus"]:
        report["recommendation"] = (
            "🎯 GO — switch to FINRA equityShortInterestStandardized. "
            f"AAPL data available with latest settlement {v['aapl_latest_settlement']}. "
            "Anonymous access, no auth needed.")
    elif (latest_all.get("n_items") or 0) > 0 and v["aapl_n_records"] == 0:
        report["recommendation"] = (
            "PARTIAL — endpoint works but exchange-listed coverage missing. "
            "OTC-only dataset. Need different group for AAPL/MSFT (likely OAuth2).")
    else:
        report["recommendation"] = (
            "Endpoint still rejecting — need to study FINRA docs further")

    report["ended_at"] = datetime.now(timezone.utc).isoformat()
    _write(report)


def _write(report):
    out_path = REPO_ROOT / "aws" / "ops" / "reports" / "1019.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2, default=str))
    print(f"[ops 1019] report written {out_path.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print(traceback.format_exc())
        sys.exit(1)
