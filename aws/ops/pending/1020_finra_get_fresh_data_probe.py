"""
ops 1020 - Confirm FINRA equityShortInterestStandardized has fresh 2026
AAPL/MSFT data via GET with query-string filters (no auth required).

Discovery from ops 1017: GET to api.finra.org with Accept: application/json
returns rich SI data WITHOUT authentication. Only POST requires OAuth2.

This probe locks down:
- Sort order (need -settlementDate to get latest first)
- Filter syntax (settlementDate range, symbol equals)
- AAPL/MSFT/NVDA coverage with 2026 settlement dates
- Exchange-listed vs OTC market class codes for tier-1 tickers

Once confirmed, justhodl-short-interest will be patched to use this GET path
as the primary source, replacing the dead Polygon /stocks/v1/short-interest.
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
PROBE_FN = "justhodl-ops-1020-finra-get-probe-tmp"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"

lam = boto3.client("lambda", region_name=REGION)

LAMBDA_CODE = '''
import json, urllib.request, urllib.error, urllib.parse

BASE = "https://api.finra.org/data/group/otcMarket/name/equityShortInterestStandardized"

def get_json(url, timeout=25):
    try:
        req = urllib.request.Request(
            url, method="GET",
            headers={"Accept": "application/json",
                     "User-Agent": "JustHodl-FINRA-Probe/2.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read()
            total = r.headers.get("Record-Total-Total")
            page_total = r.headers.get("Record-Total-Page")
            try:
                j = json.loads(body.decode("utf-8"))
            except Exception:
                j = None
            return {
                "status": r.status,
                "total_records": total,
                "page_records": page_total,
                "n_returned": len(j) if isinstance(j, list) else None,
                "first_record": j[0] if isinstance(j, list) and j else None,
                "last_record": j[-1] if isinstance(j, list) and len(j) > 1 else None,
                "all_records_brief": [
                    {"sym": r.get("securitiesInformationProcessorSymbolIdentifier"),
                     "settle": r.get("settlementDate"),
                     "si": r.get("currentShortPositionQuantity"),
                     "dtc": r.get("daysToCoverQuantity"),
                     "mkt": r.get("marketClassCode"),
                     "exch": r.get("issuerServicesGroupExchangeCode")}
                    for r in (j if isinstance(j, list) else [])[:25]
                ] if isinstance(j, list) else None,
            }
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read()[:400].decode("utf-8", errors="replace")
        except Exception:
            pass
        return {"status": e.code, "error": str(e)[:150], "body": body}
    except Exception as e:
        return {"status": None, "error": str(e)[:300]}


def handler(event, ctx):
    out = {}
    
    # 1. Latest records overall (sort by settlementDate desc, limit 5)
    out["latest_5_any"] = get_json(
        f"{BASE}?limit=5&sortFields=-settlementDate")
    
    # 2. AAPL — sort latest first
    out["AAPL_latest_5"] = get_json(
        f"{BASE}?limit=5&sortFields=-settlementDate"
        f"&securitiesInformationProcessorSymbolIdentifier=AAPL")
    
    # 3. MSFT
    out["MSFT_latest_5"] = get_json(
        f"{BASE}?limit=5&sortFields=-settlementDate"
        f"&securitiesInformationProcessorSymbolIdentifier=MSFT")
    
    # 4. NVDA
    out["NVDA_latest_3"] = get_json(
        f"{BASE}?limit=3&sortFields=-settlementDate"
        f"&securitiesInformationProcessorSymbolIdentifier=NVDA")
    
    # 5. 2026 settlement only (any ticker) — confirms data freshness
    out["any_settled_2026"] = get_json(
        f"{BASE}?limit=10&sortFields=-settlementDate"
        f"&settlementDate=2026-05-15")
    
    # 6. 2026 May any settlement (URL-encode comma for range)
    out["may_2026_window"] = get_json(
        f"{BASE}?limit=5&sortFields=-settlementDate"
        f"&dateRangeFilters=" + urllib.parse.quote(json.dumps([{
            "fieldName": "settlementDate",
            "startDate": "2026-05-01",
            "endDate": "2026-05-31"}])))
    
    # 7. Market class code distribution - is it OTC only or includes LISTED?
    out["distinct_market_classes_via_AAPL"] = get_json(
        f"{BASE}?limit=20"
        f"&securitiesInformationProcessorSymbolIdentifier=AAPL")
    
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
        FunctionName=PROBE_FN, Runtime="python3.12", Role=ROLE_ARN,
        Handler="lambda_function.handler",
        Code={"ZipFile": zip_bytes},
        Timeout=120, MemorySize=256)
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

    # Score
    pr = report.get("probe_result", {})
    aapl_records = (pr.get("AAPL_latest_5") or {}).get("all_records_brief") or []
    msft_records = (pr.get("MSFT_latest_5") or {}).get("all_records_brief") or []
    latest_overall = (pr.get("latest_5_any") or {}).get("all_records_brief") or []

    report["summary"] = {
        "AAPL_n_returned": len(aapl_records),
        "AAPL_latest_settlement": (aapl_records[0]["settle"]
                                    if aapl_records else None),
        "AAPL_latest_si": (aapl_records[0]["si"]
                            if aapl_records else None),
        "AAPL_latest_dtc": (aapl_records[0]["dtc"]
                             if aapl_records else None),
        "AAPL_market_class": (aapl_records[0]["mkt"]
                               if aapl_records else None),
        "MSFT_n_returned": len(msft_records),
        "MSFT_latest_settlement": (msft_records[0]["settle"]
                                    if msft_records else None),
        "overall_latest_settlement_any_ticker": (
            latest_overall[0]["settle"] if latest_overall else None),
        "overall_latest_top_5_tickers": [r["sym"]
                                          for r in latest_overall[:5]],
    }

    sc = {
        "AAPL_data_present": len(aapl_records) > 0,
        "AAPL_settlement_in_2024_plus":
            (aapl_records[0]["settle"] if aapl_records else "")[:4] >= "2024",
        "MSFT_data_present": len(msft_records) > 0,
        "MSFT_settlement_in_2024_plus":
            (msft_records[0]["settle"] if msft_records else "")[:4] >= "2024",
        "overall_dataset_has_2026":
            (latest_overall[0]["settle"]
             if latest_overall else "")[:4] >= "2026",
    }
    sc["all_pass"] = all(sc.values())
    report["scorecard"] = sc

    report["ended_at"] = datetime.now(timezone.utc).isoformat()
    _write(report)


def _write(report):
    out_path = REPO_ROOT / "aws" / "ops" / "reports" / "1020.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2, default=str))
    print(f"[ops 1020] report written {out_path.relative_to(REPO_ROOT)}")
    print(json.dumps(report.get("scorecard", {}), indent=2))


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print(traceback.format_exc())
        sys.exit(1)
