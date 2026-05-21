"""
ops 1018 - Final FINRA equityShortInterestStandardized probe with the
correct field names + partition-key constraint.

ops 1017 confirmed:
- Endpoint URL: /data/group/otcMarket/name/equityShortInterestStandardized
- Anonymous access works (GET returns 200)
- Correct ticker field is securitiesInformationProcessorSymbolIdentifier
  (NOT issueSymbolIdentifier - that was the deprecated dataset's field)
- POST sorting requires "all partition keys" - so date filter must be included
  in the payload before sortFields is honored

This probe answers two remaining questions:
1. With correct date filter + correct field name, does it return AAPL data?
2. What's the actual latest settlement date for AAPL in 2026?
3. Does this dataset include exchange-listed names (AAPL/MSFT/NVDA) post-2021?
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
PROBE_FN = "justhodl-ops-1018-finra-final-probe-tmp"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"

lam = boto3.client("lambda", region_name=REGION)

LAMBDA_CODE = '''
import json, urllib.request, urllib.error

URL = "https://api.finra.org/data/group/otcMarket/name/equityShortInterestStandardized"

def call(method, url, payload=None, accept="application/json", timeout=30):
    try:
        data = json.dumps(payload).encode("utf-8") if payload is not None else None
        headers = {"Accept": accept,
                   "User-Agent": "JustHodl-FINRA-SI-Final/1.0"}
        if payload is not None:
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(url, data=data, method=method,
                                      headers=headers)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read()
            ct = r.headers.get("Content-Type", "")
            rec_total = r.headers.get("Record-Total-Total")
            text_sample = body[:800].decode("utf-8", errors="replace")
            j = None
            try:
                j = json.loads(body.decode("utf-8"))
            except Exception:
                pass
            n_items = None
            first = None
            settlements = []
            symbols = []
            if isinstance(j, list):
                n_items = len(j)
                if j and isinstance(j[0], dict):
                    first = j[0]
                for r0 in j[:10]:
                    if isinstance(r0, dict):
                        settlements.append(r0.get("settlementDate"))
                        symbols.append(r0.get(
                            "securitiesInformationProcessorSymbolIdentifier"))
            return {
                "status": r.status,
                "content_type": ct,
                "record_total_header": rec_total,
                "n_items": n_items,
                "first_record": first,
                "settlement_dates_sample": settlements,
                "symbols_sample": symbols,
                "sample_first_800b": text_sample if not j else None,
            }
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
    
    # Test 1: Latest data with date partition filter
    payload_latest = {
        "limit": 10,
        "compareFilters": [
            {"compareType": "GREATER_THAN_EQUAL",
             "fieldName": "settlementDate",
             "fieldValue": "2026-01-01"}
        ],
        "sortFields": ["-settlementDate"]
    }
    out["POST_latest_2026"] = call("POST", URL, payload=payload_latest)
    
    # Test 2: AAPL specifically with date partition
    payload_aapl = {
        "limit": 10,
        "compareFilters": [
            {"compareType": "GREATER_THAN_EQUAL",
             "fieldName": "settlementDate",
             "fieldValue": "2026-01-01"},
            {"compareType": "EQUAL",
             "fieldName": "securitiesInformationProcessorSymbolIdentifier",
             "fieldValue": "AAPL"}
        ]
    }
    out["POST_AAPL_2026"] = call("POST", URL, payload=payload_aapl)
    
    # Test 3: Try a wider date range to see what is actually available
    payload_wide = {
        "limit": 5,
        "compareFilters": [
            {"compareType": "GREATER_THAN_EQUAL",
             "fieldName": "settlementDate",
             "fieldValue": "2025-01-01"}
        ],
        "sortFields": ["-settlementDate"]
    }
    out["POST_latest_2025plus"] = call("POST", URL, payload=payload_wide)
    
    # Test 4: Check MSFT
    payload_msft = {
        "limit": 5,
        "compareFilters": [
            {"compareType": "GREATER_THAN_EQUAL",
             "fieldName": "settlementDate",
             "fieldValue": "2025-01-01"},
            {"compareType": "EQUAL",
             "fieldName": "securitiesInformationProcessorSymbolIdentifier",
             "fieldValue": "MSFT"}
        ]
    }
    out["POST_MSFT_2025plus"] = call("POST", URL, payload=payload_msft)
    
    # Test 5: Get the absolute latest settlement date available
    payload_pre = {
        "limit": 3,
        "compareFilters": [
            {"compareType": "GREATER_THAN_EQUAL",
             "fieldName": "settlementDate",
             "fieldValue": "2024-01-01"}
        ],
        "sortFields": ["-settlementDate"],
        "fields": ["settlementDate",
                    "securitiesInformationProcessorSymbolIdentifier"]
    }
    out["POST_max_settlement"] = call("POST", URL, payload=payload_pre)
    
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

    pr = report.get("probe_result", {})
    # Extract the decisive answer
    latest = pr.get("POST_max_settlement") or {}
    settlement_dates_seen = latest.get("settlement_dates_sample") or []
    max_sd = max(settlement_dates_seen) if settlement_dates_seen else None
    aapl = pr.get("POST_AAPL_2026") or {}
    msft = pr.get("POST_MSFT_2025plus") or {}

    report["verdict"] = {
        "endpoint_works_anonymously": (latest.get("status") == 200),
        "max_settlement_date_seen": max_sd,
        "is_fresh_2024_plus": (max_sd or "")[:4] >= "2024" if max_sd else False,
        "is_fresh_2025_plus": (max_sd or "")[:4] >= "2025" if max_sd else False,
        "aapl_returns_data": (aapl.get("n_items") or 0) > 0,
        "aapl_settlement_dates": aapl.get("settlement_dates_sample") or [],
        "msft_returns_data": (msft.get("n_items") or 0) > 0,
        "msft_settlement_dates": msft.get("settlement_dates_sample") or [],
    }
    if report["verdict"]["aapl_returns_data"] or report["verdict"][
            "msft_returns_data"]:
        report["recommendation"] = (
            "SWITCH justhodl-short-interest to use FINRA "
            "equityShortInterestStandardized — exchange-listed AAPL/MSFT "
            "available with fresh settlement dates")
    elif (latest.get("n_items") or 0) > 0:
        report["recommendation"] = (
            "Endpoint works but exchange-listed coverage uncertain — "
            "investigate alternative groups (paid OAuth2 path)")
    else:
        report["recommendation"] = "Endpoint not usable — back to paid path"

    report["ended_at"] = datetime.now(timezone.utc).isoformat()
    _write(report)


def _write(report):
    out_path = REPO_ROOT / "aws" / "ops" / "reports" / "1018.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2, default=str))
    print(f"[ops 1018] report written {out_path.relative_to(REPO_ROOT)}")
    print(f"  verdict: {json.dumps(report.get('verdict', {}), indent=2)}")
    print(f"  recommendation: {report.get('recommendation')}")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print(traceback.format_exc())
        sys.exit(1)
