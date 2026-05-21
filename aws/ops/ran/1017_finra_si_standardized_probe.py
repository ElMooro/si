"""
ops 1017 - Probe FINRA equityShortInterestStandardized (the correct
post-April-2021 dataset name, replacing the deprecated equityShortInterest
per FINRA OTC Transparency API Changes announcement).

ops 1016 used the deprecated name and got 400. This probe targets:
- /data/group/otcMarket/name/equityShortInterestStandardized
- /data/group/equityMarket/name/equityShortInterestStandardized
- Modern lowercased dataset variants
- GET with limit query param (alt to POST payload)
- Both JSON and text/plain Accept headers

Pattern from working public Python example (Feb 2025):
  url = 'https://api.finra.org/data/group/{group}/name/{dataset}'
  headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
  POST with json={"limit": 5000}
  → returns data anonymously, no auth needed for OTC equity datasets
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
PROBE_FN = "justhodl-ops-1017-finra-si-v2-probe-tmp"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"

lam = boto3.client("lambda", region_name=REGION)

LAMBDA_CODE = '''
import json, urllib.request, urllib.error

def call(method, url, payload=None, accept="application/json", timeout=20):
    try:
        data = json.dumps(payload).encode("utf-8") if payload is not None else None
        headers = {"Accept": accept,
                   "User-Agent": "JustHodl-FINRA-SI-Probe-v2/1.0"}
        if payload is not None:
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(url, data=data, method=method,
                                      headers=headers)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read()
            ct = r.headers.get("Content-Type", "")
            rec_total = r.headers.get("Record-Total-Total") or r.headers.get(
                "Record-Total")
            text_sample = body[:1000].decode("utf-8", errors="replace")
            j = None
            try:
                j = json.loads(body.decode("utf-8"))
            except Exception:
                pass
            n_items = None
            first_record = None
            if isinstance(j, list):
                n_items = len(j)
                if j and isinstance(j[0], dict):
                    first_record = j[0]
            elif isinstance(j, dict):
                if "data" in j:
                    n_items = len(j["data"]) if isinstance(j.get("data"), list) else None
                    if j.get("data") and isinstance(j["data"][0], dict):
                        first_record = j["data"][0]
            return {
                "status": r.status,
                "content_type": ct,
                "record_total_header": rec_total,
                "n_bytes": len(body),
                "is_json": j is not None,
                "n_items": n_items,
                "first_record": first_record,
                "sample_first_1000b": text_sample if not j else None,
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
    
    base = "https://api.finra.org/data/group"
    
    # The corrected modern dataset name
    target_combos = [
        ("otcMarket", "equityShortInterestStandardized"),
        ("equityMarket", "equityShortInterestStandardized"),
        ("otc", "equityShortInterestStandardized"),
        # Standardized variants - lowercase first letter is FINRA convention
        ("otcMarket", "equityShortInterest"),       # deprecated but maybe still callable
        ("otcMarket", "consolidatedShortInterest"),
        ("equityMarket", "shortInterest"),
    ]
    
    # Test 1: POST with simple limit payload (per Feb-2025 working example)
    for g, d in target_combos:
        url = f"{base}/{g}/name/{d}"
        key = f"POST_{g}_{d}"
        out[key] = call("POST", url,
                         payload={"limit": 5,
                                   "sortFields": ["-settlementDate"]})
    
    # Test 2: AAPL filter on the most likely winner
    aapl_payload = {
        "limit": 5,
        "compareFilters": [
            {"compareType": "EQUAL",
             "fieldName": "issueSymbolIdentifier",
             "fieldValue": "AAPL"}
        ],
        "sortFields": ["-settlementDate"]
    }
    out["POST_otcMarket_equityShortInterestStandardized_AAPL"] = call(
        "POST",
        f"{base}/otcMarket/name/equityShortInterestStandardized",
        payload=aapl_payload)
    
    # Test 3: GET with text/plain (FINRA-recommended for equity datasets)
    out["GET_otcMarket_equityShortInterestStandardized_text"] = call(
        "GET",
        f"{base}/otcMarket/name/equityShortInterestStandardized?limit=3",
        accept="text/plain")
    
    # Test 4: GET with JSON
    out["GET_otcMarket_equityShortInterestStandardized_json"] = call(
        "GET",
        f"{base}/otcMarket/name/equityShortInterestStandardized?limit=3",
        accept="application/json")
    
    # Test 5: Try the dataset discovery endpoint
    out["GET_metadata_datasets"] = call(
        "GET",
        "https://api.finra.org/metadata/group/otcMarket",
        accept="application/json")
    
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

    # Summarize
    pr = report.get("probe_result", {})
    winners = []
    for name, res in pr.items():
        if not isinstance(res, dict):
            continue
        if res.get("status") == 200:
            n = res.get("n_items") or 0
            first = res.get("first_record") or {}
            sd = first.get("settlementDate") if isinstance(first, dict) else None
            sym = first.get("issueSymbolIdentifier") if isinstance(first, dict) else None
            winners.append({
                "endpoint": name,
                "n_items": n,
                "settlement_date": sd,
                "symbol": sym,
                "is_2024_plus": (sd or "")[:4] >= "2024" if sd else False,
                "fields": (list(first.keys())[:20]
                           if isinstance(first, dict) else None),
            })
    winners.sort(key=lambda x: (not (x.get("settlement_date") or ""),
                                  -(x["n_items"] or 0)))
    report["winners"] = winners
    report["best"] = winners[0] if winners else None
    report["ended_at"] = datetime.now(timezone.utc).isoformat()
    _write(report)


def _write(report):
    out_path = REPO_ROOT / "aws" / "ops" / "reports" / "1017.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2, default=str))
    print(f"[ops 1017] report written {out_path.relative_to(REPO_ROOT)}")
    print(f"  best: {report.get('best')}")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print(traceback.format_exc())
        sys.exit(1)
