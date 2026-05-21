"""
ops 1016 - Probe FINRA bi-monthly equityShortInterest API for AAPL.

Per FINRA Data File Download PDF + FINRA Equity Short Interest Files page:
- Endpoint: https://api.finra.org/data/group/otcMarket/name/EquityShortInterest
- Free, POST with JSON filter payload
- Post-June-2021 includes EXCHANGE-LISTED securities (was OTC-only before)
- Bi-monthly settlement dates (15th + last day of month)

Goal: confirm AAPL+MSFT data exists with 2026 settlement dates. If yes, switch
justhodl-short-interest from dead Polygon endpoint to this FINRA API.

Tries multiple candidate group/dataset combos because docs are slightly
inconsistent about exact name post-June-2021 expansion.
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
PROBE_FN = "justhodl-ops-1016-finra-si-probe-tmp"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"

lam = boto3.client("lambda", region_name=REGION)

LAMBDA_CODE = '''
import json, urllib.request, urllib.error

def post_json(url, payload, timeout=20):
    try:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            url, data=data, method="POST",
            headers={"Content-Type": "application/json",
                     "Accept": "application/json",
                     "User-Agent": "JustHodl-FINRA-SI-Probe/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read()
            return {"status": r.status,
                    "n_bytes": len(body),
                    "json": json.loads(body.decode("utf-8")),
                    "headers_record_count": r.headers.get("Record-Total-Total")}
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read()[:500].decode("utf-8", errors="replace")
        except Exception:
            pass
        return {"status": e.code, "error": str(e)[:200],
                "body_first_500b": body}
    except Exception as e:
        return {"status": None, "error": str(e)[:300]}

def get_url(url, timeout=20):
    try:
        req = urllib.request.Request(url, method="GET",
            headers={"Accept": "application/json",
                     "User-Agent": "JustHodl-FINRA-SI-Probe/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read()
            ct = r.headers.get("Content-Type", "")
            sample = body[:800].decode("utf-8", errors="replace")
            j = None
            try:
                j = json.loads(body.decode("utf-8"))
            except Exception:
                pass
            return {"status": r.status,
                    "content_type": ct,
                    "n_bytes": len(body),
                    "sample_first_800b": sample,
                    "is_json": j is not None,
                    "n_items": (len(j) if isinstance(j, list)
                                 else (len(j.get("data", []) if isinstance(j, dict) else 0)
                                       if isinstance(j, dict) else None))}
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read()[:500].decode("utf-8", errors="replace")
        except Exception:
            pass
        return {"status": e.code, "error": str(e)[:200],
                "body_first_500b": body}
    except Exception as e:
        return {"status": None, "error": str(e)[:300]}

def handler(event, ctx):
    out = {}
    
    # 1. POST query with AAPL ticker filter - the documented pattern
    payload_aapl_recent = {
        "limit": 5,
        "compareFilters": [
            {"compareType": "EQUAL", "fieldName": "issueSymbolIdentifier",
             "fieldValue": "AAPL"}
        ],
        "sortFields": ["-settlementDate"]
    }
    out["finra_otcMarket_EquityShortInterest_AAPL"] = post_json(
        "https://api.finra.org/data/group/otcMarket/name/EquityShortInterest",
        payload_aapl_recent)
    
    # 2. Try equityMarket group (exchange-listed may live here)
    out["finra_equityMarket_EquityShortInterest_AAPL"] = post_json(
        "https://api.finra.org/data/group/equityMarket/name/EquityShortInterest",
        payload_aapl_recent)
    
    # 3. Try regShoDaily group/dataset combo
    out["finra_otcMarket_regShoDaily_AAPL"] = post_json(
        "https://api.finra.org/data/group/otcMarket/name/regShoDaily",
        payload_aapl_recent)
    
    # 4. GET metadata for groups + datasets (discovery)
    out["finra_metadata_otcMarket"] = get_url(
        "https://api.finra.org/metadata/group/otcMarket")
    out["finra_metadata_groups"] = get_url(
        "https://api.finra.org/metadata/group")
    
    # 5. Simple recent ALL settlement date probe (no ticker filter, just latest)
    payload_latest = {
        "limit": 3,
        "sortFields": ["-settlementDate"]
    }
    out["finra_otcMarket_latest_any"] = post_json(
        "https://api.finra.org/data/group/otcMarket/name/EquityShortInterest",
        payload_latest)
    
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

    # Score
    pr = report.get("probe_result", {})
    findings = {}
    for name, res in pr.items():
        if not isinstance(res, dict):
            continue
        if res.get("status") == 200:
            j = res.get("json")
            if isinstance(j, list) and j:
                first = j[0] if isinstance(j[0], dict) else {}
                findings[name] = {
                    "status": 200,
                    "n_items": len(j),
                    "first_settlement": first.get("settlementDate"),
                    "first_symbol": first.get("issueSymbolIdentifier"),
                    "first_short_share": first.get("currentShortShareNumber"),
                    "first_avg_short": first.get("averageShortShareNumber"),
                    "all_keys": list(first.keys())[:25] if first else [],
                }
            elif res.get("is_json"):
                findings[name] = {"status": 200,
                                   "n_items": res.get("n_items"),
                                   "is_json": True}
            else:
                findings[name] = {"status": 200,
                                   "sample": (res.get("sample_first_800b")
                                              or "")[:200]}
        else:
            findings[name] = {"status": res.get("status"),
                              "error": (res.get("error") or "")[:120]}
    report["findings"] = findings

    # Recommend winner
    aapl_match = pr.get("finra_otcMarket_EquityShortInterest_AAPL") or {}
    eq_match = pr.get("finra_equityMarket_EquityShortInterest_AAPL") or {}
    winner = None
    if isinstance(aapl_match.get("json"), list) and aapl_match["json"]:
        winner = ("otcMarket/EquityShortInterest",
                   aapl_match["json"][0].get("settlementDate"))
    elif isinstance(eq_match.get("json"), list) and eq_match["json"]:
        winner = ("equityMarket/EquityShortInterest",
                   eq_match["json"][0].get("settlementDate"))
    report["winner"] = winner

    report["ended_at"] = datetime.now(timezone.utc).isoformat()
    _write(report)


def _write(report):
    out_path = REPO_ROOT / "aws" / "ops" / "reports" / "1016.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2, default=str))
    print(f"[ops 1016] report written {out_path.relative_to(REPO_ROOT)}")
    print(f"  winner: {report.get('winner')}")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print(traceback.format_exc())
        sys.exit(1)
