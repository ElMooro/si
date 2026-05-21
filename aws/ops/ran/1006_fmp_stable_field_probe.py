"""
ops 1006 - Probe FMP /stable endpoint response shapes to identify correct
field names for PE, PB, ROIC, GrossMargin (needed to fix #7 Predictability
valuation bucket + #8 Smart Beta value+quality factors).

Probes:
- /stable/quote?symbol=AAPL
- /stable/key-metrics-ttm?symbol=AAPL
- /stable/ratios-ttm?symbol=AAPL
- /stable/key-metrics?symbol=AAPL&limit=1 (annual)
- /stable/ratios?symbol=AAPL&limit=1 (annual)

Uses justhodl-fmp-probe-temp Lambda created on the fly.
"""
import json
import sys
import time
import traceback
import zipfile
import io
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

REPO_ROOT = Path(__file__).resolve().parents[3]
REGION = "us-east-1"
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"
TEMP_FN = "justhodl-fmp-stable-probe-temp"

cfg = Config(read_timeout=300, connect_timeout=10, retries={"max_attempts": 0})
lam = boto3.client("lambda", region_name=REGION, config=cfg)

PROBE_CODE = '''
import os, json, urllib.request, urllib.error

FMP_KEY = os.environ.get("FMP_KEY", "")
BASE = "https://financialmodelingprep.com/stable"

def http(url):
    try:
        with urllib.request.urlopen(url, timeout=20) as r:
            return json.loads(r.read().decode("utf-8", errors="ignore"))
    except urllib.error.HTTPError as e:
        return {"_error": f"HTTP {e.code}", "_url_suffix": url.split("/stable/")[-1].split("?")[0]}
    except Exception as e:
        return {"_error": str(e)[:200]}

def lambda_handler(event, context):
    sym = "AAPL"
    probes = {
        "quote": http(f"{BASE}/quote?symbol={sym}&apikey={FMP_KEY}"),
        "key_metrics_ttm": http(f"{BASE}/key-metrics-ttm?symbol={sym}&apikey={FMP_KEY}"),
        "ratios_ttm": http(f"{BASE}/ratios-ttm?symbol={sym}&apikey={FMP_KEY}"),
        "key_metrics_annual": http(f"{BASE}/key-metrics?symbol={sym}&limit=1&apikey={FMP_KEY}"),
        "ratios_annual": http(f"{BASE}/ratios?symbol={sym}&limit=1&apikey={FMP_KEY}"),
    }
    # Compress to just key names + a few sample values per endpoint
    summary = {}
    for name, resp in probes.items():
        if isinstance(resp, list) and resp:
            row = resp[0] if isinstance(resp[0], dict) else {}
            summary[name] = {
                "status": "list",
                "n": len(resp),
                "all_keys": sorted(list(row.keys())),
                "pe_candidates": {k: row.get(k) for k in row
                                   if "pe" in k.lower() or "earning" in k.lower()},
                "pb_candidates": {k: row.get(k) for k in row
                                   if ("pb" in k.lower() or "book" in k.lower())
                                   and "ebitda" not in k.lower()},
                "roic_candidates": {k: row.get(k) for k in row
                                     if "roic" in k.lower() or "roe" in k.lower()
                                     or "roa" in k.lower()},
                "margin_candidates": {k: row.get(k) for k in row
                                       if "margin" in k.lower() or
                                       "gross" in k.lower()},
            }
        elif isinstance(resp, dict):
            summary[name] = {"status": "dict_or_error", "raw": resp}
        else:
            summary[name] = {"status": "empty"}
    return {"statusCode": 200, "body": json.dumps(summary)}
'''


def build_zip(code):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as z:
        info = zipfile.ZipInfo("lambda_function.py")
        info.external_attr = 0o644 << 16
        z.writestr(info, code)
    return buf.getvalue()


def get_fmp_key_from_donor():
    """Pull FMP_KEY from justhodl-starmine env."""
    try:
        c = lam.get_function_configuration(FunctionName="justhodl-starmine")
        env = c.get("Environment", {}).get("Variables", {})
        return env.get("FMP_KEY", "")
    except Exception as e:
        return ""


def create_or_update_lambda(zip_bytes, fmp_key):
    try:
        lam.delete_function(FunctionName=TEMP_FN)
        time.sleep(3)
    except lam.exceptions.ResourceNotFoundException:
        pass
    except Exception:
        pass
    lam.create_function(
        FunctionName=TEMP_FN, Runtime="python3.12", Role=ROLE,
        Handler="lambda_function.lambda_handler",
        Code={"ZipFile": zip_bytes}, Timeout=120, MemorySize=512,
        Environment={"Variables": {"FMP_KEY": fmp_key}},
        Description="Pro Pack v3 #1006 FMP /stable probe (temp)",
        Publish=True)
    # wait active
    for _ in range(40):
        c = lam.get_function(FunctionName=TEMP_FN)["Configuration"]
        if (c.get("State") == "Active" and
                c.get("LastUpdateStatus") == "Successful"):
            return True
        time.sleep(3)
    return False


def invoke_and_get():
    r = lam.invoke(FunctionName=TEMP_FN, InvocationType="RequestResponse",
                   Payload=b'{}')
    raw = r["Payload"].read()
    body = json.loads(raw.decode("utf-8"))
    if isinstance(body.get("body"), str):
        try:
            body["body"] = json.loads(body["body"])
        except Exception:
            pass
    return {"function_error": r.get("FunctionError"), "payload": body}


def delete_temp():
    try:
        lam.delete_function(FunctionName=TEMP_FN)
    except Exception:
        pass


def main():
    report = {"started_at": datetime.now(timezone.utc).isoformat()}
    try:
        fmp_key = get_fmp_key_from_donor()
        report["fmp_key_inherited"] = bool(fmp_key)
        if not fmp_key:
            report["error"] = "FMP_KEY not found in justhodl-starmine env"
            return

        zip_bytes = build_zip(PROBE_CODE)
        ok = create_or_update_lambda(zip_bytes, fmp_key)
        report["lambda_created"] = ok
        if not ok:
            report["error"] = "lambda create failed"
            return

        inv = invoke_and_get()
        report["function_error"] = inv.get("function_error")
        report["probe_result"] = inv.get("payload", {}).get("body")
    finally:
        delete_temp()
        report["ended_at"] = datetime.now(timezone.utc).isoformat()
        out_path = REPO_ROOT / "aws" / "ops" / "reports" / "1006.json"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(report, indent=2, default=str))
        print(f"[ops 1006] report written {out_path.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print(traceback.format_exc())
        sys.exit(1)
