"""
ops 982 - Tier-3 Comprehensive Diagnostic
==========================================

Three checks in one report so we can write proper fixes:

  1. Feeder schemas - read s3://justhodl-dashboard-live/{insider-buys-enriched,
     buyback-scanner, 13f-positions}.json and dump deep structure so we can
     write correct extractors for insider-buyback-confluence and
     13f-price-divergence.

  2. FMP /stable/ endpoint probes for the symbols vvix-vov-regime and
     credit-equity-divergence depend on:
        ^VIX, ^VVIX, ^VIX3M, ^VIX6M (vvix-vov-regime)
        HYG, SPY, LQD, EMB (credit-equity-divergence)
     For each, hit /stable/quote AND /stable/historical-price-eod/full.

  3. Invoke the 2 long-running engines (sympathetic-momentum, gap-fill-confirm)
     to confirm they actually complete now that we're past cold-start.

All from a single Lambda-deploy via the ops runner. Writes
aws/ops/reports/982.json.
"""
import json
import os
import sys
import time
import traceback
import urllib.parse
import urllib.request
from pathlib import Path

import boto3
from botocore.config import Config

REGION = "us-east-1"
S3_BUCKET = "justhodl-dashboard-live"

# FMP key from environment (provided by Lambda runner via secrets)
FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")

FEEDERS = [
    "data/insider-buys-enriched.json",
    "data/buyback-scanner.json",
    "data/13f-positions.json",
]

VIX_SYMBOLS = ["^VIX", "^VVIX", "^VIX3M", "^VIX6M"]
CREDIT_SYMBOLS = ["HYG", "SPY", "LQD", "EMB"]

LONG_RUNNING_ENGINES = ["justhodl-sympathetic-momentum", "justhodl-gap-fill-confirm"]

REPO_ROOT = Path(__file__).resolve().parents[3]
REPORTS_DIR = REPO_ROOT / "aws" / "ops" / "reports"
REPORTS_DIR.mkdir(parents=True, exist_ok=True)
REPORT_PATH = REPORTS_DIR / "982.json"


def http_get(url, timeout=20):
    req = urllib.request.Request(url, headers={"User-Agent": "justhodl/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "ignore")


def deep_schema(obj, depth=0, max_depth=4, max_list=3):
    """Recursively describe structure without dumping everything."""
    if depth >= max_depth:
        return {"_truncated": True, "type": type(obj).__name__}
    if isinstance(obj, dict):
        keys = list(obj.keys())[:25]
        out = {"_type": "dict", "_n_keys": len(obj), "_keys": keys}
        # Recurse on first few values
        for k in keys[:8]:
            v = obj[k]
            if isinstance(v, (dict, list)):
                out[f"_sample_{k}"] = deep_schema(v, depth + 1, max_depth, max_list)
            else:
                preview = str(v)[:80]
                out[f"_sample_{k}"] = {"_type": type(v).__name__, "_value": preview}
        return out
    if isinstance(obj, list):
        out = {"_type": "list", "_len": len(obj)}
        if obj:
            out["_first"] = deep_schema(obj[0], depth + 1, max_depth, max_list)
            if len(obj) > 1:
                out["_last"] = deep_schema(obj[-1], depth + 1, max_depth, max_list)
        return out
    return {"_type": type(obj).__name__, "_value": str(obj)[:120]}


def probe_feeder(s3_c, key):
    try:
        obj = s3_c.get_object(Bucket=S3_BUCKET, Key=key)
        raw = obj["Body"].read()
        last_modified = str(obj["LastModified"])
        size = len(raw)
        try:
            data = json.loads(raw)
            return {
                "ok": True,
                "size": size,
                "last_modified": last_modified,
                "schema": deep_schema(data),
            }
        except json.JSONDecodeError as e:
            return {
                "ok": False,
                "size": size,
                "last_modified": last_modified,
                "parse_error": str(e),
                "raw_preview": raw[:500].decode("utf-8", "replace"),
            }
    except Exception as e:
        return {"ok": False, "error": str(e)}


def probe_fmp(symbol):
    """Test both /stable/quote and /stable/historical-price-eod/full."""
    q = urllib.parse.quote_plus(symbol)
    out = {"symbol": symbol}

    # Endpoint 1: /stable/quote
    url1 = f"https://financialmodelingprep.com/stable/quote?symbol={q}&apikey={FMP_KEY}"
    try:
        raw = http_get(url1)
        try:
            d = json.loads(raw)
            if isinstance(d, list) and d:
                out["quote"] = {"ok": True, "n": len(d), "first": d[0]}
            elif isinstance(d, dict):
                if d.get("Error Message") or d.get("error"):
                    out["quote"] = {"ok": False, "error": d.get("Error Message") or d.get("error")}
                else:
                    out["quote"] = {"ok": True, "data": d}
            else:
                out["quote"] = {"ok": False, "raw_preview": raw[:200]}
        except json.JSONDecodeError:
            out["quote"] = {"ok": False, "raw_preview": raw[:300]}
    except Exception as e:
        out["quote"] = {"ok": False, "error": str(e)}

    # Endpoint 2: /stable/historical-price-eod/full
    url2 = (f"https://financialmodelingprep.com/stable/historical-price-eod/full"
            f"?symbol={q}&apikey={FMP_KEY}")
    try:
        raw = http_get(url2)
        try:
            d = json.loads(raw)
            if isinstance(d, dict):
                hist = d.get("historical") or d.get("data") or []
                if d.get("Error Message") or d.get("error"):
                    out["history"] = {"ok": False, "error": d.get("Error Message") or d.get("error")}
                elif hist:
                    out["history"] = {"ok": True, "n_bars": len(hist), "first_bar": hist[0]}
                else:
                    out["history"] = {"ok": False, "empty": True, "keys": list(d.keys())[:10]}
            elif isinstance(d, list):
                if d:
                    out["history"] = {"ok": True, "n_bars": len(d), "first_bar": d[0]}
                else:
                    out["history"] = {"ok": False, "empty_list": True}
            else:
                out["history"] = {"ok": False, "raw_preview": raw[:200]}
        except json.JSONDecodeError:
            out["history"] = {"ok": False, "raw_preview": raw[:300]}
    except Exception as e:
        out["history"] = {"ok": False, "error": str(e)}

    # Endpoint 3: /stable/historical-price-eod/light (alternative)
    url3 = (f"https://financialmodelingprep.com/stable/historical-price-eod/light"
            f"?symbol={q}&apikey={FMP_KEY}")
    try:
        raw = http_get(url3)
        try:
            d = json.loads(raw)
            if isinstance(d, dict):
                hist = d.get("historical") or d.get("data") or []
                if hist:
                    out["history_light"] = {"ok": True, "n_bars": len(hist), "first_bar": hist[0]}
                elif d.get("Error Message"):
                    out["history_light"] = {"ok": False, "error": d.get("Error Message")}
                else:
                    out["history_light"] = {"ok": False, "empty": True, "keys": list(d.keys())[:10]}
            elif isinstance(d, list):
                if d:
                    out["history_light"] = {"ok": True, "n_bars": len(d), "first_bar": d[0]}
                else:
                    out["history_light"] = {"ok": False, "empty_list": True}
        except json.JSONDecodeError:
            out["history_light"] = {"ok": False, "raw_preview": raw[:300]}
    except Exception as e:
        out["history_light"] = {"ok": False, "error": str(e)}

    return out


def warm_invoke(lambda_c, fn):
    """Invoke with extended read_timeout for long-running engines."""
    try:
        long_cfg = Config(read_timeout=900, connect_timeout=10,
                          retries={"max_attempts": 1})
        lambda_long = boto3.client("lambda", region_name=REGION, config=long_cfg)
        t0 = time.time()
        r = lambda_long.invoke(FunctionName=fn, InvocationType="RequestResponse",
                               Payload=b"{}")
        elapsed = round(time.time() - t0, 1)
        payload = r["Payload"].read().decode("utf-8", "replace")
        return {
            "ok": (r["StatusCode"] == 200 and not r.get("FunctionError")),
            "status_code": r["StatusCode"],
            "function_error": r.get("FunctionError"),
            "elapsed_s": elapsed,
            "body_preview": payload[:400],
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


def main():
    report = {
        "ops_id": "982",
        "purpose": "Comprehensive Tier-3 diagnostic: feeders + FMP probes + warm invokes",
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "feeders": {},
        "fmp_probes": {},
        "warm_invokes": {},
    }

    s3_c = boto3.client("s3", region_name=REGION)
    lambda_c = boto3.client("lambda", region_name=REGION)

    try:
        # 1. Feeder schemas
        for key in FEEDERS:
            print(f"=== feeder {key} ===", flush=True)
            report["feeders"][key] = probe_feeder(s3_c, key)

        # 2. FMP probes
        for sym in VIX_SYMBOLS + CREDIT_SYMBOLS:
            print(f"=== fmp {sym} ===", flush=True)
            report["fmp_probes"][sym] = probe_fmp(sym)
            time.sleep(0.3)  # be polite to FMP

        # 3. Warm invokes
        for fn in LONG_RUNNING_ENGINES:
            print(f"=== warm invoke {fn} ===", flush=True)
            report["warm_invokes"][fn] = warm_invoke(lambda_c, fn)

    except Exception as e:
        report["fatal_error"] = str(e)
        report["fatal_traceback"] = traceback.format_exc()
    finally:
        report["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        with open(REPORT_PATH, "w") as f:
            json.dump(report, f, indent=2, default=str)
        print("\n" + "=" * 60)
        print("OPS 981 REPORT")
        print("=" * 60)
        print(json.dumps(report, indent=2, default=str)[:8000])


if __name__ == "__main__":
    main()
