"""
ops 982 - credit-equity-divergence focused post-mortem
======================================================

After ops 981 confirmed the engine STILL writes 'insufficient data: HYG=0 SPY=0'
post my /light->/full fix in 6d0e96e9, we need to determine:

  A. Did CI actually deploy the new code? (CodeSha256, LastModified)
  B. Does FMP /stable/historical-price-eod/full return data for HYG/SPY at all?
  C. Are HYG/SPY available on a different /stable/ endpoint?

This script:
  1. lambda.get_function -> CodeSha256, LastModified, Runtime, env subset
  2. Direct urllib probe of FMP /stable/quote, /stable/historical-price-eod/full,
     /stable/historical-price-eod/light for HYG, SPY, LQD, EMB
     (no auth except FMP_KEY in query string)
  3. Force lambda.invoke and capture full response body
  4. Read fresh S3 output

Writes aws/ops/reports/982.json.
"""
import json
import os
import sys
import time
import traceback
import urllib.parse
import urllib.request
import urllib.error
from pathlib import Path

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

REGION = "us-east-1"
S3_BUCKET = "justhodl-dashboard-live"
FN = "justhodl-credit-equity-divergence"

# FMP key is set in Lambda env; for the script, read from env
# (the run-ops workflow uses lambda-execution-role + GH secrets but does
#  NOT expose FMP_KEY; we'll read it from the Lambda env via get_function).

REPO_ROOT = Path(__file__).resolve().parents[3]
REPORT_PATH = REPO_ROOT / "aws" / "ops" / "reports" / "982.json"
REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)


def http_get(url, timeout=30):
    req = urllib.request.Request(url, headers={"User-Agent": "justhodl-ops-982/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return {"status": r.status, "body": r.read().decode("utf-8", errors="replace"),
                    "content_type": r.headers.get("Content-Type", "")}
    except urllib.error.HTTPError as e:
        return {"status": e.code, "body": e.read().decode("utf-8", errors="replace")[:1000],
                "error": "HTTPError"}
    except Exception as e:
        return {"status": None, "error": str(e)[:300]}


def main():
    report = {"started_at": int(time.time())}
    try:
        lambda_c = boto3.client("lambda", region_name=REGION,
                                config=Config(read_timeout=300, connect_timeout=10))
        s3 = boto3.client("s3", region_name=REGION)

        # 1. Lambda meta
        cfg = lambda_c.get_function(FunctionName=FN)
        c = cfg["Configuration"]
        report["lambda_meta"] = {
            "function_name": c.get("FunctionName"),
            "runtime": c.get("Runtime"),
            "code_sha256": c.get("CodeSha256"),
            "code_size": c.get("CodeSize"),
            "last_modified": c.get("LastModified"),
            "memory": c.get("MemorySize"),
            "timeout": c.get("Timeout"),
            "state": c.get("State"),
            "last_update_status": c.get("LastUpdateStatus"),
            "version": c.get("Version"),
            "env_keys": sorted(list((c.get("Environment", {}) or {}).get("Variables", {}).keys())),
        }
        fmp_key = (c.get("Environment", {}) or {}).get("Variables", {}).get("FMP_KEY", "")
        report["lambda_meta"]["fmp_key_present"] = bool(fmp_key)
        report["lambda_meta"]["fmp_key_tail"] = fmp_key[-6:] if fmp_key else None

        # 2. FMP probes (direct from runner)
        if fmp_key:
            probes = {}
            for sym in ["HYG", "SPY", "LQD", "EMB", "VIX", "^VIX"]:
                qs = urllib.parse.quote_plus(sym)
                for ep_name, ep_url in [
                    ("quote",            f"https://financialmodelingprep.com/stable/quote?symbol={qs}&apikey={fmp_key}"),
                    ("eod_full",         f"https://financialmodelingprep.com/stable/historical-price-eod/full?symbol={qs}&apikey={fmp_key}"),
                    ("eod_light",        f"https://financialmodelingprep.com/stable/historical-price-eod/light?symbol={qs}&apikey={fmp_key}"),
                ]:
                    r = http_get(ep_url, timeout=15)
                    rec = {"status": r.get("status"), "ct": r.get("content_type")}
                    if r.get("body"):
                        body = r["body"]
                        rec["len"] = len(body)
                        rec["preview"] = body[:300]
                        try:
                            parsed = json.loads(body)
                            if isinstance(parsed, list):
                                rec["type"] = "list"
                                rec["count"] = len(parsed)
                                if parsed:
                                    rec["first_keys"] = sorted(list(parsed[0].keys()))[:10] if isinstance(parsed[0], dict) else None
                                    rec["first_record"] = parsed[0] if isinstance(parsed[0], dict) else None
                            elif isinstance(parsed, dict):
                                rec["type"] = "dict"
                                rec["keys"] = sorted(list(parsed.keys()))[:15]
                                hist = parsed.get("historical") or parsed.get("data")
                                if hist:
                                    rec["hist_count"] = len(hist)
                                    rec["hist_first"] = hist[0] if isinstance(hist[0], dict) else None
                        except Exception as e:
                            rec["parse_err"] = str(e)[:100]
                    elif r.get("error"):
                        rec["error"] = r["error"]
                    probes[f"{sym}__{ep_name}"] = rec
                    time.sleep(0.3)  # FMP rate-limit politeness
            report["fmp_probes"] = probes

        # 3. Force invoke
        try:
            inv = lambda_c.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
            body = inv["Payload"].read().decode("utf-8", errors="ignore")
            report["invoke"] = {
                "status_code": inv.get("StatusCode"),
                "function_error": inv.get("FunctionError"),
                "log_result": inv.get("LogResult"),
                "body": body,
            }
        except Exception as e:
            report["invoke"] = {"error": str(e)[:300]}

        time.sleep(3)

        # 4. Fresh S3
        try:
            body = s3.get_object(Bucket=S3_BUCKET, Key="data/credit-equity-divergence.json")["Body"].read()
            head = s3.head_object(Bucket=S3_BUCKET, Key="data/credit-equity-divergence.json")
            report["s3"] = {
                "size": head["ContentLength"],
                "last_modified": head["LastModified"].isoformat(),
                "body": json.loads(body),
            }
        except Exception as e:
            report["s3"] = {"error": str(e)[:300]}

    except Exception as e:
        report["fatal"] = str(e)
        report["traceback"] = traceback.format_exc()
    finally:
        REPORT_PATH.write_text(json.dumps(report, indent=2, default=str))
        print(json.dumps(report, indent=2, default=str)[:8000])


if __name__ == "__main__":
    main()
