"""
ops 1025 - Remediation: re-verify Engine #7 with CORRECT Lambda name +
capture Engine #3's full runtime traceback.

ops 1024 mis-named #7 as 'corr-break-trade-router' but actual deployed name
is 'correlation-break-trade-router' (output 'correlation-break-trades.json'
not 'corr-break-trade-router.json').

ops 1024 reported #3 'forced-selling-bounce' as function_error=Unhandled but
didn't capture the exception body. This ops invokes it again with
LogType='Tail' to get base64-encoded CloudWatch tail right in the response.
"""
import base64
import json
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

REPO_ROOT = Path(__file__).resolve().parents[3]
REGION = "us-east-1"
S3_BUCKET = "justhodl-dashboard-live"

cfg = Config(read_timeout=120)
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION)


def invoke_with_tail(name):
    """Invoke + capture LogType=Tail (last 4KB of CloudWatch log)."""
    try:
        r = lam.invoke(FunctionName=name,
                       InvocationType="RequestResponse",
                       LogType="Tail",
                       Payload=b"{}")
        payload = r["Payload"].read().decode("utf-8", errors="replace")
        log_tail = ""
        if r.get("LogResult"):
            try:
                log_tail = base64.b64decode(r["LogResult"]).decode(
                    "utf-8", errors="replace")
            except Exception:
                log_tail = "<could not decode>"
        return {
            "status_code": r.get("StatusCode"),
            "function_error": r.get("FunctionError"),
            "payload": payload[:2000],
            "log_tail": log_tail[-3000:],  # last 3K of logs
        }
    except Exception as e:
        return {"error": str(e)[:400],
                "traceback": traceback.format_exc()[:2000]}


def s3_check(key):
    try:
        h = s3.head_object(Bucket=S3_BUCKET, Key=key)
        now = datetime.now(timezone.utc)
        lm = h["LastModified"]
        return {"present": True, "size_bytes": h.get("ContentLength"),
                "age_seconds": int((now - lm).total_seconds())}
    except Exception as e:
        return {"present": False, "error": str(e)[:150]}


def get_function_brief(name):
    try:
        c = lam.get_function(FunctionName=name)["Configuration"]
        return {"exists": True, "state": c.get("State"),
                "last_modified": c.get("LastModified"),
                "code_size": c.get("CodeSize"),
                "memory_mb": c.get("MemorySize"),
                "timeout_s": c.get("Timeout")}
    except lam.exceptions.ResourceNotFoundException:
        return {"exists": False}
    except Exception as e:
        return {"exists": "unknown", "error": str(e)[:200]}


def main():
    started = time.time()
    report = {"started_at": datetime.now(timezone.utc).isoformat()}

    # ── Engine #7 (correct name) ──
    print("[ops 1025] verify #7 justhodl-correlation-break-trade-router")
    e7 = {"lambda": "justhodl-correlation-break-trade-router",
           "expected_s3": "data/correlation-break-trades.json"}
    e7["function"] = get_function_brief(e7["lambda"])
    if e7["function"].get("exists") is True:
        e7["invoke"] = invoke_with_tail(e7["lambda"])
        time.sleep(2)
    e7["s3_check"] = s3_check(e7["expected_s3"])
    # also try alternate output keys
    for alt in ["data/corr-break-trades.json",
                "data/correlation-break-trade-router.json"]:
        ck = s3_check(alt)
        if ck.get("present"):
            e7[f"s3_alt_{alt}"] = ck
    report["engine_7_recheck"] = e7

    # ── Engine #3 capture full error ──
    print("[ops 1025] debug-invoke #3 justhodl-forced-selling-bounce")
    e3 = {"lambda": "justhodl-forced-selling-bounce"}
    e3["function"] = get_function_brief(e3["lambda"])
    if e3["function"].get("exists") is True:
        e3["invoke"] = invoke_with_tail(e3["lambda"])
    report["engine_3_debug"] = e3

    # Scorecard
    sc = {}
    if e7["function"].get("exists") is True:
        sc["engine_7_lambda_exists"] = True
        sc["engine_7_invoke_clean"] = (e7.get("invoke", {}).get(
            "function_error") is None
            and e7.get("invoke", {}).get("status_code", 0) < 300)
        sc["engine_7_s3_present"] = e7["s3_check"].get("present") is True
    else:
        sc["engine_7_lambda_exists"] = False
    if e3["function"].get("exists") is True:
        sc["engine_3_lambda_exists"] = True
        sc["engine_3_invoke_clean"] = (e3.get("invoke", {}).get(
            "function_error") is None
            and e3.get("invoke", {}).get("status_code", 0) < 300)
    else:
        sc["engine_3_lambda_exists"] = False
    report["scorecard"] = sc

    report["duration_seconds"] = round(time.time() - started, 1)
    report["ended_at"] = datetime.now(timezone.utc).isoformat()

    out_path = REPO_ROOT / "aws" / "ops" / "reports" / "1025.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2, default=str))
    print(f"[ops 1025] report at {out_path.relative_to(REPO_ROOT)}")
    print(json.dumps(sc, indent=2))


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print(traceback.format_exc())
        sys.exit(1)
