"""
ops 1026 - Final verify all 9 confluence engines after fixes.

Updated #7 to correct name 'justhodl-correlation-break-trade-router'.
Verifies #3 forced-selling-bounce TypeError fix landed + invokes clean.
"""
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

cfg = Config(read_timeout=180)
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION)

ENGINES = [
    {"num": 1, "lambda": "justhodl-sequence-alpha-detector",
     "s3": "data/sequence-alpha.json"},
    {"num": 2, "lambda": "justhodl-quality-on-sale",
     "s3": "data/quality-on-sale.json"},
    {"num": 3, "lambda": "justhodl-forced-selling-bounce",
     "s3": "data/forced-selling-bounce.json"},
    {"num": 4, "lambda": "justhodl-regime-conditional-router",
     "s3": "data/regime-conditional-router.json"},
    {"num": 5, "lambda": "justhodl-ma-target-predictor",
     "s3": "data/ma-target-predictor.json"},
    {"num": 6, "lambda": "justhodl-consensus-bottom",
     "s3": "data/consensus-bottom.json"},
    {"num": 7, "lambda": "justhodl-correlation-break-trade-router",
     "s3": "data/correlation-break-trades.json"},
    {"num": 8, "lambda": "justhodl-powell-pivot",
     "s3": "data/powell-pivot.json"},
    {"num": 9, "lambda": "justhodl-earnings-tone-velocity",
     "s3": "data/earnings-tone-velocity.json"},
]


def main():
    started = time.time()
    report = {"started_at": datetime.now(timezone.utc).isoformat()}

    n_lambda_ok = 0
    n_invoke_clean = 0
    n_s3_fresh = 0
    engines = []

    for eng in ENGINES:
        e = {"num": eng["num"], "lambda": eng["lambda"]}
        # Lambda exists?
        try:
            c = lam.get_function(FunctionName=eng["lambda"])["Configuration"]
            e["state"] = c.get("State")
            e["last_modified"] = c.get("LastModified")
            e["lambda_ok"] = True
            n_lambda_ok += 1
        except Exception as ex:
            e["lambda_ok"] = False
            e["lambda_err"] = str(ex)[:120]
            engines.append(e)
            continue
        # Invoke
        try:
            r = lam.invoke(FunctionName=eng["lambda"],
                           InvocationType="RequestResponse",
                           Payload=b"{}")
            e["invoke_status"] = r.get("StatusCode")
            e["function_error"] = r.get("FunctionError")
            payload = r["Payload"].read().decode("utf-8", errors="replace")
            e["payload_preview"] = payload[:300]
            e["invoke_clean"] = (not r.get("FunctionError")
                                  and r.get("StatusCode", 0) < 300)
            if e["invoke_clean"]:
                n_invoke_clean += 1
        except Exception as ex:
            e["invoke_err"] = str(ex)[:120]
            e["invoke_clean"] = False
        time.sleep(2)
        # S3 freshness
        try:
            h = s3.head_object(Bucket=S3_BUCKET, Key=eng["s3"])
            now = datetime.now(timezone.utc)
            age = int((now - h["LastModified"]).total_seconds())
            e["s3_age_seconds"] = age
            e["s3_size_bytes"] = h.get("ContentLength")
            e["s3_present"] = True
            if age < 300:
                n_s3_fresh += 1
        except Exception as ex:
            e["s3_present"] = False
            e["s3_err"] = str(ex)[:120]
        engines.append(e)

    report["engines"] = engines
    report["scorecard"] = {
        "total_engines": len(ENGINES),
        "lambdas_deployed": n_lambda_ok,
        "fresh_invoke_clean": n_invoke_clean,
        "s3_outputs_fresh": n_s3_fresh,
        "ALL_PASS": (n_lambda_ok == 9 and n_invoke_clean == 9
                      and n_s3_fresh == 9),
    }
    report["duration_seconds"] = round(time.time() - started, 1)
    report["ended_at"] = datetime.now(timezone.utc).isoformat()

    out_path = REPO_ROOT / "aws" / "ops" / "reports" / "1026.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2, default=str))
    print(f"[ops 1026] {out_path.relative_to(REPO_ROOT)}")
    print(json.dumps(report["scorecard"], indent=2))


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print(traceback.format_exc())
        sys.exit(1)
