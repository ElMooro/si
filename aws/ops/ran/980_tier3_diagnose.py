"""
ops 980 - Peek at the actual S3 outputs of all 6 Tier-3 engines
to diagnose: (a) credit-equity-divergence 368-byte stub, (b) timeouts.
"""
import json, sys, traceback, time
from pathlib import Path
import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

KEYS = [
    "data/vvix-vov-regime.json",
    "data/sympathetic-momentum.json",
    "data/insider-buyback-confluence.json",
    "data/gap-fill-confirm.json",
    "data/13f-price-divergence.json",
    "data/credit-equity-divergence.json",
]

REPO_ROOT = Path(__file__).resolve().parents[3]
REPORT_PATH = REPO_ROOT / "aws" / "ops" / "reports" / "980.json"
REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)

report = {"objects": {}, "started_at": int(time.time())}
try:
    s3 = boto3.client("s3", region_name=REGION)
    lam = boto3.client("lambda", region_name=REGION)
    for k in KEYS:
        rec = {}
        try:
            head = s3.head_object(Bucket=BUCKET, Key=k)
            rec["size"] = head["ContentLength"]
            rec["last_modified"] = head["LastModified"].isoformat()
            body = s3.get_object(Bucket=BUCKET, Key=k)["Body"].read()
            try:
                obj = json.loads(body)
                rec["parsed"] = True
                rec["state"] = obj.get("state")
                rec["signal_strength"] = obj.get("signal_strength")
                rec["error"] = obj.get("error")
                rec["picks_n"] = len(obj.get("picks") or obj.get("setups") or obj.get("divergences") or obj.get("warnings") or [])
                rec["keys"] = sorted(list(obj.keys()))[:30]
                rec["preview"] = json.dumps(obj)[:500]
            except Exception as e:
                rec["parsed"] = False
                rec["raw_preview"] = body[:500].decode("utf-8", errors="replace")
                rec["parse_err"] = str(e)
        except ClientError as e:
            rec["error"] = str(e)
        report["objects"][k] = rec

    # Also get last-execution duration for the 2 timeout suspects
    for fn in ["justhodl-sympathetic-momentum", "justhodl-gap-fill-confirm",
               "justhodl-credit-equity-divergence"]:
        try:
            cfg = lam.get_function_configuration(FunctionName=fn)
            report.setdefault("lambda_cfg", {})[fn] = {
                "timeout": cfg.get("Timeout"),
                "memory": cfg.get("MemorySize"),
                "last_update_status": cfg.get("LastUpdateStatus"),
                "state": cfg.get("State"),
            }
        except Exception as e:
            report.setdefault("lambda_cfg", {})[fn] = {"error": str(e)}
except Exception as e:
    report["fatal"] = str(e)
    report["traceback"] = traceback.format_exc()
finally:
    REPORT_PATH.write_text(json.dumps(report, indent=2, default=str))
    print(json.dumps(report, indent=2, default=str))
