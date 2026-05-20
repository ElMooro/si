"""
ops 983 - Patch credit-equity-divergence env from buyback-scanner secrets bundle
================================================================================

Root cause from ops 982: Lambda has no FMP_KEY in Environment.Variables.
The 5 sibling Tier-3 engines got it via deploy-lambdas.yml inherit_env=true
logic, but credit-equity-divergence's initial deploy via ops 978 missed it
(force-deploy path differs from the standard CI inherit_env).

Fix: read the standard secrets bundle from justhodl-buyback-scanner (canonical
donor per deploy-lambdas.yml), merge it into credit-equity-divergence's env,
update the function, invoke, and verify S3.

Idempotent. Safe to re-run. Standard pattern.
"""
import json
import time
import traceback
from pathlib import Path

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

REGION = "us-east-1"
S3_BUCKET = "justhodl-dashboard-live"
TARGET = "justhodl-credit-equity-divergence"
DONOR = "justhodl-buyback-scanner"

# Standard secrets bundle per deploy-lambdas.yml
STANDARD_KEYS = [
    "FMP_KEY", "FRED_KEY", "POLYGON_KEY", "ALPHA_VANTAGE_KEY",
    "CMC_KEY", "ANTHROPIC_API_KEY", "TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID",
    "TELEGRAM_TOKEN", "NEWSAPI_KEY", "BLS_KEY", "BEA_KEY", "CENSUS_KEY",
]

REPO_ROOT = Path(__file__).resolve().parents[3]
REPORT_PATH = REPO_ROOT / "aws" / "ops" / "reports" / "983.json"
REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)


def wait_for_settled(lambda_c, name, max_wait=180):
    deadline = time.time() + max_wait
    while time.time() < deadline:
        try:
            cfg = lambda_c.get_function_configuration(FunctionName=name)
            status = cfg.get("LastUpdateStatus")
            if status in ("Successful", None) and cfg.get("State") == "Active":
                return {"ok": True, "status": status}
            if status == "Failed":
                return {"ok": False, "reason": cfg.get("LastUpdateStatusReason")}
        except ClientError as e:
            return {"ok": False, "error": str(e)}
        time.sleep(3)
    return {"ok": False, "error": "wait_timeout"}


def main():
    report = {"started_at": int(time.time()), "target": TARGET, "donor": DONOR}
    try:
        lambda_c = boto3.client("lambda", region_name=REGION,
                                config=Config(read_timeout=300, connect_timeout=10))
        s3 = boto3.client("s3", region_name=REGION)

        # 1. Pull donor env
        donor_cfg = lambda_c.get_function_configuration(FunctionName=DONOR)
        donor_env = (donor_cfg.get("Environment", {}) or {}).get("Variables", {})
        secrets = {k: donor_env[k] for k in STANDARD_KEYS if k in donor_env}
        report["donor_env_keys"] = sorted(list(donor_env.keys()))
        report["secrets_extracted"] = sorted(list(secrets.keys()))
        report["secrets_n"] = len(secrets)
        if not secrets.get("FMP_KEY"):
            raise RuntimeError(f"Donor {DONOR} has no FMP_KEY")

        # 2. Pull target current env, merge
        target_cfg = lambda_c.get_function_configuration(FunctionName=TARGET)
        target_env = (target_cfg.get("Environment", {}) or {}).get("Variables", {})
        report["target_env_before"] = sorted(list(target_env.keys()))
        merged = {**target_env, **secrets}
        report["target_env_after"] = sorted(list(merged.keys()))

        # 3. Wait for any pending update, then patch
        report["wait_pre"] = wait_for_settled(lambda_c, TARGET)
        print(f"wait_pre: {report['wait_pre']}")

        upd = lambda_c.update_function_configuration(
            FunctionName=TARGET,
            Environment={"Variables": merged},
        )
        report["update"] = {
            "last_modified": upd.get("LastModified"),
            "last_update_status": upd.get("LastUpdateStatus"),
            "version": upd.get("Version"),
        }

        report["wait_post"] = wait_for_settled(lambda_c, TARGET)
        print(f"wait_post: {report['wait_post']}")

        # 4. Invoke
        inv = lambda_c.invoke(FunctionName=TARGET, InvocationType="RequestResponse", Payload=b"{}")
        body = inv["Payload"].read().decode("utf-8", errors="ignore")
        report["invoke"] = {
            "status_code": inv.get("StatusCode"),
            "function_error": inv.get("FunctionError"),
            "body": body,
        }
        print(f"invoke: status={inv.get('StatusCode')} fe={inv.get('FunctionError')}")
        print(f"body: {body[:500]}")

        time.sleep(3)

        # 5. Verify S3
        try:
            head = s3.head_object(Bucket=S3_BUCKET, Key="data/credit-equity-divergence.json")
            obj_body = s3.get_object(Bucket=S3_BUCKET, Key="data/credit-equity-divergence.json")["Body"].read()
            obj = json.loads(obj_body)
            report["s3"] = {
                "size": head["ContentLength"],
                "last_modified": head["LastModified"].isoformat(),
                "state": obj.get("state"),
                "signal_strength": obj.get("signal_strength"),
                "error": obj.get("error"),
                "divergences_n": len(obj.get("divergences") or []),
                "body_preview": json.dumps(obj)[:600],
            }
        except Exception as e:
            report["s3"] = {"error": str(e)}

        report["all_pass"] = (
            report["wait_post"].get("ok")
            and report["invoke"].get("status_code") == 200
            and not report["invoke"].get("function_error")
            and report["s3"].get("size", 0) > 500
            and not report["s3"].get("error")
        )
        print(f"\nall_pass={report['all_pass']}")

    except Exception as e:
        report["fatal"] = str(e)
        report["traceback"] = traceback.format_exc()
    finally:
        REPORT_PATH.write_text(json.dumps(report, indent=2, default=str))
        print("\n=== REPORT ===")
        print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    main()
