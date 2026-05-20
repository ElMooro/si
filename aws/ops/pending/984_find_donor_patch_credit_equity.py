"""
ops 984 - Find authoritative FMP_KEY donor + patch credit-equity-divergence
============================================================================

ops 983 discovered justhodl-buyback-scanner (the deploy-lambdas.yml stated
canonical donor) only has CMC_KEY today. Need to find a Lambda that actually
has FMP_KEY and patch from there.

Strategy:
  1. Probe candidate donors in order of likelihood. The 5 working Tier-3
     siblings all invoked FMP successfully, so they HAVE FMP_KEY. Use the
     first sibling that has all standard secrets.
  2. Once found, patch credit-equity-divergence env, invoke, verify S3.

Idempotent. Logs the donor choice for future-proofing.
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

# Probe order: working Tier-3 siblings first (proven FMP_KEY holders),
# then known data-heavy Lambdas, then catalog
CANDIDATE_DONORS = [
    "justhodl-gap-fill-confirm",
    "justhodl-13f-price-divergence",
    "justhodl-vvix-vov-regime",
    "justhodl-sympathetic-momentum",
    "justhodl-insider-buyback-confluence",
    "justhodl-crypto-etf-arb",
    "justhodl-earnings-pead",
    "justhodl-merger-arb",
    "justhodl-hedge-pnl",
    "justhodl-divergence-engine-v2",
    "justhodl-fundamentals-engine",
    "justhodl-buyback-scanner",
]

STANDARD_KEYS = [
    "FMP_KEY", "FRED_KEY", "POLYGON_KEY", "ALPHA_VANTAGE_KEY",
    "CMC_KEY", "ANTHROPIC_API_KEY", "TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID",
    "TELEGRAM_TOKEN", "NEWSAPI_KEY", "BLS_KEY", "BEA_KEY", "CENSUS_KEY",
]

REPO_ROOT = Path(__file__).resolve().parents[3]
REPORT_PATH = REPO_ROOT / "aws" / "ops" / "reports" / "984.json"
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
    report = {"started_at": int(time.time()), "target": TARGET, "candidate_donors": CANDIDATE_DONORS}
    try:
        lambda_c = boto3.client("lambda", region_name=REGION,
                                config=Config(read_timeout=300, connect_timeout=10))
        s3 = boto3.client("s3", region_name=REGION)

        # 1. Find authoritative donor
        donor_scan = []
        chosen_donor = None
        chosen_secrets = {}
        for cand in CANDIDATE_DONORS:
            try:
                cfg = lambda_c.get_function_configuration(FunctionName=cand)
                env = (cfg.get("Environment", {}) or {}).get("Variables", {})
                has_keys = [k for k in STANDARD_KEYS if k in env]
                rec = {"name": cand, "has_keys": has_keys, "n": len(has_keys)}
                donor_scan.append(rec)
                if "FMP_KEY" in env and chosen_donor is None:
                    chosen_donor = cand
                    chosen_secrets = {k: env[k] for k in STANDARD_KEYS if k in env}
            except ClientError as e:
                donor_scan.append({"name": cand, "error": str(e)[:120]})
        report["donor_scan"] = donor_scan
        report["chosen_donor"] = chosen_donor
        report["chosen_secrets_keys"] = sorted(list(chosen_secrets.keys()))
        report["fmp_key_tail"] = chosen_secrets.get("FMP_KEY", "")[-6:] if chosen_secrets.get("FMP_KEY") else None
        if not chosen_donor:
            raise RuntimeError("No donor with FMP_KEY found across candidates")

        # 2. Pull target current env, merge
        target_cfg = lambda_c.get_function_configuration(FunctionName=TARGET)
        target_env = (target_cfg.get("Environment", {}) or {}).get("Variables", {})
        report["target_env_before"] = sorted(list(target_env.keys()))
        merged = {**target_env, **chosen_secrets}
        report["target_env_after"] = sorted(list(merged.keys()))

        # 3. Wait + patch
        report["wait_pre"] = wait_for_settled(lambda_c, TARGET)
        upd = lambda_c.update_function_configuration(
            FunctionName=TARGET,
            Environment={"Variables": merged},
        )
        report["update"] = {
            "last_modified": upd.get("LastModified"),
            "last_update_status": upd.get("LastUpdateStatus"),
        }
        report["wait_post"] = wait_for_settled(lambda_c, TARGET)

        # 4. Invoke
        inv = lambda_c.invoke(FunctionName=TARGET, InvocationType="RequestResponse", Payload=b"{}")
        body = inv["Payload"].read().decode("utf-8", errors="ignore")
        report["invoke"] = {
            "status_code": inv.get("StatusCode"),
            "function_error": inv.get("FunctionError"),
            "body": body,
        }

        time.sleep(3)

        # 5. S3
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
                "preview": json.dumps(obj)[:600],
            }
        except Exception as e:
            report["s3"] = {"error": str(e)}

        report["all_pass"] = (
            (report.get("wait_post") or {}).get("ok")
            and report["invoke"].get("status_code") == 200
            and not report["invoke"].get("function_error")
            and report.get("s3", {}).get("size", 0) > 500
            and not report.get("s3", {}).get("error")
        )

    except Exception as e:
        report["fatal"] = str(e)
        report["traceback"] = traceback.format_exc()
    finally:
        REPORT_PATH.write_text(json.dumps(report, indent=2, default=str))
        print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    main()
