"""
ops 983 - Fix Missing Env Vars on Tier-3 Engines
=================================================

Root cause: ops 982 confirmed justhodl-credit-equity-divergence is missing
FMP_KEY (env_keys = ["CMC_KEY", "S3_BUCKET"]). When apikey= is empty in the
FMP URL, /stable/* returns no data, hence HYG=0 SPY=0. This likely affects
the other Tier-3 engines too (deploy-lambdas.yml created them without the
full env inheritance).

This script:
  1. Reads env vars from justhodl-buyback-scanner (proven donor, has every
     key the Tier-3 engines need).
  2. For each Tier-3 engine, merges donor env into its current env so we
     never lose existing values, then update_function_configuration.
  3. wait_for_settled() between code-state transitions to avoid race.
  4. After all 6 are updated, invokes each one (with 900s read_timeout) to
     trigger a fresh run with the corrected env.
  5. Reports the final state for every engine.
"""
import json
import os
import sys
import time
import traceback
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION = "us-east-1"
DONOR_FN = "justhodl-buyback-scanner"

TIER3_ENGINES = [
    "justhodl-vvix-vov-regime",
    "justhodl-sympathetic-momentum",
    "justhodl-insider-buyback-confluence",
    "justhodl-gap-fill-confirm",
    "justhodl-13f-price-divergence",
    "justhodl-credit-equity-divergence",
]

# Env vars these engines actually need at runtime
REQUIRED_KEYS = [
    "FMP_KEY", "FRED_KEY", "POLYGON_KEY", "ALPHAVANTAGE_KEY",
    "CMC_KEY", "NEWSAPI_KEY", "BEA_KEY", "BLS_KEY", "CENSUS_KEY",
    "TELEGRAM_TOKEN", "TELEGRAM_CHAT", "S3_BUCKET",
]

REPO_ROOT = Path(__file__).resolve().parents[3]
REPORTS_DIR = REPO_ROOT / "aws" / "ops" / "reports"
REPORTS_DIR.mkdir(parents=True, exist_ok=True)
REPORT_PATH = REPORTS_DIR / "983.json"


def wait_for_settled(lambda_c, name, max_wait=120):
    deadline = time.time() + max_wait
    while time.time() < deadline:
        try:
            cfg = lambda_c.get_function_configuration(FunctionName=name)
            status = cfg.get("LastUpdateStatus")
            if status in ("Successful", None):
                return True
            if status == "Failed":
                return False
        except ClientError:
            pass
        time.sleep(2)
    return False


def main():
    report = {
        "ops_id": "983",
        "purpose": "Fix missing FMP_KEY (and other env vars) on Tier-3 engines",
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "donor": {},
        "engines": {},
        "scorecard": {},
    }

    lambda_c = boto3.client("lambda", region_name=REGION)
    long_cfg = Config(read_timeout=900, connect_timeout=10, retries={"max_attempts": 1})
    lambda_long = boto3.client("lambda", region_name=REGION, config=long_cfg)

    try:
        # 1. Read donor env
        donor_cfg = lambda_c.get_function_configuration(FunctionName=DONOR_FN)
        donor_env = donor_cfg.get("Environment", {}).get("Variables", {})
        donor_keys_present = [k for k in REQUIRED_KEYS if k in donor_env]
        report["donor"] = {
            "name": DONOR_FN,
            "n_env_keys": len(donor_env),
            "all_keys": sorted(donor_env.keys()),
            "required_present": donor_keys_present,
            "required_missing": [k for k in REQUIRED_KEYS if k not in donor_env],
        }
        print(f"Donor has {len(donor_env)} env keys; required present: {len(donor_keys_present)}")

        # 2. For each engine, merge + update
        for name in TIER3_ENGINES:
            print(f"\n=== {name} ===", flush=True)
            entry = {"name": name}
            try:
                cur_cfg = lambda_c.get_function_configuration(FunctionName=name)
                cur_env = cur_cfg.get("Environment", {}).get("Variables", {}) or {}
                entry["before"] = {
                    "n_keys": len(cur_env),
                    "keys": sorted(cur_env.keys()),
                    "missing_required": [k for k in REQUIRED_KEYS
                                         if k not in cur_env and k in donor_env],
                }

                # Merge: current values WIN over donor (don't clobber custom settings),
                # but missing keys from donor get added.
                merged = dict(donor_env)
                merged.update(cur_env)

                wait_for_settled(lambda_c, name)
                lambda_c.update_function_configuration(
                    FunctionName=name,
                    Environment={"Variables": merged},
                )
                settled = wait_for_settled(lambda_c, name, max_wait=90)
                entry["update"] = {"ok": settled,
                                   "n_keys_after": len(merged),
                                   "added_keys": sorted(set(merged.keys()) - set(cur_env.keys()))}
            except ClientError as e:
                entry["update"] = {"ok": False, "error": str(e)}
                report["engines"][name] = entry
                continue

            # 3. Invoke fresh with long timeout
            try:
                t0 = time.time()
                r = lambda_long.invoke(FunctionName=name,
                                       InvocationType="RequestResponse",
                                       Payload=b"{}")
                elapsed = round(time.time() - t0, 1)
                payload = r["Payload"].read().decode("utf-8", "replace")
                # Parse inner statusCode (Lambda return body)
                inner_sc = None
                try:
                    outer = json.loads(payload)
                    inner_sc = outer.get("statusCode")
                except json.JSONDecodeError:
                    pass
                entry["invoke"] = {
                    "outer_status": r["StatusCode"],
                    "function_error": r.get("FunctionError"),
                    "inner_status_code": inner_sc,
                    "elapsed_s": elapsed,
                    "body_preview": payload[:350],
                    "ok": (r["StatusCode"] == 200 and not r.get("FunctionError")
                           and (inner_sc == 200 or inner_sc is None)),
                }
            except ClientError as e:
                entry["invoke"] = {"ok": False, "error": str(e)}
            except Exception as e:
                entry["invoke"] = {"ok": False, "error": str(e)}

            report["engines"][name] = entry

        # Scorecard
        n_updated = sum(1 for e in report["engines"].values() if e.get("update", {}).get("ok"))
        n_invoked_ok = sum(1 for e in report["engines"].values() if e.get("invoke", {}).get("ok"))
        report["scorecard"] = {
            "n_engines": len(TIER3_ENGINES),
            "n_env_updated": n_updated,
            "n_invoke_ok": n_invoked_ok,
            "all_pass": n_updated == len(TIER3_ENGINES) and n_invoked_ok == len(TIER3_ENGINES),
        }
    except Exception as e:
        report["fatal_error"] = str(e)
        report["fatal_traceback"] = traceback.format_exc()
    finally:
        report["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        with open(REPORT_PATH, "w") as f:
            json.dump(report, f, indent=2, default=str)
        print("\n" + "=" * 60)
        print("OPS 983 REPORT")
        print("=" * 60)
        print(json.dumps(report, indent=2, default=str)[:6000])

    sys.exit(0 if report.get("scorecard", {}).get("all_pass") else 1)


if __name__ == "__main__":
    main()
