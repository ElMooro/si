"""
ops 984 - Direct Env Injection on Tier-3 Engines (no donor pattern)
===================================================================

Why: ops 983 revealed the donor justhodl-buyback-scanner has only CMC_KEY,
so the donor-merge pattern can't supply FMP_KEY. The doctrine says to
inject the API keys Claude already has into the code. Doing that here.

For each of the 6 Tier-3 engines:
  1. Read current env vars (preserve any custom values)
  2. Merge a known-good baseline of 11 keys (FMP, FRED, Polygon, AV, CMC,
     NewsAPI, BEA, BLS, Census, Telegram token+chat, S3_BUCKET) on top —
     current values WIN if they already exist and aren't empty.
  3. update_function_configuration with wait_for_settled race protection.
  4. Invoke with 900s read_timeout and capture inner statusCode (not just
     outer 200) so we accurately see whether the engine returned success.
  5. Read S3 output, parse state + signal_strength, confirm it's no longer
     an error stub.

Note: Telegram token and FMP key are sensitive. They are written into
Lambda env vars (AWS encrypts at rest) and never printed in plaintext in
the report. Only key NAMES and last-4 chars of FMP/FRED are logged.
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
S3_BUCKET_NAME = "justhodl-dashboard-live"

# Known-good keys from JustHodl system constants
BASELINE_ENV = {
    "FMP_KEY": "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb",
    "FRED_KEY": "2f057499936072679d8843d7fce99989",
    "POLYGON_KEY": "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d",
    "ALPHAVANTAGE_KEY": "EOLGKSGAYZUXKPUL",
    "CMC_KEY": "17ba8e87-53f0-46f4-abe5-014d9cd99597",
    "NEWSAPI_KEY": "17d36cdd13c44e139853b3a6876cf940",
    "BEA_KEY": "997E5691-4F0E-4774-8B4E-CAE836D4AC47",
    "BLS_KEY": "a759447531f04f1f861f29a381aab863",
    "CENSUS_KEY": "8423ffa543d0e95cdba580f2e381649b6772f515",
    "TELEGRAM_TOKEN": "8679881066:AAHTE6TAhDqs0FuUelTL6Ppt1x8ihis1aGs",
    "TELEGRAM_CHAT": "8678089260",
    "S3_BUCKET": S3_BUCKET_NAME,
}

TIER3_ENGINES = [
    {"name": "justhodl-vvix-vov-regime",            "s3_key": "data/vvix-vov-regime.json"},
    {"name": "justhodl-sympathetic-momentum",        "s3_key": "data/sympathetic-momentum.json"},
    {"name": "justhodl-insider-buyback-confluence",  "s3_key": "data/insider-buyback-confluence.json"},
    {"name": "justhodl-gap-fill-confirm",            "s3_key": "data/gap-fill-confirm.json"},
    {"name": "justhodl-13f-price-divergence",        "s3_key": "data/13f-price-divergence.json"},
    {"name": "justhodl-credit-equity-divergence",    "s3_key": "data/credit-equity-divergence.json"},
]

REPO_ROOT = Path(__file__).resolve().parents[3]
REPORTS_DIR = REPO_ROOT / "aws" / "ops" / "reports"
REPORTS_DIR.mkdir(parents=True, exist_ok=True)
REPORT_PATH = REPORTS_DIR / "984.json"


def safe_redact(k, v):
    """Don't log secrets in plaintext."""
    if v is None:
        return None
    if k in ("FMP_KEY", "FRED_KEY", "POLYGON_KEY", "ALPHAVANTAGE_KEY", "CMC_KEY",
            "NEWSAPI_KEY", "BEA_KEY", "BLS_KEY", "CENSUS_KEY", "TELEGRAM_TOKEN"):
        return f"***{v[-4:]}" if len(v) > 4 else "***"
    return v


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


def update_env(lambda_c, name, baseline):
    """Merge baseline into existing env; existing non-empty values win."""
    cur_cfg = lambda_c.get_function_configuration(FunctionName=name)
    cur_env = dict(cur_cfg.get("Environment", {}).get("Variables", {}) or {})
    merged = dict(baseline)
    # Preserve existing non-empty values (in case engine has custom overrides)
    for k, v in cur_env.items():
        if v:
            merged[k] = v
    # Detect what we're adding
    added = [k for k in merged if k not in cur_env]
    overwritten = [k for k in merged if k in cur_env and cur_env.get(k, "") == ""]
    wait_for_settled(lambda_c, name)
    lambda_c.update_function_configuration(
        FunctionName=name,
        Environment={"Variables": merged},
    )
    ok = wait_for_settled(lambda_c, name, max_wait=90)
    return {
        "ok": ok,
        "n_keys_before": len(cur_env),
        "n_keys_after": len(merged),
        "added": added,
        "filled_empty": overwritten,
    }


def invoke_engine(lambda_long, name):
    t0 = time.time()
    r = lambda_long.invoke(FunctionName=name, InvocationType="RequestResponse",
                           Payload=b"{}")
    elapsed = round(time.time() - t0, 1)
    payload = r["Payload"].read().decode("utf-8", "replace")
    inner_sc = None
    try:
        outer = json.loads(payload)
        inner_sc = outer.get("statusCode")
    except json.JSONDecodeError:
        pass
    return {
        "outer_status": r["StatusCode"],
        "function_error": r.get("FunctionError"),
        "inner_status_code": inner_sc,
        "elapsed_s": elapsed,
        "body_preview": payload[:300],
        "ok": (r["StatusCode"] == 200 and not r.get("FunctionError")
               and (inner_sc == 200)),
    }


def read_s3(s3_c, key):
    try:
        obj = s3_c.get_object(Bucket=S3_BUCKET_NAME, Key=key)
        raw = obj["Body"].read()
        try:
            data = json.loads(raw)
            return {
                "size": len(raw),
                "last_modified": str(obj["LastModified"]),
                "state": data.get("state"),
                "signal_strength": data.get("signal_strength"),
                "error": data.get("error"),
                "n_keys": len(data) if isinstance(data, dict) else None,
                "is_error_stub": "error" in data and "state" not in data,
            }
        except json.JSONDecodeError as e:
            return {"size": len(raw), "parse_error": str(e),
                    "preview": raw[:200].decode("utf-8", "replace")}
    except ClientError as e:
        return {"error": str(e)}


def main():
    report = {
        "ops_id": "984",
        "purpose": "Direct env injection of 11 baseline keys into 6 Tier-3 engines",
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "baseline_keys": {k: safe_redact(k, v) for k, v in BASELINE_ENV.items()},
        "engines": {},
        "scorecard": {},
    }

    lambda_c = boto3.client("lambda", region_name=REGION)
    long_cfg = Config(read_timeout=900, connect_timeout=10, retries={"max_attempts": 1})
    lambda_long = boto3.client("lambda", region_name=REGION, config=long_cfg)
    s3_c = boto3.client("s3", region_name=REGION)

    try:
        for eng in TIER3_ENGINES:
            name = eng["name"]
            print(f"\n=== {name} ===", flush=True)
            entry = {"name": name}

            # 1. Update env
            try:
                entry["env_update"] = update_env(lambda_c, name, BASELINE_ENV)
            except ClientError as e:
                entry["env_update"] = {"ok": False, "error": str(e)}
                report["engines"][name] = entry
                continue

            # 2. Invoke
            try:
                entry["invoke"] = invoke_engine(lambda_long, name)
            except Exception as e:
                entry["invoke"] = {"ok": False, "error": str(e)}

            # 3. Read S3
            time.sleep(2)  # let S3 write settle
            entry["s3"] = read_s3(s3_c, eng["s3_key"])

            report["engines"][name] = entry

        # Scorecard
        engines = report["engines"]
        n_env_ok = sum(1 for e in engines.values() if e.get("env_update", {}).get("ok"))
        n_inv_ok = sum(1 for e in engines.values() if e.get("invoke", {}).get("ok"))
        n_s3_real = sum(1 for e in engines.values()
                        if not e.get("s3", {}).get("is_error_stub", True))
        report["scorecard"] = {
            "n_engines": len(TIER3_ENGINES),
            "n_env_updated": n_env_ok,
            "n_invoke_ok": n_inv_ok,
            "n_s3_real_data": n_s3_real,
            "all_pass": (n_env_ok == len(TIER3_ENGINES) and
                         n_inv_ok == len(TIER3_ENGINES) and
                         n_s3_real == len(TIER3_ENGINES)),
        }

    except Exception as e:
        report["fatal_error"] = str(e)
        report["fatal_traceback"] = traceback.format_exc()
    finally:
        report["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        with open(REPORT_PATH, "w") as f:
            json.dump(report, f, indent=2, default=str)
        print("\n" + "=" * 60)
        print("OPS 984 REPORT")
        print("=" * 60)
        print(json.dumps(report, indent=2, default=str)[:6000])

    sys.exit(0 if report.get("scorecard", {}).get("all_pass") else 1)


if __name__ == "__main__":
    main()
