"""
ops 954 -- set Edge #5 (russell-recon-frontrun) environment + re-invoke
========================================================================

Root cause (ops 953): the deployed Lambda has env_keys=[].
Without FMP_KEY the screener returns empty -> "insufficient universe".

This ops:
  1. Read FMP_KEY (and other keys) from another known-good Lambda's env
     -- justhodl-buyback-scanner has them and is fully working (16/16 in 952).
  2. update_function_configuration on justhodl-russell-recon-frontrun
     to merge those vars in.
  3. Re-invoke and verify S3 output appears.

Reusing keys from an existing Lambda avoids hardcoding secrets in ops.
"""
import datetime as dt
import json
import os
import time

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION = "us-east-1"
S3_BUCKET = "justhodl-dashboard-live"

lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=620, connect_timeout=10,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)

DONOR_FN = "justhodl-buyback-scanner"  # known-good Lambda with all env vars
TARGET_FN = "justhodl-russell-recon-frontrun"
S3_KEY = "data/russell-recon-frontrun.json"
REQUIRED_KEYS = ["FMP_KEY", "FRED_KEY", "POLYGON_KEY",
                 "ALPHA_VANTAGE_KEY", "CMC_KEY", "ANTHROPIC_API_KEY",
                 "TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID",
                 "S3_BUCKET", "NEWSAPI_KEY", "BLS_KEY", "BEA_KEY"]

CHECKS = []


def add(name, passed, detail=""):
    CHECKS.append({"name": name, "passed": bool(passed), "detail": str(detail)[:400]})


def main():
    print(f"ops 954 at {dt.datetime.utcnow().isoformat()}Z")

    # 1. Read donor env vars
    try:
        donor_info = lam.get_function_configuration(FunctionName=DONOR_FN)
        donor_env = donor_info.get("Environment", {}).get("Variables", {})
        present = [k for k in REQUIRED_KEYS if k in donor_env]
        add("donor.env_loaded", len(present) >= 4,
            f"donor={DONOR_FN} found_keys={present} total={len(donor_env)}")
        if not donor_env:
            add("donor.env_loaded", False, "donor has no env vars either!")
            write_report()
            return
    except ClientError as ex:
        add("donor.env_loaded", False, str(ex)[:200])
        write_report()
        return

    # 2. Read target current env
    try:
        target_info = lam.get_function_configuration(FunctionName=TARGET_FN)
        target_env = target_info.get("Environment", {}).get("Variables", {})
        add("target.env_before",
            True,
            f"keys_before={list(target_env.keys())[:10]} count={len(target_env)}")
    except ClientError as ex:
        add("target.env_before", False, str(ex)[:200])
        write_report()
        return

    # 3. Merge donor's REQUIRED_KEYS into target env (donor wins on conflict)
    merged = dict(target_env)
    for k in REQUIRED_KEYS:
        if k in donor_env:
            merged[k] = donor_env[k]
    # Ensure S3_BUCKET set
    merged.setdefault("S3_BUCKET", S3_BUCKET)

    # 4. Update target
    try:
        lam.update_function_configuration(
            FunctionName=TARGET_FN,
            Environment={"Variables": merged},
        )
        add("target.env_updated", True,
            f"merged_keys={sorted(merged.keys())[:12]} count={len(merged)}")
    except ClientError as ex:
        add("target.env_updated", False, str(ex)[:300])
        write_report()
        return

    # Wait for update to settle (Lambda needs ~5s)
    print("  sleeping 8s for update to propagate...")
    time.sleep(8)

    # 5. Verify env now present
    try:
        v_info = lam.get_function_configuration(FunctionName=TARGET_FN)
        v_env = v_info.get("Environment", {}).get("Variables", {})
        has_fmp = "FMP_KEY" in v_env and len(v_env["FMP_KEY"]) > 10
        add("target.env_verified", has_fmp,
            f"keys_after={list(v_env.keys())[:10]} fmp_present={has_fmp}")
    except ClientError as ex:
        add("target.env_verified", False, str(ex)[:200])

    # 6. Wait for any in-progress update + reinvoke
    print("  waiting for LastUpdateStatus=Successful...")
    for _ in range(20):
        v = lam.get_function_configuration(FunctionName=TARGET_FN)
        if v.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(2)

    # 7. Force re-invoke
    print(f"  invoking {TARGET_FN}...")
    t0 = time.time()
    try:
        r = lam.invoke(FunctionName=TARGET_FN, InvocationType="RequestResponse",
                       Payload=b"{}")
        dur = round(time.time() - t0, 1)
        payload = r["Payload"].read().decode()
        status = r["StatusCode"]
        fn_err = r.get("FunctionError")
        # Lambda may return 200 with body containing internal 500 -- inspect
        try:
            body = json.loads(payload)
            inner_status = body.get("statusCode", 200)
        except Exception:
            inner_status = "n/a"
        # success = inner statusCode 200 (universe loaded + output written)
        ok = status == 200 and not fn_err and inner_status == 200
        add("invoke.success", ok,
            f"dur={dur}s outer={status} inner={inner_status} body={payload[:240]}")
    except ClientError as ex:
        add("invoke.success", False, str(ex)[:240])

    # 8. Verify S3 output now present
    time.sleep(2)
    try:
        h = s3.head_object(Bucket=S3_BUCKET, Key=S3_KEY)
        age = (dt.datetime.now(dt.timezone.utc) - h["LastModified"]).total_seconds()
        add("s3.output_fresh",
            h["ContentLength"] > 500 and age < 600,
            f"size={h['ContentLength']}B age_s={int(age)}")
    except ClientError as ex:
        add("s3.output_fresh", False, str(ex)[:200])

    write_report()


def write_report():
    rep = {
        "ops": 954,
        "title": "fix Edge #5 environment + re-invoke",
        "run_at": dt.datetime.utcnow().isoformat() + "Z",
        "checks": CHECKS,
        "summary": {"total": len(CHECKS),
                    "passed": sum(1 for c in CHECKS if c["passed"]),
                    "failed": sum(1 for c in CHECKS if not c["passed"])},
        "overall_ok": all(c["passed"] for c in CHECKS),
    }
    os.makedirs("aws/ops/reports", exist_ok=True)
    with open("aws/ops/reports/954_fix_edge5_env.json", "w") as f:
        json.dump(rep, f, indent=2)
    print(f"\n=== SUMMARY  pass={rep['summary']['passed']}/{rep['summary']['total']} ===")
    for c in CHECKS:
        flag = "OK " if c["passed"] else "FAIL"
        print(f"  [{flag}] {c['name']:30} {c['detail'][:140]}")


if __name__ == "__main__":
    main()
