"""
ops 951 -- force-invoke edges 5-9 to seed first S3 outputs
===========================================================

The unified ops 950 found edges 5-9 had deployed Lambdas but no S3
output yet (their EventBridge schedules hadn't fired). This ops runs
each one synchronously to populate their first data file, making them
visible to their respective pages immediately.

Lambdas (correct names as deployed):
  #5 justhodl-russell-recon-frontrun  -> data/russell-recon-frontrun.json
  #6 justhodl-buyback-scanner         -> data/buyback-scanner.json
  #7 justhodl-stablecoin-flow         -> data/stablecoin-flow.json
  #8 justhodl-opex-calendar           -> data/opex-calendar.json
  #9 justhodl-activist-13d            -> data/activist-13d.json

Some may already have data; harmless to re-invoke (just refreshes).
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
                                 retries={"max_attempts": 0}))
s3 = boto3.client("s3", region_name=REGION)

EDGES = [
    ("5", "justhodl-russell-recon-frontrun", "data/russell-recon-frontrun.json"),
    ("6", "justhodl-buyback-scanner",         "data/buyback-scanner.json"),
    ("7", "justhodl-stablecoin-flow",         "data/stablecoin-flow.json"),
    ("8", "justhodl-opex-calendar",           "data/opex-calendar.json"),
    ("9", "justhodl-activist-13d",            "data/activist-13d.json"),
]

CHECKS = []


def add(name, passed, detail=""):
    CHECKS.append({"name": name, "passed": bool(passed), "detail": str(detail)[:400]})


def invoke_and_check(edge, fn, s3_key):
    print(f"\n--- Edge #{edge}: invoking {fn} ---")
    t0 = time.time()
    try:
        r = lam.invoke(FunctionName=fn,
                       InvocationType="RequestResponse",
                       Payload=b"{}")
        dur = round(time.time() - t0, 1)
        payload = r["Payload"].read().decode()
        status = r["StatusCode"]
        fn_err = r.get("FunctionError")
        ok = status == 200 and not fn_err
        add(f"e{edge}.invoke",
            ok,
            f"dur={dur}s status={status} err={fn_err} body={payload[:240]}")
        print(f"   status={status} dur={dur}s err={fn_err}")
        if payload:
            print(f"   body[:200]={payload[:200]}")
    except ClientError as ex:
        add(f"e{edge}.invoke", False, str(ex)[:240])
        return

    # Verify S3 output exists + is recent
    try:
        head = s3.head_object(Bucket=S3_BUCKET, Key=s3_key)
        size = head["ContentLength"]
        lm = head["LastModified"]
        age_s = (dt.datetime.now(dt.timezone.utc) - lm).total_seconds()
        add(f"e{edge}.s3_output_fresh",
            size > 100 and age_s < 600,
            f"size={size}B age_s={int(age_s)}")
        print(f"   s3://{S3_BUCKET}/{s3_key} size={size}B age={int(age_s)}s")
    except ClientError as ex:
        add(f"e{edge}.s3_output_fresh", False, str(ex)[:200])


def main():
    print(f"ops 951 -- force-invoke edges 5-9 at {dt.datetime.utcnow().isoformat()}Z")
    for edge, fn, key in EDGES:
        try:
            invoke_and_check(edge, fn, key)
        except Exception as ex:
            add(f"e{edge}.unhandled_exception", False, str(ex)[:200])

    n_pass = sum(1 for c in CHECKS if c["passed"])
    n_fail = sum(1 for c in CHECKS if not c["passed"])
    report = {
        "ops": 951,
        "title": "force-invoke edges 5-9 to seed first S3 outputs",
        "run_at": dt.datetime.utcnow().isoformat() + "Z",
        "checks": CHECKS,
        "summary": {"total": len(CHECKS), "passed": n_pass, "failed": n_fail},
        "overall_ok": n_fail == 0,
    }
    os.makedirs("aws/ops/reports", exist_ok=True)
    with open("aws/ops/reports/951_force_invoke_edges_5_9.json", "w") as f:
        json.dump(report, f, indent=2)
    print(f"\n=== SUMMARY  pass={n_pass}/{len(CHECKS)} ===")
    for c in CHECKS:
        flag = "OK " if c["passed"] else "FAIL"
        print(f"  [{flag}] {c['name']:30} {c['detail'][:90]}")


if __name__ == "__main__":
    main()
