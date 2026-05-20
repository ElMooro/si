"""
ops 960 -- post-redeploy: invoke edges 1-4 + verify S3 outputs land
====================================================================

Commit e8b69e9d touched source for:
  justhodl-vix-backwardation-trigger
  justhodl-insider-buys-enriched
  justhodl-breadth-thrust
  justhodl-vol-target-unwind

CI should have deployed all four. This ops:
  1. Confirms each Lambda now exists on AWS
  2. Force-invokes each
  3. Verifies S3 output lands at the key the page expects
  4. Reports the end-to-end deploy + invoke + S3 chain

Page-expected S3 keys:
  edge #1 -> data/vix-backwardation-trigger.json (vix-capitulation.html)
  edge #2 -> data/insider-buys-enriched.json     (insider-buys.html)
  edge #3 -> data/breadth-thrust.json            (breadth-thrust.html)
  edge #4 -> data/vol-target-unwind.json         (vol-target-unwind.html)
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
    ("1", "justhodl-vix-backwardation-trigger", "data/vix-backwardation-trigger.json"),
    ("2", "justhodl-insider-buys-enriched",     "data/insider-buys-enriched.json"),
    ("3", "justhodl-breadth-thrust",             "data/breadth-thrust.json"),
    ("4", "justhodl-vol-target-unwind",          "data/vol-target-unwind.json"),
]

CHECKS = []


def add(name, passed, detail=""):
    CHECKS.append({"name": name, "passed": bool(passed), "detail": str(detail)[:400]})


def main():
    print(f"ops 960 at {dt.datetime.utcnow().isoformat()}Z")

    for edge, fn, s3_key in EDGES:
        print(f"\n--- Edge #{edge}: {fn} ---")

        # 1. Lambda exists?
        try:
            info = lam.get_function(FunctionName=fn)
            c = info["Configuration"]
            add(f"e{edge}.lambda_deployed", True,
                f"runtime={c.get('Runtime')} mem={c.get('MemorySize')} timeout={c.get('Timeout')} mod={c.get('LastModified','')[:16]}")
        except ClientError as ex:
            add(f"e{edge}.lambda_deployed", False, str(ex)[:200])
            continue

        # 2. Wait for any in-progress update
        try:
            for _ in range(15):
                cfg = lam.get_function_configuration(FunctionName=fn)
                if cfg.get("LastUpdateStatus") == "Successful":
                    break
                time.sleep(2)
        except Exception:
            pass

        # 3. Invoke
        t0 = time.time()
        try:
            r = lam.invoke(FunctionName=fn,
                           InvocationType="RequestResponse",
                           Payload=b"{}")
            dur = round(time.time() - t0, 1)
            payload = r["Payload"].read().decode()
            status = r["StatusCode"]
            fn_err = r.get("FunctionError")
            try:
                body = json.loads(payload)
                inner = body.get("statusCode", 200)
            except Exception:
                inner = "n/a"
            ok = status == 200 and not fn_err and inner == 200
            add(f"e{edge}.invoke", ok,
                f"dur={dur}s outer={status} inner={inner} err={fn_err} body={payload[:200]}")
        except ClientError as ex:
            add(f"e{edge}.invoke", False, str(ex)[:240])
            continue

        # 4. Verify S3 output appeared at expected key
        time.sleep(2)
        try:
            h = s3.head_object(Bucket=S3_BUCKET, Key=s3_key)
            age_s = (dt.datetime.now(dt.timezone.utc) - h["LastModified"]).total_seconds()
            add(f"e{edge}.s3_fresh", h["ContentLength"] > 200 and age_s < 600,
                f"size={h['ContentLength']}B age_s={int(age_s)}")
        except ClientError as ex:
            add(f"e{edge}.s3_fresh", False, str(ex)[:200])

    rep = {
        "ops": 960,
        "title": "edges 1-4 post-redeploy: invoke + verify S3 output",
        "run_at": dt.datetime.utcnow().isoformat() + "Z",
        "checks": CHECKS,
        "summary": {"total": len(CHECKS),
                    "passed": sum(1 for c in CHECKS if c["passed"]),
                    "failed": sum(1 for c in CHECKS if not c["passed"])},
        "overall_ok": all(c["passed"] for c in CHECKS),
    }
    os.makedirs("aws/ops/reports", exist_ok=True)
    with open("aws/ops/reports/960_edges_1_4_deploy_invoke_verify.json", "w") as f:
        json.dump(rep, f, indent=2)

    print(f"\n=== SUMMARY pass={rep['summary']['passed']}/{rep['summary']['total']} ===")
    for c in CHECKS:
        flag = "OK " if c["passed"] else "FAIL"
        print(f"  [{flag}] {c['name']:30} {c['detail'][:120]}")


if __name__ == "__main__":
    main()
