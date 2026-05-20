"""
ops 961 -- POST-FIX invoke + verify edges 1-4 (after commit 99bbf79f)
======================================================================

Patches landed:
  * empty FMP_KEY / POLYGON_KEY / Telegram defaults filled with real values
  * config.json normalized to {"env": {...}} (was {"environment": {...}} which
    deploy-lambdas.yml apparently ignored, blocking deploy of edges 1-3)

This ops:
  1. Confirm each Lambda exists on AWS (proves deploy fired)
  2. Force-invoke each
  3. Verify S3 output landed at the page-expected key
  4. Spot-check schema (engine name + state field)
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
    ("1", "justhodl-vix-backwardation-trigger", "data/vix-backwardation-trigger.json", "vix-backwardation-trigger"),
    ("2", "justhodl-insider-buys-enriched",     "data/insider-buys-enriched.json",     "insider-buys-enriched"),
    ("3", "justhodl-breadth-thrust",             "data/breadth-thrust.json",            "breadth-thrust"),
    ("4", "justhodl-vol-target-unwind",          "data/vol-target-unwind.json",         "vol-target-unwind"),
]

CHECKS = []


def add(name, passed, detail=""):
    CHECKS.append({"name": name, "passed": bool(passed), "detail": str(detail)[:300]})


def main():
    print(f"ops 961 at {dt.datetime.utcnow().isoformat()}Z")
    for edge, fn, key, eng in EDGES:
        print(f"\n--- Edge #{edge}: {fn} ---")
        try:
            info = lam.get_function(FunctionName=fn)
            c = info["Configuration"]
            add(f"e{edge}.deployed", True,
                f"runtime={c.get('Runtime')} mem={c.get('MemorySize')} mod={c.get('LastModified','')[:16]}")
        except ClientError as ex:
            add(f"e{edge}.deployed", False, str(ex)[:200])
            continue

        # Wait for in-progress updates
        for _ in range(20):
            cfg = lam.get_function_configuration(FunctionName=fn)
            if cfg.get("LastUpdateStatus") == "Successful":
                break
            time.sleep(2)

        t0 = time.time()
        try:
            r = lam.invoke(FunctionName=fn, InvocationType="RequestResponse",
                           Payload=b"{}")
            dur = round(time.time() - t0, 1)
            p = r["Payload"].read().decode()
            try:
                body = json.loads(p)
                inner = body.get("statusCode", 200)
            except Exception:
                inner = "n/a"
            ok = r["StatusCode"] == 200 and not r.get("FunctionError") and inner == 200
            add(f"e{edge}.invoke", ok,
                f"dur={dur}s outer={r['StatusCode']} inner={inner} body={p[:200]}")
        except ClientError as ex:
            add(f"e{edge}.invoke", False, str(ex)[:200])
            continue

        time.sleep(2)
        # S3 freshness
        try:
            h = s3.head_object(Bucket=S3_BUCKET, Key=key)
            age = (dt.datetime.now(dt.timezone.utc) - h["LastModified"]).total_seconds()
            add(f"e{edge}.s3_fresh", h["ContentLength"] > 200 and age < 600,
                f"size={h['ContentLength']}B age_s={int(age)}")
        except ClientError as ex:
            add(f"e{edge}.s3_fresh", False, str(ex)[:200])
            continue

        # Spot-check engine id
        try:
            obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
            d = json.loads(obj["Body"].read())
            add(f"e{edge}.engine_id", d.get("engine") == eng,
                f"got={d.get('engine')} expected={eng}")
            add(f"e{edge}.has_state",
                "state" in d or "calendar_phase" in d or "phase" in d,
                f"top_keys={list(d.keys())[:10]}")
        except Exception as ex:
            add(f"e{edge}.engine_id", False, str(ex)[:150])

    rep = {
        "ops": 961,
        "title": "edges 1-4 post-fix: invoke + S3 + schema verify",
        "run_at": dt.datetime.utcnow().isoformat() + "Z",
        "checks": CHECKS,
        "summary": {"total": len(CHECKS),
                    "passed": sum(1 for c in CHECKS if c["passed"]),
                    "failed": sum(1 for c in CHECKS if not c["passed"])},
        "overall_ok": all(c["passed"] for c in CHECKS),
    }
    os.makedirs("aws/ops/reports", exist_ok=True)
    with open("aws/ops/reports/961_edges_1_4_postfix.json", "w") as f:
        json.dump(rep, f, indent=2)
    print(f"\n=== SUMMARY pass={rep['summary']['passed']}/{rep['summary']['total']} ===")
    for c in CHECKS:
        print(f"  [{'OK ' if c['passed'] else 'FAIL'}] {c['name']:30} {c['detail'][:140]}")


if __name__ == "__main__":
    main()
