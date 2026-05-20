"""
ops 959 -- edges 1-4 deploy / invoke / verify
==============================================

After commit cb032e1d touched all 4 edge-1-4 source files with a
redeploy-trigger comment, the deploy-lambdas.yml workflow should
deploy them. This ops then:
  1. Waits briefly for the deploy to settle
  2. Verifies each Lambda is now deployed
  3. Force-invokes each one
  4. Verifies the S3 output appears at the page-expected key
  5. Verifies the page is wired (HTML references the right data file)
"""
import datetime as dt
import json
import os
import time
import urllib.request

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION = "us-east-1"
S3_BUCKET = "justhodl-dashboard-live"
PAGES_BASE = "https://justhodl.ai"

lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=620, connect_timeout=10,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)

EDGES = [
    {"edge": 1, "lambda": "justhodl-vix-backwardation-trigger",
     "s3_key": "data/vix-backwardation-trigger.json",
     "page": "vix-capitulation.html"},
    {"edge": 2, "lambda": "justhodl-insider-buys-enriched",
     "s3_key": "data/insider-buys-enriched.json",
     "page": "insider-buys.html"},
    {"edge": 3, "lambda": "justhodl-breadth-thrust",
     "s3_key": "data/breadth-thrust.json",
     "page": "breadth-thrust.html"},
    {"edge": 4, "lambda": "justhodl-vol-target-unwind",
     "s3_key": "data/vol-target-unwind.json",
     "page": "vol-target-unwind.html"},
]

CHECKS = []


def add(edge, name, ok, det=""):
    CHECKS.append({"edge": edge, "name": f"e{edge}.{name}",
                   "passed": bool(ok), "detail": str(det)[:280]})


def verify_edge(cfg):
    e = cfg["edge"]
    fn = cfg["lambda"]
    s3_key = cfg["s3_key"]

    # 1. Lambda deployed?
    try:
        info = lam.get_function(FunctionName=fn)
        c = info.get("Configuration", {})
        add(e, "lambda_deployed", True,
            f"runtime={c.get('Runtime')} mem={c.get('MemorySize')} timeout={c.get('Timeout')} mod={c.get('LastModified', '')[:19]}")
    except ClientError as ex:
        add(e, "lambda_deployed", False, str(ex)[:200])
        return  # can't invoke if not deployed

    # 2. Wait for any pending update to settle
    for _ in range(20):
        v = lam.get_function_configuration(FunctionName=fn)
        if v.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(2)

    # 3. Invoke
    print(f"  invoking {fn}...")
    t0 = time.time()
    try:
        r = lam.invoke(FunctionName=fn, InvocationType="RequestResponse",
                       Payload=b"{}")
        dur = round(time.time() - t0, 1)
        payload = r["Payload"].read().decode()
        try:
            body = json.loads(payload)
            inner = body.get("statusCode", 200)
        except Exception:
            inner = "n/a"
        ok = r["StatusCode"] == 200 and not r.get("FunctionError") and inner == 200
        add(e, "invoke_success", ok,
            f"dur={dur}s outer={r['StatusCode']} inner={inner} body={payload[:200]}")
    except ClientError as ex:
        add(e, "invoke_success", False, str(ex)[:200])

    # 4. S3 output at the expected key
    time.sleep(2)
    try:
        h = s3.head_object(Bucket=S3_BUCKET, Key=s3_key)
        age = (dt.datetime.now(dt.timezone.utc) - h["LastModified"]).total_seconds()
        add(e, "s3_output_fresh",
            h["ContentLength"] > 200 and age < 600,
            f"size={h['ContentLength']}B age_s={int(age)}")
    except ClientError as ex:
        add(e, "s3_output_fresh", False, str(ex)[:120])

    # 5. Page reachable + wired
    try:
        req = urllib.request.Request(f"{PAGES_BASE}/{cfg['page']}",
                                     headers={"User-Agent": "ops/959"})
        resp = urllib.request.urlopen(req, timeout=15)
        body = resp.read().decode("utf-8", errors="ignore")
        data_file = s3_key.split("/")[-1]
        ok = resp.status == 200 and len(body) > 1000 and data_file in body
        add(e, "page_live_and_wired", ok,
            f"status={resp.status} len={len(body)} wired={data_file in body}")
    except Exception as ex:
        add(e, "page_live_and_wired", False, str(ex)[:120])


def main():
    print(f"ops 959 -- edges 1-4 deploy/invoke/verify at {dt.datetime.utcnow().isoformat()}Z")
    for cfg in EDGES:
        print(f"\n--- Edge #{cfg['edge']}: {cfg['lambda']} ---")
        try:
            verify_edge(cfg)
        except Exception as ex:
            add(cfg["edge"], "unhandled", False, str(ex)[:200])

    per_edge = {}
    for c in CHECKS:
        eid = c["edge"]
        per_edge.setdefault(eid, {"total": 0, "passed": 0, "failed": 0})
        per_edge[eid]["total"] += 1
        if c["passed"]:
            per_edge[eid]["passed"] += 1
        else:
            per_edge[eid]["failed"] += 1

    op = sum(p["passed"] for p in per_edge.values())
    ot = sum(p["total"] for p in per_edge.values())
    rep = {
        "ops": 959,
        "title": "edges 1-4 post-redeploy invoke + verify",
        "run_at": dt.datetime.utcnow().isoformat() + "Z",
        "per_edge_summary": per_edge,
        "checks": CHECKS,
        "summary": {"total": ot, "passed": op, "failed": ot - op,
                    "pct": round(100 * op / max(ot, 1), 1)},
        "overall_ok": op == ot,
    }
    os.makedirs("aws/ops/reports", exist_ok=True)
    with open("aws/ops/reports/959_edges_1_4_deploy_invoke_verify.json", "w") as f:
        json.dump(rep, f, indent=2)

    print("\n=== PER-EDGE SUMMARY ===")
    for eid in sorted(per_edge):
        p = per_edge[eid]
        flag = "GREEN" if p["passed"] == p["total"] else "RED"
        print(f"  Edge #{eid}  pass={p['passed']}/{p['total']}  [{flag}]")
    print(f"\n=== OVERALL pass={op}/{ot} ({round(100*op/max(ot,1), 1)}%) ===")
    failed = [c for c in CHECKS if not c["passed"]]
    if failed:
        print(f"\n{len(failed)} FAILED:")
        for c in failed:
            print(f"  [FAIL] {c['name']:38} {c['detail'][:120]}")


if __name__ == "__main__":
    main()
