"""
ops 958 -- after config normalisation (memory_mb -> memory, timeout_s -> timeout,
name -> function_name), verify edges 1-3 now exist on AWS, invoke them to seed
S3, also invoke edge 4 (Lambda exists but S3 empty), then run a final
scorecard across all 10 edges.

Expected outcome: 10/10 edges have Lambda + S3 output.
"""

import json
import os
import time
import boto3
import datetime as dt

REGION = "us-east-1"
S3_BUCKET = "justhodl-dashboard-live"
REPORT_PATH = "aws/ops/reports/958_final_10_edge_post_normalize.json"

session_cfg = boto3.session.Config(
    region_name=REGION,
    read_timeout=600,
    connect_timeout=20,
    retries={"max_attempts": 0},
)
lam_invoke = boto3.client("lambda", config=session_cfg)
lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


EDGES = [
    ("1", "justhodl-vix-backwardation-trigger", "data/vix-capitulation.json"),
    ("2", "justhodl-insider-buys-enriched",     "data/insider-buys.json"),
    ("3", "justhodl-breadth-thrust",            "data/breadth-thrust.json"),
    ("4", "justhodl-vol-target-unwind",         "data/vol-target-unwind.json"),
    ("5", "justhodl-russell-recon-frontrun",    "data/russell-recon-frontrun.json"),
    ("6", "justhodl-buyback-scanner",           "data/buyback-scanner.json"),
    ("7", "justhodl-stablecoin-flow",           "data/stablecoin-flow.json"),
    ("8", "justhodl-opex-calendar",             "data/opex-calendar.json"),
    ("9", "justhodl-activist-13d",              "data/activist-13d.json"),
    ("10","justhodl-rv-iv-scanner",             "data/rv-iv-scanner.json"),
]

CHECKS = []
def add(edge, name, ok, detail):
    CHECKS.append({"edge": edge, "name": name, "passed": bool(ok),
                   "detail": str(detail)[:300]})

print("=== Pass 1: Lambda presence + seed S3 if missing ===")
for edge, fn, key in EDGES:
    # Check Lambda exists
    try:
        info = lam.get_function(FunctionName=fn)
        deployed = True
        rt = info["Configuration"].get("Runtime")
        mem = info["Configuration"].get("MemorySize")
        timeout = info["Configuration"].get("Timeout")
        add(edge, "lambda_deployed", True,
            f"runtime={rt} mem={mem}MB timeout={timeout}s")
    except Exception as e:
        deployed = False
        add(edge, "lambda_deployed", False, str(e)[:200])

    # Check S3 exists
    try:
        head = s3.head_object(Bucket=S3_BUCKET, Key=key)
        sz = head["ContentLength"]
        last_mod = head["LastModified"]
        age_h = (dt.datetime.now(dt.timezone.utc) - last_mod).total_seconds()/3600
        s3_present = True
        add(edge, "s3_output_present", sz > 500,
            f"size={sz}B age_h={age_h:.1f}")
    except Exception as e:
        s3_present = False
        add(edge, "s3_output_present_initial", False, str(e)[:160])

    # If Lambda exists but S3 missing, invoke to seed
    if deployed and not s3_present:
        print(f"  edge {edge}: Lambda exists but no S3 -- invoking to seed")
        t0 = time.time()
        try:
            resp = lam_invoke.invoke(FunctionName=fn,
                                     InvocationType="RequestResponse",
                                     Payload=b"{}")
            body = resp["Payload"].read().decode("utf-8", errors="replace")
            ok = resp.get("StatusCode") == 200 and not resp.get("FunctionError")
            add(edge, "seed_invoke", ok,
                f"dur={time.time()-t0:.1f}s status={resp.get('StatusCode')} "
                f"err={resp.get('FunctionError')} body={body[:200]}")
            # Re-check S3 after invoke
            time.sleep(2)
            try:
                head = s3.head_object(Bucket=S3_BUCKET, Key=key)
                add(edge, "s3_output_present_post_seed", True,
                    f"size={head['ContentLength']}B")
            except Exception as e:
                add(edge, "s3_output_present_post_seed", False, str(e)[:160])
        except Exception as e:
            add(edge, "seed_invoke", False,
                f"dur={time.time()-t0:.1f}s err={str(e)[:200]}")

# Summary
per_edge = {}
for c in CHECKS:
    e = c["edge"]
    per_edge.setdefault(e, {"passed": 0, "total": 0})
    per_edge[e]["total"] += 1
    if c["passed"]:
        per_edge[e]["passed"] += 1

report = {
    "ops": 958,
    "title": "final 10-edge scorecard after edges 1-3 config normalisation + edge 4 seed",
    "run_at": dt.datetime.utcnow().isoformat() + "Z",
    "per_edge_summary": per_edge,
    "checks": CHECKS,
    "summary": {
        "total": len(CHECKS),
        "passed": sum(1 for c in CHECKS if c["passed"]),
        "failed": sum(1 for c in CHECKS if not c["passed"]),
    },
    "overall_ok": all(c["passed"] for c in CHECKS),
}

# Compute per-edge completion %
report["scorecard"] = {
    e: f"{v['passed']}/{v['total']}" for e, v in sorted(per_edge.items(),
                                                        key=lambda kv: int(kv[0]))
}

print("\n=== SCORECARD ===")
for e in sorted(per_edge.keys(), key=int):
    print(f"  edge {e}: {per_edge[e]['passed']}/{per_edge[e]['total']}")
print(f"\nOVERALL: {report['summary']['passed']}/{report['summary']['total']} "
      f"(ok={report['overall_ok']})")

os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
with open(REPORT_PATH, "w") as f:
    json.dump(report, f, indent=2)
print(f"report written to {REPORT_PATH}")
