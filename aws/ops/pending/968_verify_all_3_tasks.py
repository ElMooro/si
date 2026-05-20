"""
ops 968 -- verify all 3 follow-up tasks landed cleanly
========================================================

Task 1 (deploy-lambdas.yml fixes): confirm workflow file has the new
                                    fields; no live test possible.
Task 2 (delete orphan capitulation): confirm Lambda is gone + S3 key gone
Task 3 (signal-board flagship card): fetch live https://justhodl.ai/ and
                                    confirm new markers present
"""
import datetime as dt
import json
import os
import urllib.request

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
S3_BUCKET = "justhodl-dashboard-live"

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)

CHECKS = []
def add(n, ok, d): CHECKS.append({"name": n, "passed": ok, "detail": str(d)[:280]})

# ── TASK 2: orphan Lambda deleted ──
try:
    lam.get_function(FunctionName="justhodl-capitulation")
    add("task2.lambda_gone", False, "still exists -- delete did not stick")
except ClientError as e:
    if "ResourceNotFoundException" in str(e):
        add("task2.lambda_gone", True, "Lambda deleted successfully")
    else:
        add("task2.lambda_gone", False, str(e)[:120])

try:
    s3.head_object(Bucket=S3_BUCKET, Key="data/capitulation.json")
    add("task2.s3_object_gone", False, "still present")
except ClientError as e:
    if "404" in str(e) or "NoSuchKey" in str(e):
        add("task2.s3_object_gone", True, "S3 object deleted")
    else:
        add("task2.s3_object_gone", False, str(e)[:120])

# Successor still healthy
try:
    info = lam.get_function(FunctionName="justhodl-vix-backwardation-trigger")
    add("task2.successor_healthy", True,
        f"mod={info['Configuration'].get('LastModified', '')[:19]}")
except ClientError as e:
    add("task2.successor_healthy", False, str(e)[:120])

# ── TASK 3: landing page has the signal-board card ──
try:
    req = urllib.request.Request("https://justhodl.ai/",
                                 headers={"User-Agent": "ops/968 (verify)"})
    with urllib.request.urlopen(req, timeout=15) as r:
        body = r.read().decode("utf-8", errors="ignore")
    markers = [
        "signalBoardCard", "sbPosture", "sbLive", "sbMovers",
        "renderSignalBoard", 'signalBoard:', "Signal Board",
    ]
    found = [m for m in markers if m in body]
    missing = [m for m in markers if m not in body]
    add("task3.landing_page_live",
        r.status == 200 and len(body) > 10000,
        f"status={r.status} size={len(body)}")
    add("task3.signal_board_card_present",
        len(missing) == 0,
        f"found={len(found)}/{len(markers)} missing={missing}")
    # Check the signal-board source data is freshly accessible too
    if "signalBoard:" in body:
        add("task3.source_wired", True, "signalBoard source key present")
except Exception as e:
    add("task3.landing_page_live", False, str(e)[:200])

# ── TASK 1: workflow file has the new patterns ──
# Can't test live (no easy GH-API path) -- just verify file content
try:
    with open(".github/workflows/deploy-lambdas.yml") as f:
        wf = f.read()
    add("task1.env_field_canonical",
        "(.env // .environment // {})" in wf,
        "accepts both .env (canonical) and .environment (legacy)")
    add("task1.inherit_env_boolean",
        '"boolean"' in wf and "inherit_env=true" in wf,
        "handles inherit_env:true via standard secrets bundle")
    add("task1.missing_mode",
        'input = "MISSING"' in wf or '"$input" = "MISSING"' in wf,
        "workflow_dispatch MISSING mode for orphan recovery")
    add("task1.all_mode",
        '"$input" = "ALL"' in wf,
        "workflow_dispatch ALL mode for full redeploy")
except Exception as e:
    add("task1.workflow_file_inspect", False, str(e)[:200])

# ── Signal-board itself still aggregating ──
try:
    obj = s3.get_object(Bucket=S3_BUCKET, Key="data/signal-board.json")
    d = json.loads(obj["Body"].read())
    n_eng = d.get("n_engines", 0)
    n_live = d.get("n_live", 0)
    add("signal_board.live_aggregation",
        n_eng >= 20 and n_live >= 10,
        f"engines={n_eng} live={n_live} posture={d.get('composite_posture')}")
except Exception as e:
    add("signal_board.live_aggregation", False, str(e)[:200])

rep = {
    "ops": 968,
    "title": "verify all 3 follow-up tasks (deploy-lambdas fix + orphan delete + signal-board flagship)",
    "run_at": dt.datetime.utcnow().isoformat() + "Z",
    "checks": CHECKS,
    "summary": {"total": len(CHECKS),
                "passed": sum(1 for c in CHECKS if c["passed"]),
                "failed": sum(1 for c in CHECKS if not c["passed"])},
    "overall_ok": all(c["passed"] for c in CHECKS),
}
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/968_verify_all_3_tasks.json", "w") as f:
    json.dump(rep, f, indent=2)
print(f"\n=== {rep['summary']['passed']}/{rep['summary']['total']} ===")
for c in CHECKS:
    print(f"  [{'OK' if c['passed'] else 'X '}] {c['name']:36} {c['detail'][:120]}")
