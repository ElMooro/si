"""
ops 972 -- create EventBridge schedule + signal-board integration verify
==========================================================================

Following ops 971 partial success: schedule failed with wrong role name.
This ops uses the canonical justhodl-scheduler-role and verifies everything.

Tasks:
1. Create rate(4 hours) EventBridge Scheduler schedule using justhodl-scheduler-role
2. Re-deploy signal-board (now has 21st engine: crypto-opportunities)
3. Invoke signal-board to confirm it ingests crypto-opportunities feed
4. Verify final engine count and posture
"""
import datetime as dt
import io
import json
import os
import time
import zipfile

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION = "us-east-1"
S3_BUCKET = "justhodl-dashboard-live"
ACC = "857687956942"
FN = "justhodl-crypto-opportunities"
SCHED_NAME = "justhodl-crypto-opportunities-4h"
SCHED_ROLE = f"arn:aws:iam::{ACC}:role/justhodl-scheduler-role"

SB_FN = "justhodl-signal-board"
SB_SRC = "aws/lambdas/justhodl-signal-board/source"

lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=180, connect_timeout=10))
s3 = boto3.client("s3", region_name=REGION)
scheduler = boto3.client("scheduler", region_name=REGION)

CHECKS = []
def add(n, ok, d=""):
    CHECKS.append({"name": n, "passed": bool(ok), "detail": str(d)[:300]})


def zip_dir(src):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(src):
            for f in files:
                if f.endswith(".pyc") or "__pycache__" in root:
                    continue
                p = os.path.join(root, f)
                zf.write(p, os.path.relpath(p, src))
    buf.seek(0)
    return buf.getvalue()


def main():
    print(f"ops 972 at {dt.datetime.utcnow().isoformat()}Z")

    # ── 1. Create EventBridge Scheduler schedule with the canonical role ──
    target = {
        "Arn": f"arn:aws:lambda:{REGION}:{ACC}:function:{FN}",
        "RoleArn": SCHED_ROLE,
        "Input": "{}",
        "RetryPolicy": {"MaximumRetryAttempts": 2, "MaximumEventAgeInSeconds": 3600},
    }
    try:
        try:
            scheduler.get_schedule(Name=SCHED_NAME)
            scheduler.update_schedule(
                Name=SCHED_NAME,
                ScheduleExpression="rate(4 hours)",
                ScheduleExpressionTimezone="UTC",
                FlexibleTimeWindow={"Mode": "OFF"},
                State="ENABLED",
                Description="Crypto opportunities scan -- volume/social/stable/convergence every 4h",
                Target=target,
            )
            add("schedule.updated", True, "rate(4 hours)")
        except ClientError as e:
            if "ResourceNotFoundException" in str(e):
                scheduler.create_schedule(
                    Name=SCHED_NAME,
                    ScheduleExpression="rate(4 hours)",
                    ScheduleExpressionTimezone="UTC",
                    FlexibleTimeWindow={"Mode": "OFF"},
                    State="ENABLED",
                    Description="Crypto opportunities scan -- volume/social/stable/convergence every 4h",
                    Target=target,
                )
                add("schedule.created", True, "rate(4 hours)")
            else:
                add("schedule.created", False, str(e)[:240])
    except Exception as e:
        add("schedule.failed", False, str(e)[:300])

    # Verify schedule exists + enabled
    try:
        sd = scheduler.get_schedule(Name=SCHED_NAME)
        add("schedule.live",
            sd.get("State") == "ENABLED",
            f"state={sd.get('State')} expr={sd.get('ScheduleExpression')} "
            f"target_role={(sd.get('Target') or {}).get('RoleArn', '')[-30:]}")
    except ClientError as e:
        add("schedule.live", False, str(e)[:200])

    # ── 2. Update signal-board with new normaliser + FEEDS entry ──
    print("  redeploying signal-board with crypto-opportunities entry...")
    try:
        z = zip_dir(SB_SRC)
        lam.update_function_code(FunctionName=SB_FN, ZipFile=z, Publish=False)
        for _ in range(15):
            v = lam.get_function_configuration(FunctionName=SB_FN)
            if v.get("LastUpdateStatus") == "Successful":
                break
            time.sleep(2)
        add("sb.code_updated", True, f"{len(z)}B")
    except ClientError as e:
        add("sb.code_updated", False, str(e)[:200])

    # ── 3. Invoke signal-board to ingest new feed ──
    print("  invoking signal-board to ingest crypto-opportunities...")
    try:
        r = lam.invoke(FunctionName=SB_FN, InvocationType="RequestResponse",
                       Payload=b"{}")
        payload = r["Payload"].read().decode()
        ok = r["StatusCode"] == 200 and not r.get("FunctionError")
        add("sb.invoke", ok, f"status={r['StatusCode']} body={payload[:200]}")
    except ClientError as e:
        add("sb.invoke", False, str(e)[:200])

    time.sleep(3)

    # ── 4. Verify signal-board output now has 21 engines + crypto-opportunities present ──
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key="data/signal-board.json")
        d = json.loads(obj["Body"].read())
        n_eng = d.get("n_engines", 0)
        n_live = d.get("n_live", 0)
        engines = d.get("engines", [])
        co = next((e for e in engines if "Crypto Opportunities" in e.get("engine", "")), None)
        add("sb.engine_count_21", n_eng >= 21,
            f"engines={n_eng} live={n_live} posture={d.get('composite_posture')}")
        add("sb.crypto_opportunities_ingested",
            co is not None and not co.get("stale", True),
            f"present={co is not None} stale={(co or {}).get('stale')} "
            f"signal={(co or {}).get('signal')} read={(co or {}).get('read', '')[:80]}")
    except Exception as e:
        add("sb.engine_count_21", False, str(e)[:200])

    # ── 5. Final state of crypto-opportunities engine ──
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key="data/crypto-opportunities.json")
        d = json.loads(obj["Body"].read())
        sm = d.get("summary", {})
        add("engine.production_state", True,
            f"state={d.get('state')} universe={sm.get('universe_size')} "
            f"vol={sm.get('n_volume_surge')} soc={sm.get('n_social_velocity')} "
            f"stb={sm.get('n_stable_inflows')} conv={sm.get('n_convergence')}")
    except Exception as e:
        add("engine.production_state", False, str(e)[:200])

    rep = {
        "ops": 972,
        "title": "schedule crypto-opportunities + signal-board 21-engine integration",
        "run_at": dt.datetime.utcnow().isoformat() + "Z",
        "checks": CHECKS,
        "summary": {"total": len(CHECKS),
                    "passed": sum(1 for c in CHECKS if c["passed"]),
                    "failed": sum(1 for c in CHECKS if not c["passed"])},
        "overall_ok": all(c["passed"] for c in CHECKS),
    }
    os.makedirs("aws/ops/reports", exist_ok=True)
    with open("aws/ops/reports/972_crypto_opportunities_schedule_sb.json", "w") as f:
        json.dump(rep, f, indent=2)
    p, t = rep["summary"]["passed"], rep["summary"]["total"]
    print(f"\n=== {p}/{t} ===")
    for c in CHECKS:
        flag = "OK  " if c["passed"] else "FAIL"
        print(f"  [{flag}] {c['name']:34} {c['detail'][:140]}")


if __name__ == "__main__":
    main()
