"""
ops 971 -- inspect crypto-opportunities output + schedule EventBridge + fix env
================================================================================

After ops 970 deployed + invoked successfully with state=QUIET (regime-correct
for current memecoin-winter), this ops:

1. Reads the S3 output and prints actual picks (what 2 vol-surge candidates fired?)
2. Schedules the Lambda via EventBridge Scheduler every 4h
3. Fixes env: pulls CMC_KEY from a donor Lambda that actually has it
   (try crypto-intel / crypto-narratives / dex-scanner)
4. Re-invokes to confirm env works
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
FN = "justhodl-crypto-opportunities"
S3_KEY = "data/crypto-opportunities.json"
ACC = "857687956942"
SCHED_NAME = "crypto-opportunities-4h"
ROLE_ARN = f"arn:aws:iam::{ACC}:role/eventbridge-scheduler-role"

lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=300, connect_timeout=10))
s3 = boto3.client("s3", region_name=REGION)
scheduler = boto3.client("scheduler", region_name=REGION)

CHECKS = []
def add(n, ok, d=""):
    CHECKS.append({"name": n, "passed": bool(ok), "detail": str(d)[:300]})


def main():
    print(f"ops 971 at {dt.datetime.utcnow().isoformat()}Z")

    # ── 1. Inspect current S3 output ──
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
        d = json.loads(obj["Body"].read())
        print("\n=== Current crypto-opportunities output ===")
        print(f"State: {d.get('state')}")
        print(f"Summary: {json.dumps(d.get('summary', {}))}")
        vol_picks = d.get("top_volume_surge", [])
        soc_picks = d.get("top_social_velocity", [])
        stb_picks = d.get("top_stable_inflows", [])
        conv_picks = d.get("convergence", [])
        print(f"\nVOLUME SURGE picks: {len(vol_picks)}")
        for v in vol_picks[:5]:
            print(f"  - {v.get('ticker')} ({v.get('name')[:24]}): mcap=${v.get('mcap_usd'):,.0f} "
                  f"price=${v.get('price_usd')} 24h={v.get('pct_change_24h')}% "
                  f"vol/mcap={v.get('vol_mcap_ratio')} strength={v.get('signal_strength')}")
        print(f"\nSOCIAL VELOCITY picks: {len(soc_picks)}")
        for v in soc_picks[:5]:
            print(f"  - {v.get('ticker')}: cg_score={v.get('social_meta',{}).get('coingecko_score')}")
        print(f"\nSTABLE INFLOW picks: {len(stb_picks)}")
        for v in stb_picks[:5]:
            print(f"  - {v.get('ticker')}: pct_stable={v.get('stable_meta',{}).get('pct_volume_stables')}%")
        print(f"\nCONVERGENCE picks: {len(conv_picks)}")
        for v in conv_picks[:5]:
            print(f"  - {v.get('ticker')}: signals={v.get('signals_fired')} composite={v.get('composite_score')}")
        add("output.parsed", True,
            f"state={d.get('state')} v={len(vol_picks)} s={len(soc_picks)} "
            f"stb={len(stb_picks)} c={len(conv_picks)}")
        # Sample the top vol-surge picks for the report
        if vol_picks:
            sample = vol_picks[0]
            add("sample.top_vol_pick", True,
                f"{sample.get('ticker')} ({sample.get('name')[:20]}) "
                f"mcap=${sample.get('mcap_usd'):,.0f} 24h={sample.get('pct_change_24h')}% "
                f"vol/mcap={sample.get('vol_mcap_ratio')}")
    except Exception as e:
        add("output.parsed", False, str(e)[:200])

    # ── 2. Find a donor with CMC_KEY ──
    candidates = ["justhodl-crypto-intel", "justhodl-crypto-narratives",
                  "justhodl-crypto-funding", "justhodl-bagger-engine",
                  "justhodl-dex-scanner", "justhodl-coffee-can"]
    cmc_key = None
    donor_used = None
    for d_name in candidates:
        try:
            info = lam.get_function_configuration(FunctionName=d_name)
            env = info.get("Environment", {}).get("Variables", {})
            v = env.get("CMC_KEY")
            if v and len(v) > 10:
                cmc_key = v
                donor_used = d_name
                break
        except ClientError:
            continue
    add("donor.cmc_key_found",
        cmc_key is not None,
        f"donor={donor_used} key_len={len(cmc_key) if cmc_key else 0}")

    # ── 3. Update env on the Lambda to include CMC_KEY ──
    if cmc_key:
        try:
            current_cfg = lam.get_function_configuration(FunctionName=FN)
            current_env = current_cfg.get("Environment", {}).get("Variables", {})
            current_env["CMC_KEY"] = cmc_key
            current_env["S3_BUCKET"] = S3_BUCKET
            lam.update_function_configuration(
                FunctionName=FN, Environment={"Variables": current_env})
            for _ in range(15):
                v = lam.get_function_configuration(FunctionName=FN)
                if v.get("LastUpdateStatus") == "Successful":
                    break
                time.sleep(2)
            add("env.updated", True, f"env keys={sorted(current_env.keys())}")
        except ClientError as e:
            add("env.updated", False, str(e)[:200])

    # ── 4. Schedule EventBridge Scheduler every 4h ──
    # Use existing eventbridge-scheduler-role
    try:
        # Try the established role first
        try:
            iam = boto3.client("iam", region_name=REGION)
            iam.get_role(RoleName="eventbridge-scheduler-role")
            role_arn = f"arn:aws:iam::{ACC}:role/eventbridge-scheduler-role"
        except ClientError:
            role_arn = ROLE_ARN
        target = {
            "Arn": f"arn:aws:lambda:{REGION}:{ACC}:function:{FN}",
            "RoleArn": role_arn,
            "Input": "{}",
            "RetryPolicy": {"MaximumRetryAttempts": 2, "MaximumEventAgeInSeconds": 3600},
        }
        try:
            scheduler.get_schedule(Name=SCHED_NAME)
            scheduler.update_schedule(
                Name=SCHED_NAME,
                ScheduleExpression="rate(4 hours)",
                ScheduleExpressionTimezone="UTC",
                FlexibleTimeWindow={"Mode": "OFF"},
                State="ENABLED",
                Description="Crypto opportunities scan -- volume / social / stable / convergence",
                Target=target,
            )
            add("schedule.upserted", True, f"updated existing rate(4 hours)")
        except ClientError as e:
            if "ResourceNotFoundException" in str(e):
                scheduler.create_schedule(
                    Name=SCHED_NAME,
                    ScheduleExpression="rate(4 hours)",
                    ScheduleExpressionTimezone="UTC",
                    FlexibleTimeWindow={"Mode": "OFF"},
                    State="ENABLED",
                    Description="Crypto opportunities scan -- volume / social / stable / convergence",
                    Target=target,
                )
                add("schedule.upserted", True, f"created rate(4 hours)")
            else:
                add("schedule.upserted", False, str(e)[:240])
    except Exception as e:
        add("schedule.upserted", False, str(e)[:300])

    # Grant invoke permission to scheduler
    try:
        lam.add_permission(
            FunctionName=FN,
            StatementId=f"Scheduler-{SCHED_NAME}",
            Action="lambda:InvokeFunction",
            Principal="scheduler.amazonaws.com",
            SourceArn=f"arn:aws:scheduler:{REGION}:{ACC}:schedule/default/{SCHED_NAME}",
        )
        add("schedule.permission", True, "added")
    except ClientError as e:
        if "ResourceConflictException" in str(e):
            add("schedule.permission", True, "already exists (idempotent)")
        else:
            add("schedule.permission", False, str(e)[:200])

    # ── 5. Re-invoke to confirm everything works after env update ──
    print(f"\n  re-invoking {FN} with updated env...")
    try:
        t0 = time.time()
        r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
        dur = round(time.time() - t0, 1)
        payload = json.loads(r["Payload"].read().decode())
        body = json.loads(payload.get("body", "{}")) if isinstance(payload.get("body"), str) else {}
        ok = r["StatusCode"] == 200 and payload.get("statusCode") == 200
        add("reinvoke.success", ok,
            f"dur={dur}s state={body.get('state')} conv={body.get('n_convergence')} "
            f"vol={body.get('n_volume_surge')} soc={body.get('n_social_velocity')} "
            f"stb={body.get('n_stable_inflows')}")
    except Exception as e:
        add("reinvoke.success", False, str(e)[:200])

    rep = {
        "ops": 971,
        "title": "inspect crypto-opportunities output + schedule + env fix",
        "run_at": dt.datetime.utcnow().isoformat() + "Z",
        "checks": CHECKS,
        "summary": {"total": len(CHECKS),
                    "passed": sum(1 for c in CHECKS if c["passed"]),
                    "failed": sum(1 for c in CHECKS if not c["passed"])},
        "overall_ok": all(c["passed"] for c in CHECKS),
    }
    os.makedirs("aws/ops/reports", exist_ok=True)
    with open("aws/ops/reports/971_crypto_opportunities_schedule_fix.json", "w") as f:
        json.dump(rep, f, indent=2)
    p, t = rep["summary"]["passed"], rep["summary"]["total"]
    print(f"\n=== {p}/{t} ===")
    for c in CHECKS:
        flag = "OK  " if c["passed"] else "FAIL"
        print(f"  [{flag}] {c['name']:32} {c['detail'][:140]}")


if __name__ == "__main__":
    main()
