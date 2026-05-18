"""ops/824 - deploy + verify justhodl-pairs-arb (the Statistical Arbitrage /
Pairs Trading desk - the platform's first market-neutral sleeve).

Deploys the Lambda, schedules it via Amazon EventBridge Scheduler (the
go-forward path - the classic 300-rule EventBridge pool is saturated; the
justhodl-scheduler-role provisioned by ops 821 already covers justhodl-*),
invokes it, then proves the output is REAL and the strategy gates actually
held:

  - every tradeable pair is genuinely dislocated (|z| >= entry 2.0 sigma)
  - every tradeable pair mean-reverts on a sane half-life (3-45 days)
  - every tradeable pair clears the correlation floor (>= 0.85)
  - no tradeable pair is a suspected structural break; every quarantined
    pair really is past 4 sigma
  - the long and short legs are distinct and the universe formed real pairs
"""
import io
import json
import os
import time
import zipfile
from datetime import datetime, timezone

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

cfg = Config(read_timeout=300, connect_timeout=20, retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
scheduler = boto3.client("scheduler", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)

ACCT = "857687956942"
REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-pairs-arb"
SRC = f"aws/lambdas/{FN}/source/lambda_function.py"
CONF = json.load(open(f"aws/lambdas/{FN}/config.json"))
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"

report = {"ops": 824, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Deploy + verify justhodl-pairs-arb statistical-arbitrage "
                     "desk (first market-neutral sleeve)"}

# ---------------------------------------------------------------- 1. deploy
buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
    z.writestr("lambda_function.py", open(SRC, encoding="utf-8").read())
zb = buf.getvalue()
try:
    try:
        lam.get_function(FunctionName=FN)
        lam.update_function_code(FunctionName=FN, ZipFile=zb)
        for _ in range(30):
            if lam.get_function_configuration(
                    FunctionName=FN).get("LastUpdateStatus") == "Successful":
                break
            time.sleep(2)
        lam.update_function_configuration(
            FunctionName=FN, Handler=CONF["handler"], Runtime=CONF["runtime"],
            Role=ROLE, Timeout=CONF["timeout"], MemorySize=CONF["memory"],
            Description=CONF["description"][:255])
        report["deploy"] = "updated"
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(
            FunctionName=FN, Runtime=CONF["runtime"], Role=ROLE,
            Handler=CONF["handler"], Timeout=CONF["timeout"],
            MemorySize=CONF["memory"], Architectures=CONF["architectures"],
            Description=CONF["description"][:255], Code={"ZipFile": zb})
        report["deploy"] = "created"
except Exception as e:
    report["deploy"] = f"ERROR {type(e).__name__}: {str(e)[:200]}"

for _ in range(40):
    try:
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("State") == "Active" and c.get(
                "LastUpdateStatus") == "Successful":
            break
    except Exception:
        pass
    time.sleep(3)

# ------------------------------------------------ 2. EventBridge Scheduler
sch = CONF["eventbridge_scheduler"]
schedule_status = "skipped"
try:
    fn_arn = lam.get_function_configuration(FunctionName=FN)["FunctionArn"]
    target = {
        "Arn": fn_arn,
        "RoleArn": sch["role_arn"],
        "Input": "{}",
        "RetryPolicy": {"MaximumRetryAttempts": 2,
                        "MaximumEventAgeInSeconds": 3600},
    }
    params = dict(
        Name=sch["schedule_name"],
        GroupName="default",
        ScheduleExpression=sch["cron"],
        ScheduleExpressionTimezone=sch.get("timezone", "UTC"),
        FlexibleTimeWindow={"Mode": "OFF"},
        State="ENABLED",
        Description=sch.get("description", "justhodl-pairs-arb daily")[:512],
        Target=target,
    )
    last_err = None
    for _ in range(6):
        try:
            try:
                scheduler.create_schedule(**params)
                schedule_status = "created"
            except scheduler.exceptions.ConflictException:
                scheduler.update_schedule(**params)
                schedule_status = "updated"
            last_err = None
            break
        except ClientError as e:
            last_err = f"{e.response['Error']['Code']}: {str(e)[:140]}"
            time.sleep(8)
    if last_err:
        schedule_status = f"ERROR {last_err}"
except Exception as e:
    schedule_status = f"ERROR {type(e).__name__}: {str(e)[:160]}"
report["schedule"] = {"name": sch["schedule_name"], "status": schedule_status}

# ---------------------------------------------------------------- 3. invoke
try:
    r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                   Payload=b"{}")
    report["invoke"] = {"status": r.get("StatusCode"),
                        "fn_error": r.get("FunctionError"),
                        "body": json.loads(
                            r["Payload"].read() or b"{}").get("body")}
except Exception as e:
    report["invoke"] = {"error": str(e)[:200]}

time.sleep(3)

# ----------------------------------------------------------- 4. read output
ob = {}
try:
    ob = json.loads(s3.get_object(
        Bucket=BUCKET, Key="data/pairs-arb.json")["Body"].read())
except Exception as e:
    report["read_err"] = str(e)[:160]

pairs = ob.get("pairs") or []
breaks = ob.get("suspected_breaks") or []
summ = ob.get("summary") or {}

# strategy-gate integrity checks on the tradeable stack
z_ok = all((p.get("abs_z") or 0) >= 2.0 for p in pairs)
hl_ok = all(3.0 <= (p.get("half_life_days") or 0) <= 45.0 for p in pairs)
corr_ok = all((p.get("correlation") or 0) >= 0.85 for p in pairs)
no_break_in_stack = all(not p.get("suspected_break") for p in pairs)
breaks_really_wide = all((b.get("abs_z") or 0) > 4.0 for b in breaks)
legs_distinct = all(
    (p.get("long_leg") or {}).get("symbol")
    and (p.get("short_leg") or {}).get("symbol")
    and (p.get("long_leg") or {}).get("symbol")
    != (p.get("short_leg") or {}).get("symbol")
    for p in pairs)

report["pairs_arb"] = {
    "ok": ob.get("ok"),
    "headline": ob.get("headline"),
    "universe_symbols": ob.get("universe_symbols"),
    "symbols_with_history": ob.get("symbols_with_history"),
    "sectors_scanned": ob.get("sectors_scanned"),
    "pairs_tested": ob.get("pairs_tested"),
    "n_tradeable": summ.get("n_tradeable"),
    "n_prime": summ.get("n_prime"),
    "n_strong": summ.get("n_strong"),
    "n_suspected_breaks": summ.get("n_suspected_breaks"),
    "median_half_life_days": summ.get("median_half_life_days"),
    "elapsed_s": ob.get("elapsed_s"),
    "top5": [{"pair": p.get("pair"), "tier": p.get("tier"),
              "trade": p.get("trade"), "z": p.get("z_score"),
              "half_life": p.get("half_life_days"),
              "corr": p.get("correlation"),
              "round_trips": p.get("round_trips"),
              "conv_pct": p.get("expected_convergence_pct"),
              "score": p.get("score")} for p in pairs[:5]],
}

checks = {
    "deploy_ok": str(report.get("deploy", "")).startswith(
        ("created", "updated")),
    "scheduler_ok": schedule_status in ("created", "updated"),
    "invoke_ok": report.get("invoke", {}).get("status") == 200
                 and not report.get("invoke", {}).get("fn_error"),
    "output_ok": ob.get("ok") is True,
    "universe_formed": (ob.get("pairs_tested") or 0) >= 500,
    "has_tradeable_pairs": len(pairs) >= 5,
    "all_pairs_dislocated_2sigma": z_ok,
    "all_pairs_halflife_in_band": hl_ok,
    "all_pairs_clear_corr_floor": corr_ok,
    "no_break_leaked_into_stack": no_break_in_stack,
    "quarantined_breaks_really_wide": breaks_really_wide,
    "trade_legs_distinct": legs_distinct,
}
report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = (
    f"PAIRS DESK LIVE - {summ.get('n_tradeable')} cointegrated pairs "
    f"dislocated past 2 sigma ({summ.get('n_prime')} prime, "
    f"{summ.get('n_strong')} strong), {summ.get('n_suspected_breaks')} wide "
    f"pairs quarantined as suspected breaks; {ob.get('pairs_tested')} pairs "
    f"tested across {ob.get('sectors_scanned')} sectors. The platform's "
    "first market-neutral sleeve - all strategy gates verified holding."
    if report["all_pass"] else "REVIEW - see checks[]/pairs_arb")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/824_pairs_arb_deploy.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/824_pairs_arb_deploy.json")
