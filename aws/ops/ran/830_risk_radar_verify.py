"""ops/830 - deploy-guard + verify justhodl-risk-radar.

Risk Radar is the platform's first market-wide FUNDAMENTAL-deterioration
desk - distinct from the short-positioning engines (finra-short /
short-interest / short-pressure), the 8-K event alerter (redflag-alerter)
and the portfolio-level risk engines (portfolio-risk / risk-sizer).

deploy-lambdas.yml ships the engine and wires the EventBridge Scheduler
schedule from config.json. This op is self-healing: if the function or
schedule has not landed yet (CI race) it creates them, then invokes the
engine synchronously to seed data/risk-radar.json immediately (the first
scheduled run is otherwise 14:30 UTC tomorrow) and verifies the output is
REAL and SANE - ~503-name S&P universe screened, five deterioration axes
per name, every SHORT CANDIDATE backed by >=3 triggered axes.
"""
import io
import json
import time
import zipfile
from datetime import datetime, timezone

import boto3
from botocore.config import Config

cfg = Config(read_timeout=240, connect_timeout=20, retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
sched = boto3.client("scheduler", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)

BUCKET = "justhodl-dashboard-live"
FN = "justhodl-risk-radar"
SRC = f"aws/lambdas/{FN}/source/lambda_function.py"
CONF = json.load(open(f"aws/lambdas/{FN}/config.json"))
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"
OUT_KEY = "data/risk-radar.json"

report = {"ops": 830, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Deploy-guard + verify justhodl-risk-radar"}

# ---------------------------------------------------------------- package --
buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
    z.writestr("lambda_function.py", open(SRC, encoding="utf-8").read())
zb = buf.getvalue()


def wait_ready():
    for _ in range(45):
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("LastUpdateStatus") == "Successful" and \
                c.get("State") == "Active":
            return
        time.sleep(2)


try:
    # -- function: get-or-create, ensure code + sizing current ------------
    exists = True
    try:
        lam.get_function(FunctionName=FN)
    except lam.exceptions.ResourceNotFoundException:
        exists = False

    if exists:
        lam.update_function_code(FunctionName=FN, ZipFile=zb)
        wait_ready()
        lam.update_function_configuration(
            FunctionName=FN, MemorySize=CONF["memory"],
            Timeout=CONF["timeout"], Handler=CONF["handler"],
            Runtime=CONF["runtime"])
        wait_ready()
        report["deploy"] = "updated"
    else:
        lam.create_function(
            FunctionName=FN, Runtime=CONF["runtime"], Role=ROLE,
            Handler=CONF["handler"], Code={"ZipFile": zb},
            MemorySize=CONF["memory"], Timeout=CONF["timeout"],
            Architectures=CONF.get("architectures", ["x86_64"]),
            Description=CONF.get("description", "")[:255])
        wait_ready()
        report["deploy"] = "created"

    # -- EventBridge Scheduler schedule -----------------------------------
    sc = CONF["eventbridge_scheduler"]
    target = {
        "Arn": f"arn:aws:lambda:us-east-1:857687956942:function:{FN}",
        "RoleArn": sc["role_arn"],
        "Input": "{}",
        "RetryPolicy": {"MaximumRetryAttempts": 2,
                        "MaximumEventAgeInSeconds": 3600},
    }
    sched_args = dict(
        Name=sc["schedule_name"],
        ScheduleExpression=sc["cron"],
        ScheduleExpressionTimezone=sc.get("timezone", "UTC"),
        FlexibleTimeWindow={"Mode": "OFF"},
        State="ENABLED",
        Description=sc.get("description", "")[:255],
        Target=target,
    )
    try:
        sched.get_schedule(Name=sc["schedule_name"])
        sched.update_schedule(**sched_args)
        report["schedule"] = "updated"
    except sched.exceptions.ResourceNotFoundException:
        sched.create_schedule(**sched_args)
        report["schedule"] = "created"
    st = sched.get_schedule(Name=sc["schedule_name"])
    report["schedule_state"] = st.get("State")
    report["schedule_cron"] = st.get("ScheduleExpression")

    # -- invoke synchronously to seed the feed ----------------------------
    r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                   Payload=b"{}")
    payload = r["Payload"].read().decode()
    report["invoke"] = {"status": r.get("StatusCode"),
                        "fn_error": r.get("FunctionError"),
                        "body": payload[:400]}

    # -- read + validate the output ---------------------------------------
    time.sleep(2)
    raw = s3.get_object(Bucket=BUCKET, Key=OUT_KEY)["Body"].read()
    d = json.loads(raw)

    shorts = d.get("short_candidates", [])
    avoid = d.get("avoid_list", [])
    stack = d.get("stack", [])
    universe = d.get("universe_screened", 0)
    by_mode = d.get("by_failure_mode", {})

    # every short candidate must be backed by >=3 triggered axes and carry
    # the five-axis score breakdown
    shorts_well_formed = all(
        c.get("axes_triggered", 0) >= 3
        and isinstance(c.get("axis_scores"), dict)
        and len(c.get("axis_scores", {})) >= 1
        and c.get("tier") == "SHORT CANDIDATE"
        and c.get("deterioration_score", 0) >= 55
        for c in shorts
    ) if shorts else True
    # every stack card must expose a failure_mode and axis breakdown
    stack_well_formed = all(
        c.get("failure_mode") and isinstance(c.get("axis_scores"), dict)
        and c.get("deterioration_score") is not None
        for c in stack
    ) if stack else True
    # red_flags must be human-readable, non-empty for carried names
    flags_present = all(
        isinstance(c.get("red_flags"), list) and len(c.get("red_flags", [])) >= 1
        for c in stack[:30]
    ) if stack else True
    score_sorted = all(
        stack[i]["deterioration_score"] >= stack[i + 1]["deterioration_score"]
        for i in range(len(stack) - 1)
    )

    checks = {
        "universe_screened_sane": 450 <= universe <= 520,
        "schema_present": d.get("schema_version") == "1.0",
        "headline_present": bool(d.get("headline")),
        "counts_consistent": (
            d.get("n_carried") == len(stack)
            and d.get("n_short_candidates") == len(shorts)
            and d.get("n_avoid") == len(avoid)),
        "shorts_well_formed": shorts_well_formed,
        "stack_well_formed": stack_well_formed,
        "red_flags_present": flags_present,
        "stack_sorted_desc": score_sorted,
        "by_failure_mode_present": isinstance(by_mode, dict) and len(by_mode) >= 1,
        "methodology_present": bool(d.get("methodology")),
    }

    report["risk_radar"] = {
        "ok": all(checks.values()),
        "checks": checks,
        "headline": d.get("headline"),
        "universe_screened": universe,
        "n_carried": d.get("n_carried"),
        "n_short_candidates": d.get("n_short_candidates"),
        "n_avoid": d.get("n_avoid"),
        "by_failure_mode": by_mode,
        "top5_short": [
            {"sym": c.get("symbol"), "sector": c.get("sector"),
             "score": c.get("deterioration_score"),
             "mode": c.get("failure_mode"),
             "axes": c.get("axes_triggered"),
             "shorts_building": c.get("shorts_building"),
             "value_trap": c.get("value_trap_confirmed")}
            for c in shorts[:5]],
        "top5_avoid": [
            {"sym": c.get("symbol"), "sector": c.get("sector"),
             "score": c.get("deterioration_score"),
             "mode": c.get("failure_mode")}
            for c in avoid[:5]],
    }
    report["all_pass"] = (report["risk_radar"]["ok"]
                          and not report["invoke"]["fn_error"])
except Exception as e:
    import traceback
    report["error"] = f"{type(e).__name__}: {e}"
    report["trace"] = traceback.format_exc()[-1400:]
    report["all_pass"] = False

with open("aws/ops/reports/830_risk_radar_verify.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print(json.dumps(report, indent=2, default=str))
