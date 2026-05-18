"""ops/826 - deploy-guard + verify justhodl-metals-miners.

deploy-lambdas.yml ships the engine and wires the EventBridge Scheduler
schedule from config.json. This op is self-healing: if the function or
schedule is not up yet (CI race), it creates them, then invokes the
engine synchronously to seed screener/metals-miners.json immediately
(the first scheduled run is otherwise 14:10 UTC tomorrow) and verifies
the output is REAL and SANE - 5 metal complexes, each anchored to a
proxy-ETF regime, miners scored with metal-anchored targets.
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
FN = "justhodl-metals-miners"
SRC = f"aws/lambdas/{FN}/source/lambda_function.py"
CONF = json.load(open(f"aws/lambdas/{FN}/config.json"))
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"
OUT_KEY = "screener/metals-miners.json"

report = {"ops": 826, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Deploy-guard + verify justhodl-metals-miners"}

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
    complexes = d.get("complexes", [])
    top = d.get("top_catch_up", [])
    cx_regimes = {c["key"]: c.get("metal_regime") for c in complexes}
    miners_total = sum(len(c.get("miners", [])) for c in complexes)
    with_target = sum(1 for c in complexes
                      for m in c.get("miners", []) if m.get("target"))
    graded = {}
    for c in complexes:
        for m in c.get("miners", []):
            graded[m.get("grade", "?")] = graded.get(m.get("grade", "?"), 0) + 1

    checks = {
        "five_complexes": len(complexes) == 5,
        "all_have_regime": all(c.get("metal_regime") for c in complexes),
        "miners_scored": miners_total >= 40,
        "top_catch_up_filled": len(top) >= 6,
        "headline_present": bool(d.get("headline")),
        "has_targets": with_target >= 5,
        "universe_matches": d.get("universe_count", 0) == miners_total,
    }
    report["best"] = {
        "ok": all(checks.values()),
        "checks": checks,
        "headline": d.get("headline"),
        "complex_regimes": cx_regimes,
        "working_complexes": d.get("working_complexes"),
        "miners_total": miners_total,
        "with_target": with_target,
        "grade_dist": graded,
        "build_seconds": d.get("build_seconds"),
        "top5": [{"sym": m.get("symbol"), "complex": m.get("complex"),
                  "score": m.get("score"), "grade": m.get("grade"),
                  "target": m.get("target"),
                  "implied_gain_pct": m.get("implied_gain_pct")}
                 for m in top[:5]],
    }
    report["all_pass"] = (report["best"]["ok"]
                          and not report["invoke"]["fn_error"])
except Exception as e:
    import traceback
    report["error"] = f"{type(e).__name__}: {e}"
    report["trace"] = traceback.format_exc()[-1400:]
    report["all_pass"] = False

with open("aws/ops/reports/826_metals_miners_verify.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print(json.dumps(report, indent=2, default=str))
