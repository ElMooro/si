"""
ops/835 - justhodl-desk-allocator deploy + end-to-end verification.

The Desk Allocator is the multi-strategy capstone: it sizes the five
strategy desks (Best Ideas / Pairs / Trend / Merger-Arb / Risk Radar) by
Bayesian-shrinkage inverse-vol risk parity with a macro regime tilt, and
rolls them up into a firm net beta + diversification ratio.

This op is self-sufficient and idempotent against the deploy-lambdas race:

  1. Ship the function from source - create if missing, update code +
     config if it already exists (deploy-lambdas may have raced ahead).
  2. Wire the EventBridge Scheduler schedule from config.json - create or
     update; Scheduler invokes via justhodl-scheduler-role.
  3. Invoke the Lambda synchronously.
  4. Read back data/desk-allocator.json and prove the allocation is sane:
       - all 5 desks present, each with a valid status;
       - capital weights sum to ~100 (or 0 only if every desk is offline);
       - no desk breaches the 45% concentration cap;
       - desks are sorted by capital weight, descending;
       - the firm roll-up carries net beta + a diversification ratio;
       - the regime block is populated;
       - the decision-history sidecar was snapshotted.
  5. Confirm the schedule is live + ENABLED.

Writes aws/ops/reports/835_desk_allocator_deploy.json.
"""
import io
import json
import time
import zipfile
from datetime import datetime, timezone

import boto3
from botocore.config import Config

REGION = "us-east-1"
S3_BUCKET = "justhodl-dashboard-live"
FN = "justhodl-desk-allocator"
OUT_KEY = "data/desk-allocator.json"
HIST_KEY = "data/desk-allocator-history.json"
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"
SRC = f"aws/lambdas/{FN}/source/lambda_function.py"
CONF = json.load(open(f"aws/lambdas/{FN}/config.json"))

MAX_DESK_W = 45.0          # concentration cap the engine enforces
EXPECTED_DESKS = {"best-ideas", "pairs-arb", "trend-engine",
                  "merger-arb", "risk-radar"}
VALID_STATUS = {"FIRING", "DRY", "OFFLINE"}

cfg = Config(read_timeout=240, connect_timeout=20,
             retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
sch = boto3.client("scheduler", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)

rep = {
    "ops": 835,
    "ts": datetime.now(timezone.utc).isoformat(),
    "subject": "Deploy + verify justhodl-desk-allocator (multi-strategy "
               "capital allocator)",
    "checks": [],
}


def check(name, ok, detail=""):
    rep["checks"].append({"check": name, "ok": bool(ok),
                          "detail": str(detail)[:240]})
    return ok


# ---- 1) ship the function --------------------------------------------------
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
        rep["deploy"] = "updated"
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(
            FunctionName=FN, Runtime=CONF["runtime"], Role=ROLE,
            Handler=CONF["handler"], Timeout=CONF["timeout"],
            MemorySize=CONF["memory"], Architectures=CONF["architectures"],
            Description=CONF["description"][:255], Code={"ZipFile": zb})
        rep["deploy"] = "created"
    check("deploy_ok", True, rep["deploy"])
except Exception as e:
    rep["deploy"] = f"ERROR {type(e).__name__}: {e}"
    check("deploy_ok", False, rep["deploy"])

# wait Active + Successful
fn_arn = None
for _ in range(40):
    try:
        c = lam.get_function_configuration(FunctionName=FN)
        fn_arn = c.get("FunctionArn")
        if c.get("State") == "Active" and c.get(
                "LastUpdateStatus") == "Successful":
            break
    except Exception:
        pass
    time.sleep(3)

# ---- 2) wire the EventBridge Scheduler schedule ----------------------------
sb = CONF.get("eventbridge_scheduler", {})
SCHED = sb.get("schedule_name", f"{FN}-daily")
try:
    target = {
        "Arn": fn_arn,
        "RoleArn": sb["role_arn"],
    }
    common = dict(
        ScheduleExpression=sb["cron"],
        ScheduleExpressionTimezone=sb.get("timezone", "UTC"),
        FlexibleTimeWindow={"Mode": "OFF"},
        State="ENABLED",
        Description=sb.get("description", "")[:512],
        Target=target,
    )
    try:
        sch.get_schedule(Name=SCHED)
        sch.update_schedule(Name=SCHED, **common)
        rep["schedule"] = "updated"
    except sch.exceptions.ResourceNotFoundException:
        sch.create_schedule(Name=SCHED, **common)
        rep["schedule"] = "created"
    check("schedule_wired", True, f"{rep['schedule']} {SCHED}")
except Exception as e:
    rep["schedule"] = f"ERROR {type(e).__name__}: {e}"
    check("schedule_wired", False, rep["schedule"])

# ---- 3) invoke -------------------------------------------------------------
try:
    r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                   Payload=b"{}")
    body = r["Payload"].read().decode("utf-8", "ignore")
    fn_err = r.get("FunctionError")
    rep["invoke"] = {"status": r.get("StatusCode"), "fn_error": fn_err,
                     "body": body[:400]}
    check("invoke_ok", r.get("StatusCode") == 200 and not fn_err,
          fn_err or "200")
except Exception as e:
    rep["invoke"] = {"error": str(e)[:200]}
    check("invoke_ok", False, str(e)[:200])

time.sleep(3)

# ---- 4) read back + audit --------------------------------------------------
doc = {}
try:
    head = s3.head_object(Bucket=S3_BUCKET, Key=OUT_KEY)
    age = (datetime.now(timezone.utc) - head["LastModified"]).total_seconds()
    check("output_fresh", age < 900, f"{round(age)}s old")
    doc = json.loads(s3.get_object(
        Bucket=S3_BUCKET, Key=OUT_KEY)["Body"].read())
except Exception as e:
    check("output_fresh", False, f"{type(e).__name__}: {e}")

desks = doc.get("desks") or []
firm = doc.get("firm") or {}
regime = doc.get("regime") or {}

keys = {d.get("key") for d in desks}
check("all_five_desks", keys == EXPECTED_DESKS,
      f"{sorted(keys)}")

bad_status = [d.get("key") for d in desks
              if d.get("status") not in VALID_STATUS]
check("statuses_valid", not bad_status, bad_status)

wsum = round(sum((d.get("capital_weight_pct") or 0) for d in desks), 2)
all_offline = all(d.get("status") == "OFFLINE" for d in desks) \
    if desks else True
check("weights_sum_100",
      (abs(wsum - 100.0) <= 0.5) or (all_offline and wsum == 0.0),
      f"sum={wsum} all_offline={all_offline}")

over_cap = [(d.get("key"), d.get("capital_weight_pct"))
            for d in desks
            if (d.get("capital_weight_pct") or 0) > MAX_DESK_W + 0.5]
check("concentration_cap_held", not over_cap, over_cap)

ws = [(d.get("capital_weight_pct") or 0) for d in desks]
check("desks_sorted_desc", ws == sorted(ws, reverse=True), ws)

check("firm_rollup_present",
      isinstance(firm.get("net_equity_beta"), (int, float))
      and "diversification_ratio" in firm,
      f"net_beta={firm.get('net_equity_beta')} "
      f"div={firm.get('diversification_ratio')}")

check("regime_populated",
      regime.get("label") not in (None, "")
      and isinstance(regime.get("blended_risk_axis"), (int, float)),
      f"{regime.get('label')} axis={regime.get('blended_risk_axis')}")

# decision-history sidecar snapshotted
try:
    hist = json.loads(s3.get_object(
        Bucket=S3_BUCKET, Key=HIST_KEY)["Body"].read())
    nsnap = len(hist.get("snapshots") or [])
    check("history_snapshotted", nsnap >= 1, f"{nsnap} snapshots")
except Exception as e:
    check("history_snapshotted", False, f"{type(e).__name__}: {e}")

# ---- 5) schedule live ------------------------------------------------------
try:
    sd = sch.get_schedule(Name=SCHED)
    st = sd.get("State")
    check("schedule_live", st == "ENABLED",
          f"{st} {sd.get('ScheduleExpression')}")
except Exception as e:
    check("schedule_live", False, f"{type(e).__name__}: {e}")

# ---- summary ---------------------------------------------------------------
firing = sorted([d for d in desks if d.get("status") == "FIRING"],
                key=lambda d: -(d.get("capital_weight_pct") or 0))
rep["desk_allocator"] = {
    "headline": doc.get("headline"),
    "regime": regime.get("label"),
    "desks_firing": firm.get("desks_firing"),
    "desks_offline": firm.get("desks_offline"),
    "net_equity_beta": firm.get("net_equity_beta"),
    "portfolio_vol_est_pct": firm.get("portfolio_vol_est_pct"),
    "diversification_ratio": firm.get("diversification_ratio"),
    "dominant_desk": firm.get("dominant_desk"),
    "weights": [{"desk": d.get("name"), "key": d.get("key"),
                 "status": d.get("status"),
                 "weight_pct": d.get("capital_weight_pct"),
                 "active": d.get("active_count"),
                 "eff_vol_pct": d.get("effective_vol_pct"),
                 "freshness_h": d.get("freshness_hours")}
                for d in desks],
}

rep["all_pass"] = all(c["ok"] for c in rep["checks"])
dr = firm.get("diversification_ratio")
rep["verdict"] = (
    f"DESK ALLOCATOR LIVE - {firm.get('desks_firing')}/5 desks firing, "
    f"firm net equity beta {firm.get('net_equity_beta')}, diversification "
    f"ratio {dr}. Capital concentrates in {firm.get('dominant_desk')}. "
    f"Risk-parity + regime-tilt allocation production-clean, daily 00:30 UTC."
    if rep["all_pass"]
    else "REVIEW - see checks[]/desk_allocator")

out = json.dumps(rep, indent=2, default=str)
print(out)
try:
    s3.put_object(Bucket=S3_BUCKET,
                  Key="ops/reports/835_desk_allocator_deploy.json",
                  Body=out.encode(), ContentType="application/json")
except Exception as e:
    print(f"[ops835] S3 report write failed: {e}")
with open("aws/ops/reports/835_desk_allocator_deploy.json", "w") as f:
    f.write(out)
print("[ok] wrote aws/ops/reports/835_desk_allocator_deploy.json")
