"""
ops/840 - justhodl-desk-allocator: extend the capital allocator from five
strategy desks to SEVEN, then deploy + verify end-to-end.

WHY
---
The Desk Allocator is the multi-strategy capstone - it sizes the strategy
desks by Bayesian-shrinkage inverse-vol risk parity with a macro regime
tilt and rolls the book up into a firm net beta + diversification ratio.
It was built sizing five desks (Best Ideas / Pairs / Trend / Merger-Arb /
Risk Radar) while two genuine standalone strategy desks shipped after it:

  Spin-Off Desk   event-driven corporate separations / special situations
  Index-Recon     event-driven index-reconstitution forced-flow capture

A pod shop's central risk allocator sizes EVERY pod, not a subset - an
un-sized desk is capital that is never risk-budgeted. This op registers
both desks with archetype priors (vol / equity-beta / risk-beta / Sharpe)
grounded in their HFRI Event-Driven sub-strategy characteristics and
extends the prior cross-desk correlation matrix to a full 7x7 so the
firm-level diversification ratio stays honest.

VERIFY
------
  1. Ship the function from source (update if present, create if missing).
  2. Wire / refresh the EventBridge Scheduler schedule from config.json.
  3. Invoke synchronously.
  4. Read back data/desk-allocator.json and prove:
       - all SEVEN desks present, each a valid status;
       - the two new desks (spinoff-desk, index-recon) registered + sized;
       - capital weights sum to ~100 (or 0 only if every desk offline);
       - no desk breaches the 45% concentration cap;
       - desks sorted by capital weight descending;
       - firm roll-up carries net beta + a finite diversification ratio
         over the 7-desk book;
       - regime block populated;
       - decision-history sidecar snapshotted.
  5. Confirm the schedule is live + ENABLED.

Writes aws/ops/reports/840_desk_allocator_seven.json.
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

MAX_DESK_W = 45.0
EXPECTED_DESKS = {"best-ideas", "pairs-arb", "trend-engine",
                  "merger-arb", "spinoff-desk", "index-recon", "risk-radar"}
NEW_DESKS = {"spinoff-desk", "index-recon"}
VALID_STATUS = {"FIRING", "DRY", "OFFLINE"}

cfg = Config(read_timeout=240, connect_timeout=20,
             retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
sch = boto3.client("scheduler", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)

rep = {
    "ops": 840,
    "ts": datetime.now(timezone.utc).isoformat(),
    "subject": "Extend + verify justhodl-desk-allocator - five to seven "
               "strategy desks (Spin-Off Desk + Index-Recon registered)",
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
    target = {"Arn": fn_arn, "RoleArn": sb["role_arn"]}
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
by_key = {d.get("key"): d for d in desks}

keys = set(by_key)
check("all_seven_desks", keys == EXPECTED_DESKS,
      f"n={len(keys)} {sorted(keys)}")

check("new_desks_registered", NEW_DESKS.issubset(keys),
      f"present={sorted(NEW_DESKS & keys)} missing={sorted(NEW_DESKS - keys)}")

# the two new desks must each carry a valid status and a numeric weight
new_bad = []
for k in NEW_DESKS:
    d = by_key.get(k) or {}
    if d.get("status") not in VALID_STATUS:
        new_bad.append(f"{k}:status={d.get('status')}")
    if not isinstance(d.get("capital_weight_pct"), (int, float)):
        new_bad.append(f"{k}:weight={d.get('capital_weight_pct')}")
check("new_desks_sized", not new_bad,
      new_bad or f"spinoff={by_key.get('spinoff-desk',{}).get('status')}"
                 f"/{by_key.get('spinoff-desk',{}).get('capital_weight_pct')}% "
                 f"index-recon={by_key.get('index-recon',{}).get('status')}"
                 f"/{by_key.get('index-recon',{}).get('capital_weight_pct')}%")

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

# firm roll-up must now be computed over the 7-desk book
dr = firm.get("diversification_ratio")
check("firm_rollup_present",
      isinstance(firm.get("net_equity_beta"), (int, float))
      and firm.get("desks_total") == 7
      and (dr is None or (isinstance(dr, (int, float)) and dr > 0)),
      f"desks_total={firm.get('desks_total')} "
      f"net_beta={firm.get('net_equity_beta')} div={dr}")

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
    "desks_total": firm.get("desks_total"),
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
rep["verdict"] = (
    f"DESK ALLOCATOR EXTENDED TO 7 DESKS - {firm.get('desks_firing')}/"
    f"{firm.get('desks_total')} firing, firm net equity beta "
    f"{firm.get('net_equity_beta')}, diversification ratio "
    f"{firm.get('diversification_ratio')}. Spin-Off Desk + Index-Recon now "
    f"risk-budgeted alongside the original five. Capital concentrates in "
    f"{firm.get('dominant_desk')}. Production-clean, daily 00:30 UTC."
    if rep["all_pass"]
    else "REVIEW - see checks[]/desk_allocator")

out = json.dumps(rep, indent=2, default=str)
print(out)
try:
    with open("aws/ops/reports/840_desk_allocator_seven.json", "w") as f:
        f.write(out)
except Exception as e:
    print("report write skipped:", e)
