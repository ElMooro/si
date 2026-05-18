"""
ops/846 - justhodl-risk-monitor deploy + end-to-end verification.

The Risk Mandate Monitor is the firm's risk department: it scores the
consolidated firm book (justhodl-firm-book) against a hard institutional
mandate - gross ceiling, net band, single-name cap, sector cap, top-10
concentration, desk-capital cap, diversification floor, per-desk
drawdown stop, cross-desk conflict watch - and publishes one firm RISK
POSTURE (GREEN / AMBER / RED).

This op is self-sufficient and idempotent against the deploy-lambdas race:

  1. Ship the function from source (create or update).
  2. Wire the EventBridge Scheduler schedule from config.json.
  3. Invoke synchronously.
  4. Read back data/risk-monitor.json and prove the score is sane:
       - schema present, payload not an error;
       - risk_posture is one of GREEN / AMBER / RED;
       - the limits list is populated, every limit a valid status;
       - the firm posture reconciles with the limit statuses
         (RED iff a breach, AMBER iff a watch and no breach, else GREEN);
       - n_breaches / n_watches reconcile with the limit lists;
       - every single-name breach genuinely exceeds the mandate cap, and
         a non-empty breach list forces the single-name limit to BREACH;
       - the mandate block carries every documented limit;
       - the firm-book input was actually available.
  5. Confirm the schedule is live + ENABLED.

Writes aws/ops/reports/846_risk_monitor_deploy.json.
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
FN = "justhodl-risk-monitor"
OUT_KEY = "data/risk-monitor.json"
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"
SRC = f"aws/lambdas/{FN}/source/lambda_function.py"
CONF = json.load(open(f"aws/lambdas/{FN}/config.json"))

VALID_POSTURE = {"GREEN", "AMBER", "RED"}
VALID_STATUS = {"OK", "WATCH", "BREACH", "WARMING"}
MANDATE_KEYS = {
    "gross_ceiling_pct", "net_band_pct", "single_name_cap_pct",
    "sector_cap_pct", "top10_concentration_cap_pct",
    "desk_capital_cap_pct", "diversification_ratio_floor",
    "desk_drawdown_stop_pct", "desk_conflict_watch_count",
}

cfg = Config(read_timeout=240, connect_timeout=20,
             retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
sch = boto3.client("scheduler", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)

rep = {
    "ops": 846,
    "ts": datetime.now(timezone.utc).isoformat(),
    "subject": "Deploy + verify justhodl-risk-monitor (firm Risk Mandate "
               "Monitor - GREEN/AMBER/RED posture over the firm book)",
    "checks": [],
}


def check(name, ok, detail=""):
    rep["checks"].append({"check": name, "ok": bool(ok),
                          "detail": str(detail)[:240]})
    return ok


# ---- 1) ship ---------------------------------------------------------------
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

# ---- 2) schedule -----------------------------------------------------------
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

check("schema_ok", doc.get("schema_version") == "1.0",
      doc.get("schema_version"))
check("payload_not_error", doc.get("ok") is not False,
      doc.get("error") or "ok")

posture = doc.get("risk_posture")
check("posture_valid", posture in VALID_POSTURE, posture)

limits = doc.get("limits") or []
check("limits_present", len(limits) >= 8, f"{len(limits)} limits")

bad_status = [l.get("limit") for l in limits
              if l.get("status") not in VALID_STATUS]
check("statuses_valid", not bad_status, bad_status or "all valid")

# posture reconciles with the limit statuses
hard = [l for l in limits if l.get("status") != "WARMING"]
br = [l for l in hard if l.get("status") == "BREACH"]
wt = [l for l in hard if l.get("status") == "WATCH"]
expected = "RED" if br else ("AMBER" if wt else "GREEN")
check("posture_logic", posture == expected,
      f"posture={posture} expected={expected} "
      f"(breaches={len(br)} watches={len(wt)})")

check("counts_reconcile",
      doc.get("n_breaches") == len(doc.get("breaches") or [])
      and doc.get("n_watches") == len(doc.get("watches") or [])
      and doc.get("n_breaches") == len(br)
      and doc.get("n_watches") == len(wt),
      f"n_breaches={doc.get('n_breaches')} n_watches={doc.get('n_watches')}")

# single-name breach consistency
mandate = doc.get("mandate") or {}
sn_cap = mandate.get("single_name_cap_pct")
sn = doc.get("single_name_breaches") or []
sn_bad = []
if isinstance(sn_cap, (int, float)):
    sn_bad = [b.get("symbol") for b in sn
              if not (isinstance(b.get("net_pct"), (int, float))
                      and abs(b["net_pct"]) > sn_cap)]
sn_limit = next((l for l in limits
                 if l.get("limit") == "Single-name concentration"), {})
sn_forces_breach = (not sn) or (sn_limit.get("status") == "BREACH")
check("single_name_consistency", not sn_bad and sn_forces_breach,
      f"{len(sn)} breach(es), bad={sn_bad}, "
      f"sn_limit_status={sn_limit.get('status')}")

check("mandate_present", MANDATE_KEYS.issubset(set(mandate)),
      f"missing={sorted(MANDATE_KEYS - set(mandate))}")

inp = doc.get("inputs_available") or {}
check("firm_book_input_available", inp.get("firm_book") is True,
      f"inputs={inp}")

# ---- 5) schedule live ------------------------------------------------------
try:
    sd = sch.get_schedule(Name=SCHED)
    st = sd.get("State")
    check("schedule_live", st == "ENABLED",
          f"{st} {sd.get('ScheduleExpression')}")
except Exception as e:
    check("schedule_live", False, f"{type(e).__name__}: {e}")

# ---- summary ---------------------------------------------------------------
rep["risk_monitor"] = {
    "headline": doc.get("headline"),
    "risk_posture": posture,
    "risk_budget_utilization_pct": doc.get("risk_budget_utilization_pct"),
    "n_breaches": doc.get("n_breaches"),
    "n_watches": doc.get("n_watches"),
    "n_warming": doc.get("n_warming"),
    "breaches": doc.get("breaches"),
    "watches": doc.get("watches"),
    "single_name_breaches": [
        {"symbol": b.get("symbol"), "net_pct": b.get("net_pct"),
         "side": b.get("side")}
        for b in (doc.get("single_name_breaches") or [])][:10],
    "sector_breaches": doc.get("sector_breaches"),
    "limits": [{"limit": l.get("limit"), "current": l.get("current"),
                "utilization_pct": l.get("utilization_pct"),
                "status": l.get("status")} for l in limits],
    "firm_snapshot": doc.get("firm_snapshot"),
}

rep["all_pass"] = all(c["ok"] for c in rep["checks"])
rep["verdict"] = (
    f"RISK MONITOR LIVE - firm risk posture {posture}, "
    f"{doc.get('n_breaches')} breach(es) / {doc.get('n_watches')} watch(es), "
    f"risk-budget utilization {doc.get('risk_budget_utilization_pct')}%. "
    f"The consolidated firm book is now scored against a hard institutional "
    f"mandate every day, 01:30 UTC. The risk department is live."
    if rep["all_pass"]
    else "REVIEW - see checks[]/risk_monitor")

out = json.dumps(rep, indent=2, default=str)
print(out)
try:
    with open("aws/ops/reports/846_risk_monitor_deploy.json", "w") as f:
        f.write(out)
except Exception as e:
    print("report write skipped:", e)
