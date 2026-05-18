"""
ops/855 - justhodl-firm-risk-board DEPLOY + VERIFY.

The Firm Risk Board is the firm CRO synthesis layer. It reads the eight
firm risk engine outputs (risk-monitor, factor-risk, firm-stress,
liquidity-capacity, merger-arb-risk, pnl-attribution, desk-allocator,
firm-book) and reduces them to ONE firm posture, a binding-constraint
readout, a limit-utilisation table, day-over-day deltas, ranked top
firm risks and a deterministic CRO brief. It is a pure synthesis layer
- it never re-computes risk and never modifies an upstream engine.

This op is self-sufficient and idempotent against the deploy-lambdas
race:

  1. Ship the function from source (create or update).
  2. Wire the EventBridge Scheduler schedule from config.json.
  3. Invoke synchronously (pure S3 synthesis - no external API).
  4. Read back data/firm-risk-board.json + data/firm-risk-board-history.json
     and prove the synthesis is sound:
       - schema present;
       - firm_posture is GREEN / AMBER / RED;
       - all seven risk dimensions present and well-formed;
       - firm posture is genuinely WORST-OF: firm_severity equals the
         maximum dimension severity, and the GREEN/AMBER/RED word maps
         the severity correctly;
       - the binding constraint is the worst dimension (max severity,
         highest score breaks ties);
       - the limit-utilisation table is populated and the tightest
         limit reconciles with it;
       - the top firm risks are ranked worst-first;
       - cross-engine consistency checks are present;
       - the append-only history file carries today's snapshot;
       - confidence is HIGH / MEDIUM / LOW.
  5. Confirm the schedule is live + ENABLED.

Writes aws/ops/reports/855_firm_risk_board_deploy.json.
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
FN = "justhodl-firm-risk-board"
OUT_KEY = "data/firm-risk-board.json"
HIST_KEY = "data/firm-risk-board-history.json"
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"
SRC = f"aws/lambdas/{FN}/source/lambda_function.py"
CONF = json.load(open(f"aws/lambdas/{FN}/config.json"))

VALID_POSTURE = {"GREEN", "AMBER", "RED"}
SEV_WORD = {0: "GREEN", 1: "AMBER", 2: "RED"}

cfg = Config(read_timeout=240, connect_timeout=20,
             retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
sch = boto3.client("scheduler", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)

rep = {
    "ops": 855,
    "ts": datetime.now(timezone.utc).isoformat(),
    "subject": "Deploy + verify justhodl-firm-risk-board (the firm CRO "
               "Risk Board - one worst-of posture synthesised from the "
               "eight firm risk engines)",
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
env = {"Variables": CONF.get("environment", {})}

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
            Environment=env, Description=CONF["description"][:255])
        rep["deploy"] = "updated"
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(
            FunctionName=FN, Runtime=CONF["runtime"], Role=ROLE,
            Handler=CONF["handler"], Timeout=CONF["timeout"],
            MemorySize=CONF["memory"], Architectures=CONF["architectures"],
            Environment=env, Description=CONF["description"][:255],
            Code={"ZipFile": zb})
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
    common = dict(
        ScheduleExpression=sb["cron"],
        ScheduleExpressionTimezone=sb.get("timezone", "UTC"),
        FlexibleTimeWindow={"Mode": "OFF"},
        State="ENABLED",
        Description=sb.get("description", "")[:512],
        Target={"Arn": fn_arn, "RoleArn": sb["role_arn"]},
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

posture = doc.get("firm_posture")
sev = doc.get("firm_severity")
check("posture_valid", posture in VALID_POSTURE, posture)

# all seven dimensions present + well-formed
dims = doc.get("dimensions") or []
want_dims = {"MANDATE", "MARKET_VAR", "TAIL_STRESS", "LIQUIDITY",
             "CONCENTRATION", "EVENT_ARB", "PERFORMANCE"}
got_dims = {d.get("dimension") for d in dims}
well_formed = (len(dims) == 7 and got_dims == want_dims and all(
    d.get("label") and d.get("status") in ("OK", "WATCH", "ALERT")
    and d.get("severity") in (0, 1, 2)
    and isinstance(d.get("score"), (int, float))
    for d in dims))
check("dimensions_present", well_formed,
      f"{len(dims)} dims, set_match={got_dims == want_dims}")

# firm posture is genuinely WORST-OF the dimensions
max_sev = max([d.get("severity", 0) for d in dims] or [0])
check("posture_is_worst_of", sev == max_sev,
      f"firm_severity={sev} vs max(dimension severity)={max_sev}")

# the severity integer maps to the posture word
check("severity_posture_map", SEV_WORD.get(sev) == posture,
      f"severity {sev} -> {SEV_WORD.get(sev)} (posture says {posture})")

# binding constraint is the worst dimension (max severity, then max score)
bc = (doc.get("binding_constraint") or {}).get("dimension")
ranked = sorted(dims, key=lambda d: (-d.get("severity", 0),
                                     -d.get("score", 0)))
expect_bc = ranked[0].get("dimension") if ranked else None
check("binding_is_worst", bc == expect_bc,
      f"binding={bc} expected={expect_bc}")

# limit-utilisation table populated, tightest limit reconciles
util = doc.get("limit_utilization") or []
util_live = [u for u in util if u.get("utilization_pct") is not None]
check("utilization_present", len(util_live) >= 4,
      f"{len(util_live)} live limit rows of {len(util)}")

tl = doc.get("tightest_limit") or {}
if util_live:
    true_max = max(util_live, key=lambda u: u["utilization_pct"])
    check("tightest_limit_reconciles",
          tl.get("limit") == true_max.get("limit"),
          f"tightest={tl.get('limit')} ({tl.get('utilization_pct')}%)")
else:
    check("tightest_limit_reconciles", tl == {} or tl is None,
          "no live limits - tightest correctly empty")

# top firm risks ranked worst-first
risks = doc.get("top_firm_risks") or []
metrics = [r.get("metric_pct", 0) for r in risks]
check("top_risks_sorted",
      metrics == sorted(metrics, reverse=True) and len(risks) > 0,
      f"{len(risks)} risks, sorted_desc={metrics == sorted(metrics, reverse=True)}")

# cross-engine consistency checks present
cc = doc.get("consistency_checks") or []
check("consistency_checks_present", len(cc) >= 1,
      f"{len(cc)} checks: " + ", ".join(c.get("check", "") for c in cc))

# confidence is a valid grade
check("confidence_valid", doc.get("confidence") in ("HIGH", "MEDIUM", "LOW"),
      doc.get("confidence"))

# append-only history carries today's snapshot
today = datetime.now(timezone.utc).date().isoformat()
try:
    hist = json.loads(s3.get_object(
        Bucket=S3_BUCKET, Key=HIST_KEY)["Body"].read())
    snaps = hist.get("snapshots") or []
    last = snaps[-1] if snaps else {}
    check("history_appended",
          len(snaps) >= 1 and last.get("date") == today,
          f"{len(snaps)} snapshot(s), last date {last.get('date')}")
except Exception as e:
    check("history_appended", False, f"{type(e).__name__}: {e}")

# headline + CRO brief are non-trivial
check("brief_present",
      bool(doc.get("cro_brief")) and len(doc.get("cro_brief", "")) > 60
      and bool(doc.get("headline")),
      f"brief {len(doc.get('cro_brief', ''))} chars")

# ---- 5) schedule live ------------------------------------------------------
try:
    sd = sch.get_schedule(Name=SCHED)
    state = sd.get("State")
    expr = sd.get("ScheduleExpression")
    check("schedule_live", state == "ENABLED" and expr == sb.get("cron"),
          f"{state} {expr}")
except Exception as e:
    check("schedule_live", False, f"{type(e).__name__}: {e}")

# ---- summary ---------------------------------------------------------------
n_ok = sum(1 for c in rep["checks"] if c["ok"])
n_tot = len(rep["checks"])
rep["summary"] = f"{n_ok}/{n_tot} checks passed"
rep["all_passed"] = n_ok == n_tot
rep["firm_risk_board"] = {
    "firm_posture": doc.get("firm_posture"),
    "headline": doc.get("headline"),
    "binding_constraint": doc.get("binding_constraint"),
    "n_alert": doc.get("n_alert"),
    "n_watch": doc.get("n_watch"),
    "confidence": doc.get("confidence"),
    "cro_brief": doc.get("cro_brief"),
    "dimensions": [
        {"dimension": d.get("dimension"), "status": d.get("status"),
         "score": d.get("score"), "stale": d.get("stale")}
        for d in dims],
    "tightest_limit": doc.get("tightest_limit"),
}

out_path = "aws/ops/reports/855_firm_risk_board_deploy.json"
with open(out_path, "w", encoding="utf-8") as f:
    json.dump(rep, f, indent=1)
print(json.dumps(rep, indent=1))
