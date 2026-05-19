"""
ops/861 - justhodl-cro-escalation DEPLOY + VERIFY.

The intraday firm-risk tripwire watches the live tape through the US
session and escalates one Telegram ping only when risk has deteriorated
strictly past anything already flagged today.

Verifying a watcher that is meant to be SILENT most of the time takes
care. This op proves it without spamming:

  1. Ship the function; wire the 14/16/18/20 UTC schedule.
  2. LIVE-TAPE run - invoke with no event. This exercises the real FMP
     quote path and scores the actual tape; on a calm day it must come
     back with a low severity and escalate nothing.
  3. ESCALATION run - invoke with a simulated ALERT-grade tape and
     dry_run off. This sends ONE clearly-marked [VERIFICATION TEST]
     ping and must return escalated=true with a Telegram message_id,
     and must persist day_state with max_severity_escalated = 2.
  4. NO-SPAM proof - invoke again with the same ALERT tape, dry_run on.
     It reads the persisted state and must return should_escalate=false
     - a re-run at the same severity does NOT re-ping.
  5. WORSE-BREAK proof - invoke with a simulated SEVERE tape, dry_run
     on. Against the same persisted state it must return
     should_escalate=true - a genuine second leg down re-escalates.
  6. Read back data/cro-escalation.json and confirm the schedule.

Writes aws/ops/reports/861_cro_escalation_deploy.json.
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
FN = "justhodl-cro-escalation"
OUT_KEY = "data/cro-escalation.json"
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"
SRC = f"aws/lambdas/{FN}/source/lambda_function.py"
CONF = json.load(open(f"aws/lambdas/{FN}/config.json"))

# simulated tapes for the verification
TAPE_L2 = {"spy_pct": -2.4, "vix_pct": 20.0, "vix": 27.0, "hyg_pct": -0.5,
           "qqq_pct": -2.8, "iwm_pct": -3.0, "tlt_pct": 0.6}
TAPE_L3 = {"spy_pct": -4.1, "vix_pct": 55.0, "vix": 37.0, "hyg_pct": -1.2,
           "qqq_pct": -4.8, "iwm_pct": -5.5, "tlt_pct": 1.4}

cfg = Config(read_timeout=240, connect_timeout=20, retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
sch = boto3.client("scheduler", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)

rep = {
    "ops": 861,
    "ts": datetime.now(timezone.utc).isoformat(),
    "subject": "Deploy + verify justhodl-cro-escalation (the intraday "
               "firm-risk tripwire - escalates the CRO brief to Telegram "
               "only on a strictly worse intraday break)",
    "checks": [],
}


def check(name, ok, detail=""):
    rep["checks"].append({"check": name, "ok": bool(ok),
                          "detail": str(detail)[:240]})
    return ok


def get_json(key):
    return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())


def invoke(payload):
    r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                   Payload=json.dumps(payload).encode())
    raw = r["Payload"].read().decode("utf-8", "ignore")
    try:
        outer = json.loads(raw)
        inner = json.loads(outer.get("body") or "{}")
    except Exception:
        outer, inner = {}, {}
    return r.get("StatusCode"), r.get("FunctionError"), inner


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
SCHED = sb.get("schedule_name", f"{FN}-intraday")
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

# ---- 3) live-tape run ------------------------------------------------------
live = {}
try:
    sc, fe, live = invoke({})
    ok = sc == 200 and not fe and live.get("ok")
    check("live_tape_run_ok", ok,
          "severity=%s (%s), escalated=%s -- real FMP tape scored"
          % (live.get("severity"), live.get("severity_label"),
             live.get("escalated")))
except Exception as e:
    check("live_tape_run_ok", False, f"{type(e).__name__}: {e}")

# ---- 4) escalation run (one real [TEST] ping) -----------------------------
esc = {}
try:
    sc, fe, esc = invoke({"simulate": TAPE_L2})
    ok = (sc == 200 and not fe and esc.get("severity") == 2
          and esc.get("escalated") is True
          and esc.get("message_id") is not None)
    check("escalation_sends_ping", ok,
          "severity=%s escalated=%s message_id=%s info=%s"
          % (esc.get("severity"), esc.get("escalated"),
             esc.get("message_id"), esc.get("telegram_info")))
except Exception as e:
    check("escalation_sends_ping", False, f"{type(e).__name__}: {e}")

time.sleep(2)

# ---- 5) no-spam proof ------------------------------------------------------
try:
    sc, fe, again = invoke({"simulate": TAPE_L2, "dry_run": True})
    ok = (sc == 200 and not fe and again.get("severity") == 2
          and again.get("should_escalate") is False)
    check("no_spam_same_severity", ok,
          "re-run at ALERT -> should_escalate=%s (must be False), "
          "max escalated today=%s"
          % (again.get("should_escalate"),
             again.get("max_severity_escalated_today")))
except Exception as e:
    check("no_spam_same_severity", False, f"{type(e).__name__}: {e}")

# ---- 6) worse-break proof --------------------------------------------------
try:
    sc, fe, worse = invoke({"simulate": TAPE_L3, "dry_run": True})
    ok = (sc == 200 and not fe and worse.get("severity") == 3
          and worse.get("should_escalate") is True)
    check("worse_break_re_escalates", ok,
          "SEVERE break after ALERT -> severity=%s should_escalate=%s "
          "(must be True)"
          % (worse.get("severity"), worse.get("should_escalate")))
except Exception as e:
    check("worse_break_re_escalates", False, f"{type(e).__name__}: {e}")

# ---- 7) persisted state ----------------------------------------------------
state = {}
try:
    state = get_json(OUT_KEY)
    ds = state.get("day_state") or {}
    today = datetime.now(timezone.utc).date().isoformat()
    ok = (bool(state.get("generated_at"))
          and ds.get("date") == today
          and ds.get("max_severity_escalated") == 2
          and ds.get("n_pings", 0) >= 1)
    check("state_persisted", ok,
          "cro-escalation.json day=%s max_escalated=%s n_pings=%s"
          % (ds.get("date"), ds.get("max_severity_escalated"),
             ds.get("n_pings")))
except Exception as e:
    check("state_persisted", False, f"{type(e).__name__}: {e}")

check("thresholds_documented",
      isinstance(state.get("thresholds"), dict)
      and len(state.get("thresholds", {})) == 3,
      "tripwire ladder published in output: %s"
      % list((state.get("thresholds") or {}).keys()))

# ---- 8) schedule live ------------------------------------------------------
try:
    sd = sch.get_schedule(Name=SCHED)
    check("schedule_live", sd.get("State") == "ENABLED",
          "%s %s" % (sd.get("State"), sd.get("ScheduleExpression")))
except Exception as e:
    check("schedule_live", False, f"{type(e).__name__}: {e}")

# ---- summary ---------------------------------------------------------------
n_ok = sum(1 for c in rep["checks"] if c["ok"])
n_tot = len(rep["checks"])
rep["summary"] = "%d/%d checks passed" % (n_ok, n_tot)
rep["all_passed"] = n_ok == n_tot
rep["cro_escalation"] = {
    "live_tape_severity": live.get("severity"),
    "live_tape_label": live.get("severity_label"),
    "live_tape_escalated": live.get("escalated"),
    "test_ping_message_id": esc.get("message_id"),
    "schedule": "14/16/18/20 UTC daily",
}
if rep["all_passed"]:
    rep["verdict"] = (
        "INTRADAY RISK TRIPWIRE LIVE - the CRO brief now escalates. "
        "The watcher scored the real tape (severity %s / %s, escalated "
        "%s), sent one verified [TEST] ALERT ping (Telegram message_id "
        "%s), then PROVED its discipline: a re-run at the same severity "
        "stays silent, a genuine worse break re-escalates. Runs "
        "14/16/18/20 UTC; most days it sends nothing."
        % (live.get("severity"), live.get("severity_label"),
           live.get("escalated"), esc.get("message_id")))
else:
    bad = [c["check"] for c in rep["checks"] if not c["ok"]]
    rep["verdict"] = ("INTRADAY TRIPWIRE VERIFICATION INCOMPLETE - %d "
                      "check(s) failed: %s." % (len(bad), ", ".join(bad)))

with open("aws/ops/reports/861_cro_escalation_deploy.json", "w",
          encoding="utf-8") as f:
    json.dump(rep, f, indent=1)
print(json.dumps(rep, indent=1))
