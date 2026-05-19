"""
ops/860 - justhodl-cro-digest DEPLOY + VERIFY.

The CRO Morning Brief reads the firm risk stack's verdict (firm-risk-
board), today's hedge order ticket (hedge-planner) and the convexity
context (tail-hedge), assembles a structured CRO morning note, and
pushes it to Khalid's Telegram.

This op proves the digest end-to-end:

  1. Ship the function from source (create or update).
  2. Wire the daily 12:15 UTC EventBridge Scheduler rule.
  3. Invoke with dry_run=true - assemble the brief WITHOUT sending, and
     prove the text is sound: non-empty, under Telegram's 4096-char
     cap, and carrying every required section (header, posture,
     today's hedge, alerts, the Risk Desk link).
  4. Invoke for REAL once - this sends one live brief to Telegram and
     must come back sent=true with a Telegram message_id, proving the
     Bot API path works end-to-end.
  5. Read back data/cro-digest.json - it must be persisted, sent, and
     its firm_posture / hedge_action must match the live Firm Risk
     Board and Hedge Planner feeds (the brief reflects the real stack,
     not stale or invented numbers).
  6. Confirm the history snapshot appended and the schedule is ENABLED.

Writes aws/ops/reports/860_cro_digest_deploy.json.
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
FN = "justhodl-cro-digest"
OUT_KEY = "data/cro-digest.json"
HIST_KEY = "data/cro-digest-history.json"
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"
SRC = f"aws/lambdas/{FN}/source/lambda_function.py"
CONF = json.load(open(f"aws/lambdas/{FN}/config.json"))

REQUIRED_MARKERS = ["CRO MORNING BRIEF", "POSTURE", "TODAY'S HEDGE",
                    "ALERTS", "risk-desk.html"]
VALID_POSTURE = {"GREEN", "AMBER", "RED", "UNKNOWN"}
VALID_ACTION = {"OPEN", "ADD", "TRIM", "ROLL", "SWITCH", "HARVEST",
                "HOLD", "UNWIND", "NONE"}

cfg = Config(read_timeout=240, connect_timeout=20, retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
sch = boto3.client("scheduler", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)

rep = {
    "ops": 860,
    "ts": datetime.now(timezone.utc).isoformat(),
    "subject": "Deploy + verify justhodl-cro-digest (the CRO Morning Brief - "
               "pushes the Risk Desk verdict and today's hedge ticket to "
               "Telegram each morning)",
    "checks": [],
}


def check(name, ok, detail=""):
    rep["checks"].append({"check": name, "ok": bool(ok),
                          "detail": str(detail)[:240]})
    return ok


def get_json(key):
    return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())


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


# ---- 3) dry-run invoke -----------------------------------------------------
def invoke(payload):
    r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                   Payload=json.dumps(payload).encode())
    body = r["Payload"].read().decode("utf-8", "ignore")
    try:
        outer = json.loads(body)
        inner = json.loads(outer.get("body") or "{}")
    except Exception:
        outer, inner = {}, {}
    return r.get("StatusCode"), r.get("FunctionError"), inner


dry = {}
try:
    sc, fe, dry = invoke({"dry_run": True})
    ok = sc == 200 and not fe and dry.get("ok") and dry.get("dry_run")
    check("dry_run_invoke_ok", ok, fe or f"status {sc}, ok={dry.get('ok')}")
except Exception as e:
    check("dry_run_invoke_ok", False, f"{type(e).__name__}: {e}")

dry_text = dry.get("text") or ""
missing = [m for m in REQUIRED_MARKERS if m not in dry_text]
check("brief_has_all_sections", bool(dry_text) and not missing,
      "all %d sections present" % len(REQUIRED_MARKERS) if not missing
      else "MISSING: " + ", ".join(missing))

cc = dry.get("char_count") or len(dry_text)
check("brief_within_telegram_cap", 0 < cc <= 4096,
      "brief is %d chars vs the 4096 Telegram cap" % cc)

check("brief_posture_valid",
      (dry.get("firm_posture") in VALID_POSTURE
       and dry.get("hedge_action") in VALID_ACTION),
      "posture=%s action=%s" % (dry.get("firm_posture"),
                                dry.get("hedge_action")))

# ---- 4) real send ----------------------------------------------------------
real = {}
try:
    sc, fe, real = invoke({})
    sent = real.get("sent")
    mid = real.get("message_id")
    ok = (sc == 200 and not fe and sent is True and mid is not None)
    check("real_send_ok", ok,
          "sent=%s message_id=%s info=%s"
          % (sent, mid, real.get("info")))
except Exception as e:
    check("real_send_ok", False, f"{type(e).__name__}: {e}")

# ---- 5) persisted digest reflects the live stack --------------------------
time.sleep(2)
digest = {}
try:
    digest = get_json(OUT_KEY)
    check("digest_persisted",
          bool(digest.get("generated_at")) and digest.get("sent") is True
          and digest.get("message_id") is not None,
          "cro-digest.json sent=%s message_id=%s"
          % (digest.get("sent"), digest.get("message_id")))
except Exception as e:
    check("digest_persisted", False, f"{type(e).__name__}: {e}")

try:
    board = get_json("data/firm-risk-board.json")
    planner = get_json("data/hedge-planner.json")
    posture_match = digest.get("firm_posture") == (
        board.get("firm_posture") or "UNKNOWN").upper()
    action_match = digest.get("hedge_action") == (
        planner.get("action") or "NONE").upper()
    check("digest_matches_live_stack", posture_match and action_match,
          "digest posture=%s vs board=%s | digest action=%s vs planner=%s"
          % (digest.get("firm_posture"), board.get("firm_posture"),
             digest.get("hedge_action"), planner.get("action")))
except Exception as e:
    check("digest_matches_live_stack", False, f"{type(e).__name__}: {e}")

# ---- 6) history + schedule -------------------------------------------------
try:
    hist = get_json(HIST_KEY)
    snaps = hist.get("snapshots") or []
    today = datetime.now(timezone.utc).date().isoformat()
    check("history_appended",
          any(s.get("date") == today for s in snaps),
          "%d snapshot(s), last %s"
          % (len(snaps), snaps[-1].get("date") if snaps else "none"))
except Exception as e:
    check("history_appended", False, f"{type(e).__name__}: {e}")

try:
    sd = sch.get_schedule(Name=SCHED)
    check("schedule_live",
          sd.get("State") == "ENABLED",
          "%s %s" % (sd.get("State"), sd.get("ScheduleExpression")))
except Exception as e:
    check("schedule_live", False, f"{type(e).__name__}: {e}")

# ---- summary ---------------------------------------------------------------
n_ok = sum(1 for c in rep["checks"] if c["ok"])
n_tot = len(rep["checks"])
rep["summary"] = "%d/%d checks passed" % (n_ok, n_tot)
rep["all_passed"] = n_ok == n_tot
rep["cro_digest"] = {
    "firm_posture": digest.get("firm_posture"),
    "hedge_action": digest.get("hedge_action"),
    "action_required": digest.get("action_required"),
    "posture_changed": digest.get("posture_changed"),
    "n_alert": digest.get("n_alert"),
    "n_watch": digest.get("n_watch"),
    "feeds_fresh": digest.get("feeds_fresh"),
    "char_count": digest.get("char_count"),
    "message_id": digest.get("message_id"),
    "schedule": "12:15 UTC daily",
}
if rep["all_passed"]:
    rep["verdict"] = (
        "CRO MORNING BRIEF LIVE - the firm risk stack now sends a daily "
        "CRO note to Telegram. Today's brief (firm posture %s, hedge "
        "action %s, %d alert / %d watch, %d/12 feeds fresh) assembled, "
        "verified under the 4096-char cap, and delivered end-to-end "
        "(Telegram message_id %s). Reflects the live Firm Risk Board and "
        "Hedge Planner. Runs daily 12:15 UTC after the overnight stack."
        % (rep["cro_digest"]["firm_posture"],
           rep["cro_digest"]["hedge_action"],
           rep["cro_digest"]["n_alert"] or 0,
           rep["cro_digest"]["n_watch"] or 0,
           rep["cro_digest"]["feeds_fresh"] or 0,
           rep["cro_digest"]["message_id"]))
else:
    bad = [c["check"] for c in rep["checks"] if not c["ok"]]
    rep["verdict"] = ("CRO MORNING BRIEF VERIFICATION INCOMPLETE - %d "
                      "check(s) failed: %s." % (len(bad), ", ".join(bad)))

with open("aws/ops/reports/860_cro_digest_deploy.json", "w",
          encoding="utf-8") as f:
    json.dump(rep, f, indent=1)
print(json.dumps(rep, indent=1))
