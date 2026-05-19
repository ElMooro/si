"""
ops/858 - justhodl-hedge-planner DEPLOY + VERIFY.

The Hedge Execution Planner turns the Tail Hedge Overlay's sleeve
recommendation into a worked order ticket and tracks the standing
sleeve as state, so each run emits the rebalance delta.

This op proves the engine end-to-end:

  1. Ship the function from source (create or update).
  2. Wire the daily 05:00 UTC EventBridge Scheduler rule.
  3. Invoke it TWICE. The two-run sequence is the real proof: run one
     plans against whatever state exists; run two must read back the
     state run one wrote. If the state machine works, run two's
     "standing before" equals run one's "standing after", and once the
     sleeve matches the target the action settles to HOLD.
  4. Read back data/hedge-planner.json + data/hedge-book.json +
     history and prove the ticket is sound:
       - schema, valid action word, action->colour map;
       - the ticket is well-formed - every leg carries instrument,
         side, structure, tenor and a premium;
       - the ticket premium SIGN matches the action (buy-side positive,
         sell-side negative, no-trade zero);
       - the rebalance delta reconciles: after - before == delta;
       - the persisted hedge-book.json state matches standing-after;
       - cross-run state persistence (run 2 before == run 1 after);
       - the standing sleeve stays within the spend cap;
       - pre-trade checks present, confidence valid;
       - history carries today's snapshot; headline + CRO note set.
  5. Confirm the schedule is live + ENABLED.

Writes aws/ops/reports/858_hedge_planner_deploy.json.
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
FN = "justhodl-hedge-planner"
OUT_KEY = "data/hedge-planner.json"
STATE_KEY = "data/hedge-book.json"
HIST_KEY = "data/hedge-planner-history.json"
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"
SRC = f"aws/lambdas/{FN}/source/lambda_function.py"
CONF = json.load(open(f"aws/lambdas/{FN}/config.json"))

VALID_ACTION = {"OPEN", "ADD", "TRIM", "ROLL", "SWITCH", "HARVEST",
                "HOLD", "UNWIND", "NONE"}
ACTION_COLOR = {"OPEN": "green", "ADD": "green", "ROLL": "cyan",
                "SWITCH": "orange", "TRIM": "orange", "HARVEST": "cyan",
                "HOLD": "dim", "UNWIND": "orange", "NONE": "dim"}
BUY_ACTIONS = {"OPEN", "ADD", "ROLL", "SWITCH"}
SELL_ACTIONS = {"TRIM", "HARVEST", "UNWIND"}
MAX_HEDGE_SPEND_PCT = 1.5

cfg = Config(read_timeout=240, connect_timeout=20, retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
sch = boto3.client("scheduler", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)

rep = {
    "ops": 858,
    "ts": datetime.now(timezone.utc).isoformat(),
    "subject": "Deploy + verify justhodl-hedge-planner (the Hedge Execution "
               "Planner - turns the Tail Hedge Overlay sleeve into a worked "
               "order ticket and tracks the standing sleeve as state)",
    "checks": [],
}


def check(name, ok, detail=""):
    rep["checks"].append({"check": name, "ok": bool(ok),
                          "detail": str(detail)[:240]})
    return ok


def approx(a, b, tol=0.01):
    if a is None or b is None:
        return False
    return abs(float(a) - float(b)) <= tol


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


# ---- 3) invoke twice -------------------------------------------------------
def invoke():
    r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                   Payload=b"{}")
    body = r["Payload"].read().decode("utf-8", "ignore")
    return r.get("StatusCode"), r.get("FunctionError"), body


run1 = run2 = None
try:
    s1, e1, b1 = invoke()
    check("invoke_1_ok", s1 == 200 and not e1, e1 or "200")
    time.sleep(2)
    run1 = get_json(OUT_KEY)
    state1 = get_json(STATE_KEY)
except Exception as e:
    check("invoke_1_ok", False, f"{type(e).__name__}: {e}")
    state1 = {}

try:
    s2, e2, b2 = invoke()
    check("invoke_2_ok", s2 == 200 and not e2, e2 or "200")
    time.sleep(2)
    run2 = get_json(OUT_KEY)
except Exception as e:
    check("invoke_2_ok", False, f"{type(e).__name__}: {e}")

doc = run2 or run1 or {}

# ---- 4) audit --------------------------------------------------------------
try:
    head = s3.head_object(Bucket=S3_BUCKET, Key=OUT_KEY)
    age = (datetime.now(timezone.utc) - head["LastModified"]).total_seconds()
    check("output_fresh", age < 900, f"{round(age)}s old")
except Exception as e:
    check("output_fresh", False, f"{type(e).__name__}: {e}")

check("schema_ok", doc.get("schema_version") == "1.0",
      doc.get("schema_version"))

action = doc.get("action")
check("action_valid", action in VALID_ACTION, action)

check("action_color_maps",
      doc.get("action_color") == ACTION_COLOR.get(action),
      f"{action} -> {doc.get('action_color')}")

# ticket well-formed
tk = doc.get("ticket") or {}
legs = tk.get("legs") or []
legs_ok = all(
    l.get("instrument") and l.get("side") and l.get("structure")
    and l.get("tenor") and l.get("premium_pct_of_book") is not None
    for l in legs)
check("ticket_legs_well_formed",
      legs_ok and (len(legs) > 0 or action in ("HOLD", "NONE")),
      f"{len(legs)} leg(s), action {action}")

# ticket premium sign matches the action
signed = tk.get("signed_premium_pct_of_book")
if action in BUY_ACTIONS:
    sign_ok = signed is not None and signed > 0
elif action in SELL_ACTIONS:
    sign_ok = signed is not None and signed < 0
else:
    sign_ok = approx(signed, 0.0, 1e-6)
check("ticket_sign_matches_action", sign_ok,
      f"action {action}, signed premium {signed}")

# rebalance delta reconciles
rd = doc.get("rebalance_delta") or {}
sb_b = (doc.get("standing_sleeve_before") or {}).get("budget_pct_of_book")
sa_b = (doc.get("standing_sleeve_after") or {}).get("budget_pct_of_book")
delta = rd.get("budget_pct_delta")
recon = (approx(rd.get("from_pct"), sb_b)
         and approx(rd.get("to_pct"), sa_b)
         and approx((sa_b or 0) - (sb_b or 0), delta, 0.005))
check("rebalance_delta_reconciles", recon,
      f"before {sb_b} after {sa_b} delta {delta}")

# persisted state file matches standing-after
state2 = {}
try:
    state2 = get_json(STATE_KEY)
    sf_ok = approx(state2.get("target_budget_pct"), sa_b, 0.005)
    check("state_file_matches_standing_after", sf_ok,
          f"hedge-book.json budget {state2.get('target_budget_pct')} "
          f"vs standing-after {sa_b}")
except Exception as e:
    check("state_file_matches_standing_after", False,
          f"{type(e).__name__}: {e}")

# cross-run state persistence: run 2's "before" == run 1's "after"
if run1 and run2:
    r1_after = (run1.get("standing_sleeve_after") or {}).get(
        "budget_pct_of_book")
    r2_before = (run2.get("standing_sleeve_before") or {}).get(
        "budget_pct_of_book")
    persist_ok = approx(r1_after, r2_before, 0.005)
    # once the sleeve is established, the second run must settle (not
    # OPEN again) - proof the state machine read the persisted sleeve.
    settled = run2.get("action") != "OPEN" or run1.get("action") != "OPEN"
    check("cross_run_state_persists", persist_ok and settled,
          f"run1 after {r1_after} -> run2 before {r2_before}, "
          f"run1 {run1.get('action')} run2 {run2.get('action')}")
else:
    check("cross_run_state_persists", False, "missing a run")

# standing sleeve within spend cap
check("within_spend_cap",
      sa_b is not None and sa_b <= MAX_HEDGE_SPEND_PCT + 1e-6,
      f"standing-after {sa_b}% vs {MAX_HEDGE_SPEND_PCT}% cap")

# pre-trade checks present
ptc = doc.get("pre_trade_checks") or []
check("pre_trade_checks_present", len(ptc) >= 1,
      f"{len(ptc)} checks: " + ", ".join(c.get("check", "") for c in ptc))

# confidence valid
check("confidence_valid", doc.get("confidence") in ("HIGH", "MEDIUM", "LOW"),
      doc.get("confidence"))

# history carries today's snapshot
today = datetime.now(timezone.utc).date().isoformat()
try:
    hist = get_json(HIST_KEY)
    snaps = hist.get("snapshots") or []
    last = snaps[-1] if snaps else {}
    check("history_appended", len(snaps) >= 1 and last.get("date") == today,
          f"{len(snaps)} snapshot(s), last {last.get('date')}")
except Exception as e:
    check("history_appended", False, f"{type(e).__name__}: {e}")

# headline + CRO note non-trivial
check("brief_present",
      bool(doc.get("headline")) and len(doc.get("cro_note", "")) > 50,
      f"cro_note {len(doc.get('cro_note', ''))} chars")

# ---- 5) schedule live ------------------------------------------------------
try:
    sd = sch.get_schedule(Name=SCHED)
    check("schedule_live",
          sd.get("State") == "ENABLED"
          and sd.get("ScheduleExpression") == sb.get("cron"),
          f"{sd.get('State')} {sd.get('ScheduleExpression')}")
except Exception as e:
    check("schedule_live", False, f"{type(e).__name__}: {e}")

# ---- summary ---------------------------------------------------------------
n_ok = sum(1 for c in rep["checks"] if c["ok"])
n_tot = len(rep["checks"])
rep["summary"] = f"{n_ok}/{n_tot} checks passed"
rep["all_passed"] = n_ok == n_tot
rep["hedge_planner"] = {
    "action": doc.get("action"),
    "headline": doc.get("headline"),
    "side_summary": tk.get("side_summary"),
    "ticket_premium_pct": tk.get("total_premium_pct_of_book"),
    "ticket_premium_usd": tk.get("total_premium_usd"),
    "n_legs": len(legs),
    "standing_before_pct": sb_b,
    "standing_after_pct": sa_b,
    "scenario_class": doc.get("scenario_class"),
    "stance": doc.get("stance"),
    "n_checks_flagged": doc.get("n_checks_flagged"),
    "confidence": doc.get("confidence"),
    "cro_note": doc.get("cro_note"),
    "run1_action": (run1 or {}).get("action"),
    "run2_action": (run2 or {}).get("action"),
}
if rep["all_passed"]:
    rep["verdict"] = (
        "HEDGE EXECUTION PLANNER LIVE - the firm now turns a tail-hedge "
        "recommendation into a worked order ticket. First run actioned "
        "%s (%s, %s%% of book, %d leg[s]); the second run read the "
        "persisted sleeve and settled to %s, proving the rebalance state "
        "machine round-trips. Ticket sign, delta reconciliation, spend cap "
        "and pre-trade checks all hold. Runs daily 05:00 UTC after the "
        "Tail Hedge Overlay."
        % ((run1 or {}).get("action"), tk.get("side_summary"),
           tk.get("total_premium_pct_of_book"), len(legs),
           (run2 or {}).get("action")))
else:
    bad = [c["check"] for c in rep["checks"] if not c["ok"]]
    rep["verdict"] = ("HEDGE PLANNER VERIFICATION INCOMPLETE - %d check(s) "
                      "failed: %s." % (len(bad), ", ".join(bad)))

with open("aws/ops/reports/858_hedge_planner_deploy.json", "w",
          encoding="utf-8") as f:
    json.dump(rep, f, indent=1)
print(json.dumps(rep, indent=1))
