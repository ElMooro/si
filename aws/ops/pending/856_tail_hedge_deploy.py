"""
ops/856 - justhodl-tail-hedge DEPLOY + VERIFY.

The Tail Hedge Overlay is the firm convexity desk. It reads the
15-scenario stress desk, the factor risk model, the firm book, the CRO
Risk Board and the macro stress feeds, and sizes a deliberately convex
protection sleeve so the worst modelled scenario is pulled back inside
the soft loss limit. The sleeve is scenario-targeted (the worst named
scenario picks the instrument), cost-budgeted (sized to a premium
budget) and regime-timed (accumulate cheap, monetise into stress). It
places no trades - it sizes and recommends.

This op is self-sufficient and idempotent against the deploy-lambdas
race:

  1. Ship the function from source (create or update).
  2. Wire the EventBridge Scheduler schedule from config.json.
  3. Invoke synchronously (pure S3 synthesis - no external API).
  4. Read back data/tail-hedge.json + data/tail-hedge-history.json and
     prove the overlay is sound:
       - schema present;
       - hedge_posture is a valid posture word;
       - severity 0/1/2 maps to the status word and colour;
       - tail exposure carries the worst scenario + loss + limits;
       - the requirement logic reconciles: gap = worst - soft, and a
         hedge is required exactly when the worst case breaches soft;
       - the sleeve's scenario class re-derives from the worst scenario
         name, and the sleeve is well-formed;
       - the sizing math reconciles: budget = required / payoff_multiple
         and carry = budget * roll_factor * carry_multiplier;
       - the regime stance is valid and the carry multiplier reconciles
         with the regime score;
       - cross-engine consistency checks are present;
       - the append-only history file carries today's snapshot;
       - confidence is HIGH / MEDIUM / LOW;
       - the cost/benefit block and CRO hedge brief are populated.
  5. Confirm the schedule is live + ENABLED.

Writes aws/ops/reports/856_tail_hedge_deploy.json.
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
FN = "justhodl-tail-hedge"
OUT_KEY = "data/tail-hedge.json"
HIST_KEY = "data/tail-hedge-history.json"
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"
SRC = f"aws/lambdas/{FN}/source/lambda_function.py"
CONF = json.load(open(f"aws/lambdas/{FN}/config.json"))

VALID_POSTURE = {"HEDGE RECOMMENDED", "UNHEDGED -- NOT REQUIRED",
                 "UNDER-HEDGED", "NO STRESS FEED"}
SEV_STATUS = {0: "OK", 1: "WATCH", 2: "ALERT"}
SEV_COLOR = {0: "green", 1: "orange", 2: "red"}
ROLL_FACTOR = 3.6
# payoff multiples must mirror the engine's HEDGE_SLEEVES.
PAYOFF = {"EQUITY_CRASH": 6.0, "RATES_SHOCK": 4.0, "MOMENTUM_UNWIND": 5.0,
          "CREDIT_EVENT": 6.0, "VOL_SPIKE": 9.0}


def classify(name):
    """Mirror of the engine's classify_scenario - kept in lock-step."""
    n = (name or "").lower()
    if any(t in n for t in ("volmageddon", "vol ")):
        return "VOL_SPIKE"
    if any(t in n for t in ("momentum", "quant", "crowding")):
        return "MOMENTUM_UNWIND"
    if any(t in n for t in ("rate shock", "rates +", "rates+", "taper",
                            "stagflation")):
        return "RATES_SHOCK"
    if any(t in n for t in ("regional bank", "credit")):
        return "CREDIT_EVENT"
    return "EQUITY_CRASH"


cfg = Config(read_timeout=240, connect_timeout=20,
             retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
sch = boto3.client("scheduler", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)

rep = {
    "ops": 856,
    "ts": datetime.now(timezone.utc).isoformat(),
    "subject": "Deploy + verify justhodl-tail-hedge (the firm Tail Hedge "
               "Overlay - scenario-targeted convex protection sized so "
               "the worst stress scenario is pulled inside the soft "
               "loss limit)",
    "checks": [],
}


def check(name, ok, detail=""):
    rep["checks"].append({"check": name, "ok": bool(ok),
                          "detail": str(detail)[:240]})
    return ok


def approx(a, b, tol=0.05):
    if a is None or b is None:
        return False
    return abs(float(a) - float(b)) <= tol


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

posture = doc.get("hedge_posture")
sev = doc.get("severity")
check("posture_valid", posture in VALID_POSTURE, posture)

# severity maps to status word + colour
check("severity_maps", sev in (0, 1, 2)
      and doc.get("status") == SEV_STATUS.get(sev)
      and doc.get("status_color") == SEV_COLOR.get(sev),
      f"sev={sev} status={doc.get('status')} color={doc.get('status_color')}")

# tail exposure carries the worst scenario + loss + limits
te = doc.get("tail_exposure") or {}
worst_loss = te.get("worst_loss_pct")
soft = te.get("soft_loss_limit_pct")
hard = te.get("hard_loss_limit_pct")
te_ok = (te.get("worst_scenario") and worst_loss is not None
         and soft is not None and hard is not None)
check("tail_exposure_present", te_ok,
      f"worst={te.get('worst_scenario')} loss={worst_loss} "
      f"soft={soft} hard={hard}")

# requirement logic: gap = worst - soft; required iff worst breaches soft
hr = doc.get("hedge_requirement") or {}
req = hr.get("required")
gap = hr.get("gap_pp")
need_prot = hr.get("required_protection_pct_of_book")
if worst_loss is not None and soft is not None:
    expect_gap = round(worst_loss - soft, 2)
    expect_req = expect_gap < 0
    gap_ok = approx(gap, expect_gap, 0.02)
    req_ok = (req == expect_req)
    prot_ok = ((not req) or approx(need_prot, abs(expect_gap), 0.02))
    check("requirement_logic", gap_ok and req_ok and prot_ok,
          f"gap={gap}/{expect_gap} required={req}/{expect_req} "
          f"protection={need_prot}")
else:
    check("requirement_logic", posture == "NO STRESS FEED",
          "no stress feed - requirement correctly cannot size")

# sleeve scenario class re-derives + sleeve well-formed
hs = doc.get("hedge_sleeve") or {}
scls = hs.get("scenario_class")
expect_cls = classify(te.get("worst_scenario"))
sleeve_ok = (scls in PAYOFF and scls == expect_cls
             and hs.get("label") and hs.get("instruments")
             and hs.get("primary_leg") and hs.get("convex_leg"))
check("sleeve_valid", sleeve_ok,
      f"class={scls} expected={expect_cls} label={hs.get('label')}")

# payoff multiple matches the class
check("payoff_multiple_correct",
      approx(hs.get("payoff_multiple"), PAYOFF.get(scls), 0.01),
      f"{scls} -> {hs.get('payoff_multiple')} (want {PAYOFF.get(scls)})")

# sizing math: budget = required / payoff; carry = budget*roll*carry_mult
rg = doc.get("regime") or {}
carry_mult = rg.get("carry_multiplier")
budget = hs.get("hedge_budget_pct_of_book")
carry = hs.get("annualised_carry_pct")
if req:
    expect_budget = round(need_prot / hs.get("payoff_multiple", 1), 3)
    expect_carry = round(expect_budget * ROLL_FACTOR * (carry_mult or 1), 2)
    check("sizing_math",
          approx(budget, expect_budget, 0.02)
          and approx(carry, expect_carry, 0.05),
          f"budget={budget}/{expect_budget} carry={carry}/{expect_carry}")
else:
    check("sizing_math", (budget in (0, 0.0)) and (carry in (0, 0.0)),
          f"no hedge -> budget={budget} carry={carry}")

# regime stance valid + carry multiplier reconciles with regime score
stance = rg.get("stance")
rscore = rg.get("regime_score")
stance_ok = stance in ("ACCUMULATE", "HOLD", "MONETIZE")
mult_ok = True
if rscore is not None and carry_mult is not None:
    mult_ok = approx(carry_mult, 1.0 + rscore / 100.0, 0.02)
check("regime_valid", stance_ok and mult_ok,
      f"stance={stance} score={rscore} carry_mult={carry_mult}")

# cross-engine consistency checks present
cc = doc.get("consistency_checks") or []
check("consistency_checks_present", len(cc) >= 1,
      f"{len(cc)} checks: " + ", ".join(c.get("check", "") for c in cc))

# confidence valid
check("confidence_valid", doc.get("confidence") in ("HIGH", "MEDIUM", "LOW"),
      doc.get("confidence"))

# cost/benefit block populated
cb = doc.get("cost_benefit") or {}
check("cost_benefit_present",
      "annualised_carry_pct" in cb and "return_drag_pct" in cb
      and "tail_loss_averted_pct" in cb,
      f"keys={sorted(cb.keys())}")

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

# headline + CRO hedge brief non-trivial
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
rep["tail_hedge"] = {
    "hedge_posture": doc.get("hedge_posture"),
    "headline": doc.get("headline"),
    "tail_score": doc.get("tail_score"),
    "confidence": doc.get("confidence"),
    "worst_scenario": te.get("worst_scenario"),
    "worst_loss_pct": worst_loss,
    "soft_loss_limit_pct": soft,
    "gap_pp": gap,
    "scenario_class": scls,
    "hedge_budget_pct": budget,
    "annualised_carry_pct": carry,
    "regime_stance": stance,
    "regime_score": rscore,
    "cro_brief": doc.get("cro_brief"),
}

out_path = "aws/ops/reports/856_tail_hedge_deploy.json"
with open(out_path, "w", encoding="utf-8") as f:
    json.dump(rep, f, indent=1)
print(json.dumps(rep, indent=1))
