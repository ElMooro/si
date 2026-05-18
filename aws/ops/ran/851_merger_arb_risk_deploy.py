"""
ops/851 - justhodl-merger-arb-risk deploy + end-to-end verification.

The Merger-Arb Book Risk Monitor models the firm's risk-arbitrage sleeve on
its correct axis - deal-break risk - rather than through the equity style
factors the firm Stress Desk uses. It joins the firm book's Merger-Arb desk
positions to the Merger-Arb Spread Desk's per-deal record and computes the
cluster-break tail, worst-quartile break, deal-risk-weighted expected P&L
and full-close carry.

This op is self-sufficient and idempotent against the deploy-lambdas race:

  1. Ship the function from source (create or update).
  2. Wire the EventBridge Scheduler schedule from config.json.
  3. Invoke synchronously (pure S3 synthesis - no external API).
  4. Read back data/merger-arb-risk.json and prove the model is sound:
       - schema present;
       - posture is GREEN / AMBER / RED;
       - a merger-arb sleeve was identified in the firm book;
       - the live + no-deal counts add up to the sleeve size (accounting
         identity);
       - all four scenarios are present and well-formed;
       - the scenario P&L is economically ordered: a cluster break is the
         worst, full close the best, with the worst-quartile and the
         model-expected number bracketed between them;
       - per-deal break economics are sane (break loss <= 0, carry >= 0,
         implied break probability in [0, 1]);
       - the largest-break-risk table is sorted worst first;
       - the posture reconciles with the cluster-break loss limits.
  5. Confirm the schedule is live + ENABLED.

Writes aws/ops/reports/851_merger_arb_risk_deploy.json.
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
FN = "justhodl-merger-arb-risk"
OUT_KEY = "data/merger-arb-risk.json"
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"
SRC = f"aws/lambdas/{FN}/source/lambda_function.py"
CONF = json.load(open(f"aws/lambdas/{FN}/config.json"))

VALID_POSTURE = {"GREEN", "AMBER", "RED"}
SOFT, HARD = -8.0, -15.0

cfg = Config(read_timeout=240, connect_timeout=20,
             retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
sch = boto3.client("scheduler", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)

rep = {
    "ops": 851,
    "ts": datetime.now(timezone.utc).isoformat(),
    "subject": "Deploy + verify justhodl-merger-arb-risk (the Merger-Arb "
               "Book Risk Monitor - deal-break stress on the firm's "
               "risk-arbitrage sleeve)",
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

check("schema_ok", doc.get("schema") == "1.0", doc.get("schema"))

posture = doc.get("posture")
check("posture_valid", posture in VALID_POSTURE, posture)

sm = doc.get("summary") or {}
n_pos = sm.get("n_arb_positions") or 0
n_live = sm.get("n_live_deals") or 0
n_nd = sm.get("n_no_deal_data") or 0
check("sleeve_identified", n_pos > 0,
      f"n_arb_positions={n_pos} (sleeve {sm.get('sleeve_pct_of_book')}% of book)")

check("accounting_identity", n_live + n_nd == n_pos,
      f"live({n_live}) + no_deal({n_nd}) == positions({n_pos})")

scen = doc.get("scenarios") or []
well_formed = (len(scen) == 4 and all(
    s.get("scenario") and s.get("type")
    and isinstance(s.get("book_pnl_pct"), (int, float))
    for s in scen))
check("scenarios_present", well_formed, f"{len(scen)} scenarios")

# economic ordering: cluster <= worst_quartile <= full ; cluster <= model <= full
by_type = {s.get("type"): s.get("book_pnl_pct") for s in scen}
cb = by_type.get("tail")
wq = by_type.get("stress")
me = by_type.get("base")
fc = by_type.get("upside")
if None not in (cb, wq, me, fc):
    ordered = (cb <= wq + 1e-6 <= fc + 1e-6) and (cb <= me + 1e-6 <= fc + 1e-6)
else:
    ordered = False
check("scenario_ordering_sane", ordered,
      f"cluster={cb} worst_q={wq} model={me} full_close={fc}")

# per-deal break economics
positions = doc.get("positions") or []
live = [p for p in positions if p.get("has_live_deal")]
econ_bad = []
for p in live:
    bl = p.get("break_loss_pct")
    cc = p.get("carry_if_closes_pct")
    ib = p.get("implied_break_prob")
    dn = p.get("downside_to_unaffected_pct")
    if bl is not None and dn is not None and dn < 0 and bl > 1e-6:
        econ_bad.append(f"{p.get('symbol')}:break_loss>0")
    if cc is not None and cc < -1e-6:
        econ_bad.append(f"{p.get('symbol')}:carry<0")
    if ib is not None and not (0.0 <= ib <= 1.0):
        econ_bad.append(f"{p.get('symbol')}:impl_break={ib}")
check("break_economics_sane", not econ_bad,
      "all live deals sane" if not econ_bad else "; ".join(econ_bad[:4]))

check("positions_match_count", len(positions) == n_pos,
      f"positions[]={len(positions)} vs n_arb_positions={n_pos}")

# largest-break table sorted worst (most negative) first
tb = doc.get("top_break_risks") or []
tb_vals = [p.get("break_loss_pct") for p in tb
           if isinstance(p.get("break_loss_pct"), (int, float))]
tb_sorted = all(tb_vals[i] <= tb_vals[i + 1] + 1e-6
                for i in range(len(tb_vals) - 1))
check("top_break_risks_sorted", tb_sorted,
      f"{len(tb_vals)} ranked, sorted_worst_first={tb_sorted}")

# posture reconciles with the cluster-break loss limits
if cb is None:
    exp = None
elif cb <= HARD:
    exp = "RED"
elif cb <= SOFT:
    exp = "AMBER"
else:
    exp = "GREEN"
check("posture_reconciles", posture == exp,
      f"posture={posture} expected={exp} (cluster_break={cb})")

# ---- 5) schedule live ------------------------------------------------------
try:
    sd = sch.get_schedule(Name=SCHED)
    st = sd.get("State")
    check("schedule_live", st == "ENABLED",
          f"{st} {sd.get('ScheduleExpression')}")
except Exception as e:
    check("schedule_live", False, f"{type(e).__name__}: {e}")

# ---- summary ---------------------------------------------------------------
rep["merger_arb_risk"] = {
    "headline": doc.get("headline"),
    "posture": posture,
    "summary": sm,
    "scenarios": [{"scenario": s.get("scenario"), "type": s.get("type"),
                   "book_pnl_pct": s.get("book_pnl_pct")} for s in scen],
    "top_break_risks": [
        {"symbol": p.get("symbol"), "break_loss_pct": p.get("break_loss_pct"),
         "deal_risk": p.get("deal_risk"), "tier": p.get("tier")}
        for p in tb[:5]],
    "factor_model_cross_reference": doc.get("factor_model_cross_reference"),
}

rep["all_pass"] = all(c["ok"] for c in rep["checks"])
rep["verdict"] = (
    f"MERGER-ARB BOOK RISK MONITOR LIVE - posture {posture}. The risk-arb "
    f"sleeve is {sm.get('sleeve_pct_of_book')}% of the firm book across "
    f"{n_live} live deal(s); a full cluster break costs "
    f"{sm.get('cluster_break_pct')}% of book vs the soft -8% limit, and the "
    f"sleeve earns {sm.get('full_close_carry_pct')}% if every deal closes. "
    f"Deal-break economics sane, scenarios economically ordered, factor-model "
    f"cross-reference attached. Runs daily 03:30 UTC, completing the firm "
    f"risk stack."
    if rep["all_pass"]
    else "REVIEW - see checks[]/merger_arb_risk")

out = json.dumps(rep, indent=2, default=str)
print(out)
try:
    with open("aws/ops/reports/851_merger_arb_risk_deploy.json", "w") as f:
        f.write(out)
except Exception as e:
    print("report write skipped:", e)
