"""
ops/850 - justhodl-firm-stress deploy + end-to-end verification.

The Firm Stress Desk re-prices the actual consolidated firm book through 15
named scenarios (historical replays + hypothetical macro shocks) using the
Factor Risk Model's cached six-factor loadings, attributes each loss by desk /
sector / name, and runs a reverse stress test. It is the scenario discipline
that sits beside the Risk Monitor (exposure) and the Factor Risk Model
(decomposition).

This op is self-sufficient and idempotent against the deploy-lambdas race:

  1. Ship the function from source (create or update).
  2. Wire the EventBridge Scheduler schedule from config.json.
  3. Invoke synchronously (pure S3 synthesis - no external API).
  4. Read back data/firm-stress.json and prove the model is sound:
       - schema present, payload not an error;
       - posture is one of GREEN / AMBER / RED;
       - all 15 scenarios are present and well-formed;
       - the scenario list is sorted worst-loss first;
       - every book P&L number is in a sane range;
       - desk and sector attribution reconcile to the firm P&L;
       - top losers / gainers are populated;
       - the loadings coverage counts add up to the names modelled;
       - the reverse stress test is present and monotone;
       - the positive-control (melt-up) scenario is not the worst;
       - the worst scenario is a crisis, not the control.
  5. Confirm the schedule is live + ENABLED.

Writes aws/ops/reports/850_firm_stress_deploy.json.
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
FN = "justhodl-firm-stress"
OUT_KEY = "data/firm-stress.json"
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"
SRC = f"aws/lambdas/{FN}/source/lambda_function.py"
CONF = json.load(open(f"aws/lambdas/{FN}/config.json"))

VALID_POSTURE = {"GREEN", "AMBER", "RED"}
FACTORS = ["MKT", "SIZE", "VALUE", "MOM", "QUALITY", "LOWVOL"]

cfg = Config(read_timeout=240, connect_timeout=20,
             retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
sch = boto3.client("scheduler", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)

rep = {
    "ops": 850,
    "ts": datetime.now(timezone.utc).isoformat(),
    "subject": "Deploy + verify justhodl-firm-stress (the firm Stress Desk - "
               "15-scenario factor-driven P&L on the actual firm book, with "
               "desk/sector/name attribution and a reverse stress test)",
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

scen = doc.get("scenarios") or []
well_formed = all(
    s.get("scenario") and s.get("type") in {"historical", "hypothetical",
                                            "control"}
    and isinstance(s.get("shock"), dict)
    and isinstance(s.get("book_pnl_pct"), (int, float))
    for s in scen)
check("scenarios_populated", len(scen) == 15 and well_formed,
      f"{len(scen)} scenarios, well_formed={well_formed}")

pnls = [s["book_pnl_pct"] for s in scen
        if isinstance(s.get("book_pnl_pct"), (int, float))]
sorted_ok = all(pnls[i] <= pnls[i + 1] + 1e-6 for i in range(len(pnls) - 1))
check("scenarios_sorted_worst_first", bool(pnls) and sorted_ok,
      f"worst={pnls[0] if pnls else None} best={pnls[-1] if pnls else None}")

pnl_sane = bool(pnls) and all(-80.0 <= x <= 60.0 for x in pnls)
check("book_pnl_in_range", pnl_sane,
      f"min={min(pnls) if pnls else None} max={max(pnls) if pnls else None}")

# desk + sector attribution must reconcile to the firm P&L of each scenario
worst = scen[0] if scen else {}
recon_fail = []
for s in scen:
    bp = s.get("book_pnl_pct", 0.0)
    ds = sum(d.get("pnl_pct", 0.0) for d in (s.get("desk_pnl") or []))
    ss = sum(x.get("pnl_pct", 0.0) for x in (s.get("sector_pnl") or []))
    if abs(ds - bp) > 0.6 or abs(ss - bp) > 0.6:
        recon_fail.append(f"{s.get('scenario','?')[:20]}"
                          f"(book={bp} desk={round(ds,2)} sec={round(ss,2)})")
check("attribution_reconciles", not recon_fail,
      "all 15 scenarios reconcile" if not recon_fail
      else "MISMATCH: " + "; ".join(recon_fail[:3]))

attr_ok = all(
    s.get("desk_pnl") and s.get("sector_pnl")
    and len(s.get("top_losers") or []) > 0
    and len(s.get("top_gainers") or []) > 0
    for s in scen)
check("attribution_present", attr_ok,
      f"desk/sector/losers/gainers populated on all {len(scen)}")

sm = doc.get("summary") or {}
n_names = sm.get("n_names_modelled") or 0
check("names_modelled", n_names > 0, f"n_names_modelled={n_names}")

cov = ((sm.get("n_direct_loadings") or 0) + (sm.get("n_sector_proxy") or 0)
       + (sm.get("n_book_fallback") or 0))
check("loadings_coverage_adds_up", cov == n_names,
      f"direct={sm.get('n_direct_loadings')} + "
      f"sector={sm.get('n_sector_proxy')} + "
      f"book={sm.get('n_book_fallback')} = {cov} vs n_names={n_names}")

rev = doc.get("reverse_stress") or {}
rev_present = (rev.get("unit_vector_used") and "to_minus_15pct" in rev
               and "to_minus_25pct" in rev)
check("reverse_stress_present", bool(rev_present),
      f"axis={rev.get('unit_vector_used')} unit_pnl={rev.get('unit_pnl_pct')}")

r15 = rev.get("to_minus_15pct") or {}
r25 = rev.get("to_minus_25pct") or {}
if r15.get("reachable") and r25.get("reachable"):
    rev_mono = r25.get("multiplier", 0) > r15.get("multiplier", 0) > 0
    rev_detail = (f"15%->{r15.get('multiplier')}x  "
                  f"25%->{r25.get('multiplier')}x")
else:
    rev_mono = True   # an unreachable target is a valid hedged-book outcome
    rev_detail = "target(s) not reachable - book hedged on the loss axis"
check("reverse_stress_monotone", rev_mono, rev_detail)

# the positive control (melt-up) must not be the worst scenario
ctrl = [s for s in scen if s.get("type") == "control"]
ctrl_pnl = ctrl[0]["book_pnl_pct"] if ctrl else None
worst_type = worst.get("type")
check("control_not_worst",
      ctrl_pnl is not None and worst_type != "control"
      and ctrl_pnl > pnls[0],
      f"control_pnl={ctrl_pnl} worst_type={worst_type} worst={pnls[0] if pnls else None}")

# ---- 5) schedule live ------------------------------------------------------
try:
    sd = sch.get_schedule(Name=SCHED)
    st = sd.get("State")
    check("schedule_live", st == "ENABLED",
          f"{st} {sd.get('ScheduleExpression')}")
except Exception as e:
    check("schedule_live", False, f"{type(e).__name__}: {e}")

# ---- summary ---------------------------------------------------------------
rep["firm_stress"] = {
    "headline": doc.get("headline"),
    "posture": posture,
    "summary": sm,
    "loss_limits": doc.get("loss_limits"),
    "worst_scenario": {
        "scenario": worst.get("scenario"),
        "type": worst.get("type"),
        "book_pnl_pct": worst.get("book_pnl_pct"),
        "worst_desk": (worst.get("desk_pnl") or [{}])[0],
        "worst_sector": (worst.get("sector_pnl") or [{}])[0],
        "top_losers": [
            {"symbol": p.get("symbol"), "pnl_pct": p.get("pnl_pct")}
            for p in (worst.get("top_losers") or [])[:5]],
    },
    "scenario_pnl": [
        {"scenario": s.get("scenario"), "type": s.get("type"),
         "book_pnl_pct": s.get("book_pnl_pct")}
        for s in scen],
    "reverse_stress": rev,
}

rep["all_pass"] = all(c["ok"] for c in rep["checks"])
rep["verdict"] = (
    f"STRESS DESK LIVE - firm stress posture {posture}. Worst of 15 "
    f"scenarios: {worst.get('scenario','?')} at {worst.get('book_pnl_pct')}% "
    f"of book; {sm.get('n_losing_scenarios')}/{sm.get('n_scenarios')} "
    f"scenarios lose money. {n_names} names re-priced through cached "
    f"six-factor loadings, attribution reconciles, reverse stress test "
    f"live. Runs daily 03:00 UTC after the Factor Risk Model."
    if rep["all_pass"]
    else "REVIEW - see checks[]/firm_stress")

out = json.dumps(rep, indent=2, default=str)
print(out)
try:
    with open("aws/ops/reports/850_firm_stress_deploy.json", "w") as f:
        f.write(out)
except Exception as e:
    print("report write skipped:", e)
