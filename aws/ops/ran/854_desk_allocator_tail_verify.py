"""
ops/854 - justhodl-desk-allocator TAIL-AWARE upgrade verification.

The Multi-Strategy Capital Allocator sized the seven strategy desks by
inverse-volatility risk parity. Inverse-vol parity is a symmetric,
central-moment risk measure: it under-penalises desks with negative
skew / fat left tails - carry and event-driven books that look calm
day to day until a cluster of deals breaks - and therefore over-funds
them. The firm's own Stress Desk flagged exactly this: the Merger-Arb
sleeve was the single largest tail loser in a 2008 / COVID replay.

This upgrade makes the allocator tail-aware. Each desk now carries a
documented archetype tail factor (above 1.0 for negative-skew
event-driven and risk-arbitrage desks, below 1.0 for the positive-skew
trend / CTA desk). When the firm Stress Desk sidecar is fresh, its
realised worst-case scenario losses shrink into that factor. The
sizing volatility is scaled by the tail factor before inverse-vol
parity runs, so calm-but-fragile desks stop being over-funded and the
crisis-alpha desk is rewarded.

This op is self-sufficient and idempotent against the deploy-lambdas
race:

  1. Ship the function from source (create or update).
  2. Wire the EventBridge Scheduler schedule from config.json.
  3. Invoke synchronously (pure S3 synthesis - no external API).
  4. Read back data/desk-allocator.json and prove the upgrade:
       - schema present, seven desks, weights sum to 100, 45% cap held;
       - every desk exposes tail_kurt / tail_factor / tail_src /
         tail_adj_vol_pct;
       - every tail factor sits inside the [0.75, 1.60] clamp;
       - tail_adj_vol == effective_vol x tail_factor on every desk;
       - the Merger-Arb desk is tail-heavy (factor > 1.0 and above the
         trend desk's factor);
       - the tail scaling actually moves capital: on the isolated
         inverse-vol parity backbone, Merger-Arb's share falls and the
         trend desk's share rises once the tail factor is applied;
       - parameters expose the tail knobs and the methodology text
         documents the tail-aware change.
  5. Confirm the schedule is live + ENABLED.

Writes aws/ops/reports/854_desk_allocator_tail_verify.json.
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
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"
SRC = f"aws/lambdas/{FN}/source/lambda_function.py"
CONF = json.load(open(f"aws/lambdas/{FN}/config.json"))

TF_LO, TF_HI = 0.75, 1.60

cfg = Config(read_timeout=240, connect_timeout=20,
             retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
sch = boto3.client("scheduler", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)

rep = {
    "ops": 854,
    "ts": datetime.now(timezone.utc).isoformat(),
    "subject": "Verify the tail-aware desk-allocator (archetype skew "
               "prior + firm-stress worst-case overlay on inverse-vol "
               "risk parity)",
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

desks = doc.get("desks") or []
check("seven_desks", len(desks) == 7, f"{len(desks)} desks")

wsum = sum(d.get("capital_weight_pct", 0) for d in desks)
check("weights_sum_100", abs(wsum - 100.0) < 0.5 or wsum == 0.0,
      f"sum={round(wsum, 2)}")

cap = (doc.get("parameters") or {}).get("max_desk_weight_pct", 45.0)
over = [d["key"] for d in desks
        if d.get("capital_weight_pct", 0) > cap + 1e-6]
check("cap_respected", not over, f"cap={cap}% over={over}")

need = ("tail_kurt", "tail_factor", "tail_src", "tail_adj_vol_pct")
missing = [d.get("key") for d in desks
           if any(k not in d for k in need)]
check("tail_fields_present", not missing and bool(desks),
      "all 7 desks expose tail fields" if not missing
      else f"missing on {missing}")

clamp_bad = [f"{d['key']}={d.get('tail_factor')}" for d in desks
             if not (TF_LO - 1e-6 <= (d.get("tail_factor") or -9)
                     <= TF_HI + 1e-6)]
check("tail_factors_in_clamp", not clamp_bad and bool(desks),
      "all in [0.75, 1.60]" if not clamp_bad else "; ".join(clamp_bad))

vol_bad = []
for d in desks:
    ev = d.get("effective_vol_pct")
    tf = d.get("tail_factor")
    tav = d.get("tail_adj_vol_pct")
    if None in (ev, tf, tav):
        vol_bad.append(f"{d.get('key')}:none")
    elif abs(tav - round(ev * tf, 1)) > 0.2:
        vol_bad.append(f"{d.get('key')}:{tav}!={round(ev * tf, 1)}")
check("tail_adj_vol_consistent", not vol_bad and bool(desks),
      "tail_adj_vol == eff_vol x tail_factor on every desk"
      if not vol_bad else "; ".join(vol_bad[:4]))

by = {d.get("key"): d for d in desks}
ma = by.get("merger-arb", {})
tr = by.get("trend-engine", {})
ma_tf = ma.get("tail_factor")
tr_tf = tr.get("tail_factor")
ok_heavy = (isinstance(ma_tf, (int, float)) and isinstance(tr_tf, (int, float))
            and ma_tf > 1.0 and ma_tf > tr_tf)
check("merger_arb_tail_heavy", ok_heavy,
      f"merger-arb tail_factor={ma_tf} trend tail_factor={tr_tf} "
      f"(need merger-arb>1.0 and >trend)")

# isolate the tail mechanism: inverse-vol parity shares with vs without
# the tail factor, computed purely from the output's own vol fields.
shift_detail = "insufficient vol data"
ok_shift = False
try:
    evs = {d["key"]: d["effective_vol_pct"] / 100.0 for d in desks
           if d.get("effective_vol_pct")}
    tavs = {d["key"]: d["tail_adj_vol_pct"] / 100.0 for d in desks
            if d.get("tail_adj_vol_pct")}
    if len(evs) == 7 and len(tavs) == 7:
        pure = {k: (1.0 / v) for k, v in evs.items()}
        ps = sum(pure.values())
        pure = {k: 100.0 * v / ps for k, v in pure.items()}
        tail = {k: (1.0 / v) for k, v in tavs.items()}
        ts = sum(tail.values())
        tail = {k: 100.0 * v / ts for k, v in tail.items()}
        ma_drop = tail["merger-arb"] < pure["merger-arb"] - 0.5
        tr_rise = tail["trend-engine"] > pure["trend-engine"] + 0.5
        ok_shift = ma_drop and tr_rise
        shift_detail = (
            f"merger-arb parity share {round(pure['merger-arb'], 1)}"
            f"->{round(tail['merger-arb'], 1)}%, trend "
            f"{round(pure['trend-engine'], 1)}"
            f"->{round(tail['trend-engine'], 1)}%")
except Exception as e:
    shift_detail = f"{type(e).__name__}: {e}"
check("tail_corrects_overweight", ok_shift, shift_detail)

params = doc.get("parameters") or {}
pk = ("tail_blend" in params and "tail_factor_clamp" in params
      and "tail_inputs" in params)
check("parameters_expose_tail", pk,
      f"tail_blend={params.get('tail_blend')} "
      f"clamp={params.get('tail_factor_clamp')} "
      f"inputs={params.get('tail_inputs')}")

method = (doc.get("methodology") or "").lower()
check("methodology_updated",
      "tail-aware" in method and "tail factor" in method,
      "methodology documents the tail-aware change")

check("headline_present", bool(doc.get("headline")),
      (doc.get("headline") or "")[:120])

# ---- 5) schedule live ------------------------------------------------------
try:
    sd = sch.get_schedule(Name=SCHED)
    st = sd.get("State")
    check("schedule_live", st == "ENABLED",
          f"{st} {sd.get('ScheduleExpression')}")
except Exception as e:
    check("schedule_live", False, f"{type(e).__name__}: {e}")

# ---- summary ---------------------------------------------------------------
rep["desk_allocator"] = {
    "headline": doc.get("headline"),
    "regime": (doc.get("regime") or {}).get("label"),
    "tail_inputs": params.get("tail_inputs"),
    "desks": [
        {"key": d.get("key"), "status": d.get("status"),
         "effective_vol_pct": d.get("effective_vol_pct"),
         "tail_factor": d.get("tail_factor"),
         "tail_adj_vol_pct": d.get("tail_adj_vol_pct"),
         "capital_weight_pct": d.get("capital_weight_pct"),
         "tail_src": d.get("tail_src")}
        for d in desks],
    "firm": doc.get("firm"),
}

rep["all_pass"] = all(c["ok"] for c in rep["checks"])
rep["verdict"] = (
    "TAIL-AWARE DESK-ALLOCATOR LIVE. Inverse-vol risk parity now runs on "
    "a tail-scaled sizing volatility: each desk's daily vol is multiplied "
    "by an archetype skew factor that shrinks toward the firm Stress "
    "Desk's worst-case scenario losses. The Merger-Arb desk - the "
    "platform's largest stress-test loser - is marked tail-heavy and no "
    "longer over-funded for being daily-calm; the positive-skew trend "
    "desk gains capital. Weights sum to 100, the 45% cap holds, all tail "
    "fields verified, schedule ENABLED daily 00:30 UTC."
    if rep["all_pass"]
    else "REVIEW - see checks[]/desk_allocator")

out = json.dumps(rep, indent=2, default=str)
print(out)
try:
    with open("aws/ops/reports/854_desk_allocator_tail_verify.json", "w") as f:
        f.write(out)
except Exception as e:
    print("report write skipped:", e)
