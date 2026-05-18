"""
ops/847 - justhodl-liquidity-capacity deploy + end-to-end verification.

The Liquidity & Capacity Monitor measures every position in the
consolidated firm book in days-to-liquidate at a 20% participation cap,
buckets the book into liquidity tiers, scores firm liquidity 0-100 and
surfaces the trapped illiquid tail. It is the liquidity discipline that
sits beside the Risk Monitor's exposure discipline.

This op is self-sufficient and idempotent against the deploy-lambdas race:

  1. Ship the function from source (create or update) with FMP_KEY env.
  2. Wire the EventBridge Scheduler schedule from config.json.
  3. Invoke synchronously (the engine pulls the FMP screener).
  4. Read back data/liquidity-capacity.json and prove the score is sane:
       - schema present, payload not an error;
       - liquidity posture and score are valid;
       - positions were measured -> the FMP volume fetch matched the
         firm book's names;
       - liquidity tiers are populated;
       - days-to-liquidate are non-negative and the least-liquid list is
         sorted slowest-first;
       - the cumulative liquidatable shares are monotone (1d <= 3d <= 5d);
       - the posture reconciles with the score band;
       - the book dollars are in a sane range against the notional AUM;
       - the by-desk and by-sector breakouts are present.
  5. Confirm the schedule is live + ENABLED.

Writes aws/ops/reports/847_liquidity_capacity_deploy.json.
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
FN = "justhodl-liquidity-capacity"
OUT_KEY = "data/liquidity-capacity.json"
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"
SRC = f"aws/lambdas/{FN}/source/lambda_function.py"
CONF = json.load(open(f"aws/lambdas/{FN}/config.json"))

VALID_POSTURE = {"LIQUID", "MODERATE", "TIGHT", "ILLIQUID", "UNKNOWN"}

cfg = Config(read_timeout=240, connect_timeout=20,
             retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
sch = boto3.client("scheduler", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)

rep = {
    "ops": 847,
    "ts": datetime.now(timezone.utc).isoformat(),
    "subject": "Deploy + verify justhodl-liquidity-capacity (firm "
               "Liquidity & Capacity Monitor - days-to-liquidate, tiers, "
               "0-100 liquidity score, trapped tail)",
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
check("payload_not_error", doc.get("ok") is not False,
      doc.get("error") or "ok")

posture = doc.get("liquidity_posture")
check("posture_valid", posture in VALID_POSTURE, posture)

score = doc.get("liquidity_score")
check("score_valid",
      score is None or (isinstance(score, (int, float))
                        and 0 <= score <= 100),
      f"score={score}")

firm = doc.get("firm") or {}
n_meas = firm.get("n_measured") or 0
check("positions_measured", n_meas > 0,
      f"n_measured={n_meas} of {firm.get('n_equity_names')} "
      f"(unknown_vol={firm.get('n_unknown_volume')})")

tiers = doc.get("tiers") or []
tiers_ok = bool(tiers) and all(
    isinstance(t.get("n"), int) and "book_pct" in t and t.get("tier")
    for t in tiers)
check("tiers_present", tiers_ok, f"{len(tiers)} tiers")

ll = doc.get("least_liquid_names") or []
days_vals = [p.get("days_to_liquidate") for p in ll
             if isinstance(p.get("days_to_liquidate"), (int, float))]
days_nonneg = all(d >= 0 for d in days_vals)
days_sorted = all(days_vals[i] >= days_vals[i + 1]
                   for i in range(len(days_vals) - 1))
check("days_sane", days_nonneg and days_sorted,
      f"{len(days_vals)} measured, nonneg={days_nonneg}, "
      f"sorted_desc={days_sorted}")

p1 = firm.get("pct_liquidatable_1d")
p3 = firm.get("pct_liquidatable_3d")
p5 = firm.get("pct_liquidatable_5d")
mono = (isinstance(p1, (int, float)) and isinstance(p3, (int, float))
        and isinstance(p5, (int, float))
        and p1 <= p3 + 0.01 <= p5 + 0.02)
check("liquidatable_monotonic", mono, f"1d={p1} 3d={p3} 5d={p5}")

# posture reconciles with the score band
if score is None:
    exp_posture = "UNKNOWN"
elif score >= 75:
    exp_posture = "LIQUID"
elif score >= 50:
    exp_posture = "MODERATE"
elif score >= 30:
    exp_posture = "TIGHT"
else:
    exp_posture = "ILLIQUID"
check("posture_reconciles", posture == exp_posture,
      f"posture={posture} expected={exp_posture} (score={score})")

book_usd = firm.get("book_usd") or 0
aum = firm.get("notional_aum_usd") or 0
check("book_dollars_sane",
      isinstance(book_usd, (int, float)) and 0 < book_usd <= 2.0 * aum,
      f"book_usd={book_usd} notional_aum={aum}")

check("by_desk_present", bool(doc.get("by_desk")),
      f"{len(doc.get('by_desk') or [])} desks")
check("by_sector_present", bool(doc.get("by_sector")),
      f"{len(doc.get('by_sector') or [])} sectors")

# ---- 5) schedule live ------------------------------------------------------
try:
    sd = sch.get_schedule(Name=SCHED)
    st = sd.get("State")
    check("schedule_live", st == "ENABLED",
          f"{st} {sd.get('ScheduleExpression')}")
except Exception as e:
    check("schedule_live", False, f"{type(e).__name__}: {e}")

# ---- summary ---------------------------------------------------------------
rep["liquidity_capacity"] = {
    "headline": doc.get("headline"),
    "liquidity_posture": posture,
    "liquidity_score": score,
    "firm": firm,
    "tiers": tiers,
    "by_desk": doc.get("by_desk"),
    "by_sector": (doc.get("by_sector") or [])[:8],
    "trapped_names": [
        {"symbol": p.get("symbol"),
         "days_to_liquidate": p.get("days_to_liquidate"),
         "position_usd": p.get("position_usd")}
        for p in (doc.get("trapped_names") or [])][:10],
    "least_liquid_top5": [
        {"symbol": p.get("symbol"), "tier": p.get("liquidity_tier"),
         "days_to_liquidate": p.get("days_to_liquidate")}
        for p in ll[:5]],
}

rep["all_pass"] = all(c["ok"] for c in rep["checks"])
rep["verdict"] = (
    f"LIQUIDITY MONITOR LIVE - firm book {posture}, liquidity score "
    f"{score}/100. {n_meas} positions measured against live FMP volume; "
    f"{firm.get('pct_liquidatable_1d')}% clears in a day, "
    f"{firm.get('n_trapped')} trapped name(s). The firm book is now scored "
    f"for liquidity risk every day, 02:00 UTC, alongside the Risk Monitor."
    if rep["all_pass"]
    else "REVIEW - see checks[]/liquidity_capacity")

out = json.dumps(rep, indent=2, default=str)
print(out)
try:
    with open("aws/ops/reports/847_liquidity_capacity_deploy.json", "w") as f:
        f.write(out)
except Exception as e:
    print("report write skipped:", e)
