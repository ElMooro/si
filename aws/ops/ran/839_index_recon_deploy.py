"""
ops/839 - justhodl-index-recon deploy + end-to-end verification.

The Index Reconstitution / Forced-Flow Desk reconstructs the FTSE Russell
US map from live total market caps and projects the four reconstitution
events that move on a forced, rules-based passive schedule:
  ADDITION  / DEMOTION   -> forced passive BUYING  (BULLISH)
  GRADUATION / DELETION  -> forced passive SELLING (BEARISH)
plus an S&P 500 inclusion-candidate watch list.

This op is self-sufficient and idempotent against the deploy-lambdas race:

  1. Ship the function from source - create if missing, update code +
     config if it already exists (deploy-lambdas may have raced ahead).
  2. Wire the EventBridge Scheduler schedule from config.json - create or
     update; Scheduler invokes via justhodl-scheduler-role.
  3. Invoke the Lambda synchronously.
  4. Read back data/index-recon.json and prove it is sane:
       - ok flag true;
       - eligible universe is large enough to reach the Russell boundary;
       - the two cap breakpoints are positive and correctly ordered
         (R1000 breakpoint cap > bottom-of-R3000 breakpoint cap);
       - every projected event carries the correct directional sign -
         additions + demotions BULLISH, graduations + deletions BEARISH;
       - every event row has an edge score inside 0-100;
       - the headline counts match the projected lists;
       - S&P 500 candidates are genuinely NOT current index members.
  5. Confirm the schedule is live + ENABLED.

Writes aws/ops/reports/839_index_recon_deploy.json.
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
FN = "justhodl-index-recon"
OUT_KEY = "data/index-recon.json"
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"
SRC = f"aws/lambdas/{FN}/source/lambda_function.py"
CONF = json.load(open(f"aws/lambdas/{FN}/config.json"))

cfg = Config(read_timeout=300, connect_timeout=20,
             retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
sch = boto3.client("scheduler", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)

rep = {
    "ops": 839,
    "ts": datetime.now(timezone.utc).isoformat(),
    "subject": "Deploy + verify justhodl-index-recon (Index "
               "Reconstitution / Forced-Flow Desk)",
    "checks": [],
}


def check(name, ok, detail=""):
    rep["checks"].append({"check": name, "ok": bool(ok),
                          "detail": str(detail)[:240]})
    return ok


# ---- 1) ship the function --------------------------------------------------
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

# wait Active + Successful
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

# ---- 2) wire the EventBridge Scheduler schedule ----------------------------
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

check("ok_true", doc.get("ok") is True, f"ok={doc.get('ok')}")

uni = doc.get("universe") or {}
n_elig = uni.get("n_eligible") or 0
check("universe_sufficient", n_elig >= 1500,
      f"n_eligible={n_elig}")

bp1 = uni.get("russell_1000_breakpoint_cap_bil") or 0
bp3 = uni.get("russell_3000_breakpoint_cap_bil") or 0
check("breakpoints_sane", bp1 > 0 and bp3 > 0 and bp1 > bp3,
      f"R1000bp={bp1}B  R3000bp={bp3}B")

adds = doc.get("russell_2000_additions") or []
demos = doc.get("russell_demotions") or []
grads = doc.get("russell_graduations") or []
dels = doc.get("russell_2000_deletions") or []
spc = doc.get("sp500_candidates") or []


def all_dir(lst, want):
    return all(row.get("direction") == want for row in lst)


check("additions_bullish", all_dir(adds, "BULLISH"),
      f"{len(adds)} rows")
check("demotions_bullish", all_dir(demos, "BULLISH"),
      f"{len(demos)} rows")
check("graduations_bearish", all_dir(grads, "BEARISH"),
      f"{len(grads)} rows")
check("deletions_bearish", all_dir(dels, "BEARISH"),
      f"{len(dels)} rows")

# every projected event row carries an edge score inside 0-100
ev_rows = adds + demos + grads + dels
bad_edge = [(row.get("symbol"), row.get("edge_score")) for row in ev_rows
            if not isinstance(row.get("edge_score"), (int, float))
            or not (0.0 <= row.get("edge_score") <= 100.0)]
check("edge_scores_valid", not bad_edge, bad_edge[:6])

# each list sorted by edge_score descending
sort_ok = True
for nm, lst in (("adds", adds), ("demos", demos),
                ("grads", grads), ("dels", dels)):
    es = [row.get("edge_score") or 0 for row in lst]
    if es != sorted(es, reverse=True):
        sort_ok = False
check("lists_sorted_desc", sort_ok, "edge_score descending")

# headline counts line up with the projected lists (lists are capped
# at 40/30 so the published count must be >= the list length)
nA = doc.get("n_additions")
nD = doc.get("n_demotions")
nG = doc.get("n_graduations")
nX = doc.get("n_deletions")
nS = doc.get("n_sp500_candidates")
counts_ok = (isinstance(nA, int) and nA >= len(adds)
             and isinstance(nD, int) and nD >= len(demos)
             and isinstance(nG, int) and nG >= len(grads)
             and isinstance(nX, int) and nX >= len(dels)
             and isinstance(nS, int) and nS >= len(spc))
check("counts_consistent", counts_ok,
      f"A={nA} D={nD} G={nG} X={nX} S={nS}")

# S&P 500 candidates must NOT be current index members
sp_members = set()
try:
    sd = json.loads(s3.get_object(
        Bucket=S3_BUCKET, Key="screener/data.json")["Body"].read())
    for s in (sd.get("stocks") or []):
        sy = (s.get("symbol") or "").upper()
        if sy:
            sp_members.add(sy)
except Exception:
    sp_members = set()
collide = [c.get("symbol") for c in spc
           if (c.get("symbol") or "").upper() in sp_members]
check("sp_candidates_not_members",
      bool(sp_members) and not collide,
      f"members={len(sp_members)} collisions={collide[:6]}")

# ---- 5) schedule live ------------------------------------------------------
try:
    sd2 = sch.get_schedule(Name=SCHED)
    st = sd2.get("State")
    check("schedule_live", st == "ENABLED",
          f"{st} {sd2.get('ScheduleExpression')}")
except Exception as e:
    check("schedule_live", False, f"{type(e).__name__}: {e}")

# ---- summary ---------------------------------------------------------------
rep["index_recon"] = {
    "headline": doc.get("headline"),
    "next_effective_date": (doc.get("reconstitution") or {}).get(
        "next_effective_date"),
    "n_eligible": n_elig,
    "n_candidates_scored": uni.get("n_candidates_scored"),
    "n_with_one_year_return": uni.get("n_with_one_year_return"),
    "r1000_breakpoint_cap_bil": bp1,
    "r3000_breakpoint_cap_bil": bp3,
    "n_additions": nA, "n_demotions": nD,
    "n_graduations": nG, "n_deletions": nX,
    "n_sp500_candidates": nS,
    "top_additions": [{"sym": x.get("symbol"), "edge": x.get("edge_score"),
                       "days": x.get("passive_days_to_absorb"),
                       "ret1y": x.get("one_year_return_pct")}
                      for x in adds[:5]],
    "top_demotions": [{"sym": x.get("symbol"), "edge": x.get("edge_score"),
                       "ret1y": x.get("one_year_return_pct")}
                      for x in demos[:5]],
    "top_graduations": [{"sym": x.get("symbol"), "edge": x.get("edge_score"),
                         "ret1y": x.get("one_year_return_pct")}
                        for x in grads[:5]],
    "top_deletions": [{"sym": x.get("symbol"), "edge": x.get("edge_score"),
                       "ret1y": x.get("one_year_return_pct")}
                      for x in dels[:5]],
    "top_sp500_candidates": [{"sym": x.get("symbol"),
                              "cap_bil": x.get("market_cap_bil")}
                             for x in spc[:8]],
}

rep["all_pass"] = all(c["ok"] for c in rep["checks"])
rep["verdict"] = (
    f"INDEX-RECON LIVE - {n_elig} eligible US names ranked; "
    f"{nA} Russell 2000 additions and {nD} demotions on the forced-buy "
    f"side, {nG} graduations and {nX} deletions on the forced-sell side, "
    f"{nS} S&P 500 inclusion candidates on watch. Reconstitution effective "
    f"{(doc.get('reconstitution') or {}).get('next_effective_date')}. "
    f"Production-clean, daily 14:10 UTC."
    if rep["all_pass"]
    else "REVIEW - see checks[]/index_recon")

out = json.dumps(rep, indent=2, default=str)
print(out)
try:
    s3.put_object(Bucket=S3_BUCKET,
                  Key="ops/reports/839_index_recon_deploy.json",
                  Body=out.encode(), ContentType="application/json")
except Exception as e:
    print(f"[ops839] S3 report write failed: {e}")
with open("aws/ops/reports/839_index_recon_deploy.json", "w") as f:
    f.write(out)
print("[ok] wrote aws/ops/reports/839_index_recon_deploy.json")
