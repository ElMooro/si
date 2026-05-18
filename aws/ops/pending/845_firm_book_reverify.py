"""
ops/845 - justhodl-firm-book re-verify after the pairs-gross + position-cap fix.

The Firm Book is the consolidated cross-desk position blotter: it sizes
every strategy desk's positions to its Desk Allocator capital weight,
nets the same ticker across desks and rolls the result up into a firm
gross / net exposure, sector tilts, conviction overlap and desk
conflicts. It is the model book the desk stack implies.

Self-sufficient and idempotent against the deploy-lambdas race:

  1. Ship the function from source - create if missing, update if it
     already exists.
  2. Wire the EventBridge Scheduler schedule from config.json.
  3. Invoke synchronously.
  4. Read back data/firm-book.json and prove the book is sane:
       - schema + ok flag present;
       - the equity book is non-empty and sorted by gross descending;
       - firm gross reconciles to ~100 (each firing desk deploys its
         full allocator capital weight as gross) and net sits inside
         [-gross, +gross];
       - every conviction-overlap name is genuinely held by >=2 desks;
       - every desk-conflict name genuinely has the conflict flag;
       - sector exposure is populated;
       - all 7 desks appear in desk_contributions;
       - no fabricated tickers (every book symbol a real non-empty str).
  5. Confirm the schedule is live + ENABLED.

Writes aws/ops/reports/845_firm_book_deploy.json.
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
FN = "justhodl-firm-book"
OUT_KEY = "data/firm-book.json"
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"
SRC = f"aws/lambdas/{FN}/source/lambda_function.py"
CONF = json.load(open(f"aws/lambdas/{FN}/config.json"))

EXPECTED_DESKS = {"best-ideas", "pairs-arb", "trend-engine", "merger-arb",
                  "spinoff-desk", "index-recon", "risk-radar"}

cfg = Config(read_timeout=240, connect_timeout=20,
             retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
sch = boto3.client("scheduler", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)

rep = {
    "ops": 845,
    "ts": datetime.now(timezone.utc).isoformat(),
    "subject": "Re-verify justhodl-firm-book after the gross + concentration fix (consolidated "
               "cross-desk position blotter)",
    "checks": [],
}


def check(name, ok, detail=""):
    rep["checks"].append({"check": name, "ok": bool(ok),
                          "detail": str(detail)[:240]})
    return ok


# ---- 1) ship --------------------------------------------------------------
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

# ---- 2) schedule ----------------------------------------------------------
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

# ---- 3) invoke ------------------------------------------------------------
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

# ---- 4) read back + audit -------------------------------------------------
doc = {}
try:
    head = s3.head_object(Bucket=S3_BUCKET, Key=OUT_KEY)
    age = (datetime.now(timezone.utc) - head["LastModified"]).total_seconds()
    check("output_fresh", age < 900, f"{round(age)}s old")
    doc = json.loads(s3.get_object(
        Bucket=S3_BUCKET, Key=OUT_KEY)["Body"].read())
except Exception as e:
    check("output_fresh", False, f"{type(e).__name__}: {e}")

firm = doc.get("firm") or {}
equity = doc.get("equity_book") or []
macro = doc.get("macro_book") or []
overlap = doc.get("conviction_overlap") or []
conflicts = doc.get("desk_conflicts") or []
sectors = doc.get("sector_exposure") or []
contrib = doc.get("desk_contributions") or {}

check("schema_ok", doc.get("schema_version") == "1.0",
      doc.get("schema_version"))

check("equity_book_present", len(equity) > 0, f"{len(equity)} names")

gross = [b.get("gross_pct") or 0 for b in equity]
check("equity_sorted_desc", gross == sorted(gross, reverse=True),
      f"first5={gross[:5]}")

fg = firm.get("gross_exposure_pct")
fn_net = firm.get("net_exposure_pct")
check("gross_reconciles",
      isinstance(fg, (int, float)) and 95.0 <= fg <= 101.5,
      f"firm_gross={fg} (each firing desk deploys full capital weight)")
check("net_within_gross",
      isinstance(fn_net, (int, float)) and isinstance(fg, (int, float))
      and -fg - 0.5 <= fn_net <= fg + 0.5,
      f"net={fn_net} gross={fg}")

# every overlap name genuinely held by >=2 desks
bad_ov = [b.get("symbol") for b in overlap if (b.get("n_desks") or 0) < 2]
check("overlap_valid", not bad_ov,
      bad_ov or f"{len(overlap)} overlaps, all >=2 desks")

# every conflict name genuinely flagged
bad_cf = [b.get("symbol") for b in conflicts if not b.get("desk_conflict")]
check("conflicts_valid", not bad_cf,
      bad_cf or f"{len(conflicts)} conflicts, all flagged")

check("sectors_present", len(sectors) > 0, f"{len(sectors)} sectors")

check("all_seven_desks_contributed",
      set(contrib.keys()) == EXPECTED_DESKS,
      f"{sorted(contrib.keys())}")

# no fabricated tickers
bad_sym = [b for b in (equity + macro)
           if not isinstance(b.get("symbol"), str)
           or not b.get("symbol").strip()]
check("no_fabrication", not bad_sym, f"{len(bad_sym)} bad symbols")

# the concentration fix: no single equity name should dominate the book.
# with a 20% within-desk cap and the largest desk at ~24% capital, a
# single-desk name tops out near 5%; allow headroom for a name stacked
# across desks, but a 17%-style regression must fail this.
top_gross = max((b.get("gross_pct") or 0) for b in equity) if equity else 0
check("single_name_capped", top_gross <= 10.0,
      f"largest single-name gross = {round(top_gross, 2)}% "
      f"(was 17.3% pre-fix)")

# ---- 5) schedule live -----------------------------------------------------
try:
    sd = sch.get_schedule(Name=SCHED)
    check("schedule_live", sd.get("State") == "ENABLED",
          f"{sd.get('State')} {sd.get('ScheduleExpression')}")
except Exception as e:
    check("schedule_live", False, f"{type(e).__name__}: {e}")

# ---- summary --------------------------------------------------------------
rep["firm_book"] = {
    "headline": doc.get("headline"),
    "firm": firm,
    "n_equity": len(equity),
    "n_macro": len(macro),
    "n_overlap": len(overlap),
    "n_conflicts": len(conflicts),
    "top_positions": [{"symbol": b.get("symbol"), "side": b.get("side"),
                       "net_pct": b.get("net_pct"),
                       "gross_pct": b.get("gross_pct"),
                       "n_desks": b.get("n_desks")}
                      for b in equity[:12]],
    "top_overlap": [{"symbol": b.get("symbol"), "n_desks": b.get("n_desks"),
                     "side": b.get("side"), "net_pct": b.get("net_pct")}
                    for b in overlap[:8]],
    "desk_contributions": contrib,
}

rep["all_pass"] = all(c["ok"] for c in rep["checks"])
rep["verdict"] = (
    f"FIRM BOOK LIVE - consolidated blotter nets {len(equity)} equity "
    f"names across all 7 desks, {firm.get('gross_exposure_pct')}% gross / "
    f"{firm.get('net_exposure_pct')}% net, {len(overlap)} cross-desk "
    f"conviction overlaps, {len(conflicts)} conflicts. The model firm "
    f"book is production-clean, rebuilds daily 01:00 UTC after the "
    f"allocator."
    if rep["all_pass"]
    else "REVIEW - see checks[]/firm_book")

out = json.dumps(rep, indent=2, default=str)
print(out)
try:
    with open("aws/ops/reports/845_firm_book_deploy.json", "w") as f:
        f.write(out)
except Exception as e:
    print("report write skipped:", e)
