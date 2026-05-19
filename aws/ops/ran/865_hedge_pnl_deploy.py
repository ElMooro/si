"""
ops/865 - justhodl-hedge-pnl DEPLOY + VERIFY.

The Hedge Overlay Scorecard scores whether the tail-hedge overlay is
earning its carry: it walks the Hedge Planner's daily record, models
cumulative carry bled against convex stress payoff captured, and
returns a cost-vs-cover verdict.

This op proves it end-to-end:

  1. Ship the function; wire the daily 05:30 UTC schedule.
  2. Invoke it - it reads the real hedge-planner-history.json and
     tail-hedge.json and fetches real SPY history from FMP.
  3. Read back data/hedge-pnl.json and prove the score is sound:
       - schema + a valid maturity (WARMING/ESTABLISHED) and verdict;
       - SPY history actually loaded (the FMP historical path works);
       - the daily marks array is populated and well-formed;
       - the arithmetic reconciles - net P&L == payoff minus carry,
         and carry efficiency == payoff / carry;
       - carry is non-negative and, with a sleeve on the books, the
         track has at least one scored day.
  4. Confirm hedge-pnl.html is deployed and linked in the directory,
     and the schedule is ENABLED.

Writes aws/ops/reports/865_hedge_pnl_deploy.json.
"""
import io
import json
import time
import urllib.request
import zipfile
from datetime import datetime, timezone

import boto3
from botocore.config import Config

REGION = "us-east-1"
S3_BUCKET = "justhodl-dashboard-live"
FN = "justhodl-hedge-pnl"
OUT_KEY = "data/hedge-pnl.json"
PAGE_URL = "https://justhodl.ai/hedge-pnl.html"
DIR_URL = "https://justhodl.ai/directory.html"
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"
SRC = f"aws/lambdas/{FN}/source/lambda_function.py"
CONF = json.load(open(f"aws/lambdas/{FN}/config.json"))

VALID_MATURITY = {"WARMING", "ESTABLISHED"}
VALID_VERDICT = {"WARMING", "EARNING ITS CARRY", "FAIRLY PRICED",
                 "PURE CARRY -- NO STRESS YET", "CARRY-HEAVY"}

cfg = Config(read_timeout=240, connect_timeout=20, retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
sch = boto3.client("scheduler", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)

rep = {
    "ops": 865,
    "ts": datetime.now(timezone.utc).isoformat(),
    "subject": "Deploy + verify justhodl-hedge-pnl (the Hedge Overlay "
               "Scorecard - scores whether the tail-hedge overlay is "
               "earning its carry)",
    "checks": [],
}


def check(name, ok, detail=""):
    rep["checks"].append({"check": name, "ok": bool(ok),
                          "detail": str(detail)[:240]})
    return ok


def approx(a, b, tol=0.0005):
    if a is None or b is None:
        return False
    return abs(float(a) - float(b)) <= tol


def get_json(key):
    return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())


def http_get(url):
    req = urllib.request.Request(url, headers={"User-Agent": "justhodl-ops"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.status, r.read().decode("utf-8", "ignore")


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
    body = json.loads(r["Payload"].read().decode("utf-8", "ignore"))
    inner = json.loads(body.get("body") or "{}")
    check("invoke_ok",
          r.get("StatusCode") == 200 and not r.get("FunctionError")
          and inner.get("ok"),
          r.get("FunctionError") or "200, ok=%s" % inner.get("ok"))
except Exception as e:
    check("invoke_ok", False, f"{type(e).__name__}: {e}")

time.sleep(2)

# ---- 4) read back + prove the score ---------------------------------------
d = {}
try:
    d = get_json(OUT_KEY)
    check("schema_ok",
          d.get("schema_version") == "1.0"
          and d.get("engine") == "justhodl-hedge-pnl",
          "schema=%s engine=%s" % (d.get("schema_version"), d.get("engine")))
except Exception as e:
    check("schema_ok", False, f"{type(e).__name__}: {e}")

check("maturity_valid", d.get("maturity") in VALID_MATURITY,
      "maturity=%s" % d.get("maturity"))
check("verdict_valid", d.get("verdict") in VALID_VERDICT,
      "verdict=%s" % d.get("verdict"))

check("spy_history_loaded", (d.get("spy_history_days") or 0) > 0,
      "%s SPY trading days loaded from FMP" % d.get("spy_history_days"))

daily = d.get("daily") or []
rows_ok = bool(daily) and all(
    isinstance(r, dict) and "date" in r and "carry_pct" in r
    and "payoff_pct" in r and "day_pnl_pct" in r for r in daily)
check("daily_marks_well_formed", rows_ok,
      "%d daily mark(s), all carry/payoff/pnl-bearing" % len(daily))

carry = d.get("carry") or {}
payoff = d.get("payoff") or {}
cc = carry.get("cumulative_pct")
cp = payoff.get("cumulative_pct")
net = d.get("net_overlay_pnl_pct")
check("pnl_reconciles",
      cc is not None and cp is not None and net is not None
      and approx(net, cp - cc, 0.001),
      "net %.4f == payoff %.4f - carry %.4f" % (net or 0, cp or 0, cc or 0))

eff = d.get("carry_efficiency")
eff_ok = (eff is None and (cc or 0) <= 1e-6) or (
    eff is not None and (cc or 0) > 1e-6 and approx(eff, cp / cc, 0.05))
check("carry_efficiency_consistent", eff_ok,
      "efficiency=%s vs payoff/carry=%s"
      % (eff, round(cp / cc, 2) if (cc or 0) > 1e-6 else "n/a"))

check("track_has_scored_days",
      (d.get("track") or {}).get("n_days", 0) >= 1 and (cc or 0) >= 0,
      "%s scored day(s), cumulative carry %.4f%%"
      % ((d.get("track") or {}).get("n_days"), cc or 0))

# ---- 5) page + directory + schedule ---------------------------------------
try:
    status, page = http_get(PAGE_URL)
    check("page_deployed",
          status == 200 and "Hedge Overlay Scorecard" in page
          and "hedge-pnl.json" in page,
          "HTTP %s, scorecard page %s" % (status,
          "live" if "hedge-pnl.json" in page else "MISSING"))
except Exception as e:
    check("page_deployed", False, f"{type(e).__name__}: {e}")

try:
    dstatus, dpage = http_get(DIR_URL)
    check("directory_links_page",
          dstatus == 200 and "/hedge-pnl.html" in dpage,
          "directory %s, link %s" % (dstatus,
          "present" if "/hedge-pnl.html" in dpage else "MISSING"))
except Exception as e:
    check("directory_links_page", False, f"{type(e).__name__}: {e}")

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
rep["hedge_pnl"] = {
    "maturity": d.get("maturity"),
    "verdict": d.get("verdict"),
    "n_days": (d.get("track") or {}).get("n_days"),
    "n_stress_days": (d.get("track") or {}).get("n_stress_days"),
    "cumulative_carry_pct": cc,
    "cumulative_payoff_pct": cp,
    "net_overlay_pnl_pct": net,
    "carry_efficiency": eff,
    "cover_per_carry": (d.get("forward") or {}).get("cover_per_carry"),
    "schedule": "05:30 UTC daily",
}
if rep["all_passed"]:
    rep["verdict"] = (
        "HEDGE OVERLAY SCORECARD LIVE - the firm can now see whether the "
        "tail hedge is earning its carry. Today's read: %s (%s) over a "
        "%s-day track - %.4f%% carry bled, %.4f%% convex payoff, net "
        "%+.4f%%. The arithmetic reconciles, the FMP history path works, "
        "the page is live. Runs daily 05:30 UTC."
        % (d.get("verdict"), d.get("maturity"),
           (d.get("track") or {}).get("n_days"), cc or 0, cp or 0,
           net or 0))
else:
    bad = [c["check"] for c in rep["checks"] if not c["ok"]]
    rep["verdict"] = ("HEDGE SCORECARD VERIFICATION INCOMPLETE - %d "
                      "check(s) failed: %s." % (len(bad), ", ".join(bad)))

with open("aws/ops/reports/865_hedge_pnl_deploy.json", "w",
          encoding="utf-8") as f:
    json.dump(rep, f, indent=1)
print(json.dumps(rep, indent=1))
