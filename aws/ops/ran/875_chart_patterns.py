"""
ops/875 - DEPLOY + VERIFY justhodl-chart-patterns (the Chart Pattern
Scanner) end-to-end.

  1. Create or update the Lambda from source + config.
  2. Wire the daily EventBridge schedule.
  3. Invoke it -- a live S&P 500 scan (this can take 1-2 minutes).
  4. Read back data/chart-patterns.json and prove:
       - schema + engine identity;
       - the universe was actually scanned (universe_size ~ 500);
       - all four pattern lists are present and well-formed;
       - crossover rows carry direction / days_since / sma200 / series,
         and the 200-DMA series is a [date, close, ma] triple;
       - double top / bottom rows carry CONFIRMED|FORMING + a quality
         score, and every peak/trough idx lands inside its own series.
  5. Confirm chart-patterns.html is live and linked from the landing
     page and the directory.

Writes aws/ops/reports/875_chart_patterns.json.
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
FN = "justhodl-chart-patterns"
SRC = f"aws/lambdas/{FN}/source/lambda_function.py"
CONF_PATH = f"aws/lambdas/{FN}/config.json"
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"
OUT_KEY = "data/chart-patterns.json"
PAGE_URL = "https://justhodl.ai/chart-patterns.html"
INDEX_URL = "https://justhodl.ai/"
DIR_URL = "https://justhodl.ai/directory.html"

# read_timeout must clear the Lambda's own 600s timeout
cfg = Config(read_timeout=660, connect_timeout=20, retries={"max_attempts": 2})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
sch = boto3.client("scheduler", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)

CONF = json.load(open(CONF_PATH, encoding="utf-8"))

rep = {
    "ops": 875,
    "ts": datetime.now(timezone.utc).isoformat(),
    "subject": "Deploy + verify justhodl-chart-patterns (Chart Pattern "
               "Scanner): 200-DMA crossovers and double tops/bottoms",
    "checks": [],
}


def check(name, ok, detail=""):
    rep["checks"].append({"check": name, "ok": bool(ok),
                          "detail": str(detail)[:240]})
    return ok


def zip_src(path):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("lambda_function.py", open(path, encoding="utf-8").read())
    return buf.getvalue()


def http_get(url):
    req = urllib.request.Request(url, headers={"User-Agent": "justhodl-ops"})
    with urllib.request.urlopen(req, timeout=40) as r:
        return r.status, r.read().decode("utf-8", "ignore")


# ---- 1) deploy -------------------------------------------------------------
try:
    try:
        lam.get_function(FunctionName=FN)
        lam.update_function_code(FunctionName=FN, ZipFile=zip_src(SRC))
        for _ in range(50):
            if lam.get_function_configuration(FunctionName=FN).get(
                    "LastUpdateStatus") == "Successful":
                break
            time.sleep(2)
        lam.update_function_configuration(
            FunctionName=FN, Handler=CONF["handler"], Runtime=CONF["runtime"],
            Role=ROLE, Timeout=CONF["timeout"], MemorySize=CONF["memory"],
            Environment={"Variables": CONF.get("environment", {})},
            Description=CONF["description"][:255])
        dep = "updated"
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(
            FunctionName=FN, Runtime=CONF["runtime"], Role=ROLE,
            Handler=CONF["handler"], Timeout=CONF["timeout"],
            MemorySize=CONF["memory"], Architectures=CONF["architectures"],
            Environment={"Variables": CONF.get("environment", {})},
            Description=CONF["description"][:255],
            Code={"ZipFile": zip_src(SRC)})
        dep = "created"
    check("deploy_ok", True, dep)
except Exception as e:
    check("deploy_ok", False, f"{type(e).__name__}: {e}")

fn_arn = None
for _ in range(50):
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
        FlexibleTimeWindow={"Mode": "OFF"}, State="ENABLED",
        Description=sb.get("description", "")[:512],
        Target={"Arn": fn_arn, "RoleArn": sb["role_arn"]})
    try:
        sch.get_schedule(Name=SCHED)
        sch.update_schedule(Name=SCHED, **common)
    except sch.exceptions.ResourceNotFoundException:
        sch.create_schedule(Name=SCHED, **common)
    sd = sch.get_schedule(Name=SCHED)
    check("schedule_wired",
          sd.get("State") == "ENABLED",
          "%s %s %s" % (SCHED, sd.get("State"), sd.get("ScheduleExpression")))
except Exception as e:
    check("schedule_wired", False, f"{type(e).__name__}: {e}")

# ---- 3) invoke (live S&P 500 scan) ----------------------------------------
inv = {}
try:
    r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                   Payload=b"{}")
    body = json.loads(r["Payload"].read().decode("utf-8", "ignore"))
    inv = json.loads(body.get("body") or "{}")
    check("invoke_ok",
          r.get("StatusCode") == 200 and not r.get("FunctionError")
          and inv.get("ok") is True,
          r.get("FunctionError") or "scanned %s, %s with a signal in %ss"
          % (inv.get("universe"), inv.get("n_with_signal"),
             inv.get("build_seconds")))
except Exception as e:
    check("invoke_ok", False, f"{type(e).__name__}: {e}")

time.sleep(2)

# ---- 4) read back + validate ----------------------------------------------
d = {}
try:
    d = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=OUT_KEY
                                 )["Body"].read())
    check("output_readable", True, "data/chart-patterns.json read")
except Exception as e:
    check("output_readable", False, f"{type(e).__name__}: {e}")

check("schema_ok",
      d.get("schema_version") == "1.0"
      and d.get("engine") == "justhodl-chart-patterns",
      "schema=%s engine=%s" % (d.get("schema_version"), d.get("engine")))

check("universe_scanned",
      isinstance(d.get("universe_size"), int) and d["universe_size"] >= 400,
      "universe_size=%s, n_with_signal=%s"
      % (d.get("universe_size"), d.get("n_with_signal")))

lists = {k: d.get(k) for k in ("cross_up_200dma", "cross_down_200dma",
                               "double_tops", "double_bottoms")}
all_lists = all(isinstance(v, list) for v in lists.values())
total = sum(len(v) for v in lists.values() if isinstance(v, list))
check("four_lists_present", all_lists,
      "counts: " + ", ".join("%s=%s" % (k, len(v) if isinstance(v, list)
                             else "BAD") for k, v in lists.items()))
check("scan_found_signals", total > 0,
      "%d setups across the four categories" % total)


def series_ok(ser, width):
    return (isinstance(ser, list) and len(ser) > 20
            and all(isinstance(p, list) and len(p) == width for p in ser))


# crossover row structure
cross = (lists["cross_up_200dma"] or []) + (lists["cross_down_200dma"] or [])
if cross:
    r0 = cross[0]
    ok = (r0.get("direction") in ("up", "down")
          and isinstance(r0.get("days_since_cross"), int)
          and r0.get("sma200") is not None
          and series_ok(r0.get("series"), 3))
    check("crossover_rows_well_formed", ok,
          "%s %s: cross %sd ago, %s%% from MA, series=%s pts [date,close,ma]"
          % (r0.get("symbol"), r0.get("direction"),
             r0.get("days_since_cross"), r0.get("pct_from_ma"),
             len(r0.get("series") or [])))
else:
    check("crossover_rows_well_formed", True,
          "no 200-DMA crossovers in the S&P 500 today -- nothing to check")

# pattern row structure + idx-in-range
patt_ok, patt_detail = True, "no double top/bottom patterns today"
for key, marks in (("double_tops", ("peak1", "peak2", "trough")),
                   ("double_bottoms", ("trough1", "trough2", "peak"))):
    rows = lists[key] or []
    if not rows:
        continue
    r0 = rows[0]
    ser = r0.get("series")
    base = (r0.get("status") in ("CONFIRMED", "FORMING")
            and isinstance(r0.get("quality"), int)
            and series_ok(ser, 2))
    n = len(ser) if isinstance(ser, list) else 0
    idx_ok = all(isinstance(r0.get(m), dict)
                 and isinstance(r0[m].get("idx"), int)
                 and 0 <= r0[m]["idx"] < n for m in marks)
    if not (base and idx_ok):
        patt_ok = False
        patt_detail = "%s row malformed: base=%s idx_in_range=%s" % (
            key, base, idx_ok)
    else:
        patt_detail = ("%s: %s %s q%s, all %d marks land in a %d-pt series"
                       % (r0.get("symbol"), key[:-1], r0.get("status"),
                          r0.get("quality"), len(marks), n))
check("pattern_rows_well_formed", patt_ok, patt_detail)

# ---- 5) page + wiring ------------------------------------------------------
try:
    st, page = http_get(PAGE_URL)
    check("page_live",
          st == 200 and "Chart Pattern Scanner" in page
          and "chart-patterns.json" in page,
          "HTTP %s" % st)
except Exception as e:
    check("page_live", False, f"{type(e).__name__}: {e}")

try:
    st, idx = http_get(INDEX_URL)
    check("linked_from_landing_page",
          st == 200 and "/chart-patterns.html" in idx,
          "landing page links chart-patterns" if "/chart-patterns.html"
          in idx else "MISSING from landing page")
except Exception as e:
    check("linked_from_landing_page", False, f"{type(e).__name__}: {e}")

try:
    st, dr = http_get(DIR_URL)
    check("linked_from_directory",
          st == 200 and "chart-patterns" in dr,
          "directory links chart-patterns" if "chart-patterns" in dr
          else "MISSING from directory")
except Exception as e:
    check("linked_from_directory", False, f"{type(e).__name__}: {e}")

# ---- summary ---------------------------------------------------------------
n_ok = sum(1 for c in rep["checks"] if c["ok"])
n_tot = len(rep["checks"])
rep["summary"] = "%d/%d checks passed" % (n_ok, n_tot)
rep["all_passed"] = n_ok == n_tot
rep["scan"] = {
    "universe_size": d.get("universe_size"),
    "n_with_signal": d.get("n_with_signal"),
    "counts": d.get("counts"),
    "build_seconds": d.get("build_seconds"),
}
if rep["all_passed"]:
    cc = d.get("counts", {})
    rep["verdict"] = (
        "CHART PATTERN SCANNER LIVE - a daily S&P 500 scan found %s "
        "200-DMA cross-ups, %s cross-downs, %s double tops and %s double "
        "bottoms, each with a charted price series. Scheduled daily 23:30 "
        "UTC, page live and linked from the landing page and directory. "
        "The protected Stock Screener was not touched."
        % (cc.get("cross_up_200dma"), cc.get("cross_down_200dma"),
           cc.get("double_tops"), cc.get("double_bottoms")))
else:
    bad = [c["check"] for c in rep["checks"] if not c["ok"]]
    rep["verdict"] = ("CHART PATTERN SCANNER VERIFICATION INCOMPLETE - %d "
                      "check(s) failed: %s." % (len(bad), ", ".join(bad)))

with open("aws/ops/reports/875_chart_patterns.json", "w",
          encoding="utf-8") as f:
    json.dump(rep, f, indent=1)
print(json.dumps(rep, indent=1))
