"""
ops/868 - justhodl-vrp DEPLOY + VERIFY.

The Volatility Risk Premium engine puts implied vol (the VIX complex)
and realized vol (SPY) together: contemporaneous VRP at 9d/30d/3m
tenors, its percentile and z-score, the ex-post realized premium, and
a RICH/NORMAL/THIN/INVERTED regime.

This op proves it end-to-end:

  1. Ship the function; wire the daily 22:30 UTC schedule.
  2. Invoke it - it reads vix-curve.json + vix-curve-history.json and
     fetches SPY price history from FMP.
  3. Read back data/vrp.json and prove the read is sound:
       - schema; both data feeds healthy;
       - realized vols computed and in a sane band (3-90);
       - implied VIX present;
       - the VRP arithmetic reconciles - VRP 30d == VIX minus RV 21d;
       - the percentile sits in 0-100 and the VRP series is populated;
       - the regime resolves to a real label (not UNAVAILABLE);
       - the ex-post VRP scorecard computed.
  4. Confirm vrp.html is deployed and linked in the directory, and the
     schedule is ENABLED.

Writes aws/ops/reports/868_vrp_deploy.json.
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
FN = "justhodl-vrp"
OUT_KEY = "data/vrp.json"
PAGE_URL = "https://justhodl.ai/vrp.html"
DIR_URL = "https://justhodl.ai/directory.html"
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"
SRC = f"aws/lambdas/{FN}/source/lambda_function.py"
CONF = json.load(open(f"aws/lambdas/{FN}/config.json"))

VALID_REGIME = {"RICH", "NORMAL", "THIN", "INVERTED"}

cfg = Config(read_timeout=240, connect_timeout=20, retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
sch = boto3.client("scheduler", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)

rep = {
    "ops": 868,
    "ts": datetime.now(timezone.utc).isoformat(),
    "subject": "Deploy + verify justhodl-vrp (the Volatility Risk Premium "
               "engine - implied vol minus realized vol)",
    "checks": [],
}


def check(name, ok, detail=""):
    rep["checks"].append({"check": name, "ok": bool(ok),
                          "detail": str(detail)[:240]})
    return ok


def approx(a, b, tol=0.02):
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
inner = {}
try:
    r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                   Payload=b"{}")
    body = json.loads(r["Payload"].read().decode("utf-8", "ignore"))
    inner = json.loads(body.get("body") or "{}")
    check("invoke_ok",
          r.get("StatusCode") == 200 and not r.get("FunctionError"),
          r.get("FunctionError") or "200, regime=%s" % inner.get("regime"))
except Exception as e:
    check("invoke_ok", False, f"{type(e).__name__}: {e}")

time.sleep(2)

# ---- 4) read back + prove the VRP read ------------------------------------
d = {}
try:
    d = get_json(OUT_KEY)
    check("schema_ok",
          d.get("schema_version") == "1.0"
          and d.get("engine") == "justhodl-vrp",
          "schema=%s engine=%s" % (d.get("schema_version"), d.get("engine")))
except Exception as e:
    check("schema_ok", False, f"{type(e).__name__}: {e}")

feed = d.get("feed_status") or {}
check("data_feeds_healthy",
      feed.get("spy") in ("ok", "ok_light") and feed.get("implied") == "ok",
      "spy=%s implied=%s" % (feed.get("spy"), feed.get("implied")))

rv = d.get("realized") or {}
im = d.get("implied") or {}
v = d.get("vrp") or {}
rv21 = rv.get("rv_21d")
vix = im.get("vix")
check("realized_vol_sane",
      isinstance(rv21, (int, float)) and 3.0 <= rv21 <= 90.0
      and isinstance(rv.get("rv_10d"), (int, float))
      and isinstance(rv.get("rv_63d"), (int, float)),
      "RV 10d/21d/63d = %s / %s / %s, GK %s"
      % (rv.get("rv_10d"), rv21, rv.get("rv_63d"),
         rv.get("rv_garman_klass_21d")))

check("implied_vol_present",
      isinstance(vix, (int, float)) and vix > 0,
      "VIX=%s (%s)" % (vix, im.get("source_date")))

vrp30 = v.get("vrp_30d")
check("vrp_arithmetic_reconciles",
      vrp30 is not None and vix is not None and rv21 is not None
      and approx(vrp30, vix - rv21, 0.02),
      "VRP30d %s == VIX %s - RV21d %s" % (vrp30, vix, rv21))

pctl = v.get("vrp_30d_percentile_1y")
series = (d.get("series") or {}).get("vrp_30d") or []
check("percentile_and_series_ok",
      pctl is not None and 0 <= pctl <= 100 and len(series) >= 20,
      "percentile=%s, VRP series %d points" % (pctl, len(series)))

check("regime_resolves", d.get("regime") in VALID_REGIME,
      "regime=%s" % d.get("regime"))

check("expost_vrp_computed",
      v.get("expost_vrp_mean") is not None,
      "ex-post VRP mean=%s, positive %s%% of windows"
      % (v.get("expost_vrp_mean"), v.get("expost_positive_pct")))

# ---- 5) page + directory + schedule ---------------------------------------
try:
    status, page = http_get(PAGE_URL)
    check("page_deployed",
          status == 200 and "Volatility Risk Premium" in page
          and "vrp.json" in page,
          "HTTP %s, VRP page %s" % (status,
          "live" if "vrp.json" in page else "MISSING"))
except Exception as e:
    check("page_deployed", False, f"{type(e).__name__}: {e}")

try:
    dstatus, dpage = http_get(DIR_URL)
    check("directory_links_page",
          dstatus == 200 and "/vrp.html" in dpage,
          "directory %s, link %s" % (dstatus,
          "present" if "/vrp.html" in dpage else "MISSING"))
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
rep["vrp"] = {
    "regime": d.get("regime"),
    "vrp_30d": vrp30,
    "vrp_30d_percentile_1y": pctl,
    "vix": vix,
    "rv_21d": rv21,
    "term_structure": v.get("term_structure"),
    "expost_vrp_mean": v.get("expost_vrp_mean"),
    "expost_positive_pct": v.get("expost_positive_pct"),
    "schedule": "22:30 UTC daily",
}
if rep["all_passed"]:
    rep["verdict"] = (
        "VRP ENGINE LIVE - the platform now reads the volatility risk "
        "premium directly. Today: %s, VRP 30d %s (%sth percentile of the "
        "year), VIX %s vs realized %s. The arithmetic reconciles, the "
        "ex-post scorecard computed (%s avg). Runs daily 22:30 UTC."
        % (d.get("regime"), vrp30, int(pctl) if pctl is not None else "n/a",
           vix, rv21, v.get("expost_vrp_mean")))
else:
    bad = [c["check"] for c in rep["checks"] if not c["ok"]]
    rep["verdict"] = ("VRP VERIFICATION INCOMPLETE - %d check(s) failed: "
                      "%s." % (len(bad), ", ".join(bad)))

with open("aws/ops/reports/868_vrp_deploy.json", "w",
          encoding="utf-8") as f:
    json.dump(rep, f, indent=1)
print(json.dumps(rep, indent=1))
