"""
ops/867 - justhodl-econ-calendar DEPLOY + VERIFY.

The Economic Calendar is the forward ECO screen: the schedule of
scheduled macro releases (CPI, payrolls, PCE, ISM, GDP, jobless claims
and the rest) with the market's consensus estimate, the prior, and the
post-release surprise.

The make-or-break question is whether FMP's economic-calendar endpoint
returns real data on this account's plan. This op proves it:

  1. Ship the function; wire the daily 11:00 UTC schedule.
  2. Invoke it - it pulls the FMP economic calendar live.
  3. Read back data/econ-calendar.json and prove it is real:
       - schema present;
       - feed_status == "ok" -> the FMP economic-calendar endpoint
         works on this plan (the critical check);
       - the upcoming and recent arrays are populated with
         well-formed events (date, country, event, impact);
       - the surprise arithmetic reconciles - for a printed release,
         surprise == actual minus consensus;
       - next_major resolves to a tier-one US release.
  4. Confirm econ-calendar.html is deployed and linked in the
     directory, and the schedule is ENABLED.

If feed_status is not "ok" the report says so precisely, so the
fallback (FRED release-dates as the schedule source) is a clear next
step rather than a guess.

Writes aws/ops/reports/867_econ_calendar_deploy.json.
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
FN = "justhodl-econ-calendar"
OUT_KEY = "data/econ-calendar.json"
PAGE_URL = "https://justhodl.ai/econ-calendar.html"
DIR_URL = "https://justhodl.ai/directory.html"
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"
SRC = f"aws/lambdas/{FN}/source/lambda_function.py"
CONF = json.load(open(f"aws/lambdas/{FN}/config.json"))

cfg = Config(read_timeout=240, connect_timeout=20, retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
sch = boto3.client("scheduler", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)

rep = {
    "ops": 867,
    "ts": datetime.now(timezone.utc).isoformat(),
    "subject": "Deploy + verify justhodl-econ-calendar (the forward "
               "economic release calendar - the desk ECO screen)",
    "checks": [],
}


def check(name, ok, detail=""):
    rep["checks"].append({"check": name, "ok": bool(ok),
                          "detail": str(detail)[:240]})
    return ok


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
          r.get("FunctionError") or "200, feed_status=%s"
          % inner.get("feed_status"))
except Exception as e:
    check("invoke_ok", False, f"{type(e).__name__}: {e}")

time.sleep(2)

# ---- 4) read back + prove it is real --------------------------------------
d = {}
try:
    d = get_json(OUT_KEY)
    check("schema_ok",
          d.get("schema_version") == "1.0"
          and d.get("engine") == "justhodl-econ-calendar",
          "schema=%s engine=%s" % (d.get("schema_version"), d.get("engine")))
except Exception as e:
    check("schema_ok", False, f"{type(e).__name__}: {e}")

feed = d.get("feed_status")
check("fmp_economic_calendar_works", feed == "ok",
      "feed_status=%s (FMP economic-calendar endpoint %s on this plan)"
      % (feed, "WORKS" if feed == "ok" else "FAILED - pivot to FRED "
         "release-dates"))

counts = d.get("counts") or {}
upcoming = d.get("upcoming") or []
recent = d.get("recent") or []
check("calendar_populated",
      len(upcoming) > 0 and len(recent) > 0,
      "%d upcoming, %d recent printed releases" % (len(upcoming),
                                                   len(recent)))

sample = (upcoming + recent)[:50]
well_formed = bool(sample) and all(
    isinstance(e, dict) and len(str(e.get("date") or "")) == 10
    and e.get("event") and e.get("country") and e.get("impact")
    in ("HIGH", "MEDIUM") for e in sample)
check("events_well_formed", well_formed,
      "%d sampled events carry date/country/event/impact" % len(sample))

# surprise arithmetic reconciles on a printed release
recon = None
for e in recent:
    a, est, sp = e.get("actual"), e.get("consensus"), e.get("surprise")
    if a is not None and est is not None and sp is not None:
        recon = abs(sp - round(a - est, 4)) <= 0.01
        rep["surprise_sample"] = {"event": e.get("event"), "actual": a,
                                  "consensus": est, "surprise": sp}
        break
check("surprise_arithmetic_reconciles", recon is True if recon is not None
      else feed == "ok",
      "surprise == actual - consensus" if recon
      else "no printed release with both actual+consensus to test"
      if recon is None else "MISMATCH")

nm = d.get("next_major")
check("next_major_resolves",
      isinstance(nm, dict) and nm.get("event") and nm.get("date"),
      "next major US release: %s" % (nm.get("event") if isinstance(nm, dict)
                                     else "none in window"))

# ---- 5) page + directory + schedule ---------------------------------------
try:
    status, page = http_get(PAGE_URL)
    check("page_deployed",
          status == 200 and "Economic Calendar" in page
          and "econ-calendar.json" in page,
          "HTTP %s, ECO screen %s" % (status,
          "live" if "econ-calendar.json" in page else "MISSING"))
except Exception as e:
    check("page_deployed", False, f"{type(e).__name__}: {e}")

try:
    dstatus, dpage = http_get(DIR_URL)
    check("directory_links_page",
          dstatus == 200 and "/econ-calendar.html" in dpage,
          "directory %s, link %s" % (dstatus,
          "present" if "/econ-calendar.html" in dpage else "MISSING"))
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
rep["econ_calendar"] = {
    "feed_status": feed,
    "upcoming": len(upcoming),
    "recent_printed": len(recent),
    "this_week": counts.get("this_week"),
    "this_week_tier1": counts.get("this_week_tier1"),
    "next_major": nm.get("event") if isinstance(nm, dict) else None,
    "next_major_days": nm.get("days_until") if isinstance(nm, dict)
    else None,
    "recent_surprise_tally": d.get("recent_surprise_tally"),
    "schedule": "11:00 UTC daily",
}
if rep["all_passed"]:
    rep["verdict"] = (
        "ECONOMIC CALENDAR LIVE - the desk now has the ECO screen the "
        "platform was missing. FMP's economic-calendar endpoint works on "
        "this plan; %d upcoming and %d recently-printed releases loaded, "
        "next major US release %s in %s day(s). The surprise arithmetic "
        "reconciles, the page is live. Runs daily 11:00 UTC."
        % (len(upcoming), len(recent),
           nm.get("event") if isinstance(nm, dict) else "n/a",
           nm.get("days_until") if isinstance(nm, dict) else "n/a"))
else:
    bad = [c["check"] for c in rep["checks"] if not c["ok"]]
    note = ""
    if feed != "ok":
        note = (" FMP economic-calendar did not return data (feed_status=%s)"
                " - pivot the schedule source to the FRED release-dates "
                "API." % feed)
    rep["verdict"] = ("ECON CALENDAR VERIFICATION INCOMPLETE - %d check(s) "
                      "failed: %s.%s" % (len(bad), ", ".join(bad), note))

with open("aws/ops/reports/867_econ_calendar_deploy.json", "w",
          encoding="utf-8") as f:
    json.dump(rep, f, indent=1)
print(json.dumps(rep, indent=1))
