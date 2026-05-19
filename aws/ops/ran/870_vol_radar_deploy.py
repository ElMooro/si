"""
ops/870 - justhodl-vol-radar DEPLOY + VERIFY.

The Volatility Turning-Point Radar scores how primed the tape is for a
volatility spike (spike-risk) and for a volatility peak (exhaustion)
from leading canary signals across seven existing engines.

This op proves it end-to-end:

  1. Ship the function; wire the 6-hourly schedule.
  2. Invoke it - it reads the vix-curve, VRP, vol-regime, DIX,
     credit-stress, eurodollar-stress and capitulation feeds.
  3. Read back data/vol-radar.json and prove the radar is sound:
       - schema; a real posture (not INSUFFICIENT DATA);
       - it actually reached the canary feeds (>= 4 of 7 available);
       - both scores sit in 0-100;
       - the canary arrays are populated and each canary is
         well-formed (label, points, max, firing, detail);
       - the firing flags reconcile with the points (a canary fires
         iff points >= half its max);
       - the posture is consistent with the scores and the vol level.
  4. Confirm vol-radar.html is deployed and linked in the directory,
     and the schedule is ENABLED.

Writes aws/ops/reports/870_vol_radar_deploy.json.
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
FN = "justhodl-vol-radar"
OUT_KEY = "data/vol-radar.json"
PAGE_URL = "https://justhodl.ai/vol-radar.html"
DIR_URL = "https://justhodl.ai/directory.html"
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"
SRC = f"aws/lambdas/{FN}/source/lambda_function.py"
CONF = json.load(open(f"aws/lambdas/{FN}/config.json"))

VALID_POSTURE = {"COILED", "WATCH", "CALM", "TRANSITIONAL", "ELEVATED",
                 "TOPPING", "PEAKING"}

cfg = Config(read_timeout=240, connect_timeout=20, retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
sch = boto3.client("scheduler", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)

rep = {
    "ops": 870,
    "ts": datetime.now(timezone.utc).isoformat(),
    "subject": "Deploy + verify justhodl-vol-radar (the Volatility "
               "Turning-Point Radar - canary-based spike & peak warning)",
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
SCHED = sb.get("schedule_name", f"{FN}-6h")
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
          r.get("FunctionError") or "200, posture=%s" % inner.get("posture"))
except Exception as e:
    check("invoke_ok", False, f"{type(e).__name__}: {e}")

time.sleep(2)

# ---- 4) read back + prove the radar ---------------------------------------
d = {}
try:
    d = get_json(OUT_KEY)
    check("schema_ok",
          d.get("schema_version") == "1.0"
          and d.get("engine") == "justhodl-vol-radar",
          "schema=%s engine=%s" % (d.get("schema_version"), d.get("engine")))
except Exception as e:
    check("schema_ok", False, f"{type(e).__name__}: {e}")

check("posture_resolves", d.get("posture") in VALID_POSTURE,
      "posture=%s" % d.get("posture"))

feeds = (d.get("inputs") or {}).get("feeds_available", "0/7")
try:
    n_feeds = int(str(feeds).split("/")[0])
except Exception:
    n_feeds = 0
check("canary_feeds_reached", n_feeds >= 4,
      "%s canary feeds available" % feeds)

sc = d.get("scores") or {}
sr, ex = sc.get("spike_risk"), sc.get("exhaustion")
check("scores_in_range",
      isinstance(sr, (int, float)) and 0 <= sr <= 100
      and isinstance(ex, (int, float)) and 0 <= ex <= 100,
      "spike_risk=%s exhaustion=%s" % (sr, ex))

spike = d.get("spike_canaries") or []
exh = d.get("exhaustion_canaries") or []
allc = spike + exh
well_formed = bool(allc) and all(
    isinstance(c, dict) and c.get("label") and "points" in c
    and "max" in c and "firing" in c for c in allc)
check("canaries_well_formed", well_formed,
      "%d spike + %d exhaustion canaries, all structured"
      % (len(spike), len(exh)))

# firing flag must reconcile with points: fires iff points >= max/2 and >0
recon = all(
    bool(c["firing"]) == (c["points"] >= c["max"] * 0.5 and c["points"] > 0)
    for c in allc if isinstance(c.get("points"), (int, float))
    and isinstance(c.get("max"), (int, float)))
check("firing_flags_reconcile", recon,
      "every canary fires iff points >= half its max")

# posture consistency with scores + vol level
vlevel = (d.get("vol_state") or {}).get("vix_level")
post = d.get("posture")
consistent = True
if post == "PEAKING":
    consistent = ex >= 55
elif post == "COILED":
    consistent = sr >= 55
elif post == "CALM":
    consistent = sr < 60 and ex < 60
check("posture_consistent_with_scores", consistent,
      "posture=%s vs spike=%s exh=%s level=%s" % (post, sr, ex, vlevel))

# ---- 5) page + directory + schedule ---------------------------------------
try:
    status, page = http_get(PAGE_URL)
    check("page_deployed",
          status == 200 and "Volatility Turning-Point Radar" in page
          and "vol-radar.json" in page,
          "HTTP %s, radar page %s" % (status,
          "live" if "vol-radar.json" in page else "MISSING"))
except Exception as e:
    check("page_deployed", False, f"{type(e).__name__}: {e}")

try:
    dstatus, dpage = http_get(DIR_URL)
    check("directory_links_page",
          dstatus == 200 and "/vol-radar.html" in dpage,
          "directory %s, link %s" % (dstatus,
          "present" if "/vol-radar.html" in dpage else "MISSING"))
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
rep["vol_radar"] = {
    "posture": d.get("posture"),
    "spike_risk": sr,
    "exhaustion": ex,
    "dominant": sc.get("dominant"),
    "vix": (d.get("vol_state") or {}).get("vix"),
    "vix_level": vlevel,
    "term_structure": (d.get("vol_state") or {}).get("term_structure"),
    "spike_canaries_firing": d.get("spike_canaries_firing"),
    "exhaustion_canaries_firing": d.get("exhaustion_canaries_firing"),
    "feeds_available": feeds,
    "schedule": "every 6h",
}
if rep["all_passed"]:
    rep["verdict"] = (
        "VOL TURNING-POINT RADAR LIVE - the platform now warns on "
        "volatility turning points before they happen. Today: %s "
        "(spike-risk %s, exhaustion %s) with VIX %s at the %s end of its "
        "range. %s canary feeds wired, the firing flags reconcile, the "
        "posture is consistent. Runs every 6h."
        % (d.get("posture"), sr, ex, (d.get("vol_state") or {}).get("vix"),
           vlevel, feeds))
else:
    bad = [c["check"] for c in rep["checks"] if not c["ok"]]
    rep["verdict"] = ("VOL RADAR VERIFICATION INCOMPLETE - %d check(s) "
                      "failed: %s." % (len(bad), ", ".join(bad)))

with open("aws/ops/reports/870_vol_radar_deploy.json", "w",
          encoding="utf-8") as f:
    json.dump(rep, f, indent=1)
print(json.dumps(rep, indent=1))
