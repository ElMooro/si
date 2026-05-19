"""
ops/872 - DEPLOY + VERIFY justhodl-market-extremes, and verify the
cro-escalation vol-radar turning-point alert.

Two builds in one verification:

  PART A - justhodl-market-extremes (the Market Cycle Extremes Radar):
    ship it, wire the daily 23:00 UTC schedule, invoke it, and prove
    data/market-extremes.json is sound - a real posture, >=4 of 8
    feeds reached, top-risk and cycle-position in range, the top
    canaries well-formed with firing flags reconciling, the
    capitulation side consumed from the Capitulation engine, and the
    posture consistent with the scores. Confirm market-extremes.html
    is deployed and linked, and the schedule is ENABLED.

  PART B - cro-escalation vol-radar alert: ship the updated
    cro-escalation, invoke it with dry_run (no Telegram sent), and
    confirm the integration runs - the deployed artifact carries the
    build_vol_radar_alert function and the run resolves a vol-radar
    posture, so a COILED/PEAKING flip would fire a ping.

Writes aws/ops/reports/872_extremes_and_radar_alert.json.
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
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"

ME = "justhodl-market-extremes"
ME_SRC = f"aws/lambdas/{ME}/source/lambda_function.py"
ME_CONF = json.load(open(f"aws/lambdas/{ME}/config.json"))
ME_OUT = "data/market-extremes.json"
ME_PAGE = "https://justhodl.ai/market-extremes.html"

CE = "justhodl-cro-escalation"
CE_SRC = f"aws/lambdas/{CE}/source/lambda_function.py"

DIR_URL = "https://justhodl.ai/directory.html"
VALID_POSTURE = {"CAPITULATION", "ACCUMULATION", "EXPANSION",
                 "DISTRIBUTION", "EUPHORIA"}

cfg = Config(read_timeout=240, connect_timeout=20, retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
sch = boto3.client("scheduler", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)

rep = {
    "ops": 872,
    "ts": datetime.now(timezone.utc).isoformat(),
    "subject": "Deploy + verify justhodl-market-extremes (Market Cycle "
               "Extremes Radar) and the cro-escalation vol-radar alert",
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


def http_get_bytes(url):
    req = urllib.request.Request(url, headers={"User-Agent": "justhodl-ops"})
    with urllib.request.urlopen(req, timeout=40) as r:
        return r.read()


def zip_src(path):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("lambda_function.py", open(path, encoding="utf-8").read())
    return buf.getvalue()


# ===================== PART A: market-extremes ============================
try:
    try:
        lam.get_function(FunctionName=ME)
        lam.update_function_code(FunctionName=ME, ZipFile=zip_src(ME_SRC))
        for _ in range(30):
            if lam.get_function_configuration(
                    FunctionName=ME).get("LastUpdateStatus") == "Successful":
                break
            time.sleep(2)
        lam.update_function_configuration(
            FunctionName=ME, Handler=ME_CONF["handler"],
            Runtime=ME_CONF["runtime"], Role=ROLE,
            Timeout=ME_CONF["timeout"], MemorySize=ME_CONF["memory"],
            Environment={"Variables": ME_CONF.get("environment", {})},
            Description=ME_CONF["description"][:255])
        dep = "updated"
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(
            FunctionName=ME, Runtime=ME_CONF["runtime"], Role=ROLE,
            Handler=ME_CONF["handler"], Timeout=ME_CONF["timeout"],
            MemorySize=ME_CONF["memory"],
            Architectures=ME_CONF["architectures"],
            Environment={"Variables": ME_CONF.get("environment", {})},
            Description=ME_CONF["description"][:255],
            Code={"ZipFile": zip_src(ME_SRC)})
        dep = "created"
    check("me_deploy_ok", True, dep)
except Exception as e:
    check("me_deploy_ok", False, f"{type(e).__name__}: {e}")

me_arn = None
for _ in range(40):
    try:
        c = lam.get_function_configuration(FunctionName=ME)
        me_arn = c.get("FunctionArn")
        if c.get("State") == "Active" and c.get(
                "LastUpdateStatus") == "Successful":
            break
    except Exception:
        pass
    time.sleep(3)

sb = ME_CONF.get("eventbridge_scheduler", {})
SCHED = sb.get("schedule_name", f"{ME}-daily")
try:
    common = dict(
        ScheduleExpression=sb["cron"],
        ScheduleExpressionTimezone=sb.get("timezone", "UTC"),
        FlexibleTimeWindow={"Mode": "OFF"}, State="ENABLED",
        Description=sb.get("description", "")[:512],
        Target={"Arn": me_arn, "RoleArn": sb["role_arn"]})
    try:
        sch.get_schedule(Name=SCHED)
        sch.update_schedule(Name=SCHED, **common)
    except sch.exceptions.ResourceNotFoundException:
        sch.create_schedule(Name=SCHED, **common)
    check("me_schedule_wired", True, SCHED)
except Exception as e:
    check("me_schedule_wired", False, f"{type(e).__name__}: {e}")

try:
    r = lam.invoke(FunctionName=ME, InvocationType="RequestResponse",
                   Payload=b"{}")
    inner = json.loads(json.loads(
        r["Payload"].read().decode("utf-8", "ignore")).get("body") or "{}")
    check("me_invoke_ok",
          r.get("StatusCode") == 200 and not r.get("FunctionError"),
          r.get("FunctionError") or "200, posture=%s" % inner.get("posture"))
except Exception as e:
    check("me_invoke_ok", False, f"{type(e).__name__}: {e}")

time.sleep(2)
d = {}
try:
    d = get_json(ME_OUT)
    check("me_schema_ok",
          d.get("schema_version") == "1.0" and d.get("engine") == ME,
          "schema=%s engine=%s" % (d.get("schema_version"), d.get("engine")))
except Exception as e:
    check("me_schema_ok", False, f"{type(e).__name__}: {e}")

check("me_posture_resolves", d.get("posture") in VALID_POSTURE,
      "posture=%s" % d.get("posture"))

feeds = (d.get("inputs") or {}).get("feeds_available", "0/8")
try:
    n_feeds = int(str(feeds).split("/")[0])
except Exception:
    n_feeds = 0
check("me_feeds_reached", n_feeds >= 4, "%s feeds available" % feeds)

sc = d.get("scores") or {}
tr = sc.get("top_risk")
cyc = d.get("cycle_position")
check("me_scores_in_range",
      isinstance(tr, (int, float)) and 0 <= tr <= 100
      and isinstance(cyc, (int, float)) and 0 <= cyc <= 100,
      "top_risk=%s cycle_position=%s" % (tr, cyc))

tcan = d.get("top_canaries") or []
well = bool(tcan) and all(
    isinstance(c, dict) and c.get("label") and "points" in c
    and "max" in c and "firing" in c for c in tcan)
recon = all(
    bool(c["firing"]) == (c["points"] >= c["max"] * 0.5 and c["points"] > 0)
    for c in tcan if isinstance(c.get("points"), (int, float)))
check("me_canaries_well_formed", well and recon,
      "%d top canaries, firing flags reconcile=%s" % (len(tcan), recon))

bottom = d.get("bottom") or {}
check("me_capitulation_consumed",
      "capitulation_score" in bottom
      and "capitulation" in str(bottom.get("source") or "").lower(),
      "capitulation_score=%s source=%s"
      % (bottom.get("capitulation_score"), bottom.get("source")))

post, tr_v, ca_v = d.get("posture"), tr or 0, sc.get("capitulation")
cons = True
if post == "EUPHORIA":
    cons = tr_v >= 65
elif post == "CAPITULATION":
    cons = (ca_v or 0) >= 60
elif post == "EXPANSION":
    cons = tr_v < 65 and (ca_v or 0) < 38
check("me_posture_consistent", cons,
      "posture=%s top_risk=%s capitulation=%s" % (post, tr_v, ca_v))

try:
    st, page = http_get(ME_PAGE)
    check("me_page_deployed",
          st == 200 and "Market Cycle Extremes" in page
          and "market-extremes.json" in page,
          "HTTP %s, page %s" % (st, "live" if "market-extremes.json"
                                in page else "MISSING"))
except Exception as e:
    check("me_page_deployed", False, f"{type(e).__name__}: {e}")

try:
    ds, dp = http_get(DIR_URL)
    check("me_directory_linked",
          ds == 200 and "/market-extremes.html" in dp,
          "link %s" % ("present" if "/market-extremes.html" in dp
                       else "MISSING"))
except Exception as e:
    check("me_directory_linked", False, f"{type(e).__name__}: {e}")

try:
    sd = sch.get_schedule(Name=SCHED)
    check("me_schedule_live", sd.get("State") == "ENABLED",
          "%s %s" % (sd.get("State"), sd.get("ScheduleExpression")))
except Exception as e:
    check("me_schedule_live", False, f"{type(e).__name__}: {e}")

# ===================== PART B: cro-escalation vol-radar alert ==============
try:
    lam.update_function_code(FunctionName=CE, ZipFile=zip_src(CE_SRC))
    for _ in range(40):
        c = lam.get_function_configuration(FunctionName=CE)
        if c.get("LastUpdateStatus") == "Successful" and c.get(
                "State") == "Active":
            break
        time.sleep(3)
    check("ce_deploy_ok", True, "cro-escalation code updated")
except Exception as e:
    check("ce_deploy_ok", False, f"{type(e).__name__}: {e}")

# deployed artifact carries the vol-radar alert builder
try:
    loc = lam.get_function(FunctionName=CE)["Code"]["Location"]
    with zipfile.ZipFile(io.BytesIO(http_get_bytes(loc))) as z:
        nm = next((n for n in z.namelist()
                   if n.endswith("lambda_function.py")), None)
        csrc = z.read(nm).decode("utf-8", "ignore") if nm else ""
    has = ("def build_vol_radar_alert" in csrc
           and 'read_json("data/vol-radar.json")' in csrc
           and "posture_alerted" in csrc)
    check("ce_artifact_has_vol_radar_alert", has,
           "build_vol_radar_alert + vol-radar read + posture_alerted "
           "%s in the deployed artifact" % ("present" if has else "MISSING"))
except Exception as e:
    check("ce_artifact_has_vol_radar_alert", False,
          f"{type(e).__name__}: {e}")

# dry-run invoke -- no Telegram sent; confirms the track runs
try:
    r = lam.invoke(FunctionName=CE, InvocationType="RequestResponse",
                   Payload=json.dumps({"dry_run": True}).encode())
    inner = json.loads(json.loads(
        r["Payload"].read().decode("utf-8", "ignore")).get("body") or "{}")
    vrp = inner.get("vol_radar_posture")
    check("ce_vol_radar_track_runs",
          r.get("StatusCode") == 200 and not r.get("FunctionError")
          and "vol_radar_posture" in inner,
          "dry-run ok, vol_radar_posture=%s, would_alert=%s"
          % (vrp, inner.get("vol_radar_alerted")))
except Exception as e:
    check("ce_vol_radar_track_runs", False, f"{type(e).__name__}: {e}")

# ===================== summary =============================================
n_ok = sum(1 for c in rep["checks"] if c["ok"])
n_tot = len(rep["checks"])
rep["summary"] = "%d/%d checks passed" % (n_ok, n_tot)
rep["all_passed"] = n_ok == n_tot
rep["market_extremes"] = {
    "posture": d.get("posture"),
    "top_risk": tr,
    "capitulation": sc.get("capitulation"),
    "cycle_position": cyc,
    "top_canaries_firing": d.get("top_canaries_firing"),
    "feeds_available": feeds,
}
if rep["all_passed"]:
    rep["verdict"] = (
        "MARKET CYCLE EXTREMES RADAR LIVE + the vol-radar alert is wired "
        "into the intraday tripwire. Cycle read: %s (top-risk %s, "
        "capitulation %s, cycle position %s/100). cro-escalation now "
        "fires a Telegram ping on a vol-radar flip to COILED/PEAKING."
        % (d.get("posture"), tr, sc.get("capitulation"), cyc))
else:
    bad = [c["check"] for c in rep["checks"] if not c["ok"]]
    rep["verdict"] = ("VERIFICATION INCOMPLETE - %d check(s) failed: %s."
                      % (len(bad), ", ".join(bad)))

with open("aws/ops/reports/872_extremes_and_radar_alert.json", "w",
          encoding="utf-8") as f:
    json.dump(rep, f, indent=1)
print(json.dumps(rep, indent=1))
