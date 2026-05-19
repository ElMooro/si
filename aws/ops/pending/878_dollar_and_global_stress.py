"""
ops/878 - DEPLOY + VERIFY the Dollar Radar and the Global Stress
Matrix end-to-end.

For each new engine this op:
  1. creates or updates the Lambda from source + config;
  2. wires its EventBridge schedule;
  3. invokes it -- a live data scan;
  4. reads the S3 output back and proves the structure.

DOLLAR RADAR (data/dollar-radar.json):
  - schema + a Dollar Pressure score inside -100..+100 with a regime;
  - the pump/dump canary list is populated and every canary carries a
    PUMP/DUMP/NEUTRAL signal and an integer lean;
  - the FRED dollar family and bilateral crosses are present;
  - the technicals block carries the dollar index series.

GLOBAL STRESS MATRIX (data/global-stress.json):
  - schema + a Global Stress Index 0-100 with a level;
  - six equity markets and four bond markets, each scored 0-100 with
    a CALM/ELEVATED/STRESSED/ACUTE level and a charting series;
  - the worst market and the flashing-red list are coherent.

Finally confirms dollar.html and global-stress.html are live and
linked from the landing page. Writes
aws/ops/reports/878_dollar_and_global_stress.json.
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

cfg = Config(read_timeout=300, connect_timeout=20, retries={"max_attempts": 2})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
sch = boto3.client("scheduler", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)

rep = {
    "ops": 878,
    "ts": datetime.now(timezone.utc).isoformat(),
    "subject": "Deploy + verify the Dollar Radar (FRED dollar indices + "
               "pump/dump canaries + double tops/bottoms) and the Global "
               "Stress Matrix (world equity & bond stress)",
    "checks": [],
}


def check(name, ok, detail=""):
    rep["checks"].append({"check": name, "ok": bool(ok),
                          "detail": str(detail)[:240]})
    return ok


def http_get(url):
    req = urllib.request.Request(url, headers={"User-Agent": "justhodl-ops"})
    with urllib.request.urlopen(req, timeout=40) as r:
        return r.status, r.read().decode("utf-8", "ignore")


def deploy(fn):
    """Create or update a Lambda from aws/lambdas/<fn>/."""
    src = "aws/lambdas/%s/source/lambda_function.py" % fn
    conf = json.load(open("aws/lambdas/%s/config.json" % fn,
                          encoding="utf-8"))
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("lambda_function.py", open(src, encoding="utf-8").read())
    code = buf.getvalue()
    try:
        lam.get_function(FunctionName=fn)
        lam.update_function_code(FunctionName=fn, ZipFile=code)
        for _ in range(50):
            if lam.get_function_configuration(FunctionName=fn).get(
                    "LastUpdateStatus") == "Successful":
                break
            time.sleep(2)
        lam.update_function_configuration(
            FunctionName=fn, Handler=conf["handler"], Runtime=conf["runtime"],
            Role=ROLE, Timeout=conf["timeout"], MemorySize=conf["memory"],
            Environment={"Variables": conf.get("environment", {})},
            Description=conf["description"][:255])
        dep = "updated"
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(
            FunctionName=fn, Runtime=conf["runtime"], Role=ROLE,
            Handler=conf["handler"], Timeout=conf["timeout"],
            MemorySize=conf["memory"], Architectures=conf["architectures"],
            Environment={"Variables": conf.get("environment", {})},
            Description=conf["description"][:255], Code={"ZipFile": code})
        dep = "created"
    fn_arn = None
    for _ in range(50):
        c = lam.get_function_configuration(FunctionName=fn)
        fn_arn = c.get("FunctionArn")
        if c.get("State") == "Active" and c.get(
                "LastUpdateStatus") == "Successful":
            break
        time.sleep(3)
    # schedule
    sb = conf.get("eventbridge_scheduler", {})
    sname = sb.get("schedule_name", "%s-sched" % fn)
    common = dict(
        ScheduleExpression=sb["cron"],
        ScheduleExpressionTimezone=sb.get("timezone", "UTC"),
        FlexibleTimeWindow={"Mode": "OFF"}, State="ENABLED",
        Description=sb.get("description", "")[:512],
        Target={"Arn": fn_arn, "RoleArn": sb["role_arn"]})
    try:
        sch.get_schedule(Name=sname)
        sch.update_schedule(Name=sname, **common)
    except sch.exceptions.ResourceNotFoundException:
        sch.create_schedule(Name=sname, **common)
    return dep, sname


def invoke(fn):
    r = lam.invoke(FunctionName=fn, InvocationType="RequestResponse",
                   Payload=b"{}")
    body = json.loads(r["Payload"].read().decode("utf-8", "ignore"))
    err = r.get("FunctionError")
    return (r.get("StatusCode") == 200 and not err), err, \
        json.loads(body.get("body") or "{}")


# ===== DOLLAR RADAR =========================================================
try:
    dep, sname = deploy("justhodl-dollar-radar")
    check("dollar_deploy_ok", True, "%s, schedule %s" % (dep, sname))
except Exception as e:
    check("dollar_deploy_ok", False, f"{type(e).__name__}: {e}")

try:
    ok, err, inv = invoke("justhodl-dollar-radar")
    check("dollar_invoke_ok", ok and inv.get("ok") is True,
          err or "pressure=%s regime=%s canaries=%s indices=%s in %ss"
          % (inv.get("dollar_pressure"), inv.get("regime"),
             inv.get("canaries"), inv.get("indices"),
             inv.get("build_seconds")))
except Exception as e:
    check("dollar_invoke_ok", False, f"{type(e).__name__}: {e}")

time.sleep(2)
dr = {}
try:
    dr = json.loads(s3.get_object(Bucket=S3_BUCKET,
                                  Key="data/dollar-radar.json")["Body"].read())
    check("dollar_output_readable", True, "data/dollar-radar.json read")
except Exception as e:
    check("dollar_output_readable", False, f"{type(e).__name__}: {e}")

press = dr.get("dollar_pressure")
check("dollar_pressure_scored",
      isinstance(press, (int, float)) and -100 <= press <= 100
      and dr.get("regime"),
      "pressure=%s regime=%s" % (press, dr.get("regime")))

cans = dr.get("canaries") or []
cans_ok = (len(cans) >= 7 and all(
    c.get("signal") in ("PUMP", "DUMP", "NEUTRAL")
    and isinstance(c.get("lean"), int) for c in cans))
check("dollar_canaries_well_formed", cans_ok,
      "%d canaries, %d pump / %d dump"
      % (len(cans), dr.get("canaries_pump", 0), dr.get("canaries_dump", 0)))

idx = dr.get("indices") or []
idx_ok = (len(idx) >= 3 and all(
    i.get("fred_id") and isinstance(i.get("series"), list) and i["series"]
    for i in idx))
check("dollar_fred_family_present", idx_ok,
      "%d FRED dollar indices: %s; %d bilateral crosses"
      % (len(idx), ", ".join(i.get("fred_id", "?") for i in idx),
         len(dr.get("bilaterals") or [])))

tech = dr.get("technicals") or {}
check("dollar_technicals_present",
      isinstance(tech.get("series"), list) and len(tech["series"]) > 50
      and tech.get("level") is not None,
      "dollar index level=%s, series=%s pts, double_top=%s double_bottom=%s"
      % (tech.get("level"), len(tech.get("series") or []),
         bool(tech.get("double_top")), bool(tech.get("double_bottom"))))

# ===== GLOBAL STRESS MATRIX =================================================
try:
    dep, sname = deploy("justhodl-global-stress")
    check("global_deploy_ok", True, "%s, schedule %s" % (dep, sname))
except Exception as e:
    check("global_deploy_ok", False, f"{type(e).__name__}: {e}")

try:
    ok, err, inv = invoke("justhodl-global-stress")
    check("global_invoke_ok", ok and inv.get("ok") is True,
          err or "GSI=%s eq=%s bond=%s markets=%s flashing=%s in %ss"
          % (inv.get("global_stress_index"), inv.get("equity_stress"),
             inv.get("bond_stress"), inv.get("markets_scored"),
             inv.get("flashing_red"), inv.get("build_seconds")))
except Exception as e:
    check("global_invoke_ok", False, f"{type(e).__name__}: {e}")

time.sleep(2)
gs = {}
try:
    gs = json.loads(s3.get_object(Bucket=S3_BUCKET,
                                  Key="data/global-stress.json")["Body"]
                    .read())
    check("global_output_readable", True, "data/global-stress.json read")
except Exception as e:
    check("global_output_readable", False, f"{type(e).__name__}: {e}")

gsi = gs.get("global_stress_index")
check("global_index_scored",
      isinstance(gsi, (int, float)) and 0 <= gsi <= 100
      and gs.get("global_stress_level"),
      "GSI=%s (%s), equity=%s bond=%s"
      % (gsi, gs.get("global_stress_level"), gs.get("equity_stress"),
         gs.get("bond_stress")))

eqs, bds = gs.get("equities") or [], gs.get("bonds") or []


def market_rows_ok(rows):
    return rows and all(
        isinstance(r.get("stress"), int) and 0 <= r["stress"] <= 100
        and r.get("level") in ("CALM", "ELEVATED", "STRESSED", "ACUTE")
        and isinstance(r.get("series"), list) and len(r["series"]) > 20
        for r in rows)


check("global_equity_markets_ok",
      len(eqs) == 6 and market_rows_ok(eqs),
      "%d equity markets: %s" % (len(eqs), ", ".join(
          "%s %d" % (r.get("market", "?"), r.get("stress", -1))
          for r in eqs)))
check("global_bond_markets_ok",
      len(bds) == 4 and market_rows_ok(bds),
      "%d bond markets: %s" % (len(bds), ", ".join(
          "%s %d" % (r.get("market", "?"), r.get("stress", -1))
          for r in bds)))

# the worst market and flashing list must be internally coherent
allr = eqs + bds
worst = gs.get("worst_market") or {}
coherent = True
if allr:
    top = max(allr, key=lambda r: r["stress"])
    coherent = (worst.get("stress") == top["stress"])
    for r in allr:
        in_flash = any(r["market"] in f for f in gs.get("flashing_red", []))
        if (r["stress"] >= 75) != in_flash:
            coherent = False
check("global_worst_and_flash_coherent", coherent,
      "worst=%s %s; flashing red: %s"
      % (worst.get("market"), worst.get("stress"),
         gs.get("flashing_red") or "none"))

# ===== pages live + linked ==================================================
for page, token, url in (
        ("dollar.html", "Dollar Radar", "https://justhodl.ai/dollar.html"),
        ("global-stress.html", "Global Stress Matrix",
         "https://justhodl.ai/global-stress.html")):
    try:
        st, html = http_get(url)
        check("page_live_%s" % page.split(".")[0].replace("-", "_"),
              st == 200 and token in html, "HTTP %s" % st)
    except Exception as e:
        check("page_live_%s" % page.split(".")[0].replace("-", "_"),
              False, f"{type(e).__name__}: {e}")

try:
    st, idx = http_get("https://justhodl.ai/")
    check("linked_from_landing_page",
          st == 200 and "/dollar.html" in idx
          and "/global-stress.html" in idx,
          "landing page links both pages"
          if ("/dollar.html" in idx and "/global-stress.html" in idx)
          else "a link is MISSING from the landing page")
except Exception as e:
    check("linked_from_landing_page", False, f"{type(e).__name__}: {e}")

# ===== summary ==============================================================
n_ok = sum(1 for c in rep["checks"] if c["ok"])
n_tot = len(rep["checks"])
rep["summary"] = "%d/%d checks passed" % (n_ok, n_tot)
rep["all_passed"] = n_ok == n_tot
rep["readings"] = {
    "dollar_pressure": dr.get("dollar_pressure"),
    "dollar_regime": dr.get("regime"),
    "global_stress_index": gs.get("global_stress_index"),
    "global_stress_level": gs.get("global_stress_level"),
    "worst_market": gs.get("worst_market"),
}
if rep["all_passed"]:
    rep["verdict"] = (
        "DOLLAR RADAR + GLOBAL STRESS MATRIX LIVE. The Dollar Radar "
        "scores Dollar Pressure %s (%s) from its pump/dump canary "
        "composite over the FRED dollar family, with double top/bottom "
        "detection. The Global Stress Matrix scores world equity and "
        "bond stress at a Global Stress Index of %s (%s). Both run on a "
        "6h schedule, both pages are live and linked from the landing "
        "page." % (dr.get("dollar_pressure"), dr.get("regime"),
                   gs.get("global_stress_index"),
                   gs.get("global_stress_level")))
else:
    bad = [c["check"] for c in rep["checks"] if not c["ok"]]
    rep["verdict"] = ("VERIFICATION INCOMPLETE - %d check(s) failed: %s."
                      % (len(bad), ", ".join(bad)))

with open("aws/ops/reports/878_dollar_and_global_stress.json", "w",
          encoding="utf-8") as f:
    json.dump(rep, f, indent=1)
print(json.dumps(rep, indent=1))
