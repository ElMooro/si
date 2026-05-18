"""
ops/848 - deploy + verify justhodl-factor-risk (the firm Factor Risk Model).

Packages the engine, creates/updates the Lambda, wires the daily
EventBridge Scheduler schedule (02:30 UTC), invokes it once and then
polls S3 for the output - the first run is heavy (it regresses every
firm-book name against the factor ETFs and warms the loadings cache),
so verification is decoupled from the invoke and waits on the sidecar.

Validates the published data/factor-risk.json: six factor exposures, a
sane systematic/idiosyncratic split, monotone VaR, scenario stress P&L,
and that every name resolved to either a direct or a proxy loading.

Writes aws/ops/reports/848_factor_risk_deploy.json.
"""
import io
import json
import time
import zipfile
from datetime import datetime, timezone

import boto3
from botocore.config import Config

REGION = "us-east-1"
ACCOUNT = "857687956942"
S3_BUCKET = "justhodl-dashboard-live"
FN = "justhodl-factor-risk"
SRC = "aws/lambdas/justhodl-factor-risk/source/lambda_function.py"
CFG = "aws/lambdas/justhodl-factor-risk/config.json"
OUT_KEY = "data/factor-risk.json"
CACHE_KEY = "data/factor-loadings-cache.json"
SCHED_ROLE = "arn:aws:iam::%s:role/justhodl-scheduler-role" % ACCOUNT
LAMBDA_ARN = "arn:aws:lambda:%s:%s:function:%s" % (REGION, ACCOUNT, FN)

cfg = Config(read_timeout=120, connect_timeout=20,
             retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
sch = boto3.client("scheduler", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)

rep = {"ops": 848, "ts": datetime.now(timezone.utc).isoformat(),
       "subject": "Deploy + verify justhodl-factor-risk - the firm "
                  "Factor Risk Model", "checks": []}


def check(name, ok, detail=""):
    rep["checks"].append({"check": name, "ok": bool(ok),
                          "detail": str(detail)[:240]})
    return ok


def wait_updated(timeout=120):
    t0 = time.time()
    while time.time() - t0 < timeout:
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("LastUpdateStatus") == "Successful":
            return True
        if c.get("LastUpdateStatus") == "Failed":
            return False
        time.sleep(3)
    return False


# ---- 1) package -----------------------------------------------------------
conf = json.load(open(CFG))
buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
    z.write(SRC, "lambda_function.py")
code = buf.getvalue()
check("package_built", len(code) > 800, "%d bytes" % len(code))

# ---- 2) create / update the Lambda ----------------------------------------
exists = True
try:
    lam.get_function(FunctionName=FN)
except lam.exceptions.ResourceNotFoundException:
    exists = False

desc = conf["description"][:255]
if exists:
    lam.update_function_code(FunctionName=FN, ZipFile=code)
    ok_code = wait_updated()
    lam.update_function_configuration(
        FunctionName=FN, Runtime=conf["runtime"], Handler=conf["handler"],
        Timeout=conf["timeout"], MemorySize=conf["memory"],
        Description=desc, Role=conf["role"])
    ok_cfg = wait_updated()
    check("lambda_deployed", ok_code and ok_cfg, "updated existing function")
else:
    lam.create_function(
        FunctionName=FN, Runtime=conf["runtime"], Role=conf["role"],
        Handler=conf["handler"], Code={"ZipFile": code},
        Timeout=conf["timeout"], MemorySize=conf["memory"],
        Architectures=conf.get("architectures", ["x86_64"]),
        Description=desc)
    time.sleep(8)
    check("lambda_deployed", True, "created new function")

# ---- 3) wire the EventBridge Scheduler schedule ---------------------------
sb = conf["eventbridge_scheduler"]
sname = sb["schedule_name"]
sched_args = dict(
    Name=sname,
    ScheduleExpression=sb["cron"],
    FlexibleTimeWindow={"Mode": "OFF"},
    State="ENABLED",
    Description=sb.get("description", "")[:200],
    Target={"Arn": LAMBDA_ARN, "RoleArn": SCHED_ROLE,
            "Input": json.dumps({"src": "schedule"})},
)
try:
    sch.get_schedule(Name=sname)
    sch.update_schedule(**sched_args)
    sched_action = "updated"
except sch.exceptions.ResourceNotFoundException:
    sch.create_schedule(**sched_args)
    sched_action = "created"
try:
    sd = sch.get_schedule(Name=sname)
    check("schedule_wired", sd.get("State") == "ENABLED",
          "%s %s = %s %s" % (sched_action, sname, sd.get("State"),
                             sd.get("ScheduleExpression")))
except Exception as e:
    check("schedule_wired", False, "%s: %s" % (type(e).__name__, e))

# ---- 4) invoke once (async) + poll the sidecar ----------------------------
t_invoke = datetime.now(timezone.utc)
try:
    lam.invoke(FunctionName=FN, InvocationType="Event",
               Payload=json.dumps({"src": "ops848"}).encode())
    check("invoke_accepted", True, "async invoke dispatched")
except Exception as e:
    check("invoke_accepted", False, "%s: %s" % (type(e).__name__, e))

fresh = None
for _ in range(30):                       # up to ~15 min
    time.sleep(30)
    try:
        head = s3.head_object(Bucket=S3_BUCKET, Key=OUT_KEY)
        if head["LastModified"] > t_invoke:
            fresh = head["LastModified"]
            break
    except Exception:
        pass
check("output_written", fresh is not None,
      "factor-risk.json refreshed at %s" % fresh if fresh
      else "no fresh output within 15 min")

# ---- 5) validate the published model --------------------------------------
data = {}
if fresh:
    try:
        data = json.loads(s3.get_object(
            Bucket=S3_BUCKET, Key=OUT_KEY)["Body"].read())
    except Exception as e:
        check("output_readable", False, "%s: %s" % (type(e).__name__, e))

if data and not data.get("error"):
    firm = data.get("firm", {})
    fx = data.get("factor_exposures", [])
    sc = data.get("scenarios", [])
    cv = data.get("coverage", {})
    rc = data.get("risk_contributors", [])
    hg = data.get("hedges", [])

    check("six_factors", len(fx) == 6,
          "%d factor exposures: %s"
          % (len(fx), [x.get("factor") for x in fx]))

    av = firm.get("annual_vol_pct")
    check("vol_sane", isinstance(av, (int, float)) and 0 < av < 200,
          "annual vol = %s%%" % av)

    psys = firm.get("pct_systematic")
    pidio = firm.get("pct_idiosyncratic")
    split_ok = (isinstance(psys, (int, float))
                and isinstance(pidio, (int, float))
                and abs((psys + pidio) - 100.0) < 0.5)
    check("risk_split_ok", split_ok,
          "systematic %s%% + idiosyncratic %s%%" % (psys, pidio))

    v95, v99 = firm.get("var_95_1d_pct"), firm.get("var_99_1d_pct")
    check("var_monotone",
          isinstance(v95, (int, float)) and isinstance(v99, (int, float))
          and v99 >= v95 > 0,
          "VaR95 %s%% <= VaR99 %s%%" % (v95, v99))

    check("mctr_sums_100",
          abs(sum(x.get("mctr_pct_of_systematic", 0) for x in fx)
              - 100.0) < 2.0,
          "MCTR total = %.1f%%"
          % sum(x.get("mctr_pct_of_systematic", 0) for x in fx))

    check("scenarios_present", len(sc) >= 6,
          "%d scenarios, worst = %s"
          % (len(sc), sc[0].get("book_pnl_pct") if sc else None))

    nd = cv.get("n_direct_loadings", 0)
    npx = cv.get("n_proxy_loadings", 0)
    nn = cv.get("n_names", 0)
    check("coverage_complete", nd + npx == nn and nn > 0,
          "%d direct + %d proxy = %d names" % (nd, npx, nn))

    check("risk_contributors_present", len(rc) > 0,
          "%d contributors, top = %s (%s%%)"
          % (len(rc), rc[0].get("symbol") if rc else None,
             rc[0].get("risk_contribution_pct") if rc else None))

    check("net_beta_finite",
          isinstance(firm.get("net_market_beta"), (int, float)),
          "net market beta = %s" % firm.get("net_market_beta"))

    # loadings cache persisted?
    try:
        ch = json.loads(s3.get_object(
            Bucket=S3_BUCKET, Key=CACHE_KEY)["Body"].read())
        nl = len(ch.get("loadings", {}))
        check("loadings_cache_persisted", nl > 0,
              "%d names cached" % nl)
    except Exception as e:
        check("loadings_cache_persisted", False,
              "%s: %s" % (type(e).__name__, e))

    rep["model"] = {
        "annual_vol_pct": av, "pct_systematic": psys,
        "var_95_1d_pct": v95, "var_99_1d_pct": v99,
        "es_95_1d_pct": firm.get("es_95_1d_pct"),
        "net_market_beta": firm.get("net_market_beta"),
        "factor_exposures": [(x.get("factor"), x.get("book_exposure_pct"))
                             for x in fx],
        "worst_scenario": (sc[0].get("scenario"),
                           sc[0].get("book_pnl_pct")) if sc else None,
        "hedges": [h.get("suggested_trade") for h in hg],
        "coverage": "%d direct / %d proxy / %d failed"
                    % (nd, npx, cv.get("failed_history", 0)),
        "headline": data.get("headline"),
    }
elif data.get("error"):
    check("model_no_error", False, "engine error: %s" % data.get("error"))

# ---- verdict --------------------------------------------------------------
rep["all_pass"] = all(c["ok"] for c in rep["checks"])
fails = [c["check"] for c in rep["checks"] if not c["ok"]]
m = rep.get("model", {})
rep["verdict"] = (
    "FACTOR RISK MODEL LIVE - the firm book is now decomposed into six "
    "tradable factors daily at 02:30 UTC. Annualised vol %s%% (%s%% "
    "systematic), VaR95 %s%%. %s. The risk-analytics layer above the "
    "risk-monitor is operational."
    % (m.get("annual_vol_pct"), m.get("pct_systematic"),
       m.get("var_95_1d_pct"), m.get("coverage"))
    if rep["all_pass"]
    else "REVIEW - failed: %s" % fails)

out = json.dumps(rep, indent=2, default=str)
print(out)
try:
    with open("aws/ops/reports/848_factor_risk_deploy.json", "w") as f:
        f.write(out)
except Exception as e:
    print("report write skipped:", e)
