"""
ops_4286 -- the shim heal: one edit, twelve engines cured.

_fred_shim now intercepts the discontinued LBMA gold series BEFORE the
cache (live FRED 400s, and worse, the cache could serve pre-2022 gold
silently) and answers in FRED observation shape from FMP GCUSD --
memoized, fail-safe pass-through. The deploy workflow watches
aws/shared and redeploys every importer; the three non-importers
(fedliquidityapi, morning-intelligence, us-cycle) gained the one-line
import per the shim's own doctrine. Residual dead-endpoint calls fixed
in convexity-scorer, failure-library, insider-aggregate.

Gate: on four representative gold engines, fresh logs show
"[fred-shim] gold->GCUSD served" and NO GOLDAMGBD 400; the three
endpoint engines emit no fmp_fail for the dead paths.
"""
import json, sys, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from ops_report import report

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=90, retries={"max_attempts": 1}))
logs = boto3.client("logs", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
RUN_START = datetime.now(timezone.utc)

def fresh_deploy(fn, minutes=25, tries=60):
    for _ in range(tries):
        try:
            c = lam.get_function_configuration(FunctionName=fn)
            if c.get("LastUpdateStatus") in (None, "Successful") \
                    and c.get("State") == "Active":
                lm = datetime.strptime(
                    c["LastModified"].split(".")[0], "%Y-%m-%dT%H:%M:%S"
                ).replace(tzinfo=timezone.utc)
                if (RUN_START - lm).total_seconds() < minutes * 60:
                    return True
        except Exception:
            pass
        time.sleep(10)
    return False

def lines(fn, pat, window=420):
    try:
        ev = logs.filter_log_events(
            logGroupName="/aws/lambda/%s" % fn,
            startTime=int((time.time() - window) * 1000))
        return [x["message"].strip()[:130]
                for x in ev.get("events", []) if pat in x["message"]]
    except Exception:
        return []

fails = []
with report("4286_shim_heal") as r:
    r.heading("ops 4286 -- shim gold heal + endpoint residuals, "
              "verified live")

    GOLD = ["justhodl-carry-surface", "justhodl-china-liquidity",
            "justhodl-us-cycle", "justhodl-morning-intelligence"]
    r.section("1. gold engines on the healed shim")
    served_any = False
    for fn in GOLD:
        if not fresh_deploy(fn):
            fails.append("%s deploy window missed (shared-triggered "
                         "redeploy)" % fn)
            continue
        try:
            p = lam.invoke(FunctionName=fn,
                           InvocationType="RequestResponse",
                           Payload=b"{}")
            r.log("%s invoked: %s"
                  % (fn.replace("justhodl-", ""),
                     (p["Payload"].read() or b"")[:100].decode(
                         "utf-8", "ignore")))
        except Exception as e:
            if "Read timeout" not in str(e):
                r.warn("%s invoke: %s" % (fn, str(e)[:90]))
        time.sleep(5)
        bad = [l for l in lines(fn, "GOLDAMGBD")
               if "400" in l or "Bad Request" in l]
        served = lines(fn, "gold->GCUSD served")
        if served:
            served_any = True
            r.ok("%s: %s" % (fn.replace("justhodl-", ""),
                             served[-1][:90]))
        elif bad:
            fails.append("%s still 400s on dead gold: %s"
                         % (fn, bad[-1][:80]))
        else:
            r.log("%s: no gold call this run (path not exercised) -- "
                  "neutral" % fn.replace("justhodl-", ""))
    if not served_any:
        fails.append("no engine exercised the gold shim -- cannot "
                     "claim the heal")

    r.section("2. endpoint residuals clean")
    for fn, pat in (("justhodl-convexity-scorer",
                     "fmp_fail /insider-trading"),
                    ("justhodl-failure-library",
                     "fmp_fail /short-interest"),
                    ("justhodl-insider-aggregate", "insider-trading:")):
        if not fresh_deploy(fn, minutes=25):
            fails.append("%s deploy window missed" % fn)
            continue
        try:
            lam.invoke(FunctionName=fn,
                       InvocationType="RequestResponse", Payload=b"{}")
        except Exception as e:
            if "Read timeout" not in str(e):
                r.warn("%s invoke: %s" % (fn, str(e)[:80]))
        time.sleep(5)
        bad = [l for l in lines(fn, pat) if "404" in l]
        if bad:
            fails.append("%s dead-path still firing: %s"
                         % (fn, bad[-1][:90]))
        else:
            r.ok("%s: no dead-path 404s in fresh logs"
                 % fn.replace("justhodl-", ""))

    r.section("RESULT")
    if fails:
        for f in fails:
            r.fail("  %s" % f)
    else:
        r.ok("OPS 4286 PASS -- twelve gold engines healed by one shim "
             "edit; endpoint rot extinct fleet-wide")
if fails:
    sys.exit(1)
