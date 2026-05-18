"""
ops/849 - deploy + verify justhodl-pnl-attribution (the firm Performance
& P&L Attribution desk).

Packages the engine, creates/updates the Lambda, wires the daily
EventBridge Scheduler schedule (03:00 UTC, after the factor-risk model),
invokes it once and polls S3 for the published sidecar.

The desk-return feed appends one mark per trading day and is still
warming, so the engine has two valid postures: WARMING (fewer than five
ledger rows) or LIVE. This verifier treats a correct WARMING payload as
a pass and only runs the deep performance-analytics checks when the
engine reports LIVE. In both cases it confirms the append-only firm
ledger (data/pnl-ledger.json) persisted and that its row count agrees
with the published observation count.

Writes aws/ops/reports/849_pnl_attribution_deploy.json.
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
FN = "justhodl-pnl-attribution"
SRC = "aws/lambdas/justhodl-pnl-attribution/source/lambda_function.py"
CFG = "aws/lambdas/justhodl-pnl-attribution/config.json"
OUT_KEY = "data/pnl-attribution.json"
LEDGER_KEY = "data/pnl-ledger.json"
SCHED_ROLE = "arn:aws:iam::%s:role/justhodl-scheduler-role" % ACCOUNT
LAMBDA_ARN = "arn:aws:lambda:%s:%s:function:%s" % (REGION, ACCOUNT, FN)

cfg = Config(read_timeout=120, connect_timeout=20,
             retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
sch = boto3.client("scheduler", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)

rep = {"ops": 849, "ts": datetime.now(timezone.utc).isoformat(),
       "subject": "Deploy + verify justhodl-pnl-attribution - the firm "
                  "Performance & P&L Attribution desk", "checks": []}


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
               Payload=json.dumps({"src": "ops849"}).encode())
    check("invoke_accepted", True, "async invoke dispatched")
except Exception as e:
    check("invoke_accepted", False, "%s: %s" % (type(e).__name__, e))

fresh = None
for _ in range(14):                        # up to ~7 min
    time.sleep(30)
    try:
        head = s3.head_object(Bucket=S3_BUCKET, Key=OUT_KEY)
        if head["LastModified"] > t_invoke:
            fresh = head["LastModified"]
            break
    except Exception:
        pass
check("output_written", fresh is not None,
      "pnl-attribution.json refreshed at %s" % fresh if fresh
      else "no fresh output within 7 min")

# ---- 5) validate the published payload ------------------------------------
data = {}
if fresh:
    try:
        data = json.loads(s3.get_object(
            Bucket=S3_BUCKET, Key=OUT_KEY)["Body"].read())
        check("output_readable", True, "parsed ok")
    except Exception as e:
        check("output_readable", False, "%s: %s" % (type(e).__name__, e))

if data and not data.get("error"):
    posture = data.get("posture")
    check("posture_valid", posture in ("WARMING", "LIVE"),
          "posture = %s" % posture)

    n_obs = data.get("ledger_observations")
    check("ledger_obs_present", isinstance(n_obs, int) and n_obs >= 0,
          "ledger_observations = %s" % n_obs)

    # append-only ledger persisted + row count agrees
    led_rows = None
    try:
        led = json.loads(s3.get_object(
            Bucket=S3_BUCKET, Key=LEDGER_KEY)["Body"].read())
        led_rows = led.get("rows")
        check("ledger_persisted", isinstance(led_rows, list),
              "%d rows in data/pnl-ledger.json"
              % (len(led_rows) if isinstance(led_rows, list) else -1))
    except Exception as e:
        check("ledger_persisted", False, "%s: %s" % (type(e).__name__, e))

    if isinstance(led_rows, list):
        check("ledger_count_consistent", len(led_rows) == n_obs,
              "ledger %d == published %s" % (len(led_rows), n_obs))
        # rows carry the contract fields
        if led_rows:
            r0 = led_rows[-1]
            ok_row = all(k in r0 for k in
                         ("date", "firm_return", "desk_returns", "weights"))
            check("ledger_row_schema", ok_row,
                  "last row keys: %s" % sorted(r0.keys()))
        else:
            check("ledger_row_schema", True, "ledger empty (no marks yet)")

    if posture == "WARMING":
        hl = (data.get("headline") or "").lower()
        check("warming_payload_ok",
              "warm" in hl and isinstance(data.get("firm"), dict),
              "warming headline + firm block present")
        check("warming_curve_present",
              isinstance(data.get("equity_curve"), list),
              "%d marked days listed"
              % len(data.get("equity_curve", [])))
        rep["pnl"] = {
            "posture": posture,
            "ledger_observations": n_obs,
            "rows_added_this_run": data.get("rows_added_this_run"),
            "spy_history_ok": data.get("spy_history_ok"),
            "headline": data.get("headline"),
        }
    else:  # LIVE - run the deep analytics checks
        firm = data.get("firm", {})
        bd = data.get("attribution_by_desk", [])
        ec = data.get("equity_curve", [])
        fa = data.get("factor_attribution", {})
        ae = data.get("allocation_effect", {})

        pr = firm.get("period_returns", {})
        check("period_returns_complete",
              all(k in pr for k in ("d1", "w1", "mtd", "qtd", "ytd", "itd")),
              "period keys: %s" % sorted(pr.keys()))

        sh = firm.get("sharpe")
        check("sharpe_finite", isinstance(sh, (int, float)),
              "sharpe = %s" % sh)

        mdd = firm.get("max_drawdown_pct")
        check("drawdown_sane",
              isinstance(mdd, (int, float)) and mdd <= 0.0001,
              "max drawdown = %s%%" % mdd)

        check("desk_attribution_present", len(bd) == 7,
              "%d desks attributed" % len(bd))

        # by-desk contributions reconcile to the reported P&L total
        s_contrib = sum(x.get("contribution_pct") or 0.0 for x in bd)
        tot = data.get("pnl_total_pct")
        check("attribution_reconciles",
              isinstance(tot, (int, float))
              and abs(s_contrib - tot) < 0.05,
              "sum(desk contrib) %.4f vs pnl_total %s" % (s_contrib, tot))

        check("equity_curve_complete", len(ec) == n_obs,
              "%d curve points == %s obs" % (len(ec), n_obs))
        if ec:
            last_cum = ec[-1].get("firm_cum_pct")
            cum = firm.get("cumulative_return_pct")
            check("curve_endpoint_matches",
                  isinstance(last_cum, (int, float))
                  and isinstance(cum, (int, float))
                  and abs(last_cum - cum) < 0.05,
                  "curve end %s vs cumulative %s" % (last_cum, cum))

        check("allocation_effect_present",
              "allocator_value_add_pct" in ae,
              "value-add = %s%%" % ae.get("allocator_value_add_pct"))

        check("factor_attribution_present",
              isinstance(fa, dict) and "available" in fa,
              "factor split available = %s" % fa.get("available"))

        rep["pnl"] = {
            "posture": posture,
            "ledger_observations": n_obs,
            "cumulative_return_pct": firm.get("cumulative_return_pct"),
            "annualized_return_pct": firm.get("annualized_return_pct"),
            "sharpe": sh, "sortino": firm.get("sortino"),
            "max_drawdown_pct": mdd,
            "hit_rate_pct": firm.get("hit_rate_pct"),
            "benchmark": firm.get("benchmark"),
            "top_desk": (bd[0].get("name") if bd else None),
            "allocator_value_add_pct": ae.get("allocator_value_add_pct"),
            "factor_attribution": fa,
            "headline": data.get("headline"),
        }
elif data.get("error"):
    check("engine_no_error", False, "engine error: %s" % data.get("error"))

# ---- verdict --------------------------------------------------------------
rep["all_pass"] = all(c["ok"] for c in rep["checks"])
fails = [c["check"] for c in rep["checks"] if not c["ok"]]
p = rep.get("pnl", {})
if rep["all_pass"] and p.get("posture") == "LIVE":
    rep["verdict"] = (
        "P&L ATTRIBUTION LIVE - the firm now strikes a daily marked P&L "
        "with full performance analytics. Cumulative %s%%, Sharpe %s, max "
        "drawdown %s%%, top contributor %s. The performance desk above "
        "the factor-risk layer is operational, running daily 03:00 UTC."
        % (p.get("cumulative_return_pct"), p.get("sharpe"),
           p.get("max_drawdown_pct"), p.get("top_desk")))
elif rep["all_pass"]:
    rep["verdict"] = (
        "P&L ATTRIBUTION DEPLOYED (WARMING) - engine, schedule and the "
        "append-only firm ledger are live and correct. The ledger holds "
        "%s of 5 daily marks; full performance analytics unlock as the "
        "desk-return feed appends one observation per trading day. Running "
        "daily 03:00 UTC." % p.get("ledger_observations"))
else:
    rep["verdict"] = "REVIEW - failed: %s" % fails

out = json.dumps(rep, indent=2, default=str)
print(out)
try:
    with open("aws/ops/reports/849_pnl_attribution_deploy.json", "w") as f:
        f.write(out)
except Exception as e:
    print("report write skipped:", e)
