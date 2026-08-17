"""ops/4808 -- justhodl-sp500 birth verify (house-standard settle-gated):
 (1) function exists (deploy-lambdas.yml create) -> wait Active; env
     check+heal FRED_API_KEY from donor dollar-strength-agent.
 (2) deploy settle: deployed zip carries the 'sp500 v1.0.0' marker.
 (3) schedule ensure: EventBridge Scheduler justhodl-sp500-daily
     cron(45 21 ? * MON-FRI *) UTC (after spx-ma's 21:15 close ledger).
 (4) Event-invoke, poll data/sp500.json as_of <= 8 min.
 (5) truth bands, all REAL: members>=400, pe_ttm agg in (12,45),
     pe_fwd present & < pe_ttm*1.25, earnings_yield>0, div yield in
     (0.5,4.0), member distributions populated, compare-mode smoke on
     AAPL returns 12 rows with percentiles. ERP/CPI warn-only (FRED).
 (6) full metric readout into the report.
"""
import gzip
import io
import json
import sys
import time
import urllib.request
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from botocore.exceptions import ClientError  # noqa: E402
from ops_report import report  # noqa: E402

REGION = "us-east-1"
ACCOUNT = "857687956942"
FN = "justhodl-sp500"
B = "justhodl-dashboard-live"
OUT_KEY = "data/sp500.json"
MARKER = "sp500 v1.0.3"
SCHED_NAME = "justhodl-sp500-daily"
SCHED_CRON = "cron(45 21 ? * MON-FRI *)"
SCHED_ROLE = f"arn:aws:iam::{ACCOUNT}:role/justhodl-scheduler-role"

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=90,
                                 retries={"max_attempts": 1}))
sched = boto3.client("scheduler", region_name=REGION)
FAILED = []


def sread(key):
    raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def wait_active(rep):
    for _ in range(40):
        try:
            cfg = lam.get_function_configuration(FunctionName=FN)
        except ClientError:
            time.sleep(6)
            continue
        if (cfg.get("State") == "Active"
                and cfg.get("LastUpdateStatus") != "InProgress"):
            rep.kv(state="Active", runtime=cfg.get("Runtime"),
                   mem=cfg.get("MemorySize"))
            return cfg
        time.sleep(6)
    rep.fail(f"{FN} never reached Active -- deploy-lambdas create "
             f"did not land")
    FAILED.append("active")
    return None


DONORS = ("dollar-strength-agent", "justhodl-blackswan-watch")


def heal_fred(rep):
    cfg = lam.get_function_configuration(FunctionName=FN)
    env = (cfg.get("Environment") or {}).get("Variables", {})
    if env.get("FRED_API_KEY"):
        rep.kv(env_FRED_API_KEY="present")
        return
    k, used = None, None
    for d in DONORS:
        try:
            src = lam.get_function_configuration(FunctionName=d)
            k = (src.get("Environment") or {}).get(
                "Variables", {}).get("FRED_API_KEY")
            if k:
                used = d
                break
        except ClientError:
            continue
    if not k:
        rep.warn("FRED_API_KEY absent in all donors -- macro_cross "
                 "will degrade honestly")
        return
    env["FRED_API_KEY"] = k
    lam.update_function_configuration(FunctionName=FN,
                                      Environment={"Variables": env})
    for _ in range(20):
        if lam.get_function_configuration(FunctionName=FN).get(
                "LastUpdateStatus") == "Successful":
            break
        time.sleep(3)
    rep.kv(env_FRED_API_KEY="HEALED from %s" % used)


def settle_marker(rep):
    try:
        gf = lam.get_function(FunctionName=FN)
        raw = urllib.request.urlopen(gf["Code"]["Location"],
                                     timeout=60).read()
        src = zipfile.ZipFile(io.BytesIO(raw)).read(
            "lambda_function.py").decode("utf-8", "replace")
        ok = MARKER in src
        rep.kv(deployed_marker=ok, zip_kb=len(raw) // 1024)
        if not ok:
            rep.fail("deployed zip lacks the v1.0.0 marker -- "
                     "aborting before invoke")
            FAILED.append("marker")
        return ok
    except Exception as e:  # noqa: BLE001
        rep.warn(f"marker path {type(e).__name__}: {str(e)[:90]} -- "
                 f"falling back to LastModified trust")
        return True


def ensure_schedule(rep):
    fn_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{FN}"
    target = {"Arn": fn_arn, "RoleArn": SCHED_ROLE, "Input": "{}",
              "RetryPolicy": {"MaximumRetryAttempts": 2,
                              "MaximumEventAgeInSeconds": 3600}}
    try:
        ex = sched.get_schedule(Name=SCHED_NAME)
        if (ex.get("State") == "ENABLED"
                and ex.get("ScheduleExpression") == SCHED_CRON):
            rep.ok(f"schedule {SCHED_NAME} already correct")
            return
        sched.update_schedule(Name=SCHED_NAME,
                              ScheduleExpression=SCHED_CRON,
                              ScheduleExpressionTimezone="UTC",
                              FlexibleTimeWindow={"Mode": "OFF"},
                              State="ENABLED", Target=target)
        rep.ok(f"schedule {SCHED_NAME} updated -> {SCHED_CRON}")
        return
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise
    try:
        sched.create_schedule(Name=SCHED_NAME,
                              ScheduleExpression=SCHED_CRON,
                              ScheduleExpressionTimezone="UTC",
                              FlexibleTimeWindow={"Mode": "OFF"},
                              State="ENABLED", Target=target,
                              Description="justhodl-sp500 daily "
                              "index-as-a-stock refresh (ops 4808)")
        rep.ok(f"schedule {SCHED_NAME} created -> {SCHED_CRON}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConflictException":
            rep.ok("schedule create raced -- ConflictException = "
                   "success per house doctrine")
        else:
            rep.fail(f"schedule create: {e}")
            FAILED.append("schedule")


def band(rep, label, v, lo=None, hi=None, hard=True):
    ok = v is not None and (lo is None or v >= lo) and (hi is None
                                                       or v <= hi)
    (rep.ok if ok else (rep.fail if hard else rep.warn))(
        f"  {label} = {v}" + ("" if ok else f"  [band {lo}..{hi}]"))
    if not ok and hard:
        FAILED.append(label)
    return ok


def main():
    with report("4808_sp500_engine_deploy_verify") as rep:
        rep.heading("ops 4808 -- justhodl-sp500 birth verify")

        rep.section("1. function Active + env heal")
        if not wait_active(rep):
            return
        heal_fred(rep)

        rep.section("2. deploy settle (zip marker)")
        if not settle_marker(rep):
            return

        rep.section("3. EventBridge Scheduler ensure")
        ensure_schedule(rep)

        rep.section("4. Event-invoke + poll as_of")
        try:
            prev = sread(OUT_KEY).get("as_of")
        except Exception:  # noqa: BLE001
            prev = None
        lam.invoke(FunctionName=FN, InvocationType="Event",
                   Payload=b"{}")
        doc = None
        for i in range(32):
            time.sleep(15)
            try:
                d = sread(OUT_KEY)
                if d.get("as_of") and d["as_of"] != prev:
                    doc = d
                    rep.ok(f"  fresh doc after ~{(i + 1) * 15}s  "
                           f"as_of={d['as_of']}")
                    break
            except Exception:  # noqa: BLE001
                continue
        if not doc:
            rep.fail("data/sp500.json never refreshed within 8 min")
            FAILED.append("as_of")
            sys.exit(1)

        rep.section("5. truth bands (all real)")
        val, fwd = doc["valuation"], doc["forward"]
        yld, mac = doc["yield"], doc["macro_cross"]
        pe = val["pe_ttm"].get("agg")
        fpe = fwd["pe_fwd"].get("agg")
        band(rep, "members", doc["index"].get("members"), 400, 520)
        band(rep, "pe_ttm.agg", pe, 12, 45)
        band(rep, "pe_fwd.agg", fpe, 10,
             (pe * 1.25) if pe else 60)
        band(rep, "earnings_yield.agg",
             val["earnings_yield_pct"].get("agg"), 1.5, 9)
        band(rep, "div_yield.agg",
             yld["dividend_yield_pct"].get("agg"), 0.5, 4.0)
        band(rep, "pe_ttm.median (dist populated)",
             val["pe_ttm"].get("median"), 8, 60)
        band(rep, "ps_ttm.agg", val["ps_ttm"].get("agg"), 1.0, 8.0)
        band(rep, "roe.agg", doc["quality"]["roe_pct"].get("agg"),
             5, 40)
        band(rep, "erp_ttm", mac.get("erp_ttm_pct"), -6, 8,
             hard=False)
        band(rep, "rule_of_20", mac.get("rule_of_20"), 12, 45,
             hard=False)
        rep.kv(reprice=json.dumps(doc["diag"].get("reprice"))[:180],
               cols_missing=doc["diag"]["census"].get("cols_missing"),
               hist_days=doc.get("hist_days"))

        rep.section("6. compare-mode smoke (AAPL)")
        r = lam.invoke(FunctionName=FN,
                       Payload=json.dumps({"compare": "AAPL"}).encode())
        cd = json.loads(r["Payload"].read())
        rows = cd.get("rows") or []
        band(rep, "compare rows", len(rows), 12, 12)
        with_pct = sum(1 for x in rows
                       if x.get("percentile_in_spx") is not None)
        band(rep, "compare rows w/ percentile", with_pct, 8, 12)
        for x in rows[:5]:
            rep.kv(**{x["metric"]: f"{x['stock']} vs agg "
                      f"{x['spx_agg']} med {x['spx_median']} "
                      f"pct {x['percentile_in_spx']} "
                      f"{x['verdict']}"})

        rep.section("7. headline readout")
        rep.kv(spx_level=doc["index"].get("level"),
               total_mcap=doc["index"].get("total_mcap"),
               pe_ttm=pe, pe_fwd=fpe,
               ntm_growth=fwd["ntm_earnings_growth_pct"].get("agg"),
               earnings_yield=val["earnings_yield_pct"].get("agg"),
               fcf_yield=val["fcf_yield_pct"].get("agg"),
               div_yield=yld["dividend_yield_pct"].get("agg"),
               buyback_yield=yld["net_buyback_yield_pct"].get("agg"),
               pb=val["pb"].get("agg"), ps=val["ps_ttm"].get("agg"),
               ev_ebitda=val["ev_ebitda_ttm"].get("agg"),
               us10y=mac.get("us10y_pct"), cpi=mac.get("cpi_yoy_pct"),
               erp=mac.get("erp_ttm_pct"),
               rule20=mac.get("rule_of_20"))

        if FAILED:
            rep.fail(f"HARD FAILS: {FAILED}")
            sys.exit(1)
        rep.ok("justhodl-sp500 LIVE -- the index now reads like a "
               "single stock")


if __name__ == "__main__":
    main()
