"""ops 3286 — US10Y 5% RED-FLAG SENTINEL (Khalid: "10Y anywhere near 5%
should be a major red flag for risk and stocks" — the fleet had curve
shape and regime-change consensus but no absolute-level watchdog).
Deploys justhodl-us10y-sentinel (danger ladder BENIGN→CRITICAL,
velocity bump, DFII10 real-yield note, data-driven SPX episode study on
first 4.50/4.75/5.00 crosses since 1962, 60d stock-vs-yield corr
regime, tier-cross Telegram), Scheduler 5x/day, sentinel strip live on
yield-curve.html. Truth bands: level 3.0–6.5, ≥2 episode buckets with
n≥1, history_260d ≥ 200 points."""
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3

from ops_report import report

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "shared"))
from _lambda_deploy_helpers import deploy_lambda  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
ACCT = "857687956942"
FN = "justhodl-us10y-sentinel"
S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)
SCH = boto3.client("scheduler", region_name=REGION)
AWS_DIR = Path(__file__).resolve().parents[2]


def s3_json(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return None


def get(url):
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (jh-ops-3286)"})
    return urllib.request.urlopen(req, timeout=30).read().decode(
        "utf-8", "ignore")


with report("3286_us10y_sentinel") as rep:
    fails = []
    env = (LAM.get_function_configuration(
        FunctionName="justhodl-confluence-meta")
        .get("Environment") or {}).get("Variables") or {}
    rep.kv(inherited_env_keys=len(env))

    deploy_lambda(report=rep, function_name=FN,
                  source_dir=AWS_DIR / "lambdas" / FN / "source",
                  env_vars=env, eb_rule_name=None, eb_schedule=None,
                  timeout=300, memory=512,
                  description="US10Y 5% red-flag sentinel: danger "
                  "ladder + SPX episode study -> "
                  "data/us10y-sentinel.json (ops 3286)", smoke=False)

    rep.section("2. EventBridge Scheduler (classic cap saturated)")
    SNAME = "justhodl-us10y-sentinel-5x"
    arn = "arn:aws:lambda:%s:%s:function:%s" % (REGION, ACCT, FN)
    role = "arn:aws:iam::%s:role/justhodl-scheduler-role" % ACCT
    try:
        SCH.get_schedule(Name=SNAME)
        rep.log("  schedule exists")
    except SCH.exceptions.ResourceNotFoundException:
        SCH.create_schedule(
            Name=SNAME,
            ScheduleExpression="cron(20 0,6,12,16,20 * * ? *)",
            FlexibleTimeWindow={"Mode": "OFF"},
            Target={"Arn": arn, "RoleArn": role},
            State="ENABLED",
            Description="US10Y sentinel 5x/day (ops 3286)")
        rep.log("  created cron(20 0,6,12,16,20 * * ? *) UTC")

    rep.section("3. First run + truth bands")
    mark = datetime.now(timezone.utc).isoformat()
    LAM.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
    d = None
    for i in range(40):
        time.sleep(12)
        d = s3_json("data/us10y-sentinel.json")
        if d and d.get("as_of", "") >= mark:
            break
    if not d or d.get("as_of", "") < mark:
        fails.append("sentinel output never freshened")
    else:
        lvl = d.get("level")
        rep.kv(level=lvl, tier=d.get("tier"),
               dist_bps=d.get("distance_to_5pct_bps"),
               d60=(d.get("velocity") or {}).get("d60_bps"),
               real10=d.get("real_10y"),
               corr60=d.get("corr60_spx_vs_dy"))
        if not (lvl and 3.0 <= lvl <= 6.5):
            fails.append("level outside truth band: %s" % lvl)
        eps = d.get("episode_study") or {}
        n_ok = sum(1 for v in eps.values() if (v or {}).get("n", 0) >= 1)
        rep.kv(episode_buckets_with_n=n_ok,
               eps={k: (v or {}).get("n") for k, v in eps.items()})
        if n_ok < 2:
            fails.append("episode study too thin: %s" % n_ok)
        if len(d.get("history_260d") or []) < 200:
            fails.append("history_260d too short")
        if d.get("tier") not in ("BENIGN", "WATCH", "ELEVATED",
                                 "HIGH", "RED", "CRITICAL"):
            fails.append("bad tier %s" % d.get("tier"))

    rep.section("4. yield-curve.html sentinel strip live")
    ok = False
    for i in range(20):
        try:
            pg = get("https://justhodl.ai/yield-curve.html?cb=%d"
                     % time.time())
            if "us10y-sentinel" in pg and "5% Red-Flag Sentinel" in pg:
                ok = True
                break
        except Exception:
            pass
        time.sleep(18)
    if not ok:
        fails.append("yield-curve.html strip not live")
    else:
        rep.log("  strip markers live")

    rep.kv(fails=fails)
    if fails:
        raise SystemExit("FAILS: %s" % "; ".join(fails))
    rep.log("OPS 3286 PASS — the 10Y watchdog is armed.")
sys.exit(0)
