"""ops 3288 — COMEBACK SCREENER (Khalid: comeback companies — the
American-Eagle penny→70c class — were a coverage hole). Deploys
justhodl-comeback-screener: full FinViz-Elite universe scan (~11.3k,
one export), comeback = ≥+75% off 52w low while still ≥50% below the
52w high, positive quarter, ≥$300k/day; tiers EARLY_TURN / CONFIRMED
(>SMA200) / MOONSHOT (≥+300%); DILUTION GUARD reroutes ≥+40%/yr share
printers to a traps board (the BMNR rule). New comeback.html desk +
COMEBACKS section on opportunities.html. Scheduler daily 20:45 UTC.
Truth bands: universe ≥ 3000, candidates ≥ 5, all four boards present,
every board row carries score+read."""
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
FN = "justhodl-comeback-screener"
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
        "User-Agent": "Mozilla/5.0 (jh-ops-3288)"})
    return urllib.request.urlopen(req, timeout=30).read().decode(
        "utf-8", "ignore")


with report("3288_comeback_screener") as rep:
    fails = []
    env = (LAM.get_function_configuration(
        FunctionName="justhodl-confluence-meta")
        .get("Environment") or {}).get("Variables") or {}

    deploy_lambda(report=rep, function_name=FN,
                  source_dir=AWS_DIR / "lambdas" / FN / "source",
                  env_vars=env, eb_rule_name=None, eb_schedule=None,
                  timeout=840, memory=1024,
                  description="Comeback screener: crash->recover scan "
                  "with dilution guard -> data/comeback-screener.json "
                  "(ops 3288)", smoke=False)

    rep.section("2. Scheduler daily 20:45 UTC")
    SNAME = "justhodl-comeback-screener-daily"
    arn = "arn:aws:lambda:%s:%s:function:%s" % (REGION, ACCT, FN)
    role = "arn:aws:iam::%s:role/justhodl-scheduler-role" % ACCT
    try:
        SCH.get_schedule(Name=SNAME)
        rep.log("  schedule exists")
    except SCH.exceptions.ResourceNotFoundException:
        SCH.create_schedule(
            Name=SNAME, ScheduleExpression="cron(45 20 * * ? *)",
            FlexibleTimeWindow={"Mode": "OFF"},
            Target={"Arn": arn, "RoleArn": role}, State="ENABLED",
            Description="Comeback screener daily (ops 3288)")
        rep.log("  created cron(45 20 * * ? *) UTC")

    rep.section("3. First run + truth bands")
    mark = datetime.now(timezone.utc).isoformat()
    LAM.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
    d = None
    for i in range(60):
        time.sleep(14)
        d = s3_json("data/comeback-screener.json")
        if d and d.get("as_of", "") >= mark:
            break
    if not d or d.get("as_of", "") < mark:
        fails.append("screener output never freshened")
    else:
        rep.kv(universe=d.get("universe_n"),
               candidates=d.get("candidates_n"),
               fmp_pulls=d.get("fmp_dilution_pulls"),
               warns=d.get("warns"))
        if (d.get("universe_n") or 0) < 3000:
            fails.append("universe thin: %s" % d.get("universe_n"))
        if (d.get("candidates_n") or 0) < 5:
            fails.append("candidates thin: %s" % d.get("candidates_n"))
        b = d.get("boards") or {}
        for k in ("confirmed", "early_turn", "moonshots",
                  "dilution_traps"):
            if k not in b:
                fails.append("board missing: %s" % k)
        rep.kv(boards={k: len(v or []) for k, v in b.items()})
        for k, rows in b.items():
            for c in (rows or [])[:3]:
                if "comeback_score" not in c or "read" not in c:
                    fails.append("%s row lacks score/read" % k)
                    break
        top = ((b.get("confirmed") or b.get("early_turn")
                or [{}])[0])
        rep.kv(sample=dict(t=top.get("ticker"),
                           off_low=top.get("off_low_pct"),
                           below_high=top.get("below_high_pct"),
                           sh1y=top.get("sh_1y_cagr_pct"),
                           score=top.get("comeback_score")))

    rep.section("4. Pages live: comeback.html + opportunities join")
    ok1 = ok2 = False
    for i in range(20):
        try:
            pg = get("https://justhodl.ai/comeback.html?cb=%d"
                     % time.time())
            ok1 = ("Comeback Desk" in pg and "Dilution Traps" in pg)
            pg2 = get("https://justhodl.ai/opportunities.html?cb=%d"
                      % time.time())
            ok2 = ("jh-comeback" in pg2)
            if ok1 and ok2:
                break
        except Exception:
            pass
        time.sleep(18)
    if not ok1:
        fails.append("comeback.html not live")
    if not ok2:
        fails.append("opportunities.html comeback section not live")
    if ok1 and ok2:
        rep.log("  both pages live")

    rep.kv(fails=fails)
    if fails:
        raise SystemExit("FAILS: %s" % "; ".join(fails))
    rep.log("OPS 3288 PASS — comebacks covered, dilution-guarded.")
sys.exit(0)
