"""ops 3294 — CLONE-ALPHA flagship (13F roadmap #1): per-manager
followable skill from filing HISTORY, so boards weight skilled money
over famous money. NEW justhodl-13f-clone-alpha: last ~13 quarterly
13F-HRs per fund via data.sec.gov submissions; top-15 equity longs
value-weighted (12% cap, options excluded, value-units auto-detect);
clone window = first close AFTER filed date -> next filing's entry
(honest disclosure lag); alpha vs SPY per closed window; >=6 windows ->
annualized alpha, hit rate, IR, SKILL 0-100 with labels WORTH CLONING /
SELECTIVE EDGE / MARKET-LIKE / FAMOUS != SKILLED. Budget-converging
backfill self-chains async (<=10 hops); weekly Scheduler keeps warm.
Board live on 13f.html (renders CONVERGING honestly). Truth bands:
output fresh, managers=18, pct_complete rising across polls, and once
any fund has >=6 windows its skill fields exist and are sane."""
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
FN = "justhodl-13f-clone-alpha"
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
        "User-Agent": "Mozilla/5.0 (jh-ops-3294)"})
    return urllib.request.urlopen(req, timeout=30).read().decode(
        "utf-8", "ignore")


with report("3294_clone_alpha") as rep:
    fails, warns = [], []
    donor = LAM.get_function_configuration(
        FunctionName="justhodl-13f-positions")
    env = (donor.get("Environment") or {}).get("Variables") or {}
    rep.kv(donor_env_keys=len(env))

    deploy_lambda(report=rep, function_name=FN,
                  source_dir=AWS_DIR / "lambdas" / FN / "source",
                  env_vars=env, eb_rule_name=None, eb_schedule=None,
                  timeout=880, memory=2048,
                  description="Clone-Alpha: per-manager followable "
                  "skill from historical 13F filings (ops 3294)",
                  smoke=False)

    rep.section("2. Weekly Scheduler")
    SNAME = "justhodl-13f-clone-alpha-weekly"
    arn = "arn:aws:lambda:%s:%s:function:%s" % (REGION, ACCT, FN)
    role = "arn:aws:iam::%s:role/justhodl-scheduler-role" % ACCT
    try:
        SCH.get_schedule(Name=SNAME)
        rep.log("  schedule exists")
    except SCH.exceptions.ResourceNotFoundException:
        SCH.create_schedule(
            Name=SNAME, ScheduleExpression="cron(30 8 ? * MON *)",
            FlexibleTimeWindow={"Mode": "OFF"},
            Target={"Arn": arn, "RoleArn": role}, State="ENABLED",
            Description="Clone-Alpha weekly refresh (ops 3294)")
        rep.log("  created cron(30 8 ? * MON *) UTC")

    rep.section("3. Kick backfill + watch convergence")
    mark = datetime.now(timezone.utc).isoformat()
    LAM.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
    d, last_pct, rises = None, -1.0, 0
    for i in range(70):                 # ~21 min of self-chaining
        time.sleep(18)
        d = s3_json("data/13f-clone-alpha.json")
        if not d or d.get("as_of", "") < mark:
            continue
        pct = float(d.get("pct_complete") or 0)
        if pct > last_pct:
            rises += 1
            rep.log("  pct %.1f -> %.1f (hop %s, filings %s/%s)"
                    % (max(0, last_pct), pct, d.get("hop"),
                       d.get("filings_done"), d.get("filings_total")))
            last_pct = pct
        if d.get("status") == "COMPLETE":
            break
    if not d or d.get("as_of", "") < mark:
        fails.append("output never freshened")
    else:
        rep.kv(status=d.get("status"), pct=d.get("pct_complete"),
               filings="%s/%s" % (d.get("filings_done"),
                                  d.get("filings_total")),
               prices_pending=d.get("prices_pending"),
               warns=(d.get("warns") or [])[:4])
        M = d.get("managers") or {}
        if len(M) != 18:
            fails.append("managers != 18: %d" % len(M))
        if rises < 1:
            fails.append("pct never advanced")
        scored = {k: m for k, m in M.items()
                  if m.get("skill_score") is not None}
        rep.kv(scored_n=len(scored))
        for k, m in list(scored.items())[:6]:
            rep.kv(**{k.lower(): "%s score=%s a=%s hit=%s n=%s"
                   % (m.get("label"), m.get("skill_score"),
                      m.get("ann_alpha_pct"), m.get("hit_rate"),
                      m.get("n_windows"))})
        for k, m in scored.items():
            if not (0 <= m["skill_score"] <= 100):
                fails.append("%s score out of range" % k)
            if not (-80 <= (m.get("ann_alpha_pct") or 0) <= 120):
                fails.append("%s alpha implausible: %s"
                             % (k, m.get("ann_alpha_pct")))
            if m.get("n_windows", 0) < 6:
                fails.append("%s scored with <6 windows" % k)
        if d.get("status") == "COMPLETE" and len(scored) < 8:
            fails.append("complete but only %d scored" % len(scored))
        if d.get("status") == "CONVERGING":
            warns.append("still converging at %s%% — weekly schedule "
                         "+ self-chain finish it" % d.get(
                             "pct_complete"))

    rep.section("4. 13f.html board live")
    ok = False
    for i in range(20):
        try:
            pg = get("https://justhodl.ai/13f.html?cb=%d"
                     % time.time())
            if "jh-clone-alpha" in pg and "CLONE-ALPHA" in pg:
                ok = True
                break
        except Exception:
            pass
        time.sleep(18)
    if not ok:
        fails.append("13f.html clone board not live")
    else:
        rep.log("  board markers live")

    rep.kv(warns=warns, fails=fails)
    if fails:
        raise SystemExit("FAILS: %s" % "; ".join(fails))
    rep.log("OPS 3294 PASS — skill now measurable; famous money no "
            "longer rides for free.")
sys.exit(0)
