"""ops 3221 — the anomaly instrumented, not theorized. The runner gains a
per-item guard on pull (the naked ex.map crash site, 3200 doctrine) and a
WL_TRACE env tracer that follows named symbols end-to-end:
need → cache_pre → todo → weekly-obs → zc. Deploy forces a COLD
container (the staleness suspect), traces the five 3219 curations through
one run, pulls the trace lines from CloudWatch into this report, and
re-reads the two engines' rows. Whatever the mechanism is, it gets named
in one run."""
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3

from ops_report import report

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "shared"))
from _lambda_deploy_helpers import deploy_lambda  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)
LOGS = boto3.client("logs", region_name=REGION)
AWS_DIR = Path(__file__).resolve().parents[2]
FN = "justhodl-wl-engines"
TRACE = ("TVC:DE10Y-TVC:IT10Y,TVC:FR10Y-TVC:IT10Y,TVC:ES10Y-TVC:IT10Y,"
         "ECONOMICS:EUDIR,ECONOMICS:GBDIR")


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


with report("3221_trace_run") as rep:
    fails, warns = [], []
    rep.heading("ops 3221 — the five curations traced end-to-end")

    rep.section("1. Deploy instrumented runner (cold) + traced run")
    cfg = json.loads((AWS_DIR / "lambdas" / FN / "config.json").read_text())
    sch = cfg.get("schedule")
    rule, cron = (sch.get("rule_name"), sch.get("cron")) \
        if isinstance(sch, dict) else (None, None)
    live = (LAM.get_function_configuration(FunctionName=FN)
            .get("Environment") or {}).get("Variables") or {}
    live["WL_TRACE"] = TRACE
    try:
        deploy_lambda(report=rep, function_name=FN,
                      source_dir=AWS_DIR / "lambdas" / FN / "source",
                      env_vars=live, eb_rule_name=rule, eb_schedule=cron,
                      timeout=cfg.get("timeout", 900),
                      memory=cfg.get("memory", 3008),
                      description=str(cfg.get("description", ""))[:250],
                      smoke=False)
    except Exception as e:
        fails.append(f"deploy: {str(e)[:90]}")
    mark = datetime.now(timezone.utc).isoformat()
    try:
        LAM.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
    except Exception as e:
        fails.append(f"invoke: {str(e)[:70]}")
    idx2 = None
    for _ in range(60):
        time.sleep(10)
        d = s3_json("data/wl-engines.json") or {}
        if str(d.get("generated_at", "")) > mark:
            idx2 = d
            break
    if not idx2:
        warns.append("index not fresh in window — trace may still be in "
                     "the logs")

    rep.section("2. The trace, verbatim")
    time.sleep(8)
    shown = 0
    try:
        grp = f"/aws/lambda/{FN}"
        for st in LOGS.describe_log_streams(
                logGroupName=grp, orderBy="LastEventTime",
                descending=True, limit=2).get("logStreams") or []:
            for e in LOGS.get_log_events(
                    logGroupName=grp, logStreamName=st["logStreamName"],
                    limit=300, startFromHead=False).get("events") or []:
                m = (e.get("message") or "").strip()
                if "[trace]" in m or "pull FAIL" in m:
                    rep.log("  " + m[:160])
                    shown += 1
            if shown:
                break
    except Exception as e:
        warns.append(f"logs: {str(e)[:70]}")
    if not shown:
        fails.append("no trace lines found — tracer did not run")

    rep.section("3. The two engines now")
    if idx2:
        for nm in ("Europe Liquidity", "Global Deposit Rates"):
            e = next((x for x in (idx2.get("engines") or [])
                      if nm.lower() in str(x.get("name", "")).lower()),
                     None)
            if e:
                rep.log(f"  {str(e.get('name'))[:40]:<40} "
                        f"state={e.get('state')} "
                        f"resolved={e.get('members_resolved')} "
                        f"reason={str(e.get('reason') or 'ACTIVE')[:60]}")
        act = sum(1 for x in (idx2.get("engines") or [])
                  if str(x.get("state")) == "ACTIVE")
        rep.kv(active_now=act)

    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
