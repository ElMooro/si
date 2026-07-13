"""ops 3226 — starvation by ordering, fixed: the CoinGecko politeness gate
serializes all workers through a crypto-heavy todo prefix, exhausting the
budget before FRED items run. todo now fetches FRED/MARKET first (cheap,
engine-critical), budget 600s. Deploy, run, pull the cache-trajectory +
trace lines as evidence, count wakes."""
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


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


with report("3226_converge") as rep:
    fails, warns = [], []
    rep.heading("ops 3226 — FRED-first ordering; converge and count")

    cfg = json.loads((AWS_DIR / "lambdas" / FN / "config.json").read_text())
    sch = cfg.get("schedule")
    rule, cron = (sch.get("rule_name"), sch.get("cron")) \
        if isinstance(sch, dict) else (None, None)
    live = (LAM.get_function_configuration(FunctionName=FN)
            .get("Environment") or {}).get("Variables") or {}
    try:
        deploy_lambda(report=rep, function_name=FN,
                      source_dir=AWS_DIR / "lambdas" / FN / "source",
                      env_vars=live, eb_rule_name=rule, eb_schedule=cron,
                      timeout=cfg.get("timeout", 900),
                      memory=cfg.get("memory", 3008),
                      description=str(cfg.get("description", ""))[:250],
                      smoke=False)
        LAM.get_waiter("function_updated_v2").wait(
            FunctionName=FN, WaiterConfig={"Delay": 2, "MaxAttempts": 30})
    except Exception as e:
        fails.append(f"deploy: {str(e)[:80]}")

    idx = s3_json("data/wl-engines.json") or {}
    prev_active = {e["engine_id"] for e in (idx.get("engines") or [])
                   if str(e.get("state")) == "ACTIVE"}
    mark = datetime.now(timezone.utc).isoformat()
    if not fails:
        LAM.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
    idx2 = None
    for _ in range(80):
        time.sleep(10)
        d = s3_json("data/wl-engines.json") or {}
        if str(d.get("generated_at", "")) > mark:
            idx2 = d
            break

    rep.section("Evidence: cache trajectory + trace")
    time.sleep(30)
    shown = 0
    grp = f"/aws/lambda/{FN}"
    try:
        for st in LOGS.describe_log_streams(
                logGroupName=grp, orderBy="LastEventTime",
                descending=True, limit=3).get("logStreams") or []:
            for e in LOGS.get_log_events(
                    logGroupName=grp, logStreamName=st["logStreamName"],
                    limit=400, startFromHead=False).get("events") or []:
                m = (e.get("message") or "").strip()
                if ("[wl] cache=" in m or "[trace]" in m
                        and "weekly=" in m):
                    rep.log("  " + m[:150])
                    shown += 1
            if shown >= 10:
                break
    except Exception as e:
        warns.append(f"logs: {str(e)[:60]}")

    rep.section("Wakes")
    if idx2:
        eng2 = idx2.get("engines") or []
        act2 = {e["engine_id"] for e in eng2
                if str(e.get("state")) == "ACTIVE"}
        woken = sorted(act2 - prev_active)
        rep.kv(active_before=len(prev_active), active_now=len(act2),
               woken=len(woken), series_cached=idx2.get("series_cached"))
        for w in woken[:12]:
            nm = next((e.get("name") for e in eng2
                       if e.get("engine_id") == w), w)
            rep.log(f"  ⏰ WOKE: {nm}")
        if woken:
            rep.ok(f"{len(woken)} panels WOKEN")
        else:
            warns.append("no wakes this run — trajectory above says "
                         "whether the fill is converging")
    else:
        warns.append("index not fresh in window")

    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
