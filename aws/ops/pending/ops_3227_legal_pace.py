"""ops 3227 — the 429 class ended: FRED gate legal (0.55s ≈ 109/min under
the 120/min cap; 0.12s was a self-DDoS returning silent empties) and a
3-day, mapping-keyed misses-tombstone ledger so 1,300 perpetually-dry
symbols stop being retried every run. Two back-to-back runs: run 1 pays
the tombstoning cost, run 2 shows the steady state (todo collapses,
FRED-class members land in the first minute). Wakes by name."""
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


def run_once(tag, rep):
    mark = datetime.now(timezone.utc).isoformat()
    LAM.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
    for _ in range(80):
        time.sleep(10)
        d = s3_json("data/wl-engines.json") or {}
        if str(d.get("generated_at", "")) > mark:
            return d
    rep.warn(f"{tag}: index not fresh in window")
    return None


with report("3227_legal_pace") as rep:
    fails, warns = [], []
    rep.heading("ops 3227 — legal pace + tombstones; two runs to steady "
                "state")

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

    if not fails:
        rep.section("Run 1 — pays the tombstoning cost")
        run_once("run1", rep)
        rep.section("Run 2 — steady state")
        idx2 = run_once("run2", rep)
    else:
        idx2 = None

    rep.section("Evidence")
    time.sleep(30)
    shown = 0
    grp = f"/aws/lambda/{FN}"
    try:
        for st in LOGS.describe_log_streams(
                logGroupName=grp, orderBy="LastEventTime",
                descending=True, limit=4).get("logStreams") or []:
            for e in LOGS.get_log_events(
                    logGroupName=grp, logStreamName=st["logStreamName"],
                    limit=400, startFromHead=False).get("events") or []:
                m = (e.get("message") or "").strip()
                if ("[wl] cache=" in m or "todo=" in m and "[trace]" in m
                        or "[trace]" in m and "weekly=" in m):
                    rep.log("  " + m[:150])
                    shown += 1
            if shown >= 14:
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
        for nm in ("Europe Liquidity", "Global Deposit Rates"):
            e = next((x for x in eng2
                      if nm.lower() in str(x.get("name", "")).lower()),
                     None)
            if e:
                rep.log(f"  → {str(e.get('name'))[:36]:<36} "
                        f"{e.get('state')} "
                        f"resolved={e.get('members_resolved')}")
        if woken:
            rep.ok(f"{len(woken)} panels WOKEN")

    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
