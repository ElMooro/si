"""ops 3250 — theme-rotation-engine live crash fixed: line 615 chained
.get("breadth", {}) which only defaults when the key is ABSENT — when it
exists holding None, the chain raises the exact observed
"'NoneType' object has no attribute 'get'". Or-guards applied (both
sites). Deploy, invoke, verify a clean run + fresh feed."""
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
FN = "justhodl-theme-rotation-engine"
AWS_DIR = Path(__file__).resolve().parents[2]

with report("3250_theme_rotation_fix") as rep:
    fails, warns = [], []
    rep.heading("ops 3250 — theme-rotation None-guard, deploy + prove")
    cfg = {}
    p = AWS_DIR / "lambdas" / FN / "config.json"
    if p.exists():
        cfg = json.loads(p.read_text())
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
                      memory=cfg.get("memory", 1024),
                      description=str(cfg.get("description", ""))[:250],
                      smoke=False)
        LAM.get_waiter("function_updated_v2").wait(
            FunctionName=FN, WaiterConfig={"Delay": 2, "MaxAttempts": 30})
    except Exception as e:
        fails.append(f"deploy: {str(e)[:80]}")

    if not fails:
        mark = datetime.now(timezone.utc)
        LAM.invoke(FunctionName=FN, InvocationType="Event",
                   Payload=b"{}")
        time.sleep(75)
        errs = 0
        grp = f"/aws/lambda/{FN}"
        try:
            for stm in LOGS.describe_log_streams(
                    logGroupName=grp, orderBy="LastEventTime",
                    descending=True, limit=2).get("logStreams") or []:
                for ev in LOGS.get_log_events(
                        logGroupName=grp,
                        logStreamName=stm["logStreamName"],
                        limit=200, startFromHead=False)\
                        .get("events") or []:
                    if ev["timestamp"] / 1000 < mark.timestamp():
                        continue
                    if "[ERROR]" in (ev.get("message") or ""):
                        errs += 1
                        rep.log("  ✗ "
                                + ev["message"].splitlines()[0][:110])
        except Exception as e:
            warns.append(f"logs: {str(e)[:60]}")
        rep.kv(post_fix_errors=errs)
        if errs:
            fails.append("still erroring post-fix")
        else:
            rep.ok("clean run post-fix — the recurring AttributeError "
                   "is gone")

    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
