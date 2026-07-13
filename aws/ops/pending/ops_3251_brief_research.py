"""ops 3251 — the daily brief carries panel research + survives dead
LLM credits: his_research (top themes by pressure, firing panels by
name, top divergence, n_active) is bundled into the LLM context AND the
persisted payload; on LLM failure the brief now ships a deterministic
data-driven digest instead of 500ing. Deploy, invoke, verify the fresh
payload's his_research + which composer ran."""
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
FN = "justhodl-alpha-daily-brief"
AWS_DIR = Path(__file__).resolve().parents[2]

with report("3251_brief_research") as rep:
    fails, warns = [], []
    rep.heading("ops 3251 — daily brief × panel research + graceful "
                "fallback")
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
        mark = datetime.now(timezone.utc).isoformat()
        LAM.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
        got = None
        for _ in range(40):
            time.sleep(8)
            try:
                d = json.loads(S3.get_object(
                    Bucket=BUCKET, Key="data/alpha-brief.json")
                    ["Body"].read())
            except Exception:
                d = {}
            if str(d.get("generated_at", "")) > mark:
                got = d
                break
        if not got:
            fails.append("brief did not persist a fresh payload "
                         "(fallback path may not have engaged)")
        else:
            hr = got.get("his_research") or {}
            tops = hr.get("top_themes") or []
            rep.kv(composer=got.get("model"),
                   n_active=hr.get("n_active"),
                   top_themes=len(tops),
                   firing=len(hr.get("firing") or []))
            for ln in (hr.get("markdown") or "").splitlines()[:6]:
                rep.log("  " + ln[:110])
            if tops:
                rep.ok("brief persisted WITH panel research — composer: "
                       + str(got.get("model")))
            else:
                fails.append("his_research empty in fresh payload")

    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
