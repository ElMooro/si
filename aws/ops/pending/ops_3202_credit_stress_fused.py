"""ops 3202 — the sixteenth engine: credit-stress fused and verified.

3201 deployed 15/16; credit-stress failed because its config.json carries
`"schedule": "cron(...)"` as a plain STRING (older config style) and the
deploy loop assumed a dict. Fix: type-safe schedule parsing — a string
schedule keeps its EXISTING EventBridge rule untouched (deploy code only,
never churn a live schedule on a format guess). Then invoke and prove
wl_research lands in data/credit-stress.json.
"""
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
AWS_DIR = Path(__file__).resolve().parents[2]
S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)
FN = "justhodl-credit-stress"
OUT = "data/credit-stress.json"


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


with report("3202_credit_stress_fused") as rep:
    fails, warns = [], []
    rep.heading("ops 3202 — credit-stress: schedule-safe deploy + fusion "
                "proof")

    cfg = json.loads((AWS_DIR / "lambdas" / FN / "config.json").read_text())
    sch = cfg.get("schedule")
    rule, cron = (sch.get("rule_name"), sch.get("cron")) \
        if isinstance(sch, dict) else (None, None)
    if isinstance(sch, str):
        rep.log(f"  string schedule ({sch}) — existing EB rule left "
                "untouched, code-only deploy")
    live = (LAM.get_function_configuration(FunctionName=FN)
            .get("Environment") or {}).get("Variables") or {}
    try:
        deploy_lambda(report=rep, function_name=FN,
                      source_dir=AWS_DIR / "lambdas" / FN / "source",
                      env_vars=live, eb_rule_name=rule, eb_schedule=cron,
                      timeout=cfg.get("timeout", 120),
                      memory=cfg.get("memory", 512),
                      description=str(cfg.get("description", ""))[:250],
                      smoke=False)
    except Exception as e:
        fails.append(f"deploy: {str(e)[:100]}")

    mark = datetime.now(timezone.utc).isoformat()
    try:
        LAM.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
    except Exception as e:
        fails.append(f"invoke: {str(e)[:80]}")
    ok = False
    for _ in range(20):
        time.sleep(6)
        d = s3_json(OUT) or {}
        if str(d.get("generated_at", "")) > mark and "wl_research" in d:
            wr = d.get("wl_research") or {}
            ctx = (wr or {}).get("context") or {}
            cred = ctx.get("CREDIT") or {}
            rep.kv(generated_at=str(d.get("generated_at"))[:19],
                   wl_research="present",
                   credit_pressure=cred.get("pressure_pctile"),
                   credit_firing=f"{cred.get('firing')}/{cred.get('of')}"
                   if cred else None)
            rep.ok("all SIXTEEN target engines now fused — wave 1 complete")
            ok = True
            break
    if not ok:
        fails.append("credit-stress feed did not show wl_research in the "
                     "window")

    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
