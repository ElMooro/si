"""ops 3153 (iter 2) — land GLM timeout fix in a policy test-window.

Cold container + mode=normal + wall 1.1s means the FIRST GLM call dies
in milliseconds (then breaker + credit-dead Haiku cascade). Every CW
pull so far ran BEFORE the fresh invoke and windowed onto stale
circuit-open lines. This op: flip mode for the window → cold invoke →
wait for doc → sleep for CW ingest → pull the run's OWN
'[llm_router] GLM failed (<exc>)' FIRST line — that string is the root
cause (SSM AccessDenied? DNS? key format?). Fix in-op where the class
is IAM/SSM; restore on_demand policy at close.
"""

import json
import sys
import time
from datetime import datetime, timezone

import boto3

from ops_report import report
from _lambda_deploy_helpers import _retry_on_conflict

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-premortem-engine"

S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)
LOGS = boto3.client("logs", region_name=REGION)
SSM = boto3.client("ssm", region_name=REGION)
IAM = boto3.client("iam", region_name=REGION)


def s3_json(key):
    o = S3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(o["Body"].read().decode("utf-8"))


with report("3153_first_exception") as rep:
    fails, warns = [], []
    rep.heading("ops 3153 — first GLM exception, verbatim")

    rep.section("0. Redeploy premortem w/ patched router (GLM timeout 130s)")
    from pathlib import Path
    from _lambda_deploy_helpers import deploy_lambda
    AWS_DIR = Path(__file__).resolve().parents[1]
    live = LAM.get_function_configuration(FunctionName=FN)
    cfgf = json.loads((AWS_DIR / "lambdas" / FN / "config.json").read_text())
    deploy_lambda(report=rep, function_name=FN,
                  source_dir=AWS_DIR / "lambdas" / FN / "source",
                  env_vars=(live.get("Environment") or {}).get("Variables") or {},
                  eb_rule_name=(cfgf.get("schedule") or {}).get("rule_name"),
                  eb_schedule=(cfgf.get("schedule") or {}).get("cron"),
                  timeout=cfgf.get("timeout", 600),
                  memory=cfgf.get("memory", 1024),
                  description=(cfgf.get("description") or "")[:250],
                  smoke=False)

    SSM.put_parameter(Name="/justhodl/llm/mode", Value="normal",
                      Type="String", Overwrite=True)
    cfg = LAM.get_function_configuration(FunctionName=FN)
    env = (cfg.get("Environment") or {}).get("Variables") or {}
    env["LLM_CFG_NONCE"] = str(int(time.time()))
    _retry_on_conflict(LAM.update_function_configuration,
                       FunctionName=FN, Environment={"Variables": env})
    LAM.get_waiter("function_updated").wait(
        FunctionName=FN, WaiterConfig={"Delay": 3, "MaxAttempts": 40})
    time.sleep(4)
    t0 = datetime.now(timezone.utc)
    t0_ms = int(time.time() * 1000) - 1000
    LAM.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
    rep.log("cold invoke fired")

    doc = None
    deadline = time.time() + 700
    while time.time() < deadline:
        try:
            d = s3_json("data/kill-theses.json")
            ts = d.get("generated_at") or d.get("as_of")
            if ts and datetime.fromisoformat(ts) >= t0:
                doc = d
                break
        except Exception:
            pass
        time.sleep(10)
    rich = [t for t in ((doc or {}).get("theses") or [])
            if isinstance(t, dict) and t.get("kill_conditions")]
    rep.kv(doc_fresh=bool(doc), rich=len(rich))

    rep.section("CW of THIS run")
    time.sleep(25)
    first_exc = None
    try:
        ev = LOGS.filter_log_events(
            logGroupName=f"/aws/lambda/{FN}", startTime=t0_ms, limit=60)
        for e in ev.get("events") or []:
            m = e["message"].strip()
            if "GLM failed" in m and "circuit open" not in m:
                first_exc = m
                break
        shown = 0
        for e in ev.get("events") or []:
            m = e["message"].strip()
            if any(k in m for k in ("llm_router", "Traceback", "Error",
                                     "premortem")):
                rep.log(f"CW: {m[:240]}")
                shown += 1
                if shown >= 8:
                    break
    except Exception as e:
        warns.append(f"CW: {str(e)[:90]}")
    if first_exc:
        rep.ok(f"ROOT: {first_exc[:260]}")
    else:
        warns.append("first GLM-failed line not captured — see raw lines")

    rep.section("Class-specific fix")
    fixed = False
    if first_exc and ("AccessDenied" in first_exc
                      or "not authorized" in first_exc):
        role = cfg["Role"].split("/")[-1]
        IAM.put_role_policy(
            RoleName=role, PolicyName="justhodl-ssm-llm-read",
            PolicyDocument=json.dumps({
                "Version": "2012-10-17",
                "Statement": [{"Effect": "Allow",
                               "Action": ["ssm:GetParameter"],
                               "Resource": "arn:aws:ssm:us-east-1:*:"
                                           "parameter/justhodl/*"}]}))
        rep.ok(f"role {role}: ssm:GetParameter /justhodl/* attached")
        fixed = True
    if fixed:
        time.sleep(8)
        env["LLM_CFG_NONCE"] = str(int(time.time()))
        _retry_on_conflict(LAM.update_function_configuration,
                           FunctionName=FN,
                           Environment={"Variables": env})
        LAM.get_waiter("function_updated").wait(
            FunctionName=FN, WaiterConfig={"Delay": 3, "MaxAttempts": 40})
        t1 = datetime.now(timezone.utc)
        LAM.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
        doc2 = None
        deadline = time.time() + 660
        while time.time() < deadline:
            try:
                d = s3_json("data/kill-theses.json")
                ts = d.get("generated_at") or d.get("as_of")
                if ts and datetime.fromisoformat(ts) >= t1:
                    doc2 = d
                    break
            except Exception:
                pass
            time.sleep(15)
        rich2 = [t for t in ((doc2 or {}).get("theses") or [])
                 if isinstance(t, dict) and t.get("kill_conditions")]
        rep.kv(rich_after_fix=len(rich2))
        if len(rich2) >= 5:
            rep.ok(f"KILL PIPELINE LIVE: {len(rich2)} rich theses")
            for t in rich2[:4]:
                kc = (t.get("kill_conditions") or [{}])[0]
                rep.log(f"  · {t.get('symbol')}: "
                        f"{str(kc.get('risk') or kc.get('condition') or kc)[:140]}")
        else:
            fails.append("post-fix still thin — CW above")
    elif len(rich) >= 5:
        rep.ok(f"KILL PIPELINE LIVE: {len(rich)} rich theses (GLM lane)")
        for t in [x for x in ((doc or {}).get("theses") or [])
                  if x.get("kill_conditions")][:4]:
            kc = (t.get("kill_conditions") or [{}])[0]
            rep.log(f"  · {t.get('symbol')}: "
                    f"{str(kc.get('risk') or kc.get('condition') or kc)[:140]}")
    else:
        fails.append(f"{len(rich)} rich post-timeout-fix — CW above names it")

    SSM.put_parameter(Name="/justhodl/llm/mode", Value="on_demand",
                      Type="String", Overwrite=True)
    rep.log("FinOps policy restored (on_demand). Owner switch to re-enable "
            "background LLM fleet-wide: /justhodl/llm/mode = normal")

    for w in warns:
        rep.warn(w)
    for f in fails:
        rep.fail(f)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        sys.exit(1)
    sys.exit(0)
