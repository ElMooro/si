#!/usr/bin/env python3
"""530 — Direct-deploy justhodl-cb-stance + schedule + invoke + verify."""
import io, json, os, time as _time, zipfile, base64
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/530_cb_stance_deploy.json"
NAME = "justhodl-cb-stance"
SOURCE = "aws/lambdas/justhodl-cb-stance/source/lambda_function.py"
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"
SCHEDULE = "cron(0 0/6 ? * * *)"

lam = boto3.client("lambda", region_name="us-east-1")
eb = boto3.client("events", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")


def zip_source(path):
    with open(path, "rb") as f: code = f.read()
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", code)
    return buf.getvalue()


def inherit_env():
    env = {}
    for src in ("justhodl-earnings-nlp", "justhodl-ai-chat",
                  "justhodl-morning-intelligence", "justhodl-investor-agents"):
        try:
            cfg = lam.get_function_configuration(FunctionName=src)
            v = (cfg.get("Environment") or {}).get("Variables") or {}
            if v.get("ANTHROPIC_API_KEY") and "ANTHROPIC_API_KEY" not in env:
                env["ANTHROPIC_API_KEY"] = v["ANTHROPIC_API_KEY"]
            if v.get("ANTHROPIC_MODEL") and "ANTHROPIC_MODEL" not in env:
                env["ANTHROPIC_MODEL"] = v["ANTHROPIC_MODEL"]
        except Exception: pass
    for src in ("justhodl-dix", "justhodl-credit-stress", "justhodl-earnings-nlp"):
        try:
            cfg = lam.get_function_configuration(FunctionName=src)
            v = (cfg.get("Environment") or {}).get("Variables") or {}
            for k in ("TELEGRAM_TOKEN", "TELEGRAM_CHAT_ID"):
                if v.get(k) and k not in env: env[k] = v[k]
            break
        except Exception: pass
    return env


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    zb = zip_source(SOURCE)
    env = inherit_env()
    out["env_keys"] = sorted(env.keys())
    out["has_anthropic_key"] = bool(env.get("ANTHROPIC_API_KEY"))

    try:
        try:
            lam.create_function(FunctionName=NAME, Runtime="python3.12",
                Handler="lambda_function.lambda_handler", Role=ROLE,
                MemorySize=1024, Timeout=300, Code={"ZipFile": zb},
                Description="Central Bank Hawkish/Dovish — Fed FOMC NLP",
                Environment={"Variables": env})
            out["lambda"] = "created"
        except lam.exceptions.ResourceConflictException:
            lam.update_function_code(FunctionName=NAME, ZipFile=zb)
            lam.get_waiter("function_updated").wait(FunctionName=NAME)
            lam.update_function_configuration(FunctionName=NAME, MemorySize=1024, Timeout=300,
                Environment={"Variables": env})
            out["lambda"] = "updated"
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except Exception as e:
        out["lambda_err"] = str(e)[:300]
        with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str); return

    RULE = f"{NAME}-6h"
    try:
        eb.put_rule(Name=RULE, ScheduleExpression=SCHEDULE, State="ENABLED",
                     Description="CB Stance refresh every 6h")
        arn = lam.get_function(FunctionName=NAME)["Configuration"]["FunctionArn"]
        eb.put_targets(Rule=RULE, Targets=[{"Id": "1", "Arn": arn}])
        try:
            lam.add_permission(FunctionName=NAME, StatementId=f"{NAME}-eb-permit",
                Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                SourceArn=eb.describe_rule(Name=RULE)["Arn"])
        except lam.exceptions.ResourceConflictException: pass
        out["schedule"] = SCHEDULE
        out["rule_state"] = "ENABLED"
    except Exception as e:
        out["schedule_err"] = str(e)[:300]

    _time.sleep(3)
    try:
        resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse",
                           LogType="Tail", Payload=b"{}")
        out["invoke_status"] = resp.get("StatusCode")
        out["fn_error"] = resp.get("FunctionError")
        body = resp["Payload"].read().decode("utf-8")
        try:
            p = json.loads(body)
            out["response"] = json.loads(p["body"]) if p.get("body") else p
        except: out["raw"] = body[:2000]
        if resp.get("LogResult"):
            out["log_tail"] = base64.b64decode(resp["LogResult"]).decode("utf-8","replace")[-3500:]
    except Exception as e:
        out["invoke_err"] = str(e)[:400]

    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/cb-stance.json")
        body = obj["Body"].read()
        p = json.loads(body)
        fed = p.get("fed") or {}
        latest = fed.get("latest_statement") or {}
        out["sidecar"] = {
            "size_kb": round(len(body)/1024, 1),
            "modified": obj["LastModified"].isoformat()[:19],
            "version": p.get("version"),
            "model": p.get("model"),
            "n_scored": p.get("n_fomc_statements_scored"),
            "regime": fed.get("regime"),
            "regime_signal": fed.get("regime_signal"),
            "delta_hawkish": fed.get("delta_hawkish_score"),
            "shift_classification": fed.get("shift_classification"),
            "latest_statement_full": {k: latest.get(k) for k in (
                "date","title","hawkish_score","policy_action","policy_action_size_bps",
                "forward_guidance","inflation_concern","growth_concern","labor_concern",
                "balance_sheet_stance","key_themes","notable_phrases","summary")},
            "recent_count": len(fed.get("recent_statements") or []),
            "recent_dates_and_scores": [
                {"date": s.get("date"), "hawkish": s.get("hawkish_score"),
                  "action": s.get("action") or s.get("policy_action")}
                for s in (fed.get("recent_statements") or [])
            ],
        }
    except Exception as e:
        out["sidecar_err"] = str(e)[:300]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
