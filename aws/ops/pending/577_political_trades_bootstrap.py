#!/usr/bin/env python3
"""577 — Bootstrap justhodl-political-trades."""
import io, json, os, time as _time, base64
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/577_political_trades_bootstrap.json"
NAME = "justhodl-political-trades"
ACCOUNT = "857687956942"
REGION = "us-east-1"
RULE_NAME = f"{NAME}-daily"
SCHEDULE = "cron(45 13 ? * MON-FRI *)"

lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    for i in range(20):
        try:
            cfg = lam.get_function(FunctionName=NAME)["Configuration"]
            if cfg.get("State") == "Active" and cfg.get("LastUpdateStatus") == "Successful":
                out["lambda_last_modified"] = cfg.get("LastModified")
                break
        except Exception: pass
        _time.sleep(6)

    # Patch Telegram env
    try:
        cfg = lam.get_function_configuration(FunctionName=NAME)
        env = (cfg.get("Environment") or {}).get("Variables", {}) or {}
        patched = False
        for ssm_path, env_key in [("/justhodl/telegram/token", "TELEGRAM_TOKEN"),
                                     ("/justhodl/telegram/chat_id", "TELEGRAM_CHAT_ID")]:
            if not env.get(env_key):
                try:
                    env[env_key] = ssm.get_parameter(Name=ssm_path, WithDecryption=True
                        )["Parameter"]["Value"]
                    patched = True
                except Exception: pass
        if patched:
            lam.update_function_configuration(FunctionName=NAME,
                                                Environment={"Variables": env})
            lam.get_waiter("function_updated").wait(FunctionName=NAME)
            out["env_patched"] = "OK"
    except Exception as e:
        out["env_err"] = str(e)[:200]

    # Register EB rule
    try:
        events.put_rule(Name=RULE_NAME, ScheduleExpression=SCHEDULE, State="ENABLED",
                         Description="Political trades daily")
        try:
            lam.add_permission(FunctionName=NAME, StatementId=f"{RULE_NAME}-invoke",
                Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT}:rule/{RULE_NAME}")
        except lam.exceptions.ResourceConflictException: pass
        events.put_targets(Rule=RULE_NAME, Targets=[{
            "Id": "1",
            "Arn": f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{NAME}",
        }])
        out["eventbridge_rule"] = "OK"
    except Exception as e:
        out["eventbridge_err"] = str(e)[:200]

    _time.sleep(3)

    # Invoke (FMP fetch may take 30-60s)
    try:
        resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse",
                           LogType="Tail", Payload=b"{}")
        out["invoke_status"] = resp.get("StatusCode")
        out["fn_error"] = resp.get("FunctionError")
        body = resp["Payload"].read().decode("utf-8")
        try:
            p = json.loads(body)
            out["response"] = json.loads(p["body"]) if isinstance(p.get("body"), str) else p
        except: out["raw"] = body[:500]
        if resp.get("LogResult"):
            out["log_tail"] = base64.b64decode(resp["LogResult"]).decode("utf-8", "replace")[-3000:]
    except Exception as e:
        out["invoke_err"] = str(e)[:200]

    _time.sleep(2)
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/political-trades.json")
        body = obj["Body"].read()
        p = json.loads(body)
        out["sidecar_size_kb"] = round(len(body)/1024, 2)
        out["sidecar_stats"] = p.get("stats")
        out["endpoints_used"] = p.get("endpoints")
        out["sample_trades"] = (p.get("trades_recent_50") or [])[:8]
        out["clusters"] = p.get("clusters_top_10") or []
        out["large_trades"] = (p.get("large_trades_top_15") or [])[:5]
    except Exception as e:
        out["sidecar_err"] = str(e)[:200]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
