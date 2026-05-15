#!/usr/bin/env python3
"""580 — Triple bootstrap: justhodl-reversal-radar, justhodl-auction-grader,
justhodl-margin-lending. Longer waits since multi-Lambda deploy."""
import io, json, os, time as _time, base64
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/580_triple_bootstrap.json"
ACCOUNT = "857687956942"
REGION = "us-east-1"

CONFIGS = [
    {"name": "justhodl-reversal-radar",
     "rule": "justhodl-reversal-radar-hourly",
     "schedule": "cron(30 * ? * * *)",
     "sidecar": "data/reversal-radar.json"},
    {"name": "justhodl-auction-grader",
     "rule": "justhodl-auction-grader-daily",
     "schedule": "cron(0 16 ? * MON-FRI *)",
     "sidecar": "data/auction-grades.json"},
    {"name": "justhodl-margin-lending",
     "rule": "justhodl-margin-lending-daily",
     "schedule": "cron(0 14 ? * MON-FRI *)",
     "sidecar": "data/margin-lending.json"},
]

lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)
s3 = boto3.client("s3", region_name="us-east-1")


def bootstrap_one(spec):
    name, rule, schedule, sidecar_key = spec["name"], spec["rule"], spec["schedule"], spec["sidecar"]
    result = {"name": name}

    # Wait up to 5 min for deploy
    for i in range(50):
        try:
            cfg = lam.get_function(FunctionName=name)["Configuration"]
            if cfg.get("State") == "Active" and cfg.get("LastUpdateStatus") == "Successful":
                result["last_modified"] = cfg.get("LastModified")
                break
        except Exception: pass
        _time.sleep(6)
    else:
        result["error"] = "lambda not deployed within 5min"
        return result

    # Patch Telegram env
    try:
        cfg = lam.get_function_configuration(FunctionName=name)
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
            lam.update_function_configuration(FunctionName=name,
                                                Environment={"Variables": env})
            lam.get_waiter("function_updated").wait(FunctionName=name)
            result["env_patched"] = "OK"
    except Exception as e:
        result["env_err"] = str(e)[:200]

    # EB rule
    try:
        events.put_rule(Name=rule, ScheduleExpression=schedule, State="ENABLED",
                         Description=f"{name} cron")
        try:
            lam.add_permission(FunctionName=name, StatementId=f"{rule}-invoke",
                Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT}:rule/{rule}")
        except lam.exceptions.ResourceConflictException: pass
        events.put_targets(Rule=rule, Targets=[{
            "Id": "1",
            "Arn": f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{name}",
        }])
        result["eb_rule"] = "OK"
    except Exception as e:
        result["eb_err"] = str(e)[:200]

    _time.sleep(2)

    # Invoke
    try:
        resp = lam.invoke(FunctionName=name, InvocationType="RequestResponse",
                           LogType="Tail", Payload=b"{}")
        result["invoke_status"] = resp.get("StatusCode")
        result["fn_error"] = resp.get("FunctionError")
        body = resp["Payload"].read().decode("utf-8")
        try:
            p = json.loads(body)
            result["response"] = json.loads(p["body"]) if isinstance(p.get("body"), str) else p
        except: result["raw"] = body[:300]
        if resp.get("LogResult"):
            result["log_tail"] = base64.b64decode(resp["LogResult"]).decode("utf-8", "replace")[-1500:]
    except Exception as e:
        result["invoke_err"] = str(e)[:200]

    _time.sleep(2)

    # Read sidecar
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key=sidecar_key)
        body = obj["Body"].read()
        p = json.loads(body)
        result["sidecar_size_kb"] = round(len(body)/1024, 2)
        # Trim large lists for report
        if isinstance(p, dict):
            trimmed = {}
            for k, v in p.items():
                if isinstance(v, list) and len(v) > 5:
                    trimmed[k] = v[:5] + [f"...truncated {len(v)-5} more"]
                else: trimmed[k] = v
            result["sidecar"] = trimmed
    except Exception as e:
        result["sidecar_err"] = str(e)[:200]

    return result


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    out["lambdas"] = []
    for spec in CONFIGS:
        out["lambdas"].append(bootstrap_one(spec))
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
