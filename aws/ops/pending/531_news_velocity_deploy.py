#!/usr/bin/env python3
"""530 — Direct-deploy justhodl-news-velocity (BUILD 10) + schedule + verify."""
import io, json, os, time as _time, zipfile, base64
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/531_news_velocity_deploy.json"
NAME = "justhodl-news-velocity"
SOURCE = "aws/lambdas/justhodl-news-velocity/source/lambda_function.py"
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"
SCHEDULE = "cron(0 * ? * * *)"

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
    for src in ("justhodl-credit-stress", "justhodl-dix", "justhodl-dealer-gex"):
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

    try:
        try:
            lam.create_function(FunctionName=NAME, Runtime="python3.12",
                Handler="lambda_function.lambda_handler", Role=ROLE,
                MemorySize=512, Timeout=600, Code={"ZipFile": zb},
                Description="GDELT news article velocity engine",
                Environment={"Variables": env})
            out["lambda"] = "created"
        except lam.exceptions.ResourceConflictException:
            lam.update_function_code(FunctionName=NAME, ZipFile=zb)
            lam.get_waiter("function_updated").wait(FunctionName=NAME)
            lam.update_function_configuration(FunctionName=NAME, MemorySize=512, Timeout=600,
                Environment={"Variables": env})
            out["lambda"] = "updated"
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except Exception as e:
        out["lambda_err"] = str(e)[:300]
        with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str); return

    RULE = f"{NAME}-hourly"
    try:
        eb.put_rule(Name=RULE, ScheduleExpression=SCHEDULE, State="ENABLED",
                     Description="Hourly GDELT news velocity refresh")
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
        except: out["raw"] = body[:1500]
        if resp.get("LogResult"):
            out["log_tail"] = base64.b64decode(resp["LogResult"]).decode("utf-8","replace")[-3500:]
    except Exception as e:
        out["invoke_err"] = str(e)[:400]

    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/news-velocity.json")
        body = obj["Body"].read()
        p = json.loads(body)
        ranked = p.get("ranked") or {}
        out["sidecar"] = {
            "size_kb": round(len(body) / 1024, 1),
            "modified": obj["LastModified"].isoformat()[:19],
            "version": p.get("version"),
            "n_tickers": p.get("n_tickers"),
            "n_with_data": p.get("n_with_data"),
            "n_with_err": p.get("n_with_err"),
            "n_surge": p.get("n_surge"),
            "n_elevated": p.get("n_elevated"),
            "n_subdued": p.get("n_subdued"),
            "composite_regime": p.get("composite_regime"),
            "composite_signal": p.get("composite_signal"),
            "top_5_velocity": ranked.get("top_5_velocity"),
            "top_5_attention": ranked.get("top_5_attention"),
            "bottom_5_subdued": ranked.get("bottom_5_subdued"),
        }
    except Exception as e:
        out["sidecar_err"] = str(e)[:300]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
