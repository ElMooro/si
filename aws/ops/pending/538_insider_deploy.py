#!/usr/bin/env python3
"""538 — Direct-deploy justhodl-insider-transactions (BUILD 12 pivoted) + schedule + verify."""
import io, json, os, time as _time, zipfile, base64
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/538_insider_deploy.json"
NAME = "justhodl-insider-transactions"
SOURCE = "aws/lambdas/justhodl-insider-transactions/source/lambda_function.py"
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"
SCHEDULE = "cron(0 1 ? * MON-SAT *)"

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
    for src in ("justhodl-credit-stress", "justhodl-dix"):
        try:
            cfg = lam.get_function_configuration(FunctionName=src)
            v = (cfg.get("Environment") or {}).get("Variables") or {}
            for k in ("TELEGRAM_TOKEN", "TELEGRAM_CHAT_ID"):
                if v.get(k) and k not in env: env[k] = v[k]
            break
        except: pass
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
                MemorySize=1024, Timeout=600, Code={"ZipFile": zb},
                Description="SEC Form 4 insider transactions tracker",
                Environment={"Variables": env})
            out["lambda"] = "created"
        except lam.exceptions.ResourceConflictException:
            lam.update_function_code(FunctionName=NAME, ZipFile=zb)
            lam.get_waiter("function_updated").wait(FunctionName=NAME)
            lam.update_function_configuration(FunctionName=NAME, MemorySize=1024, Timeout=600,
                Environment={"Variables": env})
            out["lambda"] = "updated"
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except Exception as e:
        out["lambda_err"] = str(e)[:300]
        with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str); return

    RULE = f"{NAME}-daily"
    try:
        eb.put_rule(Name=RULE, ScheduleExpression=SCHEDULE, State="ENABLED",
                     Description="Daily SEC Form 4 insider scan at 01:00 UTC")
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
            out["log_tail"] = base64.b64decode(resp["LogResult"]).decode("utf-8", "replace")[-3500:]
    except Exception as e:
        out["invoke_err"] = str(e)[:400]

    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/insider-transactions.json")
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
            "n_cluster_buys": p.get("n_cluster_buys"),
            "total_buy_value_30d_usd": p.get("total_buy_value_30d_usd"),
            "total_sell_value_30d_usd": p.get("total_sell_value_30d_usd"),
            "composite_regime": p.get("composite_regime"),
            "composite_signal": p.get("composite_signal"),
            "biggest_buy_dollars_30d": ranked.get("biggest_buy_dollars_30d"),
            "biggest_sell_dollars_30d": ranked.get("biggest_sell_dollars_30d"),
            "cluster_buys": ranked.get("cluster_buys"),
            "most_active_14d": ranked.get("most_active_14d"),
        }
    except Exception as e:
        out["sidecar_err"] = str(e)[:300]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
