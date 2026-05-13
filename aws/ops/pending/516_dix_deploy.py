#!/usr/bin/env python3
"""516 — Direct-deploy justhodl-dix (new Lambda) + EventBridge daily + verify."""
import io, json, os, time as _time, zipfile, base64
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/516_dix_deploy.json"
NAME = "justhodl-dix"
SOURCE = "aws/lambdas/justhodl-dix/source/lambda_function.py"
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"
SCHEDULE = "cron(0 22 ? * MON-FRI *)"  # 22:00 UTC = 5PM ET = after market close

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
    """Pull TELEGRAM_TOKEN/CHAT_ID from a known-good Lambda."""
    for src in ("justhodl-dealer-gex", "justhodl-finra-short"):
        try:
            cfg = lam.get_function_configuration(FunctionName=src)
            env = (cfg.get("Environment") or {}).get("Variables") or {}
            return {k: v for k, v in env.items()
                     if k in ("TELEGRAM_TOKEN", "TELEGRAM_CHAT_ID")}
        except Exception: continue
    return {}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    zb = zip_source(SOURCE)
    env = inherit_env()
    out["env_inherited_keys"] = list(env.keys())

    # ─── Step 1: Create or update Lambda ───
    try:
        try:
            lam.create_function(
                FunctionName=NAME, Runtime="python3.12",
                Handler="lambda_function.lambda_handler", Role=ROLE,
                MemorySize=512, Timeout=60, Code={"ZipFile": zb},
                Description="Squeezemetrics DIX/GEX engine — daily institutional accumulation tracker",
                Environment={"Variables": env},
            )
            out["lambda"] = "created"
        except lam.exceptions.ResourceConflictException:
            lam.update_function_code(FunctionName=NAME, ZipFile=zb)
            lam.get_waiter("function_updated").wait(FunctionName=NAME)
            lam.update_function_configuration(
                FunctionName=NAME, MemorySize=512, Timeout=60,
                Environment={"Variables": env},
            )
            out["lambda"] = "updated"
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except Exception as e:
        out["lambda_err"] = str(e)[:300]
        with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str); return

    # ─── Step 2: EventBridge schedule ───
    RULE = f"{NAME}-daily"
    try:
        eb.put_rule(Name=RULE, ScheduleExpression=SCHEDULE,
                     State="ENABLED",
                     Description="Daily DIX/GEX fetch + regime classification")
        arn = lam.get_function(FunctionName=NAME)["Configuration"]["FunctionArn"]
        eb.put_targets(Rule=RULE, Targets=[{"Id": "1", "Arn": arn}])
        try:
            lam.add_permission(
                FunctionName=NAME, StatementId=f"{NAME}-eb-permit",
                Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                SourceArn=eb.describe_rule(Name=RULE)["Arn"],
            )
        except lam.exceptions.ResourceConflictException: pass
        out["schedule"] = SCHEDULE
        out["rule_state"] = "ENABLED"
    except Exception as e:
        out["schedule_err"] = str(e)[:300]

    # ─── Step 3: Invoke + verify ───
    _time.sleep(2)
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
            out["log_tail"] = base64.b64decode(resp["LogResult"]).decode("utf-8","replace")[-2500:]
    except Exception as e:
        out["invoke_err"] = str(e)[:400]

    # ─── Step 4: Read sidecar ───
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/dix.json")
        body = obj["Body"].read()
        p = json.loads(body)
        out["sidecar"] = {
            "size_kb": round(len(body) / 1024, 1),
            "modified": obj["LastModified"].isoformat()[:19],
            "version": p.get("version"),
            "data_date": p.get("data_date"),
            "current": p.get("current"),
            "dix_regime": p.get("dix_regime"),
            "gex_regime": p.get("gex_regime"),
            "combined_regime": p.get("combined_regime"),
            "combined_signal": p.get("combined_signal"),
            "statistics": p.get("statistics"),
            "moving_averages": p.get("moving_averages"),
            "sustained_signals": p.get("sustained_signals"),
            "day_over_day": p.get("day_over_day"),
            "backtest_20d_top": (p.get("backtest_forward_returns_by_dix_bucket") or {}).get("forward_20d"),
            "n_history_days": p.get("n_history_days"),
        }
    except Exception as e:
        out["sidecar_err"] = str(e)[:300]

    # ─── Step 5: Verify history sidecar ───
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/dix-history.json")
        body = obj["Body"].read()
        p = json.loads(body)
        out["history_sidecar"] = {
            "size_kb": round(len(body) / 1024, 1),
            "n_days": p.get("n_days"),
            "first_date": p.get("first_date"),
            "last_date": p.get("last_date"),
        }
    except Exception as e:
        out["history_sidecar_err"] = str(e)[:200]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
