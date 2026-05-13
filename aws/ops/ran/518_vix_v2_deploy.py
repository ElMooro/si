#!/usr/bin/env python3
"""518 — Deploy VIX v2.0, ensure schedule, verify CBOE data flows."""
import io, json, os, time as _time, zipfile, base64
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/518_vix_v2_deploy.json"
NAME = "justhodl-vix-curve"
SOURCE = "aws/lambdas/justhodl-vix-curve/source/lambda_function.py"
SCHEDULE = "cron(0,30 13-21 ? * MON-FRI *)"  # every 30 min during US market hours

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
    out = {"S3_BUCKET": "justhodl-dashboard-live", "S3_KEY": "data/vix-curve.json"}
    for src in ("justhodl-dealer-gex", "justhodl-finra-short", "justhodl-dix"):
        try:
            cfg = lam.get_function_configuration(FunctionName=src)
            env = (cfg.get("Environment") or {}).get("Variables") or {}
            for k in ("TELEGRAM_TOKEN", "TELEGRAM_CHAT_ID"):
                if k in env and k not in out:
                    out[k] = env[k]
        except Exception: continue
    return out


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    zb = zip_source(SOURCE)
    env = inherit_env()
    out["env_keys"] = sorted(env.keys())

    # ─── Update Lambda code + config ───
    try:
        lam.update_function_code(FunctionName=NAME, ZipFile=zb)
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
        lam.update_function_configuration(
            FunctionName=NAME, MemorySize=512, Timeout=120,
            Environment={"Variables": env},
        )
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
        out["lambda"] = "updated"
    except lam.exceptions.ResourceNotFoundException:
        # Create fresh if missing
        lam.create_function(
            FunctionName=NAME, Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            Role="arn:aws:iam::857687956942:role/lambda-execution-role",
            MemorySize=512, Timeout=120, Code={"ZipFile": zb},
            Description="VIX Term Structure v2 — CBOE CDN sourced",
            Environment={"Variables": env},
        )
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
        out["lambda"] = "created"
    except Exception as e:
        out["deploy_err"] = str(e)[:300]
        with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str); return

    # ─── EventBridge schedule (every 30 min during market hours) ───
    RULE = "justhodl-vix-curve-30min"
    try:
        eb.put_rule(Name=RULE, ScheduleExpression=SCHEDULE,
                     State="ENABLED",
                     Description="VIX curve every 30 min during US market hours")
        arn = lam.get_function(FunctionName=NAME)["Configuration"]["FunctionArn"]
        eb.put_targets(Rule=RULE, Targets=[{"Id": "1", "Arn": arn}])
        try:
            lam.add_permission(
                FunctionName=NAME, StatementId=f"{NAME}-eb-permit-30min",
                Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                SourceArn=eb.describe_rule(Name=RULE)["Arn"],
            )
        except lam.exceptions.ResourceConflictException: pass
        out["schedule"] = SCHEDULE
        out["rule_state"] = "ENABLED"
    except Exception as e:
        out["schedule_err"] = str(e)[:300]

    # ─── Invoke + verify ───
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
            out["log_tail"] = base64.b64decode(resp["LogResult"]).decode("utf-8","replace")[-3000:]
    except Exception as e:
        out["invoke_err"] = str(e)[:400]

    # ─── Read sidecar ───
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/vix-curve.json")
        body = obj["Body"].read()
        p = json.loads(body)
        out["sidecar"] = {
            "size_kb": round(len(body) / 1024, 1),
            "modified": obj["LastModified"].isoformat()[:19],
            "version": p.get("version"),
            "data_date": p.get("data_date"),
            "current": p.get("current"),
            "spreads": p.get("spreads"),
            "spread_regimes": p.get("spread_regimes"),
            "composite_regime": p.get("composite_regime"),
            "composite_signal": p.get("composite_signal"),
            "z_scores_60d": p.get("z_scores_60d"),
            "percentile_ranks": p.get("percentile_ranks"),
            "sustained_signals": p.get("sustained_signals"),
            "cross_asset_dispersion": p.get("cross_asset_dispersion"),
            "n_history_days": p.get("n_history_days"),
            "history_first_date": p.get("history_first_date"),
        }
    except Exception as e:
        out["sidecar_err"] = str(e)[:300]

    # ─── Read history sidecar ───
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/vix-curve-history.json")
        body = obj["Body"].read()
        p = json.loads(body)
        out["history_sidecar"] = {
            "size_kb": round(len(body) / 1024, 1),
            "n_days": p.get("n_days"),
            "first_date": p.get("first_date"),
            "last_date": p.get("last_date"),
            "series_keys": list((p.get("series") or {}).keys()),
        }
    except Exception as e:
        out["history_sidecar_err"] = str(e)[:200]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
