#!/usr/bin/env python3
"""578 — Bootstrap justhodl-reversal-radar + re-invoke political-trades
now that the FMP /stable/ endpoints are correct."""
import io, json, os, time as _time, base64
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/578_reversal_radar_bootstrap.json"
NAME = "justhodl-reversal-radar"
POLITICAL_NAME = "justhodl-political-trades"
ACCOUNT = "857687956942"
REGION = "us-east-1"
RULE_NAME = f"{NAME}-hourly"
SCHEDULE = "cron(30 * ? * * *)"

lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # ─── Reversal Radar ─────────────────────────────────────────────────
    for i in range(20):
        try:
            cfg = lam.get_function(FunctionName=NAME)["Configuration"]
            if cfg.get("State") == "Active" and cfg.get("LastUpdateStatus") == "Successful":
                out["reversal_lambda_last_modified"] = cfg.get("LastModified")
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
            out["reversal_env_patched"] = "OK"
    except Exception as e:
        out["reversal_env_err"] = str(e)[:200]

    # Register EB rule
    try:
        events.put_rule(Name=RULE_NAME, ScheduleExpression=SCHEDULE, State="ENABLED",
                         Description="Reversal radar hourly")
        try:
            lam.add_permission(FunctionName=NAME, StatementId=f"{RULE_NAME}-invoke",
                Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT}:rule/{RULE_NAME}")
        except lam.exceptions.ResourceConflictException: pass
        events.put_targets(Rule=RULE_NAME, Targets=[{
            "Id": "1",
            "Arn": f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{NAME}",
        }])
        out["reversal_eventbridge_rule"] = "OK"
    except Exception as e:
        out["reversal_eventbridge_err"] = str(e)[:200]

    _time.sleep(3)

    # Invoke reversal-radar
    try:
        resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse",
                           LogType="Tail", Payload=b"{}")
        out["reversal_invoke_status"] = resp.get("StatusCode")
        out["reversal_fn_error"] = resp.get("FunctionError")
        body = resp["Payload"].read().decode("utf-8")
        try:
            p = json.loads(body)
            out["reversal_response"] = json.loads(p["body"]) if isinstance(p.get("body"), str) else p
        except: out["reversal_raw"] = body[:500]
        if resp.get("LogResult"):
            out["reversal_log_tail"] = base64.b64decode(resp["LogResult"]).decode("utf-8", "replace")[-2200:]
    except Exception as e:
        out["reversal_invoke_err"] = str(e)[:200]

    _time.sleep(2)
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/reversal-radar.json")
        body = obj["Body"].read()
        p = json.loads(body)
        out["reversal_sidecar"] = {
            "size_kb": round(len(body)/1024, 2),
            "state": p.get("state"),
            "top_score": p.get("top_score"),
            "bottom_score": p.get("bottom_score"),
            "interpretation": p.get("interpretation"),
            "top_reasons": p.get("top_reasons"),
            "bottom_reasons": p.get("bottom_reasons"),
        }
    except Exception as e:
        out["reversal_sidecar_err"] = str(e)[:200]

    # ─── Re-invoke Political Trades with fixed endpoints ─────────────────
    try:
        # Wait for political-trades to also auto-deploy
        for i in range(15):
            try:
                cfg = lam.get_function(FunctionName=POLITICAL_NAME)["Configuration"]
                if cfg.get("State") == "Active" and cfg.get("LastUpdateStatus") == "Successful":
                    out["political_lambda_last_modified"] = cfg.get("LastModified")
                    break
            except Exception: pass
            _time.sleep(6)

        resp = lam.invoke(FunctionName=POLITICAL_NAME, InvocationType="RequestResponse",
                           LogType="Tail", Payload=b"{}")
        out["political_invoke_status"] = resp.get("StatusCode")
        out["political_fn_error"] = resp.get("FunctionError")
        body = resp["Payload"].read().decode("utf-8")
        try:
            p = json.loads(body)
            out["political_response"] = json.loads(p["body"]) if isinstance(p.get("body"), str) else p
        except: out["political_raw"] = body[:500]
        if resp.get("LogResult"):
            out["political_log_tail"] = base64.b64decode(resp["LogResult"]).decode("utf-8", "replace")[-2500:]

        _time.sleep(2)
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/political-trades.json")
        body = obj["Body"].read()
        p = json.loads(body)
        out["political_sidecar"] = {
            "size_kb": round(len(body)/1024, 2),
            "endpoints_used": p.get("endpoints"),
            "stats": p.get("stats"),
            "sample_trades": (p.get("trades_recent_50") or [])[:5],
            "clusters": (p.get("clusters_top_10") or [])[:5],
            "large_trades": (p.get("large_trades_top_15") or [])[:5],
        }
    except Exception as e:
        out["political_err"] = str(e)[:200]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
