#!/usr/bin/env python3
"""520 — Direct-deploy justhodl-crypto-funding (new Lambda) + EventBridge hourly."""
import io, json, os, time as _time, zipfile, base64
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/520_crypto_funding_deploy.json"
NAME = "justhodl-crypto-funding"
SOURCE = "aws/lambdas/justhodl-crypto-funding/source/lambda_function.py"
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"
SCHEDULE = "cron(15 * ? * * *)"  # every hour at :15

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
    out = {"S3_BUCKET": "justhodl-dashboard-live"}
    for src in ("justhodl-dix", "justhodl-dealer-gex"):
        try:
            cfg = lam.get_function_configuration(FunctionName=src)
            env = (cfg.get("Environment") or {}).get("Variables") or {}
            for k in ("TELEGRAM_TOKEN", "TELEGRAM_CHAT_ID"):
                if k in env and k not in out: out[k] = env[k]
        except Exception: continue
    return out


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    zb = zip_source(SOURCE)
    env = inherit_env()
    out["env_keys"] = sorted(env.keys())

    # Create or update
    try:
        try:
            lam.create_function(
                FunctionName=NAME, Runtime="python3.12",
                Handler="lambda_function.lambda_handler", Role=ROLE,
                MemorySize=512, Timeout=60, Code={"ZipFile": zb},
                Description="Crypto perp funding rates + OI engine (OKX top-10 coins)",
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

    # Schedule
    RULE = f"{NAME}-hourly"
    try:
        eb.put_rule(Name=RULE, ScheduleExpression=SCHEDULE,
                     State="ENABLED",
                     Description="Crypto funding rates every hour at :15")
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

    # Invoke
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

    # Sidecar
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/crypto-funding.json")
        body = obj["Body"].read()
        p = json.loads(body)
        out["sidecar"] = {
            "size_kb": round(len(body) / 1024, 1),
            "modified": obj["LastModified"].isoformat()[:19],
            "version": p.get("version"),
            "composite_regime": p.get("composite_regime"),
            "composite_signal": p.get("composite_signal"),
            "market_composite": p.get("market_composite"),
            "squeeze_candidates": p.get("squeeze_candidates"),
            "by_coin_keys": list((p.get("by_coin") or {}).keys()),
            "btc_summary": {
                k: (p.get("by_coin") or {}).get("BTC", {}).get(k)
                for k in ("spot_price", "change_24h_pct", "annualized_pct",
                            "funding_z_score", "oi_usd_b", "regime", "crowding_flag")
            },
            "eth_summary": {
                k: (p.get("by_coin") or {}).get("ETH", {}).get(k)
                for k in ("spot_price", "change_24h_pct", "annualized_pct",
                            "funding_z_score", "oi_usd_b", "regime")
            },
        }
    except Exception as e:
        out["sidecar_err"] = str(e)[:300]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
