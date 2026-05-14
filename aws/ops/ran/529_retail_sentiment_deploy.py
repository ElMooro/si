#!/usr/bin/env python3
"""529 — Direct-deploy justhodl-retail-sentiment (new Lambda) + schedule + verify."""
import io, json, os, time as _time, zipfile, base64
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/529_retail_sentiment_deploy.json"
NAME = "justhodl-retail-sentiment"
SOURCE = "aws/lambdas/justhodl-retail-sentiment/source/lambda_function.py"
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"
SCHEDULE = "cron(0,30 * ? * * *)"

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
    for src in ("justhodl-dix", "justhodl-crypto-funding"):
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

    try:
        try:
            lam.create_function(FunctionName=NAME, Runtime="python3.12",
                Handler="lambda_function.lambda_handler", Role=ROLE,
                MemorySize=512, Timeout=180, Code={"ZipFile": zb},
                Description="Retail sentiment via ApeWisdom + StockTwits — 30-min refresh",
                Environment={"Variables": env})
            out["lambda"] = "created"
        except lam.exceptions.ResourceConflictException:
            lam.update_function_code(FunctionName=NAME, ZipFile=zb)
            lam.get_waiter("function_updated").wait(FunctionName=NAME)
            lam.update_function_configuration(FunctionName=NAME, MemorySize=512, Timeout=180,
                Environment={"Variables": env})
            out["lambda"] = "updated"
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except Exception as e:
        out["lambda_err"] = str(e)[:300]
        with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str); return

    RULE = f"{NAME}-30min"
    try:
        eb.put_rule(Name=RULE, ScheduleExpression=SCHEDULE, State="ENABLED",
                     Description="Retail sentiment refresh every 30 min around the clock")
        arn = lam.get_function(FunctionName=NAME)["Configuration"]["FunctionArn"]
        eb.put_targets(Rule=RULE, Targets=[{"Id": "1", "Arn": arn}])
        try:
            lam.add_permission(FunctionName=NAME, StatementId=f"{NAME}-eb-permit",
                Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                SourceArn=eb.describe_rule(Name=RULE)["Arn"])
        except lam.exceptions.ResourceConflictException: pass
        out["schedule"] = SCHEDULE
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
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/retail-sentiment.json")
        body = obj["Body"].read()
        p = json.loads(body)
        # Sample
        out["sidecar"] = {
            "size_kb": round(len(body)/1024, 1),
            "modified": obj["LastModified"].isoformat()[:19],
            "version": p.get("version"),
            "n_all_stocks": p.get("n_all_stocks"),
            "n_wsb": p.get("n_wsb"),
            "n_stocks": p.get("n_stocks"),
            "n_investing": p.get("n_investing"),
            "n_with_stwt_data": p.get("n_with_stwt_data"),
            "market_regime": p.get("market_regime"),
            "market_regime_signal": p.get("market_regime_signal"),
            "market_regime_data": p.get("market_regime_data"),
            "top_10": (p.get("top_30_by_mentions") or [])[:10],
            "biggest_velocity_surges_top_5": ((p.get("ranked") or {}).get("biggest_velocity_surges") or [])[:5],
            "biggest_rank_climbers_top_5": ((p.get("ranked") or {}).get("biggest_rank_climbers") or [])[:5],
            "most_bullish_stwt_top_3": ((p.get("ranked") or {}).get("most_bullish_stwt") or [])[:3],
            "stocktwits_trending_top_5": (p.get("stocktwits_trending") or [])[:5],
            "subreddit_breakdown": p.get("subreddit_breakdown"),
        }
    except Exception as e:
        out["sidecar_err"] = str(e)[:300]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
