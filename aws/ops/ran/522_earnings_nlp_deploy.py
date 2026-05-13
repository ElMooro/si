#!/usr/bin/env python3
"""522 — Direct-deploy justhodl-earnings-nlp (new Lambda) + schedule + verify."""
import io, json, os, time as _time, zipfile, base64
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/522_earnings_nlp_deploy.json"
NAME = "justhodl-earnings-nlp"
SOURCE = "aws/lambdas/justhodl-earnings-nlp/source/lambda_function.py"
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"
SCHEDULE = "cron(0 14 ? * MON-FRI *)"  # 14:00 UTC = 10:00 ET (covers AMC/BMO transcripts)

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
    """Pull ANTHROPIC_API_KEY + TELEGRAM creds from known-good Lambdas."""
    env = {}
    # ANTHROPIC from ai-chat (most likely to have current key)
    for src in ("justhodl-ai-chat", "justhodl-morning-intelligence",
                  "justhodl-investor-agents", "autonomous-ai-processor"):
        try:
            cfg = lam.get_function_configuration(FunctionName=src)
            v = (cfg.get("Environment") or {}).get("Variables") or {}
            if v.get("ANTHROPIC_API_KEY") and "ANTHROPIC_API_KEY" not in env:
                env["ANTHROPIC_API_KEY"] = v["ANTHROPIC_API_KEY"]
            if v.get("ANTHROPIC_MODEL") and "ANTHROPIC_MODEL" not in env:
                env["ANTHROPIC_MODEL"] = v["ANTHROPIC_MODEL"]
        except Exception: pass
    # TELEGRAM from any known Lambda
    for src in ("justhodl-dix", "justhodl-crypto-funding",
                  "justhodl-dealer-gex", "justhodl-finra-short"):
        try:
            cfg = lam.get_function_configuration(FunctionName=src)
            v = (cfg.get("Environment") or {}).get("Variables") or {}
            for k in ("TELEGRAM_TOKEN", "TELEGRAM_CHAT_ID"):
                if v.get(k) and k not in env: env[k] = v[k]
            break
        except Exception: pass
    # FMP from a known Lambda
    for src in ("justhodl-13f-positions", "justhodl-stock-screener",
                  "justhodl-stock-analyzer"):
        try:
            cfg = lam.get_function_configuration(FunctionName=src)
            v = (cfg.get("Environment") or {}).get("Variables") or {}
            if v.get("FMP_KEY") and "FMP_KEY" not in env:
                env["FMP_KEY"] = v["FMP_KEY"]
                break
        except Exception: pass
    return env


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    zb = zip_source(SOURCE)
    env = inherit_env()
    out["env_inherited_keys"] = sorted(env.keys())
    out["has_anthropic_key"] = bool(env.get("ANTHROPIC_API_KEY"))
    out["anthropic_model"] = env.get("ANTHROPIC_MODEL", "(unset)")

    # ─── Create or update Lambda ───
    try:
        try:
            lam.create_function(
                FunctionName=NAME, Runtime="python3.12",
                Handler="lambda_function.lambda_handler", Role=ROLE,
                MemorySize=1024, Timeout=600, Code={"ZipFile": zb},
                Description="Earnings call NLP via FMP + Claude Haiku — tone, guidance, QoQ shifts",
                Environment={"Variables": env},
            )
            out["lambda"] = "created"
        except lam.exceptions.ResourceConflictException:
            lam.update_function_code(FunctionName=NAME, ZipFile=zb)
            lam.get_waiter("function_updated").wait(FunctionName=NAME)
            lam.update_function_configuration(
                FunctionName=NAME, MemorySize=1024, Timeout=600,
                Environment={"Variables": env},
            )
            out["lambda"] = "updated"
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except Exception as e:
        out["lambda_err"] = str(e)[:300]
        with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str); return

    # ─── EventBridge schedule ───
    RULE = f"{NAME}-daily"
    try:
        eb.put_rule(Name=RULE, ScheduleExpression=SCHEDULE, State="ENABLED",
                     Description="Daily earnings transcript NLP scoring")
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

    # ─── Invoke + verify ───
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

    # ─── Read sidecar ───
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/earnings-nlp.json")
        body = obj["Body"].read()
        p = json.loads(body)
        # Sample first 3 scored entries to verify data quality
        by_t = p.get("by_ticker") or {}
        scored = [(k, v) for k, v in by_t.items() if v.get("management_tone") is not None]
        sample = scored[:3]
        out["sidecar"] = {
            "size_kb": round(len(body) / 1024, 1),
            "modified": obj["LastModified"].isoformat()[:19],
            "version": p.get("version"),
            "model": p.get("model"),
            "n_tickers": p.get("n_tickers"),
            "n_with_data": p.get("n_with_data"),
            "n_with_err": p.get("n_with_err"),
            "market_summary": p.get("market_summary"),
            "sample_scored_tickers": [
                {"ticker": k,
                  "period": v.get("period"),
                  "tone": v.get("management_tone"),
                  "guidance": v.get("guidance_direction"),
                  "confidence": v.get("confidence"),
                  "demand": v.get("demand_signal"),
                  "margin": v.get("margin_signal"),
                  "themes": v.get("key_themes"),
                  "summary": (v.get("summary") or "")[:200]}
                for k, v in sample
            ],
            "n_improvers": len((p.get("ranked") or {}).get("biggest_improvers") or []),
            "n_deteriorators": len((p.get("ranked") or {}).get("biggest_deteriorators") or []),
        }
    except Exception as e:
        out["sidecar_err"] = str(e)[:300]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
