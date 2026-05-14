#!/usr/bin/env python3
"""550 — Deploy justhodl-regime-composite Lambda + hourly EventBridge cron,
force-invoke for first sidecar generation, then audit sidecar + page."""
import io, json, os, time as _time, zipfile, base64, urllib.request
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/550_regime_composite_deploy.json"
NAME = "justhodl-regime-composite"
SOURCE = "aws/lambdas/justhodl-regime-composite/source/lambda_function.py"
CONFIG = "aws/lambdas/justhodl-regime-composite/config.json"

ACCOUNT = "857687956942"
REGION = "us-east-1"
ROLE = f"arn:aws:iam::{ACCOUNT}:role/lambda-execution-role"
RULE_NAME = "justhodl-regime-composite-hourly"
SCHEDULE = "cron(15 * ? * * *)"

lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)


def zip_source(path):
    with open(path, "rb") as f: code = f.read()
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", code)
    return buf.getvalue()


def get_telegram_creds():
    try:
        token = ssm.get_parameter(Name="/justhodl/telegram/token",
                                    WithDecryption=True)["Parameter"]["Value"]
    except Exception:
        token = ""
    try:
        chat_id = ssm.get_parameter(Name="/justhodl/telegram/chat_id",
                                      WithDecryption=True)["Parameter"]["Value"]
    except Exception:
        chat_id = ""
    return token, chat_id


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    cfg = json.load(open(CONFIG))
    zb = zip_source(SOURCE)
    env_vars = (cfg.get("Environment") or {}).get("Variables", {})
    tg_token, tg_chat = get_telegram_creds()
    if tg_token: env_vars["TELEGRAM_TOKEN"] = tg_token
    if tg_chat: env_vars["TELEGRAM_CHAT_ID"] = tg_chat

    # Create or update Lambda
    try:
        lam.get_function(FunctionName=NAME)
        # Exists — update code + config
        lam.update_function_code(FunctionName=NAME, ZipFile=zb)
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
        lam.update_function_configuration(
            FunctionName=NAME,
            Runtime=cfg["Runtime"],
            Handler=cfg["Handler"],
            MemorySize=cfg["MemorySize"],
            Timeout=cfg["Timeout"],
            Description=cfg["Description"],
            Environment={"Variables": env_vars},
        )
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
        out["lambda_op"] = "updated"
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(
            FunctionName=NAME,
            Runtime=cfg["Runtime"],
            Handler=cfg["Handler"],
            Role=ROLE,
            Code={"ZipFile": zb},
            MemorySize=cfg["MemorySize"],
            Timeout=cfg["Timeout"],
            Description=cfg["Description"],
            Environment={"Variables": env_vars},
        )
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
        out["lambda_op"] = "created"
    except Exception as e:
        out["lambda_err"] = str(e)[:300]
        with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str); return

    # EventBridge rule
    try:
        events.put_rule(Name=RULE_NAME, ScheduleExpression=SCHEDULE, State="ENABLED",
                         Description="justhodl-regime-composite hourly")
        events.put_targets(Rule=RULE_NAME, Targets=[{
            "Id": "1",
            "Arn": f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{NAME}",
        }])
        # Add invoke permission (idempotent)
        try:
            lam.add_permission(
                FunctionName=NAME, StatementId=f"{RULE_NAME}-invoke",
                Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT}:rule/{RULE_NAME}",
            )
        except lam.exceptions.ResourceConflictException: pass
        out["eventbridge"] = "OK"
    except Exception as e:
        out["eventbridge_err"] = str(e)[:300]

    _time.sleep(3)

    # Force-invoke for first sidecar
    try:
        resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse",
                           LogType="Tail", Payload=b"{}")
        out["invoke_status"] = resp.get("StatusCode")
        out["fn_error"] = resp.get("FunctionError")
        body = resp["Payload"].read().decode("utf-8")
        try:
            p = json.loads(body)
            out["response_body"] = json.loads(p["body"]) if p.get("body") else p
        except Exception:
            out["raw_response"] = body[:1500]
        if resp.get("LogResult"):
            out["log_tail"] = base64.b64decode(resp["LogResult"]).decode("utf-8", "replace")[-3000:]
    except Exception as e:
        out["invoke_err"] = str(e)[:300]

    _time.sleep(2)

    # Audit sidecar
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/regime-composite.json")
        body = obj["Body"].read()
        p = json.loads(body)
        out["sidecar"] = {
            "size_kb": round(len(body) / 1024, 1),
            "modified": obj["LastModified"].isoformat()[:19],
            "meta_regime": p.get("meta_regime"),
            "meta_class": p.get("meta_class"),
            "meta_narrative": p.get("meta_narrative"),
            "composite_score": p.get("composite_score"),
            "n_modules_with_data": p.get("n_modules_with_data"),
            "n_modules_missing": p.get("n_modules_missing"),
            "duration_s": p.get("duration_s"),
            "dimensions": p.get("dimensions"),
            "module_summary": [
                {"label": m.get("label"), "regime": m.get("regime"),
                  "polarity": m.get("polarity"), "dimension": m.get("dimension"),
                  "missing": m.get("missing", False), "age_minutes": m.get("age_minutes")}
                for m in (p.get("modules") or [])
            ],
        }
    except Exception as e:
        out["sidecar_err"] = str(e)[:300]

    # Audit page rendered
    try:
        req = urllib.request.Request("https://justhodl.ai/composite/",
                                       headers={"User-Agent": "JustHodl.AI ops/550"})
        with urllib.request.urlopen(req, timeout=10) as r:
            html = r.read().decode("utf-8", "replace")
        out["page"] = {
            "size": len(html),
            "loads_composite_json": "regime-composite.json" in html,
            "loads_history_json": "regime-composite-history.json" in html,
            "has_meta_banner": "meta-banner" in html,
            "has_dims_grid": "dims-grid" in html,
            "has_mods_grid": "mods-grid" in html,
            "has_history": "history-bar" in html,
            "has_methodology": "Meta-Regime" in html and "7 Dimensions" in html,
        }
    except Exception as e:
        out["page_err"] = str(e)[:200]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
