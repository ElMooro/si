#!/usr/bin/env python3
"""526 — Direct-deploy justhodl-credit-stress + schedule + invoke + verify."""
import io, json, os, time as _time, zipfile, base64
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/526_credit_stress_deploy.json"
NAME = "justhodl-credit-stress"
SOURCE = "aws/lambdas/justhodl-credit-stress/source/lambda_function.py"
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"
SCHEDULE = "cron(0 20 ? * MON-FRI *)"

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
    env = {"FRED_KEY": "2f057499936072679d8843d7fce99989"}
    for src in ("justhodl-dix", "justhodl-crypto-funding", "justhodl-dealer-gex"):
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
                MemorySize=512, Timeout=120, Code={"ZipFile": zb},
                Description="FRED ICE BofA OAS credit stress engine",
                Environment={"Variables": env})
            out["lambda"] = "created"
        except lam.exceptions.ResourceConflictException:
            lam.update_function_code(FunctionName=NAME, ZipFile=zb)
            lam.get_waiter("function_updated").wait(FunctionName=NAME)
            lam.update_function_configuration(FunctionName=NAME, MemorySize=512, Timeout=120,
                Environment={"Variables": env})
            out["lambda"] = "updated"
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except Exception as e:
        out["lambda_err"] = str(e)[:300]
        with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str); return

    RULE = f"{NAME}-daily"
    try:
        eb.put_rule(Name=RULE, ScheduleExpression=SCHEDULE, State="ENABLED",
                     Description="Daily credit stress refresh at 20:00 UTC")
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
        except: out["raw"] = body[:2000]
        if resp.get("LogResult"):
            out["log_tail"] = base64.b64decode(resp["LogResult"]).decode("utf-8","replace")[-3500:]
    except Exception as e:
        out["invoke_err"] = str(e)[:400]

    # Read sidecar
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/credit-stress.json")
        body = obj["Body"].read()
        p = json.loads(body)
        out["sidecar"] = {
            "size_kb": round(len(body) / 1024, 1),
            "modified": obj["LastModified"].isoformat()[:19],
            "version": p.get("version"),
            "data_date": p.get("data_date"),
            "current_bps": p.get("current_bps"),
            "derived_spreads": p.get("derived_spreads"),
            "regimes": p.get("regimes"),
            "composite_regime": p.get("composite_regime"),
            "composite_signal": p.get("composite_signal"),
            "n_series_loaded": sum(1 for m in (p.get("metrics") or {}).values() if not m.get("err")),
            "sample_metric_hy": (p.get("metrics") or {}).get("BAMLH0A0HYM2"),
            "sample_metric_ig": (p.get("metrics") or {}).get("BAMLC0A0CM"),
            "sample_metric_ccc": (p.get("metrics") or {}).get("BAMLH0A3HYC"),
        }
    except Exception as e:
        out["sidecar_err"] = str(e)[:300]

    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/credit-stress-history.json")
        body = obj["Body"].read()
        p = json.loads(body)
        out["history_sidecar"] = {
            "size_kb": round(len(body)/1024, 1),
            "n_days": p.get("n_days"),
            "first_date": p.get("first_date"),
            "last_date": p.get("last_date"),
            "series_keys": list((p.get("series") or {}).keys()),
        }
    except Exception as e:
        out["history_err"] = str(e)[:200]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
