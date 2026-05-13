#!/usr/bin/env python3
"""Step 508 — Deploy justhodl-finra-short + schedule + verify first run."""
import io, json, os, time as _time, zipfile, base64
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/508_finra_short_deploy.json"
SOURCE = "aws/lambdas/justhodl-finra-short/source/lambda_function.py"
NAME = "justhodl-finra-short"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"

lam = boto3.client("lambda", region_name="us-east-1")
events = boto3.client("events", region_name="us-east-1")


def zip_source(path):
    with open(path, "rb") as f: code = f.read()
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", code)
    return buf.getvalue()


def inherit_env(source_fn, keys):
    try:
        cfg = lam.get_function_configuration(FunctionName=source_fn)
        src_env = (cfg.get("Environment") or {}).get("Variables", {}) or {}
        return {k: src_env[k] for k in keys if k in src_env}
    except Exception as e:
        print(f"  inherit_env from {source_fn} err: {e}")
        return {}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    zb = zip_source(SOURCE)

    env = {}
    env.update(inherit_env("justhodl-alpha-score", ["FMP_KEY"]))
    if "FMP_KEY" not in env:
        # Try alternatives
        for fn in ["justhodl-screener", "justhodl-stock-screener", "justhodl-fmp-fundamentals"]:
            env.update(inherit_env(fn, ["FMP_KEY"]))
            if "FMP_KEY" in env: break
    env.update(inherit_env("justhodl-options-flow-scanner", ["POLY_KEY"]))
    env.update(inherit_env("justhodl-screener-alerts", ["TELEGRAM_TOKEN", "TELEGRAM_CHAT_ID"]))
    out["env_keys_inherited"] = sorted(env.keys())

    if "FMP_KEY" not in env:
        out["warning"] = "FMP_KEY not found in any source Lambda — sp500 universe will be empty"

    try:
        lam.get_function_configuration(FunctionName=NAME)
        lam.update_function_code(FunctionName=NAME, ZipFile=zb)
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
        lam.update_function_configuration(
            FunctionName=NAME, MemorySize=1024, Timeout=300,
            Description="FINRA Short Volume v1.0 (squeeze-setup engine)",
            Environment={"Variables": env},
        )
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
        out["deploy"] = "updated"
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(
            FunctionName=NAME, Runtime="python3.12",
            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
            MemorySize=1024, Timeout=300, Code={"ZipFile": zb},
            Environment={"Variables": env},
            Description="FINRA Short Volume v1.0 (squeeze-setup engine)",
            Tags={"Project": "JustHodl", "Roadmap": "Bloomberg-Gap-2"},
        )
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
        out["deploy"] = "created"
    except Exception as e:
        out["deploy_err"] = str(e)[:400]
        with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)
        return

    # Schedule
    try:
        events.put_rule(
            Name="justhodl-finra-short-daily",
            ScheduleExpression="cron(0 1 ? * TUE-SAT *)",
            State="ENABLED",
            Description="Daily 1 AM UTC (8 PM ET prior day) Tue-Sat",
        )
        try:
            lam.add_permission(
                FunctionName=NAME, StatementId="EventBridgeInvoke",
                Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                SourceArn="arn:aws:events:us-east-1:857687956942:rule/justhodl-finra-short-daily",
            )
        except lam.exceptions.ResourceConflictException: pass
        events.put_targets(
            Rule="justhodl-finra-short-daily",
            Targets=[{"Id": "1",
                       "Arn": f"arn:aws:lambda:us-east-1:857687956942:function:{NAME}"}],
        )
        out["schedule"] = "ok"
    except Exception as e:
        out["schedule_err"] = str(e)[:300]

    # First invocation
    _time.sleep(3)
    try:
        resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse",
                           LogType="Tail", Payload=b"{}")
        out["invoke_status"] = resp.get("StatusCode")
        out["fn_error"] = resp.get("FunctionError")
        body = resp["Payload"].read().decode("utf-8")
        try:
            parsed = json.loads(body)
            out["invoke_response"] = json.loads(parsed["body"]) if parsed.get("body") else parsed
        except: out["invoke_raw"] = body[:2000]
        if resp.get("LogResult"):
            out["log_tail"] = base64.b64decode(resp["LogResult"]).decode("utf-8", "replace")[-3500:]
    except Exception as e:
        out["invoke_err"] = str(e)[:400]

    # Read sidecar
    try:
        s3 = boto3.client("s3", region_name="us-east-1")
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/finra-short.json")
        body = obj["Body"].read()
        p = json.loads(body)
        out["sidecar"] = {
            "size_kb": round(len(body) / 1024, 1),
            "modified": obj["LastModified"].isoformat()[:19],
            "version": p.get("version"),
            "data_date": p.get("data_date"),
            "elapsed_seconds": p.get("elapsed_seconds"),
            "market_composite": p.get("market_composite"),
            "n_squeeze_candidates": len(p.get("squeeze_candidates") or []),
            "top_squeeze_5": [
                {k: c.get(k) for k in ["symbol","name","sector","svr_pct",
                                          "z_score","days_to_cover","squeeze_score",
                                          "squeeze_flags","price_strength"]}
                for c in (p.get("squeeze_candidates") or [])[:5]
            ],
            "top_svr_5": [
                {k: c.get(k) for k in ["symbol","name","sector","svr_pct","z_score"]}
                for c in (p.get("top_svr") or [])[:5]
            ],
            "top_zscore_5": [
                {k: c.get(k) for k in ["symbol","name","sector","z_score","svr_pct"]}
                for c in (p.get("top_zscore") or [])[:5]
            ],
            "sectors_top_3": dict(list((p.get("sectors") or {}).items())[:3]),
        }
    except Exception as e:
        out["sidecar_err"] = str(e)[:300]

    # Also check history file
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/finra-short-history.json")
        body = obj["Body"].read()
        p = json.loads(body)
        out["history"] = {
            "size_kb": round(len(body) / 1024, 1),
            "modified": obj["LastModified"].isoformat()[:19],
            "n_tickers_with_history": len(p.get("tickers") or {}),
        }
    except Exception as e:
        out["history_err"] = str(e)[:200]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
