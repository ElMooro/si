#!/usr/bin/env python3
"""Step 499 — Deploy justhodl-dealer-gex + schedule + verify first run."""
import io, json, os, time as _time, zipfile, base64
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/499_dealer_gex_deploy.json"
SOURCE = "aws/lambdas/justhodl-dealer-gex/source/lambda_function.py"
NAME = "justhodl-dealer-gex"
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
    env.update(inherit_env("justhodl-options-flow-scanner", ["POLY_KEY"]))
    env.update(inherit_env("justhodl-screener-alerts", ["TELEGRAM_TOKEN", "TELEGRAM_CHAT_ID"]))
    out["env_keys"] = sorted(env.keys())

    try:
        lam.get_function_configuration(FunctionName=NAME)
        lam.update_function_code(FunctionName=NAME, ZipFile=zb)
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
        lam.update_function_configuration(
            FunctionName=NAME, MemorySize=2048, Timeout=600,
            Description="Dealer GEX & Positioning v1.0 (SpotGamma-grade)",
            Environment={"Variables": env},
        )
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
        out["deploy"] = "updated"
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(
            FunctionName=NAME, Runtime="python3.12",
            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
            MemorySize=2048, Timeout=600, Code={"ZipFile": zb},
            Environment={"Variables": env},
            Description="Dealer GEX & Positioning v1.0 (SpotGamma-grade)",
            Tags={"Project": "JustHodl", "Roadmap": "Bloomberg-Gap-1"},
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
            Name="justhodl-dealer-gex-hourly",
            ScheduleExpression="cron(7 13-21 ? * MON-FRI *)",
            State="ENABLED",
            Description="Hourly :07 during US market hours Mon-Fri",
        )
        try:
            lam.add_permission(
                FunctionName=NAME, StatementId="EventBridgeInvoke",
                Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                SourceArn="arn:aws:events:us-east-1:857687956942:rule/justhodl-dealer-gex-hourly",
            )
        except lam.exceptions.ResourceConflictException: pass
        events.put_targets(
            Rule="justhodl-dealer-gex-hourly",
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
        except: out["invoke_raw"] = body[:1500]
        if resp.get("LogResult"):
            out["log_tail"] = base64.b64decode(resp["LogResult"]).decode("utf-8", "replace")[-2500:]
    except Exception as e:
        out["invoke_err"] = str(e)[:400]

    # Read sidecar
    try:
        s3 = boto3.client("s3", region_name="us-east-1")
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/dealer-gex.json")
        body = obj["Body"].read()
        p = json.loads(body)
        out["sidecar"] = {
            "size_kb": round(len(body) / 1024, 1),
            "modified": obj["LastModified"].isoformat()[:19],
            "version": p.get("version"),
            "market_composite": p.get("market_composite"),
            "squeeze_candidates_count": len(p.get("squeeze_candidates") or []),
            "squeeze_top_3": (p.get("squeeze_candidates") or [])[:3],
            "underlyings": {
                sym: {
                    "spot": r.get("spot"),
                    "total_gex_b": r.get("total_dealer_gex_billions"),
                    "flip": r.get("zero_gamma_flip_level"),
                    "pct_to_flip": r.get("spot_pct_to_flip"),
                    "regime": r.get("regime"),
                    "pcr_oi": r.get("pcr_oi"),
                    "pcr_vol": r.get("pcr_volume"),
                    "n_contracts": r.get("n_contracts_modeled"),
                    "max_pain": list((r.get("max_pain_by_expiry") or {}).items())[:2],
                    "zero_dte_pct": (r.get("zero_dte") or {}).get("vol_pct"),
                    "skew": (r.get("iv_skew_30d") or {}).get("skew"),
                    "err": r.get("err"),
                }
                for sym, r in (p.get("underlyings") or {}).items()
            },
        }
    except Exception as e:
        out["sidecar_err"] = str(e)[:300]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
