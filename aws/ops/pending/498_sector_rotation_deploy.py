#!/usr/bin/env python3
"""Step 498 — Direct-deploy sector-rotation Lambda + schedule + first-run verification."""
import io, json, os, time as _time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/498_sector_rotation_deploy.json"
SOURCE = "aws/lambdas/justhodl-sector-rotation/source/lambda_function.py"
NAME = "justhodl-sector-rotation"
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

    # Build env
    env = {}
    env.update(inherit_env("justhodl-options-flow-scanner", ["POLY_KEY"]))
    env.update(inherit_env("justhodl-screener-alerts", ["TELEGRAM_TOKEN", "TELEGRAM_CHAT_ID"]))
    out["env_keys"] = sorted(env.keys())

    # Deploy (update or create)
    try:
        lam.get_function_configuration(FunctionName=NAME)
        # Exists — update
        lam.update_function_code(FunctionName=NAME, ZipFile=zb)
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
        lam.update_function_configuration(
            FunctionName=NAME,
            MemorySize=1024, Timeout=300,
            Description="Sector Rotation & Money Flow v1.0 (#4)",
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
            Description="Sector Rotation & Money Flow v1.0 (#4)",
            Tags={"Project": "JustHodl", "Roadmap": "4"},
        )
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
        out["deploy"] = "created"
    except Exception as e:
        out["deploy_err"] = str(e)[:300]
        with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)
        return

    # Schedule (hourly :38)
    try:
        events.put_rule(
            Name="justhodl-sector-rotation-hourly",
            ScheduleExpression="cron(38 * * * ? *)",
            State="ENABLED",
            Description="Hourly :38 sector-rotation run",
        )
        try:
            lam.add_permission(
                FunctionName=NAME, StatementId="EventBridgeInvoke",
                Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                SourceArn=f"arn:aws:events:us-east-1:857687956942:rule/justhodl-sector-rotation-hourly",
            )
        except lam.exceptions.ResourceConflictException: pass
        events.put_targets(
            Rule="justhodl-sector-rotation-hourly",
            Targets=[{"Id": "1",
                       "Arn": f"arn:aws:lambda:us-east-1:857687956942:function:{NAME}"}],
        )
        out["schedule"] = "ok"
    except Exception as e:
        out["schedule_err"] = str(e)[:300]

    # First-run invocation
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
        except: out["invoke_raw"] = body[:1000]
        if resp.get("LogResult"):
            import base64
            out["log_tail"] = base64.b64decode(resp["LogResult"]).decode("utf-8", "replace")[-2000:]
    except Exception as e:
        out["invoke_err"] = str(e)[:400]

    # Verify sidecar
    try:
        s3 = boto3.client("s3", region_name="us-east-1")
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/sector-rotation.json")
        body = obj["Body"].read()
        p = json.loads(body)
        out["sidecar"] = {
            "size_kb": round(len(body) / 1024, 1),
            "modified": obj["LastModified"].isoformat()[:19],
            "version": p.get("version"),
            "macro_context": p.get("macro_context"),
            "risk_appetite": p.get("risk_appetite"),
            "summary": p.get("summary"),
            "n_sectors": len(p.get("sectors") or []),
            "n_ratios": len(p.get("ratios") or []),
            "rotation_alerts": p.get("rotation_alerts"),
            "top_5_sectors": [
                {"sym": s.get("symbol"), "score": s.get("rotation_score"),
                 "rs_3m": s.get("rs_3m_pct"), "cmf": s.get("chaikin_money_flow_20"),
                 "mfi": s.get("money_flow_index_14"),
                 "rotating_in": s.get("rotating_in"),
                 "rotating_out": s.get("rotating_out"),
                 "in_cycle": s.get("in_current_cycle")}
                for s in (p.get("sectors") or [])[:5]],
            "key_ratios": [
                {"label": r.get("label"), "ret_21d": r.get("ret_21d_pct"),
                 "z_1y": r.get("z_score_1y"), "direction": r.get("direction")}
                for r in (p.get("ratios") or []) if r.get("label") in
                  ["Financials/Utilities", "Discretionary/Staples",
                   "HY Credit/Treasuries", "Small-Cap/Large-Cap", "Tech/Energy"]],
        }
    except Exception as e:
        out["sidecar_err"] = str(e)[:300]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
