#!/usr/bin/env python3
"""Step 496 — Direct-deploy alpha-calibrator (boto3) + force-update alpha-score
to v1.2.0. Bypasses GH Actions because the new-Lambda creation case isn't
handled by the deploy-lambdas workflow's update-only path."""
import io, json, os, sys, time as _time, zipfile, subprocess
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/496_deploy_calibrator.json"

# Read source code from repo (ops runner checks out at root of repo)
CALIBRATOR_SRC = "aws/lambdas/justhodl-alpha-calibrator/source/lambda_function.py"
ALPHA_SCORE_SRC = "aws/lambdas/justhodl-alpha-score/source/lambda_function.py"

lam = boto3.client("lambda", region_name="us-east-1")
events = boto3.client("events", region_name="us-east-1")

ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"


def zip_source(path):
    """Read a single .py file and return a zip with it as lambda_function.py."""
    with open(path, "rb") as f:
        code = f.read()
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", code)
    return buf.getvalue()


def inherit_env(source_fn, keys):
    """Pull env values from another Lambda."""
    try:
        cfg = lam.get_function_configuration(FunctionName=source_fn)
        src_env = (cfg.get("Environment") or {}).get("Variables", {}) or {}
        return {k: src_env[k] for k in keys if k in src_env}
    except Exception as e:
        print(f"  inherit_env from {source_fn} err: {e}")
        return {}


def deploy_alpha_calibrator():
    out = {}
    zb = zip_source(CALIBRATOR_SRC)
    # Build environment
    env = {"LOOKBACK_DAYS": "120", "MIN_OBS_FOR_STAT": "30",
            "MIN_OBS_FOR_WEIGHT_UPDATE": "60"}
    env.update(inherit_env("justhodl-options-flow-scanner", ["POLY_KEY"]))
    env.update(inherit_env("justhodl-screener-alerts", ["TELEGRAM_TOKEN", "TELEGRAM_CHAT_ID"]))
    out["env_keys"] = sorted(env.keys())

    try:
        lam.create_function(
            FunctionName="justhodl-alpha-calibrator",
            Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            Role=ROLE_ARN,
            MemorySize=1024, Timeout=600,
            Code={"ZipFile": zb},
            Environment={"Variables": env},
            Description="Institutional self-improvement engine (#1). Weekly OLS factor attribution + Bayesian shrinkage with guardrails.",
            Tags={"Project": "JustHodl", "Roadmap": "1"},
        )
        lam.get_waiter("function_active_v2").wait(FunctionName="justhodl-alpha-calibrator")
        out["create"] = "ok"
    except lam.exceptions.ResourceConflictException:
        # Function exists — update
        lam.update_function_code(FunctionName="justhodl-alpha-calibrator", ZipFile=zb)
        lam.get_waiter("function_updated").wait(FunctionName="justhodl-alpha-calibrator")
        lam.update_function_configuration(
            FunctionName="justhodl-alpha-calibrator",
            MemorySize=1024, Timeout=600,
            Environment={"Variables": env},
        )
        out["update"] = "ok"
    except Exception as e:
        out["err"] = str(e)[:300]
        return out

    # Create EventBridge rule + permission
    try:
        events.put_rule(
            Name="justhodl-alpha-calibrator-weekly",
            ScheduleExpression="cron(0 9 ? * SUN *)",
            State="ENABLED",
            Description="Weekly Sunday 09:00 UTC alpha-calibrator run",
        )
        # Permission for EventBridge to invoke
        try:
            lam.add_permission(
                FunctionName="justhodl-alpha-calibrator",
                StatementId="EventBridgeInvoke",
                Action="lambda:InvokeFunction",
                Principal="events.amazonaws.com",
                SourceArn=f"arn:aws:events:us-east-1:857687956942:rule/justhodl-alpha-calibrator-weekly",
            )
        except lam.exceptions.ResourceConflictException:
            pass  # already exists
        events.put_targets(
            Rule="justhodl-alpha-calibrator-weekly",
            Targets=[{"Id": "1",
                      "Arn": f"arn:aws:lambda:us-east-1:857687956942:function:justhodl-alpha-calibrator"}],
        )
        out["schedule"] = "ok"
    except Exception as e:
        out["schedule_err"] = str(e)[:300]

    # Invoke once to write initial sidecars
    _sleep_safe(3)
    try:
        resp = lam.invoke(FunctionName="justhodl-alpha-calibrator",
                           InvocationType="RequestResponse",
                           Payload=b"{}")
        body = resp["Payload"].read().decode("utf-8")
        try:
            parsed = json.loads(body)
            out["first_run"] = {"status": resp.get("StatusCode"),
                                  "fn_error": resp.get("FunctionError"),
                                  "response": json.loads(parsed["body"]) if parsed.get("body") else parsed}
        except: out["first_run"] = {"raw": body[:600]}
    except Exception as e:
        out["first_run_err"] = str(e)[:300]
    return out


def force_update_alpha_score():
    """Re-deploy alpha-score so v1.2.0 takes effect."""
    out = {}
    zb = zip_source(ALPHA_SCORE_SRC)
    try:
        lam.update_function_code(FunctionName="justhodl-alpha-score", ZipFile=zb)
        lam.get_waiter("function_updated").wait(FunctionName="justhodl-alpha-score")
        out["update"] = "ok"
    except Exception as e:
        out["err"] = str(e)[:300]
        return out
    # Invoke once to refresh sidecar
    _sleep_safe(2)
    try:
        resp = lam.invoke(FunctionName="justhodl-alpha-score",
                           InvocationType="RequestResponse",
                           Payload=b"{}")
        body = resp["Payload"].read().decode("utf-8")
        try:
            parsed = json.loads(body)
            out["invoke"] = {"status": resp.get("StatusCode"),
                              "response": json.loads(parsed["body"]) if parsed.get("body") else parsed}
        except: out["invoke"] = {"raw": body[:300]}
    except Exception as e:
        out["invoke_err"] = str(e)[:300]
    return out


def _sleep_safe(secs):
    _sleep_safe_target = _time.time() + secs
    while _time.time() < _sleep_safe_target:
        _time.sleep(0.5)


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    out["alpha_calibrator"] = deploy_alpha_calibrator()
    out["alpha_score"] = force_update_alpha_score()
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
