#!/usr/bin/env python3
"""Step 484 — Diagnose silent-failing Lambdas + verify newly deployed sizer
+ catalysts Lambdas write their sidecars."""
import io, json, os, time as _time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/484_gap_close_verify.json"
NAME = "justhodl-tmp-484"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, boto3
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")
events = boto3.client("events", region_name="us-east-1")

def lambda_handler(event, context):
    out = {"silent_diag": {}, "new_lambdas": {}, "sidecars": {}, "schedules": {}}

    # ─── 1. Diagnose 4 silent-failing Lambdas by invoking them ───
    silent = ["justhodl-risk-sizer", "justhodl-earnings-sentiment",
              "justhodl-event-study", "justhodl-sector-earnings-diffusion"]
    for name in silent:
        diag = {}
        try:
            cfg = lam.get_function_configuration(FunctionName=name)
            diag["env_keys"] = list((cfg.get("Environment") or {}).get("Variables", {}).keys())
            diag["memory"] = cfg["MemorySize"]
            diag["timeout"] = cfg["Timeout"]
            diag["modified"] = cfg["LastModified"][:19]
        except Exception as e:
            diag["err_config"] = str(e)[:200]
            out["silent_diag"][name] = diag
            continue
        # Invoke with empty payload and LogType=Tail to get last 4 KB of logs
        try:
            resp = lam.invoke(FunctionName=name, InvocationType="RequestResponse",
                                LogType="Tail", Payload=b"{}")
            body = resp["Payload"].read().decode("utf-8", errors="replace")
            try: body_parsed = json.loads(body)
            except Exception: body_parsed = {"_raw": body[:500]}
            diag["status_code"] = resp.get("StatusCode")
            diag["fn_error"] = resp.get("FunctionError")
            diag["response"] = body_parsed
            # Decode log tail
            import base64
            log_tail_b64 = resp.get("LogResult")
            if log_tail_b64:
                tail = base64.b64decode(log_tail_b64).decode("utf-8", errors="replace")
                diag["log_tail"] = tail[-2500:]  # last 2.5 KB
        except Exception as e:
            diag["invoke_err"] = str(e)[:300]
        out["silent_diag"][name] = diag

    # ─── 2. Verify NEW lambdas deployed correctly ───
    NEW = ["justhodl-portfolio-sizer", "justhodl-portfolio-catalysts"]
    for name in NEW:
        try:
            cfg = lam.get_function_configuration(FunctionName=name)
            out["new_lambdas"][name] = {
                "deployed": True,
                "modified": cfg["LastModified"][:19],
                "memory": cfg["MemorySize"],
                "timeout": cfg["Timeout"],
                "env_keys": list((cfg.get("Environment") or {}).get("Variables", {}).keys()),
            }
        except Exception as e:
            out["new_lambdas"][name] = {"deployed": False, "err": str(e)[:200]}

    # ─── 3. Invoke new lambdas to write their sidecars ───
    for name in NEW:
        if not out["new_lambdas"].get(name, {}).get("deployed"): continue
        try:
            resp = lam.invoke(FunctionName=name, InvocationType="RequestResponse",
                                LogType="Tail", Payload=b"{}")
            body = resp["Payload"].read().decode("utf-8", errors="replace")
            try: body_parsed = json.loads(body)
            except Exception: body_parsed = {"_raw": body[:500]}
            out["new_lambdas"][name]["invoke_status"] = resp.get("StatusCode")
            out["new_lambdas"][name]["fn_error"] = resp.get("FunctionError")
            out["new_lambdas"][name]["invoke_response"] = body_parsed
            import base64
            log_tail_b64 = resp.get("LogResult")
            if log_tail_b64:
                tail = base64.b64decode(log_tail_b64).decode("utf-8", errors="replace")
                out["new_lambdas"][name]["log_tail"] = tail[-2000:]
        except Exception as e:
            out["new_lambdas"][name]["invoke_err"] = str(e)[:300]

    # ─── 4. Read new sidecars ───
    for key in ["portfolio/sizing.json", "portfolio/catalysts.json"]:
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key=key)
            body = obj["Body"].read()
            p = json.loads(body)
            out["sidecars"][key] = {
                "exists": True,
                "size_kb": round(len(body) / 1024, 1),
                "generated_at": p.get("generated_at"),
                "version": p.get("version"),
                "summary": p.get("summary"),
                "n_positions": len(p.get("positions") or []) if "positions" in p else None,
                "n_entry_candidates": len(p.get("entry_candidates") or []) if "entry_candidates" in p else None,
                "n_position_catalysts": len(p.get("position_catalysts") or []) if "position_catalysts" in p else None,
                "n_macro_catalysts": len(p.get("macro_catalysts") or []) if "macro_catalysts" in p else None,
                "alerts_sent": p.get("alerts_sent"),
                "sample_position": (p.get("positions") or [None])[0] if "positions" in p else None,
                "sample_entry": (p.get("entry_candidates") or [None])[0] if "entry_candidates" in p else None,
                "sample_catalyst": (p.get("position_catalysts") or [None])[0] if "position_catalysts" in p else None,
                "sample_macro": (p.get("macro_catalysts") or [None])[0] if "macro_catalysts" in p else None,
            }
        except Exception as e:
            out["sidecars"][key] = {"exists": False, "err": str(e)[:200]}

    # ─── 5. Check EventBridge rules ───
    rules_to_check = [
        "justhodl-portfolio-sizer-hourly",
        "justhodl-portfolio-catalysts-3x-daily",
        "justhodl-catalyst-calendar-daily",
    ]
    for r in rules_to_check:
        try:
            rule = events.describe_rule(Name=r)
            out["schedules"][r] = {
                "exists": True,
                "schedule": rule.get("ScheduleExpression"),
                "state": rule.get("State"),
            }
        except Exception:
            out["schedules"][r] = {"exists": False}

    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    print("Waiting 180s for deploy...")
    _time.sleep(180)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", DIAG)
    zb = buf.getvalue()
    try:
        lam.create_function(FunctionName=NAME, Runtime="python3.12",
                            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
                            MemorySize=512, Timeout=900, Code={"ZipFile": zb})
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except Exception:
        lam.update_function_code(FunctionName=NAME, ZipFile=zb)
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
    _time.sleep(2)
    resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    body = resp["Payload"].read().decode("utf-8")
    try:
        parsed = json.loads(body)
        out["test"] = json.loads(parsed["body"]) if "body" in parsed and parsed["body"] else parsed
    except Exception:
        out["raw"] = body[:30000]
    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
