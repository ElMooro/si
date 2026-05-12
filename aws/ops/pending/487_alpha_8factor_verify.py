#!/usr/bin/env python3
"""Step 487 — Verify alpha-score v1.1.0 deployed with options-flow as 8th
factor. Invoke the Lambda, check the output sidecar, and surface specific
stocks that moved tier because of the new factor."""
import io, json, os, time as _time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/487_alpha_8factor_verify.json"
NAME = "justhodl-tmp-487"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, boto3
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")

def lambda_handler(event, context):
    out = {}
    # 1. Capture BEFORE state from current alpha sidecar
    try:
        before = json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                                          Key="screener/alpha-score.json")["Body"].read())
        out["before"] = {
            "model_version": before.get("model_version"),
            "weights": before.get("weights"),
            "tier_distribution": before.get("tier_distribution"),
            "inputs": before.get("inputs"),
            "top_3_tier_S_or_A": [{"sym": s.get("symbol"), "alpha": s.get("alpha_score"),
                                     "tier": s.get("tier"), "components": s.get("components")}
                                    for s in (before.get("stocks") or [])[:3]],
        }
    except Exception as e:
        out["before_err"] = str(e)[:200]
    
    # 2. Invoke updated alpha-score
    try:
        resp = lam.invoke(FunctionName="justhodl-alpha-score",
                            InvocationType="RequestResponse",
                            LogType="Tail", Payload=b"{}")
        body = resp["Payload"].read().decode("utf-8")
        try: parsed = json.loads(body)
        except: parsed = {"_raw": body[:500]}
        out["invoke_status"] = resp.get("StatusCode")
        out["fn_error"] = resp.get("FunctionError")
        out["invoke_response"] = json.loads(parsed["body"]) if parsed.get("body") else parsed
        import base64
        if resp.get("LogResult"):
            tail = base64.b64decode(resp["LogResult"]).decode("utf-8", "replace")
            out["log_tail"] = tail[-1500:]
    except Exception as e:
        out["invoke_err"] = str(e)[:400]
    
    # 3. Capture AFTER state
    try:
        after = json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                                         Key="screener/alpha-score.json")["Body"].read())
        out["after"] = {
            "model_version": after.get("model_version"),
            "weights": after.get("weights"),
            "tier_distribution": after.get("tier_distribution"),
            "inputs": after.get("inputs"),
            "n_stocks": len(after.get("stocks") or []),
            "n_with_options_flow": sum(1 for s in (after.get("stocks") or [])
                                          if (s.get("components") or {}).get("options_flow") is not None),
        }
        
        # Find stocks where options-flow is firing
        of_top = sorted(
            [(s.get("symbol"), s.get("alpha_score"), s.get("tier"),
              (s.get("components") or {}).get("options_flow"),
              s.get("top_signals") or [],
              (s.get("components") or {}))
             for s in (after.get("stocks") or [])
             if (s.get("components") or {}).get("options_flow") is not None
             and (s.get("components") or {}).get("options_flow") >= 70],
            key=lambda x: -x[3]
        )[:15]
        out["after"]["top_options_flow_in_alpha"] = [
            {"symbol": s, "alpha": a, "tier": t, "options_flow_score": of,
              "top_signals": ts[:3], "components": c}
            for s, a, t, of, ts, c in of_top
        ]
        
        # Top 5 overall to see if they incorporate options flow
        out["after"]["top_5_overall"] = [
            {"symbol": s.get("symbol"), "alpha": s.get("alpha_score"),
              "tier": s.get("tier"), "components": s.get("components"),
              "top_signals": (s.get("top_signals") or [])[:4]}
            for s in (after.get("stocks") or [])[:5]
        ]
    except Exception as e:
        out["after_err"] = str(e)[:200]
    
    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    print("Waiting 150s for deploy...")
    _time.sleep(150)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", DIAG)
    zb = buf.getvalue()
    try:
        lam.create_function(FunctionName=NAME, Runtime="python3.12",
                            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
                            MemorySize=512, Timeout=300, Code={"ZipFile": zb})
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
