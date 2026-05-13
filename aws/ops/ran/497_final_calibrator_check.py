#!/usr/bin/env python3
"""Step 497 — Final sanity check on calibration deploy."""
import io, json, os, time as _time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/497_final_calibrator_check.json"
NAME = "justhodl-tmp-497"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, boto3
s3 = boto3.client("s3", region_name="us-east-1")
def lambda_handler(event, context):
    out = {}
    for k in ["data/calibration-latest.json", "data/calibration-history.json",
              "screener/alpha-weights.json", "screener/alpha-score.json"]:
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key=k)
            body = obj["Body"].read()
            p = json.loads(body)
            sc = {"exists": True, "size_kb": round(len(body)/1024, 1),
                   "modified": obj["LastModified"].isoformat()[:19]}
            if "calibration-latest" in k:
                sc["version"] = p.get("version")
                sc["summary"] = p.get("summary")
                sc["deployment_decision"] = p.get("deployment_decision")
                sc["current_weights"] = p.get("current_weights")
                sc["proposed_weights"] = p.get("proposed_weights")
                sc["n_per_strategy"] = len(p.get("per_strategy") or [])
                sc["per_strategy_summary"] = [
                    {"strategy": s.get("strategy"),
                     "n_total": s.get("n_total"),
                     "n_evaluated_30d": s.get("n_evaluated_30d"),
                     "statistically_sufficient": s.get("statistically_sufficient")}
                    for s in (p.get("per_strategy") or [])
                ]
                sc["attribution"] = {
                    "n_obs": (p.get("factor_attribution") or {}).get("n_obs"),
                    "insufficient": (p.get("factor_attribution") or {}).get("insufficient"),
                    "required": (p.get("factor_attribution") or {}).get("required"),
                }
            elif "alpha-weights" in k:
                sc["auto_apply_calibrations"] = p.get("auto_apply_calibrations")
                sc["active_weights"] = p.get("active_weights")
                sc["proposed_weights"] = p.get("proposed_weights")
                sc["last_calibration_version"] = p.get("last_calibration_version")
            elif "alpha-score" in k:
                sc["model_version"] = p.get("model_version")
                sc["weights_source"] = p.get("weights_source")
                sc["weights"] = p.get("weights")
                sc["n_stocks"] = len(p.get("stocks") or [])
            elif "calibration-history" in k:
                sc["n_entries"] = len(p.get("history") or [])
                sc["last_updated"] = p.get("last_updated")
            out[k] = sc
        except Exception as e:
            out[k] = {"exists": False, "err": str(e)[:200]}
    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", DIAG)
    zb = buf.getvalue()
    try:
        lam.create_function(FunctionName=NAME, Runtime="python3.12",
                            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
                            MemorySize=512, Timeout=30, Code={"ZipFile": zb})
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
