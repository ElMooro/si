#!/usr/bin/env python3
"""Step 495 — Verify #1 institutional alpha-calibrator end-to-end."""
import io, json, os, time as _time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/495_alpha_calibrator_verify.json"
NAME = "justhodl-tmp-495"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, boto3, base64, urllib.request
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")
events = boto3.client("events", region_name="us-east-1")

def lambda_handler(event, context):
    out = {}

    # 1. Verify Lambdas deployed
    for name in ["justhodl-alpha-calibrator", "justhodl-alpha-score"]:
        try:
            cfg = lam.get_function_configuration(FunctionName=name)
            out[name] = {"deployed": True, "modified": cfg["LastModified"][:19],
                          "memory": cfg["MemorySize"], "timeout": cfg["Timeout"],
                          "env": list((cfg.get("Environment") or {}).get("Variables", {}).keys())}
        except Exception as e:
            out[name] = {"deployed": False, "err": str(e)[:200]}

    # 2. Schedule
    try:
        rule = events.describe_rule(Name="justhodl-alpha-calibrator-weekly")
        out["schedule"] = {"exists": True, "expression": rule.get("ScheduleExpression"),
                            "state": rule.get("State")}
    except Exception: out["schedule"] = {"exists": False}

    # 3. Invoke calibrator manually
    try:
        resp = lam.invoke(FunctionName="justhodl-alpha-calibrator",
                            InvocationType="RequestResponse", LogType="Tail",
                            Payload=b"{}")
        out["calibrator_invoke"] = {"status": resp.get("StatusCode"),
                                       "fn_error": resp.get("FunctionError")}
        body = resp["Payload"].read().decode("utf-8")
        try:
            parsed = json.loads(body)
            out["calibrator_invoke"]["response"] = json.loads(parsed["body"]) if parsed.get("body") else parsed
        except: out["calibrator_invoke"]["raw"] = body[:1000]
        if resp.get("LogResult"):
            out["calibrator_invoke"]["log_tail"] = base64.b64decode(resp["LogResult"]).decode("utf-8", "replace")[-1800:]
    except Exception as e:
        out["calibrator_invoke_err"] = str(e)[:400]

    # 4. Invoke alpha-score to confirm v1.2.0 still works with graceful fallback
    try:
        resp = lam.invoke(FunctionName="justhodl-alpha-score",
                            InvocationType="RequestResponse", LogType="Tail",
                            Payload=b"{}")
        body = resp["Payload"].read().decode("utf-8")
        try:
            parsed = json.loads(body)
            out["alpha_score_invoke"] = {"status": resp.get("StatusCode"),
                                           "fn_error": resp.get("FunctionError"),
                                           "response": json.loads(parsed["body"]) if parsed.get("body") else parsed}
        except: out["alpha_score_invoke"] = {"raw": body[:500]}
        if resp.get("LogResult"):
            out["alpha_score_invoke"]["log_tail"] = base64.b64decode(resp["LogResult"]).decode("utf-8", "replace")[-800:]
    except Exception as e:
        out["alpha_score_err"] = str(e)[:400]

    # 5. Read calibration sidecars
    out["sidecars"] = {}
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
                sc["guardrails"] = p.get("guardrails")
                sc["current_weights"] = p.get("current_weights")
                sc["proposed_weights"] = p.get("proposed_weights")
                sc["weight_deltas"] = p.get("weight_deltas")
                sc["n_strategies"] = len(p.get("per_strategy") or [])
                sc["attribution_summary"] = {
                    "n_obs": (p.get("factor_attribution") or {}).get("n_obs"),
                    "r_squared": (p.get("factor_attribution") or {}).get("r_squared"),
                    "insufficient": (p.get("factor_attribution") or {}).get("insufficient"),
                }
            elif "calibration-history" in k:
                sc["n_entries"] = len((p.get("history") or []))
            elif "alpha-weights" in k:
                sc["auto_apply_calibrations"] = p.get("auto_apply_calibrations")
                sc["active_weights"] = p.get("active_weights")
                sc["proposed_weights"] = p.get("proposed_weights")
                sc["last_calibration_version"] = p.get("last_calibration_version")
            elif "alpha-score" in k:
                sc["model_version"] = p.get("model_version")
                sc["weights_source"] = p.get("weights_source")
                sc["weights_calibration_version"] = p.get("weights_calibration_version")
                sc["weights"] = p.get("weights")
                sc["n_stocks"] = len(p.get("stocks") or [])
            out["sidecars"][k] = sc
        except Exception as e:
            out["sidecars"][k] = {"exists": False, "err": str(e)[:200]}

    # 6. Public pages
    out["pages"] = {}
    for url in ["https://justhodl.ai/", "https://justhodl.ai/calibration/"]:
        try:
            req = urllib.request.Request(url + "?_t=" + str(int(__import__("time").time())),
                headers={"User-Agent": "JustHodl-Verify/1.0"})
            with urllib.request.urlopen(req, timeout=10) as r:
                body = r.read().decode("utf-8", errors="replace")
            o = {"status": r.status, "size_kb": round(len(body)/1024, 1)}
            if url == "https://justhodl.ai/":
                o["has_calibration_nav"] = \'href="/calibration/"\' in body
                o["n_calibration_links"] = body.count(\'/calibration/\')
                o["has_intelligence_pipeline_block"] = "INTELLIGENCE PIPELINE" in body
            else:
                import re
                m = re.search(r"<title>([^<]+)</title>", body)
                o["title"] = m.group(1) if m else None
                o["has_methodology_box"] = "Bayesian" in body or "Information Coefficients" in body
                o["has_weight_evolution"] = "Weight Evolution" in body
                o["has_ic_table"] = "ic-table" in body or "Information Coefficients" in body
                o["has_ols_section"] = "OLS" in body or "Factor Attribution" in body
            out["pages"][url] = o
        except Exception as e:
            out["pages"][url] = {"err": str(e)[:200]}
    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    print("Waiting 200s for deploys...")
    _time.sleep(200)
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
