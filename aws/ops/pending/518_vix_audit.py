#!/usr/bin/env python3
"""518 — Audit BUILD 6 (VIX term structure) state."""
import json, os
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/518_vix_audit.json"
lam = boto3.client("lambda", region_name="us-east-1")
eb = boto3.client("events", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")


def audit_lambda(name):
    info = {"name": name}
    try:
        cfg = lam.get_function_configuration(FunctionName=name)
        info["exists"] = True
        info["memory"] = cfg.get("MemorySize")
        info["timeout"] = cfg.get("Timeout")
        info["last_modified"] = cfg.get("LastModified")
        info["env_keys"] = sorted((cfg.get("Environment", {}) or {}).get("Variables", {}).keys())
    except: info["exists"] = False; return info

    rules = []
    for r in eb.list_rules()["Rules"]:
        try:
            targets = eb.list_targets_by_rule(Rule=r["Name"])["Targets"]
            if any(name in t.get("Arn", "") for t in targets):
                rules.append({"name": r["Name"], "schedule": r.get("ScheduleExpression"),
                                "state": r.get("State")})
        except: pass
    info["rules"] = rules

    # Test invoke
    try:
        resp = lam.invoke(FunctionName=name, InvocationType="RequestResponse",
                           LogType="None", Payload=b"{}")
        info["invoke_status"] = resp.get("StatusCode")
        info["invoke_fn_err"] = resp.get("FunctionError")
        body = resp["Payload"].read().decode("utf-8")
        try:
            p = json.loads(body)
            info["invoke_response"] = json.loads(p["body"]) if p.get("body") else p
        except: info["invoke_raw"] = body[:500]
    except Exception as e: info["invoke_err"] = str(e)[:200]
    return info


def check_sidecar(key):
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key=key)
        body = obj["Body"].read()
        info = {"exists": True, "size_kb": round(len(body)/1024, 1),
                 "modified": obj["LastModified"].isoformat()[:19]}
        try:
            p = json.loads(body)
            info["top_keys"] = list(p.keys())[:20]
            # Sample some fields
            for k in ["vix_spot", "vix9d", "vix_9d", "vix_3m", "vix3m", "vix_6m",
                       "regime", "slope", "contango", "backwardation", "spot",
                       "vix", "term_structure", "version", "generated_at"]:
                if k in p: info[f"sample_{k}"] = p[k] if not isinstance(p[k], (list, dict)) else str(p[k])[:200]
        except Exception as e: info["parse_err"] = str(e)[:80]
        return info
    except s3.exceptions.NoSuchKey: return {"exists": False}
    except Exception as e: return {"err": str(e)[:200]}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    out["lambdas"] = {n: audit_lambda(n) for n in [
        "justhodl-vix-curve", "justhodl-vol-regime",
        "justhodl-volatility-squeeze-hunter"]}
    out["sidecars"] = {k: check_sidecar(k) for k in [
        "data/vix-curve.json", "data/vol-regime.json",
        "data/volatility-squeeze.json"]}
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
