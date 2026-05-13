#!/usr/bin/env python3
"""513 — Audit 13F-positions + sec-13f Lambdas. Before building, check:
- Are they deployed?
- Are they scheduled?
- Do they produce S3 output?
- Does the output match what's in the repo?
"""
import json, os, base64
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/513_13f_audit.json"
lam = boto3.client("lambda", region_name="us-east-1")
eb = boto3.client("events", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")
logs = boto3.client("logs", region_name="us-east-1")


def audit_lambda(name):
    info = {"name": name}
    try:
        cfg = lam.get_function_configuration(FunctionName=name)
        info["exists"] = True
        info["runtime"] = cfg.get("Runtime")
        info["last_modified"] = cfg.get("LastModified")
        info["memory"] = cfg.get("MemorySize")
        info["timeout"] = cfg.get("Timeout")
        info["state"] = cfg.get("State")
        info["env_keys"] = sorted((cfg.get("Environment", {}) or {}).get("Variables", {}).keys())
    except lam.exceptions.ResourceNotFoundException:
        info["exists"] = False
        return info
    except Exception as e:
        info["err"] = str(e)[:200]
        return info

    # Find any EventBridge rules pointing at this Lambda
    rules = []
    try:
        paginator = eb.get_paginator("list_rule_names_by_target")
        # Simpler: list_rules + check each
        all_rules = eb.list_rules()["Rules"]
        for r in all_rules:
            try:
                targets = eb.list_targets_by_rule(Rule=r["Name"])["Targets"]
                if any(name in t.get("Arn", "") for t in targets):
                    rules.append({"name": r["Name"], "schedule": r.get("ScheduleExpression"),
                                    "state": r.get("State")})
            except Exception:
                pass
    except Exception as e:
        info["rule_scan_err"] = str(e)[:100]
    info["eventbridge_rules"] = rules

    # CloudWatch log group recent invocations
    try:
        lg = f"/aws/lambda/{name}"
        streams = logs.describe_log_streams(logGroupName=lg, orderBy="LastEventTime",
                                              descending=True, limit=3)["logStreams"]
        info["recent_log_streams"] = [{
            "name": s["logStreamName"],
            "last_event": datetime.fromtimestamp(s["lastEventTimestamp"] / 1000,
                                                   tz=timezone.utc).isoformat()[:19]
            if s.get("lastEventTimestamp") else None,
        } for s in streams[:3]]
    except logs.exceptions.ResourceNotFoundException:
        info["recent_log_streams"] = "no log group"
    except Exception as e:
        info["log_err"] = str(e)[:100]

    return info


def check_sidecar(key):
    info = {"key": key}
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key=key)
        body = obj["Body"].read()
        info["exists"] = True
        info["size_kb"] = round(len(body) / 1024, 1)
        info["modified"] = obj["LastModified"].isoformat()[:19]
        try:
            p = json.loads(body)
            info["top_keys"] = list(p.keys())[:20]
            info["generated_at"] = p.get("generated_at")
            info["version"] = p.get("version")
            # Sample some counts
            if "by_fund" in p:
                info["n_funds"] = len(p.get("by_fund") or {})
            if "aggregate_by_ticker" in p:
                info["n_tickers"] = len(p.get("aggregate_by_ticker") or {})
            if "filings" in p:
                info["n_filings"] = len(p.get("filings") or [])
        except: pass
    except s3.exceptions.NoSuchKey:
        info["exists"] = False
    except Exception as e:
        info["err"] = str(e)[:200]
    return info


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    out["lambdas"] = {
        "justhodl-13f-positions": audit_lambda("justhodl-13f-positions"),
        "justhodl-sec-13f": audit_lambda("justhodl-sec-13f"),
    }
    out["sidecars"] = {
        "data/13f-positions.json": check_sidecar("data/13f-positions.json"),
        "data/sec-13f.json": check_sidecar("data/sec-13f.json"),
        "data/13f-filings.json": check_sidecar("data/13f-filings.json"),
    }

    # If 13f-positions exists, try a synchronous invoke to see if it works
    if out["lambdas"]["justhodl-13f-positions"].get("exists"):
        try:
            resp = lam.invoke(FunctionName="justhodl-13f-positions",
                                InvocationType="RequestResponse",
                                LogType="Tail", Payload=b"{}")
            out["invoke"] = {"status": resp.get("StatusCode"), "fn_error": resp.get("FunctionError")}
            body = resp["Payload"].read().decode("utf-8")
            try:
                p = json.loads(body)
                out["invoke"]["response"] = json.loads(p["body"]) if p.get("body") else p
            except: out["invoke"]["raw"] = body[:1500]
            if resp.get("LogResult"):
                out["invoke"]["log_tail"] = base64.b64decode(resp["LogResult"]).decode("utf-8", "replace")[-2500:]
        except Exception as e:
            out["invoke_err"] = str(e)[:300]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
