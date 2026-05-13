#!/usr/bin/env python3
"""517 — Audit vix-curve Lambda before building/upgrading BUILD 5."""
import json, os, base64
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/517_vix_curve_audit.json"
lam = boto3.client("lambda", region_name="us-east-1")
eb = boto3.client("events", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")
logs = boto3.client("logs", region_name="us-east-1")


def audit(name):
    info = {"name": name}
    try:
        cfg = lam.get_function_configuration(FunctionName=name)
        info["exists"] = True
        info["last_modified"] = cfg.get("LastModified")
        info["memory"] = cfg.get("MemorySize")
        info["timeout"] = cfg.get("Timeout")
        info["env_keys"] = sorted((cfg.get("Environment", {}) or {}).get("Variables", {}).keys())
        info["description"] = cfg.get("Description", "")[:200]
    except lam.exceptions.ResourceNotFoundException:
        info["exists"] = False
        return info

    rules = []
    for r in eb.list_rules()["Rules"]:
        try:
            ts = eb.list_targets_by_rule(Rule=r["Name"])["Targets"]
            if any(name in t.get("Arn", "") for t in ts):
                rules.append({"name": r["Name"],
                                "schedule": r.get("ScheduleExpression"),
                                "state": r.get("State")})
        except: pass
    info["rules"] = rules

    try:
        lg = f"/aws/lambda/{name}"
        streams = logs.describe_log_streams(logGroupName=lg, orderBy="LastEventTime",
                                              descending=True, limit=2)["logStreams"]
        info["recent_logs"] = [{"name": s["logStreamName"][:50],
                                  "last": datetime.fromtimestamp(s["lastEventTimestamp"]/1000, tz=timezone.utc).isoformat()[:19] if s.get("lastEventTimestamp") else None}
                                 for s in streams]
    except: info["recent_logs"] = []

    # Invoke once to check it still works
    try:
        r = lam.invoke(FunctionName=name, InvocationType="RequestResponse",
                        LogType="Tail", Payload=b"{}")
        info["invoke_status"] = r.get("StatusCode")
        info["fn_error"] = r.get("FunctionError")
        body = r["Payload"].read().decode("utf-8")
        try:
            p = json.loads(body)
            info["response"] = json.loads(p["body"]) if p.get("body") else p
        except: info["raw"] = body[:1200]
        if r.get("LogResult"):
            info["log_tail"] = base64.b64decode(r["LogResult"]).decode("utf-8","replace")[-2000:]
    except Exception as e:
        info["invoke_err"] = str(e)[:300]

    return info


def check_sidecar(key):
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key=key)
        body = obj["Body"].read()
        info = {"exists": True, "size_kb": round(len(body)/1024,1),
                 "modified": obj["LastModified"].isoformat()[:19]}
        try:
            p = json.loads(body)
            info["version"] = p.get("version")
            info["generated_at"] = p.get("generated_at")
            info["top_keys"] = list(p.keys())[:25]
            # Sample the per-index data
            for k in ("vix9d","vix","vix3m","vix6m","vvix","vxn","rvx"):
                if k in p:
                    info[f"{k}_sample"] = p[k] if isinstance(p[k], (int, float)) else (p[k] if not isinstance(p[k], dict) else {kk: p[k][kk] for kk in list(p[k].keys())[:5]})
            for k in ("composite_regime","composite_signal","slope","slope_3m_spot","spread_9d_30d","spread_30d_3m","spread_3m_6m","spreads","regimes"):
                if k in p: info[k] = p[k]
        except Exception as e: info["parse_err"] = str(e)[:100]
        return info
    except s3.exceptions.NoSuchKey:
        return {"exists": False}
    except Exception as e:
        return {"err": str(e)[:200]}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    out["vix_lambdas"] = {
        "justhodl-vix-curve": audit("justhodl-vix-curve"),
        "justhodl-vol-regime": audit("justhodl-vol-regime"),
    }
    out["sidecars"] = {
        "data/vix-curve.json": check_sidecar("data/vix-curve.json"),
        "data/vix-curve-history.json": check_sidecar("data/vix-curve-history.json"),
        "data/vol-regime.json": check_sidecar("data/vol-regime.json"),
    }

    # Page exists?
    try:
        obj = s3.head_object(Bucket="justhodl-dashboard-live", Key="vix/index.html")
        out["vix_page"] = {"exists": True, "size": obj["ContentLength"]}
    except:
        out["vix_page"] = {"exists": False}

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
