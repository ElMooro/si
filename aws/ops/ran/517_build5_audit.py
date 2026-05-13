#!/usr/bin/env python3
"""517 — Audit BUILD 5 (earnings-sentiment) deployed state + sidecars."""
import json, os
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/517_build5_audit.json"
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
    except lam.exceptions.ResourceNotFoundException:
        info["exists"] = False
        return info

    rules = []
    for r in eb.list_rules()["Rules"]:
        try:
            targets = eb.list_targets_by_rule(Rule=r["Name"])["Targets"]
            if any(name in t.get("Arn", "") for t in targets):
                rules.append({"name": r["Name"], "schedule": r.get("ScheduleExpression"),
                                "state": r.get("State")})
        except: pass
    info["rules"] = rules
    return info


def check_sidecar(key):
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key=key)
        body = obj["Body"].read()
        info = {"exists": True, "size_kb": round(len(body)/1024, 1),
                 "modified": obj["LastModified"].isoformat()[:19]}
        try:
            p = json.loads(body)
            info["top_keys"] = list(p.keys())[:15]
            info["generated_at"] = p.get("generated_at")
            if "transcripts" in p:
                ts = p.get("transcripts") or []
                info["n_transcripts"] = len(ts)
                info["sample_3_transcripts"] = [
                    {"symbol": t.get("symbol"), "date": t.get("transcript_date"),
                      "sentiment": t.get("overall_sentiment"),
                      "confidence": t.get("confidence_score"),
                      "guidance": t.get("forward_guidance"),
                      "summary": (t.get("one_line_summary") or "")[:120]}
                    for t in ts[-3:]
                ]
            if "summary" in p:
                info["summary"] = p.get("summary")
        except Exception as e:
            info["parse_err"] = str(e)[:100]
        return info
    except s3.exceptions.NoSuchKey:
        return {"exists": False}
    except Exception as e:
        return {"err": str(e)[:200]}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    out["lambdas"] = {
        n: audit_lambda(n) for n in [
            "justhodl-earnings-sentiment",
            "justhodl-earnings-tracker",
            "justhodl-earnings-pead",
            "justhodl-earnings-whisper",
        ]
    }
    out["sidecars"] = {
        "screener/earnings-sentiment.json": check_sidecar("screener/earnings-sentiment.json"),
        "data/earnings-tracker.json": check_sidecar("data/earnings-tracker.json"),
        "data/earnings-pead.json": check_sidecar("data/earnings-pead.json"),
    }
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
