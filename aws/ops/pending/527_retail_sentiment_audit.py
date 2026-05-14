#!/usr/bin/env python3
"""527 — Audit BUILD 9 (retail-sentiment) deployed state before patch/extend."""
import json, os, base64
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/527_retail_sentiment_audit.json"
NAME = "justhodl-retail-sentiment"
lam = boto3.client("lambda", region_name="us-east-1")
eb = boto3.client("events", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")
logs = boto3.client("logs", region_name="us-east-1")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # Lambda exists?
    try:
        cfg = lam.get_function_configuration(FunctionName=NAME)
        out["lambda"] = {
            "exists": True,
            "last_modified": cfg.get("LastModified"),
            "memory": cfg.get("MemorySize"),
            "timeout": cfg.get("Timeout"),
            "env_keys": sorted((cfg.get("Environment", {}) or {}).get("Variables", {}).keys()),
            "state": cfg.get("State"),
        }
    except lam.exceptions.ResourceNotFoundException:
        out["lambda"] = {"exists": False}

    # Rules
    if out["lambda"].get("exists"):
        rules = []
        for r in eb.list_rules()["Rules"]:
            try:
                ts = eb.list_targets_by_rule(Rule=r["Name"])["Targets"]
                if any(NAME in t.get("Arn", "") for t in ts):
                    rules.append({"name": r["Name"], "schedule": r.get("ScheduleExpression"),
                                    "state": r.get("State")})
            except: pass
        out["lambda"]["rules"] = rules

        # Recent log streams
        try:
            lg = f"/aws/lambda/{NAME}"
            streams = logs.describe_log_streams(logGroupName=lg, orderBy="LastEventTime",
                                                  descending=True, limit=3)["logStreams"]
            out["lambda"]["recent_logs"] = [{
                "name": s["logStreamName"][:50],
                "last_event": datetime.fromtimestamp(s["lastEventTimestamp"] / 1000,
                                                       tz=timezone.utc).isoformat()[:19]
                if s.get("lastEventTimestamp") else None
            } for s in streams[:3]]
        except: out["lambda"]["recent_logs"] = []

        # Invoke once to check it still works
        try:
            r = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse",
                            LogType="Tail", Payload=b"{}")
            out["invoke"] = {"status": r.get("StatusCode"), "fn_error": r.get("FunctionError")}
            body = r["Payload"].read().decode("utf-8")
            try:
                p = json.loads(body)
                out["invoke"]["response"] = json.loads(p["body"]) if p.get("body") else p
            except: out["invoke"]["raw"] = body[:1500]
            if r.get("LogResult"):
                out["invoke"]["log_tail"] = base64.b64decode(r["LogResult"]).decode("utf-8","replace")[-3500:]
        except Exception as e:
            out["invoke_err"] = str(e)[:300]

    # Sidecar check
    for key in ("data/retail-sentiment.json", "data/retail-sentiment-history.json"):
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key=key)
            body = obj["Body"].read()
            try:
                p = json.loads(body)
                top_keys = list(p.keys())[:20] if isinstance(p, dict) else []
                # Sample some interesting fields if dict
                info = {"size_kb": round(len(body)/1024, 1),
                          "modified": obj["LastModified"].isoformat()[:19],
                          "top_keys": top_keys}
                if isinstance(p, dict):
                    info["generated_at"] = p.get("generated_at")
                    info["version"] = p.get("version")
                    info["market_regime"] = p.get("market_regime")
                    info["regime_label"] = p.get("regime_label")
                    info["signal"] = p.get("signal")
                    info["n_tickers"] = p.get("n_tickers")
                    info["top_3"] = (p.get("top_30") or [])[:3]
                    info["sample_velocity_surges"] = (p.get("biggest_velocity_surges") or [])[:3]
                    info["sample_rank_climbers"] = (p.get("biggest_rank_climbers") or [])[:3]
                    info["most_bullish_tickers"] = (p.get("most_bullish_tickers") or [])[:3]
                    info["stocktwits_trending"] = (p.get("stocktwits_trending") or [])[:5]
                    info["subreddit_breakdown"] = p.get("subreddit_breakdown")
                out[key] = info
            except: out[key] = {"size_kb": round(len(body)/1024,1), "modified": obj["LastModified"].isoformat()[:19], "parse_err": True}
        except s3.exceptions.NoSuchKey:
            out[key] = {"exists": False}
        except Exception as e:
            out[key] = {"err": str(e)[:200]}

    # Page exists?
    try:
        obj = s3.head_object(Bucket="justhodl-dashboard-live", Key="retail/index.html")
        out["page"] = {"s3_exists": True, "size": obj["ContentLength"]}
    except: out["page"] = {"s3_exists": False}

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
