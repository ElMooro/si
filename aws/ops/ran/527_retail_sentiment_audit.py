#!/usr/bin/env python3
"""527 — Audit justhodl-retail-sentiment before BUILD 9 finishing."""
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

    # Lambda state
    try:
        cfg = lam.get_function_configuration(FunctionName=NAME)
        out["lambda"] = {
            "exists": True,
            "last_modified": cfg.get("LastModified"),
            "memory": cfg.get("MemorySize"),
            "timeout": cfg.get("Timeout"),
            "state": cfg.get("State"),
            "env_keys": sorted((cfg.get("Environment", {}) or {}).get("Variables", {}).keys()),
            "description": cfg.get("Description", "")[:200],
        }
        # EventBridge rules
        rules = []
        for r in eb.list_rules()["Rules"]:
            try:
                ts = eb.list_targets_by_rule(Rule=r["Name"])["Targets"]
                if any(NAME in t.get("Arn", "") for t in ts):
                    rules.append({"name": r["Name"],
                                    "schedule": r.get("ScheduleExpression"),
                                    "state": r.get("State")})
            except: pass
        out["lambda"]["rules"] = rules
        # Recent logs
        try:
            streams = logs.describe_log_streams(logGroupName=f"/aws/lambda/{NAME}",
                                                  orderBy="LastEventTime", descending=True, limit=2)["logStreams"]
            out["lambda"]["recent_logs"] = [
                {"name": s["logStreamName"][:60],
                  "last": datetime.fromtimestamp(s["lastEventTimestamp"]/1000, tz=timezone.utc).isoformat()[:19] if s.get("lastEventTimestamp") else None}
                for s in streams]
        except Exception as e: out["lambda"]["log_err"] = str(e)[:100]
        # Invoke to test
        try:
            r = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse",
                            LogType="Tail", Payload=b"{}")
            out["lambda"]["invoke_status"] = r.get("StatusCode")
            out["lambda"]["fn_error"] = r.get("FunctionError")
            body = r["Payload"].read().decode("utf-8")
            try:
                p = json.loads(body)
                out["lambda"]["invoke_response"] = json.loads(p["body"]) if p.get("body") else p
            except: out["lambda"]["invoke_raw"] = body[:1500]
            if r.get("LogResult"):
                out["lambda"]["log_tail"] = base64.b64decode(r["LogResult"]).decode("utf-8","replace")[-2500:]
        except Exception as e:
            out["lambda"]["invoke_err"] = str(e)[:300]
    except lam.exceptions.ResourceNotFoundException:
        out["lambda"] = {"exists": False}

    # Sidecar
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/retail-sentiment.json")
        body = obj["Body"].read()
        p = json.loads(body)
        out["sidecar"] = {
            "size_kb": round(len(body)/1024, 1),
            "modified": obj["LastModified"].isoformat()[:19],
            "version": p.get("version"),
            "generated_at": p.get("generated_at"),
            "top_keys": list(p.keys())[:25],
            "market_regime": p.get("market_regime"),
            "n_tickers_tracked": (
                len(p.get("top_30") or p.get("top_tickers") or p.get("by_ticker") or [])
            ),
            "sample_top_5": (p.get("top_30") or p.get("top_tickers") or [])[:5],
            "biggest_velocity_surges_top_3": (p.get("biggest_velocity_surges") or [])[:3],
            "biggest_rank_climbers_top_3": (p.get("biggest_rank_climbers") or [])[:3],
            "most_bullish_top_3": (p.get("most_bullish_tickers") or [])[:3],
            "most_bearish_top_3": (p.get("most_bearish_tickers") or [])[:3],
            "stocktwits_trending_top_5": (p.get("stocktwits_trending") or [])[:5],
            "subreddit_breakdown_keys": list((p.get("subreddit_breakdown") or {}).keys()),
        }
    except s3.exceptions.NoSuchKey:
        out["sidecar"] = {"exists": False}
    except Exception as e:
        out["sidecar_err"] = str(e)[:200]

    # Sentiment page
    for key in ("sentiment/index.html", "social/index.html", "retail/index.html"):
        try:
            obj = s3.head_object(Bucket="justhodl-dashboard-live", Key=key)
            out[f"page_{key}"] = {"exists": True, "bytes": obj["ContentLength"]}
        except: pass

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
