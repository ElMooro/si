#!/usr/bin/env python3
"""528 — Add EventBridge cron(0,30 * * * ? *) to retail-sentiment Lambda."""
import json, os
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/528_retail_schedule.json"
NAME = "justhodl-retail-sentiment"
SCHEDULE = "cron(0,30 * ? * * *)"  # every 30 minutes

lam = boto3.client("lambda", region_name="us-east-1")
eb = boto3.client("events", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    RULE = f"{NAME}-30min"
    try:
        eb.put_rule(Name=RULE, ScheduleExpression=SCHEDULE, State="ENABLED",
                     Description="Retail sentiment refresh every 30 minutes (ApeWisdom + StockTwits)")
        arn = lam.get_function(FunctionName=NAME)["Configuration"]["FunctionArn"]
        eb.put_targets(Rule=RULE, Targets=[{"Id": "1", "Arn": arn}])
        try:
            lam.add_permission(FunctionName=NAME, StatementId=f"{NAME}-eb-permit",
                Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                SourceArn=eb.describe_rule(Name=RULE)["Arn"])
        except lam.exceptions.ResourceConflictException: pass
        out["schedule"] = SCHEDULE
        out["rule_state"] = "ENABLED"
    except Exception as e:
        out["schedule_err"] = str(e)[:300]

    # Re-read sidecar to confirm it's fresh
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/retail-sentiment.json")
        body = obj["Body"].read()
        p = json.loads(body)
        # Capture top 5 mentions + top 3 velocity surges + top 3 rank climbers
        top_30 = p.get("top_30_by_mentions") or []
        ranked = p.get("ranked") or {}
        out["sidecar"] = {
            "size_kb": round(len(body) / 1024, 1),
            "modified": obj["LastModified"].isoformat()[:19],
            "version": p.get("version"),
            "generated_at": p.get("generated_at"),
            "sources": p.get("sources"),
            "market_regime": p.get("market_regime"),
            "market_regime_signal": p.get("market_regime_signal"),
            "subreddit_breakdown": p.get("subreddit_breakdown"),
            "n_with_stwt_data": p.get("n_with_stwt_data"),
            "top_5_by_mentions": [
                {"ticker": (x.get("ticker") or x.get("symbol")),
                  "mentions": x.get("mentions") or x.get("count"),
                  "rank": x.get("rank"),
                  "rank_24h_ago": x.get("rank_24h_ago"),
                  "rank_climb": x.get("rank_climb"),
                  "stwt_bull_pct": x.get("stwt_bull_pct") or x.get("stocktwits_bull_pct"),
                  "stwt_msg_count": x.get("stwt_message_count") or x.get("stocktwits_messages")}
                for x in top_30[:5]
            ],
            "velocity_surges_top_3": [
                {"ticker": x.get("ticker") or x.get("symbol"),
                  "mentions": x.get("mentions") or x.get("count"),
                  "velocity_pct": x.get("velocity_pct") or x.get("surge_pct"),
                  "stwt_bull_pct": x.get("stwt_bull_pct")}
                for x in (ranked.get("velocity_surges") or [])[:3]
            ],
            "rank_climbers_top_3": [
                {"ticker": x.get("ticker") or x.get("symbol"),
                  "rank": x.get("rank"), "prev_rank": x.get("rank_24h_ago"),
                  "climb": x.get("rank_climb")}
                for x in (ranked.get("rank_climbers") or [])[:3]
            ],
            "new_entrants_top_3": [
                {"ticker": x.get("ticker") or x.get("symbol"),
                  "mentions": x.get("mentions") or x.get("count")}
                for x in (ranked.get("new_entrants") or [])[:3]
            ],
            "stocktwits_trending_top_5": (p.get("stocktwits_trending") or [])[:5],
        }
    except Exception as e:
        out["sidecar_err"] = str(e)[:300]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
