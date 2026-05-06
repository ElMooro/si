"""Inventory data sources we can aggregate into a 'what's happening' feed."""
import json, boto3
from ops_report import report

s3 = boto3.client("s3", region_name="us-east-1")


def main():
    with report("inventory_feed_sources") as r:
        candidates = [
            "data/insiders-recent.json",
            "data/insider-transactions.json",
            "data/insider-trades.json",
            "data/13f-changes.json",
            "data/13f.json",
            "data/13f-aggregated.json",
            "data/earnings-tracker.json",
            "data/short-interest.json",
            "data/etf-flows.json",
            "data/cot-extremes.json",
            "data/divergence-current.json",
            "divergence/current.json",
            "data/correlation-surface.json",
            "data/auction-crisis.json",
            "data/eurodollar-stress.json",
            "data/whats-changed.json",
            "data/macro-surprise.json",
            "data/yield-curve.json",
            "data/alert-history.json",
            "data/historical-analogs.json",
            "data/event-study.json",
            "data/ab-test-results.json",
        ]
        for k in candidates:
            try:
                obj = s3.head_object(Bucket="justhodl-dashboard-live", Key=k)
                r.ok(f"  ✓ {k:40s} {obj['ContentLength']:>9,}b  modified={obj['LastModified'].isoformat()}")
            except Exception:
                r.log(f"  ✗ {k}  not found")

        # Also list anything with "insider" or "13f" in the name
        r.heading("All keys with 'insider' or '13f'")
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket="justhodl-dashboard-live"):
            for it in page.get("Contents", []):
                k = it["Key"].lower()
                if "insider" in k or "13f" in k:
                    r.log(f"  {it['Key']:60s} {it['Size']:>9,}b")


if __name__ == "__main__":
    main()
