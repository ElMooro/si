"""Probe DDB outcomes for screener_top_pick to find why calibrator doesn't score it.

Hypothesis: 450 outcomes in 60d have correct=None (calibrator skips these).
"""
import json
from collections import Counter
from datetime import datetime, timezone, timedelta

import boto3
from boto3.dynamodb.conditions import Attr

from ops_report import report

REGION = "us-east-1"
ddb = boto3.resource("dynamodb", region_name=REGION)


def main():
    with report("probe_screener_top_pick") as r:
        r.heading("1) Count outcomes by correct-value for screener_top_pick (last 60d)")
        cutoff = (datetime.now(timezone.utc) - timedelta(days=60)).isoformat()
        tbl = ddb.Table("justhodl-outcomes")
        items = []
        last_key = None
        pages = 0
        while True:
            kw = {
                "Limit": 1000,
                "FilterExpression": (
                    Attr("checked_at").gte(cutoff)
                    & Attr("is_legacy").ne(True)
                    & Attr("signal_type").eq("screener_top_pick")
                ),
            }
            if last_key:
                kw["ExclusiveStartKey"] = last_key
            resp = tbl.scan(**kw)
            items.extend(resp.get("Items", []))
            last_key = resp.get("LastEvaluatedKey")
            pages += 1
            if not last_key or pages > 12:
                break
        r.log(f"  total screener_top_pick outcomes 60d: {len(items)} ({pages} pages)")

        # Distribution of correct field
        c_dist = Counter()
        for it in items:
            c_dist[str(it.get("correct"))] += 1
        r.log("")
        r.log("  Distribution of 'correct' field:")
        for k, v in c_dist.most_common():
            r.log(f"    correct={k:10s}  n={v}")

        # Sample a few items
        r.log("")
        r.log("  Sample 3 items:")
        for i, it in enumerate(items[:3]):
            r.log(f"  [{i}] keys: {sorted(it.keys())}")
            for k in ["signal_type", "predicted_dir", "correct", "outcome",
                      "logged_at", "checked_at", "window_key", "measure_against",
                      "baseline_price", "is_legacy"]:
                v = it.get(k)
                r.log(f"      {k:25s} = {str(v)[:120]}")

        # Now also check WITHOUT 60d filter — are there OLDER scored ones?
        r.heading("2) Total screener_top_pick (all-time, non-legacy)")
        all_items = []
        last_key = None
        pages = 0
        while True:
            kw = {
                "Limit": 1000,
                "FilterExpression": (
                    Attr("is_legacy").ne(True)
                    & Attr("signal_type").eq("screener_top_pick")
                ),
                "ProjectionExpression": "correct, logged_at",
            }
            if last_key:
                kw["ExclusiveStartKey"] = last_key
            resp = tbl.scan(**kw)
            all_items.extend(resp.get("Items", []))
            last_key = resp.get("LastEvaluatedKey")
            pages += 1
            if not last_key or pages > 30:
                break
        r.log(f"  total all-time non-legacy: {len(all_items)} ({pages} pages)")
        c2 = Counter()
        for it in all_items:
            c2[str(it.get("correct"))] += 1
        for k, v in c2.most_common():
            r.log(f"    correct={k:10s}  n={v}")

        # 3. Check what fields outcome.* has when correct is None
        r.heading("3) For correct=None records, inspect outcome dict")
        none_items = [it for it in items if it.get("correct") is None][:5]
        r.log(f"  Sample {len(none_items)} correct=None records:")
        for i, it in enumerate(none_items):
            outcome = it.get("outcome", {})
            r.log(f"  [{i}] outcome keys: {list(outcome.keys()) if outcome else 'EMPTY'}")
            r.log(f"      predicted_dir: {it.get('predicted_dir')}")
            r.log(f"      window_key: {it.get('window_key')}")
            r.log(f"      logged_at: {it.get('logged_at')}")
            r.log(f"      checked_at: {it.get('checked_at')}")
            for k, v in (outcome.items() if outcome else {}):
                r.log(f"        outcome.{k}: {str(v)[:60]}")


if __name__ == "__main__":
    main()
