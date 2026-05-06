"""Inventory: which Wave 1+2 signals exist on disk but aren't yet logging to DDB?

Cross-references:
  - Lambdas matching justhodl-* with 'signal'-flavored output
  - DDB justhodl-signals signal_types observed in last 30d
"""
import json
import boto3
from boto3.dynamodb.conditions import Attr
from datetime import datetime, timezone, timedelta
from collections import Counter
from ops_report import report

ddb = boto3.resource("dynamodb", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")

SIGNALS_TBL = ddb.Table("justhodl-signals")


def main():
    with report("inventory_signals_not_logging") as r:
        r.heading("Step 1 — DDB justhodl-signals: signal_types observed in last 30d")
        cutoff = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
        last_key = None
        types_observed = Counter()
        pages = 0
        while True:
            kw = {"Limit": 1000, "FilterExpression": Attr("logged_at").gte(cutoff)}
            if last_key:
                kw["ExclusiveStartKey"] = last_key
            resp = SIGNALS_TBL.scan(**kw)
            for it in resp.get("Items", []):
                types_observed[it.get("signal_type", "?")] += 1
            last_key = resp.get("LastEvaluatedKey")
            pages += 1
            if not last_key or pages > 8:
                break
        r.log(f"  {pages} pages, {sum(types_observed.values())} signals total in last 30d")
        for st, n in types_observed.most_common(40):
            r.log(f"    {st:36s} n={n}")

        r.heading("Step 2 — Wave 1+2 outputs that should be logging signals")
        wave1_outputs = {
            "data/earnings-tracker.json": "earnings_pead, earnings_drift",
            "data/short-interest.json": "squeeze_risk",
            "data/etf-flows.json": "etf_flow_extreme",
            "data/macro-surprise.json": "macro_composite_z",
            "data/yield-curve.json": "yc_regime",
            "data/historical-analogs.json": "analog_signal",
            "data/event-study.json": "event_signal",
            "data/correlation-surface.json": "corr_break",
            "data/auction-crisis.json": "auction_crisis_score",
            "data/eurodollar-stress.json": "eurodollar_stress",
            "data/sector-rotation.json": "sector_breadth",
            "data/momentum-scanner.json": "momentum_top_pick",
            "data/calibration-snapshot.json": "(meta — N/A)",
        }
        r.log(f"  {len(wave1_outputs)} S3 outputs to consider:")
        for key, signal_idea in wave1_outputs.items():
            try:
                obj = s3.head_object(Bucket="justhodl-dashboard-live", Key=key)
                size = obj["ContentLength"]
                last = obj["LastModified"]
                already_logging = signal_idea.split(",")[0].strip().split()[0] in str(types_observed)
                tag = "✅ logging" if already_logging else "⛔ NOT logging"
                r.log(f"    {key:42s} {size:>9,}b  → would log: {signal_idea}  {tag}")
            except Exception as e:
                r.log(f"    ✗ {key} {e}")

        r.heading("Step 3 — Mismatch summary")
        # Currently logged signal_types
        current_types = set(types_observed.keys())
        # Expected types from Wave 1+2
        expected = {
            "earnings_pead", "squeeze_risk", "etf_flow_extreme",
            "macro_composite_z", "yc_regime", "analog_signal",
            "event_signal", "corr_break", "auction_crisis_score",
            "eurodollar_stress", "sector_breadth", "momentum_top_pick",
        }
        missing = expected - current_types
        r.log(f"  expected new signal_types not yet logging: {len(missing)}")
        for m in sorted(missing):
            r.log(f"    • {m}")
        already = expected & current_types
        r.log(f"  expected and already logging: {len(already)}")
        for a in sorted(already):
            r.log(f"    ✓ {a}")


if __name__ == "__main__":
    main()
