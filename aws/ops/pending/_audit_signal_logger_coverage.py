"""Check which Wave 1 signals (earnings, short-int, etf-flows, macro-surprise, yield-curve)
are being logged into DDB justhodl-signals so the calibrator can score them."""
import json
import boto3
from collections import Counter
from datetime import datetime, timezone, timedelta
from boto3.dynamodb.conditions import Attr
from ops_report import report

ddb = boto3.resource("dynamodb", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")
events = boto3.client("events", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")


def main():
    with report("audit_signal_logger_coverage") as r:
        # 1. What's in DDB right now (last 14d)
        r.heading("DDB justhodl-signals — last 14 days, by signal_type")
        cutoff = (datetime.now(timezone.utc) - timedelta(days=14)).isoformat()
        tbl = ddb.Table("justhodl-signals")
        types = Counter()
        last_key = None; pages = 0
        while True:
            kw = {"Limit": 1000, "FilterExpression": Attr("logged_at").gte(cutoff)}
            if last_key: kw["ExclusiveStartKey"] = last_key
            resp = tbl.scan(**kw)
            for it in resp.get("Items", []):
                types[it.get("signal_type", "?")] += 1
            last_key = resp.get("LastEvaluatedKey")
            pages += 1
            if not last_key or pages > 8: break

        for t, n in types.most_common(30):
            r.log(f"  {t:35s} n={n}")

        # 2. What Wave 1 signals SHOULD be tracked but aren't?
        r.heading("Wave 1 signal candidates (for calibration scoring)")
        candidates = {
            "earnings_pead":           "data/earnings-tracker.json → pead_signals[].signal",
            "squeeze_risk":            "data/short-interest.json → top_squeeze_risk[]",
            "etf_flow_extreme":        "data/etf-flows.json → by_category[*].signal HEAVY_*",
            "macro_surprise_z":        "data/macro-surprise.json → composite > 2σ",
            "yc_regime":               "data/yield-curve.json → regime",
            "correlation_break":       "data/correlation-surface.json → regime_breaks[]",
            "auction_crisis_score":    "data/auction-crisis.json → composite_score > 60",
            "eurodollar_stress":       "data/eurodollar-stress.json → composite > 70",
            "sector_breadth":          "data/sector-rotation.json → market_breadth",
            "momentum_top_pick":       "data/momentum-scanner.json → top composite > 95",
            "historical_analog":       "data/historical-analogs.json (mean fwd return)",
            "event_study":             "data/event-study.json (vix_spike, fomc, etc)",
            "divergence_extreme":      "divergence/current.json → residual_z >2.5",
            "cot_extreme":             "data/cot-extremes.json → percentile <5 or >95",
        }
        for sname, path in candidates.items():
            if sname in types:
                r.log(f"  ✓ {sname:30s} ALREADY logged ({types[sname]}× in 14d)")
            else:
                r.log(f"  ✗ {sname:30s} NOT logged   ← from {path}")


if __name__ == "__main__":
    main()
