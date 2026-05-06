"""Probe screener_top_pick signal records in DDB justhodl-signals to verify
baseline_benchmark_price is the missing field.
"""
import json
from collections import Counter

import boto3
from boto3.dynamodb.conditions import Attr

from ops_report import report

REGION = "us-east-1"
ddb = boto3.resource("dynamodb", region_name=REGION)


def main():
    with report("probe_screener_signals") as r:
        r.heading("1) Sample 20 screener_top_pick signals from justhodl-signals")
        tbl = ddb.Table("justhodl-signals")
        items = []
        last_key = None
        pages = 0
        while True:
            kw = {
                "Limit": 1000,
                "FilterExpression": Attr("signal_type").eq("screener_top_pick"),
            }
            if last_key:
                kw["ExclusiveStartKey"] = last_key
            resp = tbl.scan(**kw)
            items.extend(resp.get("Items", []))
            last_key = resp.get("LastEvaluatedKey")
            pages += 1
            if not last_key or pages > 12 or len(items) >= 200:
                break
        r.log(f"  total screener_top_pick signal records: {len(items)} (after {pages} pages, capped at 200)")

        # Field presence summary
        baseline_present = Counter()
        bench_present    = Counter()
        bench_value      = Counter()
        baseline_zero    = 0
        for it in items:
            bp = it.get("baseline_price")
            bbp = it.get("baseline_benchmark_price")
            baseline_present["present" if bp is not None else "missing"] += 1
            bench_present["present" if bbp is not None else "missing"] += 1
            try:
                if float(bp or 0) == 0:
                    baseline_zero += 1
            except Exception:
                pass
            bench_value[str(bbp)[:30]] += 1

        r.log(f"")
        r.log(f"  baseline_price field:          {dict(baseline_present)}")
        r.log(f"  baseline_benchmark_price:      {dict(bench_present)}")
        r.log(f"  baseline_price == 0:           {baseline_zero}")
        r.log(f"")
        r.log(f"  baseline_benchmark_price values (top 5 most common):")
        for v, n in bench_value.most_common(5):
            r.log(f"    {v:30s}  n={n}")

        # Sample 5 records in detail
        r.log("")
        r.heading("2) Sample 5 individual records")
        for i, it in enumerate(items[:5]):
            r.log(f"  [{i}] signal_id={it.get('signal_id')}")
            for k in ["signal_type", "signal_value", "predicted_direction",
                      "measure_against", "benchmark", "baseline_price",
                      "baseline_benchmark_price", "logged_at", "status"]:
                v = it.get(k)
                r.log(f"      {k:32s} = {str(v)[:80]}")

        # Sample some metadata too
        r.heading("3) Date distribution of signal records (when were they logged?)")
        date_counts = Counter()
        for it in items:
            d = (it.get("logged_at") or "")[:10]
            date_counts[d] += 1
        for d, n in sorted(date_counts.items()):
            r.log(f"    {d}: {n}")


if __name__ == "__main__":
    main()
