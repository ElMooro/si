#!/usr/bin/env python3
"""
Verify the 7 enriched call sites are now setting magnitude/rationale
on freshly-logged signals.

Looks at signals from last 5 minutes, breaks down by signal_type, and
confirms which have rationale and magnitude populated.
"""
from datetime import datetime, timezone, timedelta
from collections import Counter, defaultdict
from ops_report import report
import boto3

REGION = "us-east-1"
ddb = boto3.resource("dynamodb", region_name=REGION)


with report("verify_enriched_signals") as r:
    r.heading("Verify enriched signals — rationale and magnitude populated")

    table = ddb.Table("justhodl-signals")
    cutoff_ts = int((datetime.now(timezone.utc) - timedelta(minutes=10)).timestamp())

    from boto3.dynamodb.conditions import Attr
    resp = table.scan(
        FilterExpression=Attr("logged_epoch").gte(cutoff_ts),
        ProjectionExpression="signal_type, rationale, predicted_magnitude_pct, "
                              "predicted_target_price, baseline_price, predicted_direction",
    )
    items = resp.get("Items", [])
    while "LastEvaluatedKey" in resp:
        resp = table.scan(
            FilterExpression=Attr("logged_epoch").gte(cutoff_ts),
            ExclusiveStartKey=resp["LastEvaluatedKey"],
            ProjectionExpression="signal_type, rationale, predicted_magnitude_pct, "
                                  "predicted_target_price, baseline_price, predicted_direction",
        )
        items += resp.get("Items", [])

    r.log(f"  Signals from last 10 min: {len(items)}")
    if not items:
        r.warn("  No fresh signals found — logger may not have run yet")
        r.log("Done")
        raise SystemExit(0)

    # Per signal_type — has rationale, has magnitude
    types_total = Counter()
    types_with_rat = Counter()
    types_with_mag = Counter()
    types_with_target = Counter()
    samples = {}
    for item in items:
        st = item.get("signal_type", "?")
        types_total[st] += 1
        if item.get("rationale"):
            types_with_rat[st] += 1
        if item.get("predicted_magnitude_pct") is not None:
            types_with_mag[st] += 1
        if item.get("predicted_target_price") is not None:
            types_with_target[st] += 1
        if st not in samples and item.get("rationale"):
            samples[st] = item

    r.section("Coverage by signal_type")
    r.log(f"  {'signal_type':25} {'count':>5} {'rat':>5} {'mag':>5} {'tgt':>5}")
    for st, total in sorted(types_total.items(), key=lambda x: -x[1]):
        r.log(f"  {st:25} {total:>5} {types_with_rat.get(st,0):>5} "
              f"{types_with_mag.get(st,0):>5} {types_with_target.get(st,0):>5}")

    r.section("Sample rationales")
    for st, item in list(samples.items())[:5]:
        rat = item.get("rationale", "(none)")
        mag = item.get("predicted_magnitude_pct")
        tgt = item.get("predicted_target_price")
        bp = item.get("baseline_price")
        r.log(f"\n  {st}:")
        r.log(f"    baseline:  {bp}")
        r.log(f"    magnitude: {mag}")
        r.log(f"    target:    {tgt}")
        r.log(f"    rationale: {rat}")

    # Targeted check — the 7 enriched signal types should all have rationale
    enriched_types = ["momentum_gld", "momentum_spy", "momentum_uso", "khalid_index",
                      "crypto_fear_greed", "btc_mvrv", "plumbing_stress",
                      "cape_ratio", "buffett_indicator"]
    r.section("Specifically expected to have rationale (the 7 enriched + variants)")
    found_with_rat = 0
    for et in enriched_types:
        # Try exact match and pattern matches
        matching = [i for i in items if i.get("signal_type") == et]
        if matching:
            with_rat = sum(1 for i in matching if i.get("rationale"))
            flag = "✓" if with_rat == len(matching) else "⚠"
            r.log(f"  {flag} {et:30} {with_rat}/{len(matching)} have rationale")
            if with_rat == len(matching):
                found_with_rat += 1
        else:
            r.log(f"  - {et:30} (not in this batch — fires conditionally)")

    r.kv(
        fresh_signals=len(items),
        signal_types=len(types_total),
        types_with_any_rationale=sum(1 for st in types_total if types_with_rat.get(st,0) > 0),
        enriched_types_landing=found_with_rat,
    )
    r.log("Done")
