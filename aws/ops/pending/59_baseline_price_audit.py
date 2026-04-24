#!/usr/bin/env python3
"""
The new bug: 497 outcomes computed but ALL have correct=None,
actual=UNKNOWN. Hypothesis: signal records were logged without
baseline_price, so score_directional() early-returns at the
'if not baseline_price' guard.

Verify by sampling justhodl-signals records — do they have
baseline_price set?
"""
import json
from collections import Counter
from ops_report import report
import boto3

REGION = "us-east-1"
ddb = boto3.resource("dynamodb", region_name=REGION)


with report("baseline_price_audit") as r:
    r.heading("Audit baseline_price coverage in justhodl-signals")

    table = ddb.Table("justhodl-signals")
    r.section("A. Sample 500 signals, count baseline_price coverage")

    resp = table.scan(Limit=500,
                      ProjectionExpression="signal_type, baseline_price, measure_against, predicted_direction")
    items = resp.get("Items", [])
    r.log(f"  Scanned {len(items)} signal records")

    # Counts
    by_type_total = Counter()
    by_type_with_baseline = Counter()
    sample_with = []
    sample_without = []

    for item in items:
        stype = item.get("signal_type", "?")
        bp = item.get("baseline_price")
        by_type_total[stype] += 1
        if bp not in (None, "", "0", 0):
            by_type_with_baseline[stype] += 1
            if len(sample_with) < 3:
                sample_with.append((stype, bp, item.get("measure_against")))
        else:
            if len(sample_without) < 3:
                sample_without.append((stype, item.get("measure_against"), item.get("predicted_direction")))

    r.log("\n  Signal types — baseline_price coverage:")
    for stype, total in sorted(by_type_total.items(), key=lambda x: -x[1]):
        with_bp = by_type_with_baseline.get(stype, 0)
        pct = 100 * with_bp / total if total else 0
        flag = "✓" if pct == 100 else "✗" if pct == 0 else "⚠"
        r.log(f"    {flag} {stype:30} {with_bp}/{total} ({pct:.0f}%)")

    r.log("\n  Sample WITH baseline:")
    for stype, bp, against in sample_with[:3]:
        r.log(f"    {stype}: bp={bp}, against={against}")

    r.log("\n  Sample WITHOUT baseline:")
    for stype, against, pred in sample_without[:3]:
        r.log(f"    {stype}: against={against}, predicted={pred}")

    total = len(items)
    with_bp = sum(by_type_with_baseline.values())
    pct = 100 * with_bp / total if total else 0
    r.log(f"\n  Overall: {with_bp}/{total} ({pct:.0f}%) signals have baseline_price")
    if pct < 50:
        r.fail(f"  CONFIRMED: most signals lack baseline_price — score_directional early-returns None")
    else:
        r.ok(f"  Coverage decent — different bug")

    r.kv(total_sampled=total, with_baseline=with_bp, coverage_pct=round(pct, 1))
    r.log("Done")
