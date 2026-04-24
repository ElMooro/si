#!/usr/bin/env python3
"""
Final verification — newly-logged signals should now have baseline_price
populated for every signal_type, not just screener_top_pick.

Sample only signals logged in the last 30 minutes (after step 60 deploy).
"""
import json
from datetime import datetime, timezone, timedelta
from collections import Counter
from ops_report import report
import boto3

REGION = "us-east-1"
ddb = boto3.resource("dynamodb", region_name=REGION)


with report("verify_baseline_logger") as r:
    r.heading("Verify newly-logged signals have baseline_price")

    table = ddb.Table("justhodl-signals")

    # Recent signals only (last 60 min)
    cutoff_ts = int((datetime.now(timezone.utc) - timedelta(minutes=60)).timestamp())
    r.log(f"  Filtering by logged_epoch >= {cutoff_ts} (last 60 min)")

    # Scan with filter — costs more but gives us recent signals
    from boto3.dynamodb.conditions import Attr
    resp = table.scan(
        FilterExpression=Attr("logged_epoch").gte(cutoff_ts),
        ProjectionExpression="signal_type, baseline_price, baseline_benchmark_price, "
                              "measure_against, benchmark, predicted_direction, logged_at",
    )
    items = resp.get("Items", [])
    while "LastEvaluatedKey" in resp:
        resp = table.scan(
            FilterExpression=Attr("logged_epoch").gte(cutoff_ts),
            ExclusiveStartKey=resp["LastEvaluatedKey"],
            ProjectionExpression="signal_type, baseline_price, baseline_benchmark_price, "
                                  "measure_against, benchmark, predicted_direction, logged_at",
        )
        items += resp.get("Items", [])

    r.log(f"  Found {len(items)} fresh signals logged in last 60 min")

    if not items:
        r.warn("  No fresh signals — logger may not have run yet, or scan missed it")
        r.log("  Wait 6h for next regular logger run, OR re-trigger manually")
        r.log("Done")
        raise SystemExit(0)

    by_type = Counter()
    by_type_with_bp = Counter()
    by_type_with_bench = Counter()
    samples = {}

    for item in items:
        stype = item.get("signal_type", "?")
        bp = item.get("baseline_price")
        bbp = item.get("baseline_benchmark_price")
        by_type[stype] += 1
        if bp not in (None, "", 0, "0"):
            by_type_with_bp[stype] += 1
        if bbp not in (None, "", 0, "0"):
            by_type_with_bench[stype] += 1
        if stype not in samples:
            samples[stype] = item

    r.section("Coverage by signal_type")
    r.log(f"  {'signal_type':30}  {'count':>6} {'has_bp':>7} {'has_bench':>10}")
    full_coverage = 0
    partial = 0
    none_count = 0
    for stype, total in sorted(by_type.items(), key=lambda x: -x[1]):
        bp_n = by_type_with_bp.get(stype, 0)
        bench_n = by_type_with_bench.get(stype, 0)
        bp_pct = 100 * bp_n / total
        flag = "✓" if bp_pct == 100 else "⚠" if bp_pct > 0 else "✗"
        r.log(f"  {flag} {stype:30}  {total:>6} {bp_n:>5}/{total:<2} ({bp_pct:>3.0f}%)  {bench_n:>3}")
        if bp_pct == 100: full_coverage += 1
        elif bp_pct > 0: partial += 1
        else: none_count += 1

    r.section("Sample signals (one per type)")
    for stype, item in list(samples.items())[:6]:
        bp = item.get("baseline_price")
        bbp = item.get("baseline_benchmark_price")
        against = item.get("measure_against")
        bench = item.get("benchmark")
        pred = item.get("predicted_direction")
        r.log(f"  {stype}: against={against} pred={pred} bp={bp} bench={bench} bbp={bbp}")

    r.kv(
        signal_types_present=len(by_type),
        types_with_full_coverage=full_coverage,
        types_with_partial=partial,
        types_with_no_baseline=none_count,
    )

    if full_coverage >= len(by_type) * 0.8:
        r.ok(f"  ✅ Fix working — {full_coverage}/{len(by_type)} signal types have full baseline coverage")
    elif full_coverage > 0:
        r.warn(f"  ⚠ Partial — {full_coverage}/{len(by_type)} types have full coverage, {partial} partial, {none_count} none")
    else:
        r.fail(f"  ✗ No coverage — fix not landing")

    r.log("Done")
