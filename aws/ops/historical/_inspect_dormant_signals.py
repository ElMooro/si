"""Inspect why these specific signals never get scored."""
import json
import boto3
from boto3.dynamodb.conditions import Attr
from ops_report import report

ddb = boto3.resource("dynamodb", region_name="us-east-1")
sigs_tbl = ddb.Table("justhodl-signals")
out_tbl = ddb.Table("justhodl-outcomes")

DORMANT_SAMPLES = [
    "earnings_pead", "etf_rotation", "correlation_break",
    "crisis_index_kcfsi", "crisis_dfii10_vs_spy", "crisis_t10yie_extreme",
    "crisis_sloos_tighten", "crisis_ig_bbb_oas",
    "squeeze_risk", "yc_regime", "macro_composite_z",
    "sector_breadth", "momentum_top_pick",
]


def main():
    with report("inspect_dormant_signals") as r:
        for sig_type in DORMANT_SAMPLES:
            r.heading(f"sig_type = {sig_type}")
            try:
                # Get a few sample signals
                resp = sigs_tbl.scan(
                    FilterExpression=Attr("signal_type").eq(sig_type),
                    Limit=200,
                )
                items = resp.get("Items", [])
                if not items:
                    r.log("  (no items)")
                    continue
                r.log(f"  found {len(items)} signals")
                # Aggregate
                statuses = {}
                has_baseline = 0
                no_baseline = 0
                with_outcomes = 0
                no_check_ts = 0
                for item in items:
                    s = item.get("status", "unset")
                    statuses[s] = statuses.get(s, 0) + 1
                    if float(item.get("baseline_price") or 0) > 0:
                        has_baseline += 1
                    else:
                        no_baseline += 1
                    if item.get("outcomes"):
                        with_outcomes += 1
                    if not item.get("check_timestamps"):
                        no_check_ts += 1
                r.log(f"  statuses: {statuses}")
                r.log(f"  has baseline_price: {has_baseline} / no baseline: {no_baseline}")
                r.log(f"  has outcomes already: {with_outcomes}")
                r.log(f"  missing check_timestamps: {no_check_ts}")

                # Show sample item structure
                sample = items[0]
                r.log(f"  Sample fields: {sorted(sample.keys())}")
                r.log(f"    signal_value: {sample.get('signal_value')}")
                r.log(f"    predicted_direction: {sample.get('predicted_direction')}")
                r.log(f"    measure_against: {sample.get('measure_against')}")
                r.log(f"    benchmark: {sample.get('benchmark')}")
                r.log(f"    baseline_price: {sample.get('baseline_price')}")
                r.log(f"    baseline_benchmark_price: {sample.get('baseline_benchmark_price')}")
                r.log(f"    check_timestamps: {sample.get('check_timestamps')}")
                r.log(f"    timestamp: {sample.get('timestamp')}")
                r.log(f"    logged_at: {sample.get('logged_at')}")
                # Check if outcomes-checker has actually processed
                last_checked = sample.get('last_checked')
                r.log(f"    last_checked: {last_checked}")
            except Exception as e:
                r.log(f"  ✗ {e}")


if __name__ == "__main__":
    main()
