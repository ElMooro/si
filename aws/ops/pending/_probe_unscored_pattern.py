"""Investigate the common failure mode across signals with correct=None outcomes.

For each target signal type:
  1. Pull a sample of correct=None outcomes
  2. For each, fetch the source SIGNAL record from justhodl-signals
  3. Inspect: predicted_dir, baseline_price, baseline_benchmark_price,
     outcome.{asset_price, benchmark_price}, current age vs window
  4. Determine: is it a baseline issue (like screener_top_pick was)? a predicted_dir
     issue (NEUTRAL not handled)? a benchmark missing? a get_price failure on the
     directional path?

Targets: edge_regime, carry_risk, market_phase, khalid_index, ml_risk, plumbing_stress
"""
import json
from collections import Counter

import boto3
from boto3.dynamodb.conditions import Attr

from ops_report import report

REGION = "us-east-1"
ddb = boto3.resource("dynamodb", region_name=REGION)

TARGETS = [
    "edge_regime",
    "carry_risk",
    "market_phase",
    "khalid_index",
    "ml_risk",
    "plumbing_stress",
    "momentum_tlt",
    "momentum_spy",
]


def main():
    sig_tbl = ddb.Table("justhodl-signals")
    out_tbl = ddb.Table("justhodl-outcomes")

    with report("probe_unscored_pattern") as r:
        for stype in TARGETS:
            r.heading(f"=== {stype} ===")

            # Pull all non-legacy outcomes
            outs = []
            last_key = None
            pages = 0
            while True:
                kw = {
                    "Limit": 1000,
                    "FilterExpression": Attr("signal_type").eq(stype) & Attr("is_legacy").ne(True),
                }
                if last_key:
                    kw["ExclusiveStartKey"] = last_key
                resp = out_tbl.scan(**kw)
                outs.extend(resp.get("Items", []))
                last_key = resp.get("LastEvaluatedKey")
                pages += 1
                if not last_key or pages > 10:
                    break

            none_outs = [o for o in outs if o.get("correct") is None]
            scored = [o for o in outs if o.get("correct") is not None]
            r.log(f"  total: {len(outs)}, scored: {len(scored)}, correct=None: {len(none_outs)}")

            # Distribution of windows for unscored
            window_dist = Counter(o.get("window_key") for o in none_outs)
            r.log(f"  unscored by window: {dict(window_dist)}")

            # Predicted direction distribution
            pred_dist = Counter(o.get("predicted_dir") for o in none_outs)
            r.log(f"  unscored predicted_dir: {dict(pred_dist)}")

            # Sample 3 correct=None outcomes + their source signals
            r.log(f"  Sample 3 correct=None outcomes + their source signals:")
            for i, o in enumerate(none_outs[:3]):
                r.log(f"  ── unscored outcome [{i}] ──")
                r.log(f"    outcome_id:    {o.get('outcome_id')}")
                r.log(f"    predicted_dir: {o.get('predicted_dir')}")
                r.log(f"    window_key:    {o.get('window_key')}")
                r.log(f"    correct:       {o.get('correct')}")
                inner = o.get("outcome") or {}
                for k in ["correct", "actual_direction", "return_pct",
                          "asset_price", "benchmark_price",
                          "price_at_signal", "price_at_check",
                          "excess_return", "checked_at"]:
                    if k in inner:
                        r.log(f"      outcome.{k:20s} = {str(inner.get(k))[:80]}")
                # Pull source signal
                sid = o.get("signal_id")
                if sid:
                    try:
                        sig = sig_tbl.get_item(Key={"signal_id": sid}).get("Item", {})
                        r.log(f"    source signal:")
                        for k in ["signal_type", "predicted_direction",
                                  "measure_against", "benchmark",
                                  "baseline_price", "baseline_benchmark_price",
                                  "logged_at", "status"]:
                            r.log(f"      {k:32s} = {str(sig.get(k))[:80]}")
                    except Exception as e:
                        r.log(f"    ✗ couldn't load signal: {e}")
                r.log("")

            # Compare: sample 1 SCORED outcome to see what worked
            if scored:
                r.log(f"  ── Sample 1 SCORED outcome for comparison ──")
                so = scored[0]
                r.log(f"    outcome_id:  {so.get('outcome_id')}")
                r.log(f"    correct:     {so.get('correct')}")
                inner = so.get("outcome") or {}
                for k in ["correct", "actual_direction", "return_pct", "excess_return",
                          "asset_price", "benchmark_price",
                          "price_at_signal", "price_at_check"]:
                    if k in inner:
                        r.log(f"      outcome.{k:20s} = {str(inner.get(k))[:80]}")
                sid = so.get("signal_id")
                if sid:
                    try:
                        sig = sig_tbl.get_item(Key={"signal_id": sid}).get("Item", {})
                        r.log(f"    source signal predicted_direction: {sig.get('predicted_direction')}")
                        r.log(f"    source signal baseline_price:      {sig.get('baseline_price')}")
                        r.log(f"    source signal baseline_benchmark:  {sig.get('baseline_benchmark_price')}")
                    except Exception:
                        pass

            r.log("")


if __name__ == "__main__":
    main()
