"""Investigate the 7 high-weight zero-outcome signals.

Check for each:
  1. Count in justhodl-signals (is it being written?)
  2. Count in justhodl-outcomes (is it being scored?)
  3. Distribution of correct field (None vs True vs False)
  4. Sample one record to see the schema/predicted_dir
  5. Date distribution (when written)

Targets:
  valuation_composite, cftc_gold, cftc_spx, cftc_bitcoin, cape_ratio,
  buffett_indicator, cftc_crude

Also includes diagnostic: scan all signal_types in both tables to find any
others sitting in the same hidden state.
"""
import json
from collections import Counter
from datetime import datetime, timezone, timedelta

import boto3
from boto3.dynamodb.conditions import Attr

from ops_report import report

REGION = "us-east-1"
ddb = boto3.resource("dynamodb", region_name=REGION)

TARGETS = [
    "valuation_composite", "cftc_gold", "cftc_spx", "cftc_bitcoin",
    "cape_ratio", "buffett_indicator", "cftc_crude",
]


def scan_signals(stype):
    tbl = ddb.Table("justhodl-signals")
    items = []
    last_key = None
    pages = 0
    while True:
        kw = {"Limit": 1000, "FilterExpression": Attr("signal_type").eq(stype)}
        if last_key:
            kw["ExclusiveStartKey"] = last_key
        resp = tbl.scan(**kw)
        items.extend(resp.get("Items", []))
        last_key = resp.get("LastEvaluatedKey")
        pages += 1
        if not last_key or pages > 12:
            break
    return items


def scan_outcomes(stype):
    tbl = ddb.Table("justhodl-outcomes")
    items = []
    last_key = None
    pages = 0
    while True:
        kw = {"Limit": 1000, "FilterExpression": Attr("signal_type").eq(stype) & Attr("is_legacy").ne(True)}
        if last_key:
            kw["ExclusiveStartKey"] = last_key
        resp = tbl.scan(**kw)
        items.extend(resp.get("Items", []))
        last_key = resp.get("LastEvaluatedKey")
        pages += 1
        if not last_key or pages > 12:
            break
    return items


def main():
    with report("probe_zero_outcome_signals") as r:
        for stype in TARGETS:
            r.heading(f"=== {stype} ===")
            sigs = scan_signals(stype)
            outs = scan_outcomes(stype)
            r.log(f"  signals  in DDB: {len(sigs)}")
            r.log(f"  outcomes in DDB: {len(outs)}")

            if sigs:
                # Date distribution
                dates = Counter()
                preds = Counter()
                statuses = Counter()
                bench_present = Counter()
                baseline_present = Counter()
                for s in sigs:
                    dates[(s.get("logged_at") or "")[:10]] += 1
                    preds[s.get("predicted_direction")] += 1
                    statuses[s.get("status")] += 1
                    bench_present["present" if s.get("baseline_benchmark_price") is not None else "missing"] += 1
                    baseline_present["present" if s.get("baseline_price") is not None else "missing"] += 1
                r.log(f"  date counts: {dict(sorted(dates.items())[-5:])}")
                r.log(f"  predicted_dir distribution: {dict(preds)}")
                r.log(f"  status distribution: {dict(statuses)}")
                r.log(f"  baseline_price: {dict(baseline_present)}")
                r.log(f"  baseline_benchmark_price: {dict(bench_present)}")

                # Sample one
                s = sigs[0]
                r.log(f"  sample[0] keys: {sorted(s.keys())}")
                for k in ["signal_type", "signal_value", "predicted_direction",
                          "measure_against", "benchmark", "baseline_price",
                          "baseline_benchmark_price", "logged_at",
                          "status", "check_windows"]:
                    v = s.get(k)
                    r.log(f"      {k:32s} = {str(v)[:80]}")

            if outs:
                c_dist = Counter()
                for o in outs:
                    c_dist[str(o.get("correct"))] += 1
                r.log(f"  outcome.correct distribution: {dict(c_dist)}")

            r.log("")

        # Now scan ALL signal types in both tables for full inventory
        r.heading("Bonus — all signal_types in DDB, all-time")
        sig_types = Counter()
        out_types = Counter()
        out_correct = {}  # stype -> Counter(correct)

        # Scan all signals
        tbl = ddb.Table("justhodl-signals")
        last_key = None
        pages = 0
        while True:
            kw = {"Limit": 1000, "ProjectionExpression": "signal_type"}
            if last_key:
                kw["ExclusiveStartKey"] = last_key
            resp = tbl.scan(**kw)
            for it in resp.get("Items", []):
                sig_types[it.get("signal_type", "?")] += 1
            last_key = resp.get("LastEvaluatedKey")
            pages += 1
            if not last_key or pages > 30:
                break

        # Scan all outcomes (non-legacy)
        tbl = ddb.Table("justhodl-outcomes")
        last_key = None
        pages2 = 0
        while True:
            kw = {
                "Limit": 1000,
                "FilterExpression": Attr("is_legacy").ne(True),
                "ProjectionExpression": "signal_type, correct",
            }
            if last_key:
                kw["ExclusiveStartKey"] = last_key
            resp = tbl.scan(**kw)
            for it in resp.get("Items", []):
                stype = it.get("signal_type", "?")
                out_types[stype] += 1
                if stype not in out_correct:
                    out_correct[stype] = Counter()
                out_correct[stype][str(it.get("correct"))] += 1
            last_key = resp.get("LastEvaluatedKey")
            pages2 += 1
            if not last_key or pages2 > 30:
                break

        r.log(f"  signals scanned: {sum(sig_types.values())}, distinct types: {len(sig_types)}")
        r.log(f"  outcomes scanned (non-legacy): {sum(out_types.values())}, distinct types: {len(out_types)}")
        r.log("")

        # Combined view sorted by signal count
        all_types = set(sig_types.keys()) | set(out_types.keys())
        rows = []
        for t in all_types:
            sigs_n = sig_types.get(t, 0)
            outs_n = out_types.get(t, 0)
            cc = out_correct.get(t, Counter())
            scored_n = cc.get("True", 0) + cc.get("False", 0)
            none_n = cc.get("None", 0)
            scored_pct = (scored_n / outs_n * 100) if outs_n else 0
            rows.append((t, sigs_n, outs_n, scored_n, none_n, scored_pct))
        rows.sort(key=lambda x: -x[1])
        r.log("  signal_type                            sigs  outs  scored  none  scored%")
        r.log("  " + "─" * 80)
        for t, sigs_n, outs_n, scored_n, none_n, pct in rows:
            badge = "★" if outs_n > 50 and pct < 50 else " "
            r.log(f"  {badge} {t:38s}  {sigs_n:>5}  {outs_n:>4}  {scored_n:>5}  {none_n:>4}   {pct:>5.1f}%")


if __name__ == "__main__":
    main()
