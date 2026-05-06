"""Fix the screener_top_pick calibration gap by backfilling missing baseline_benchmark_price
on signals and recomputing correct/excess_return on outcomes.

Root cause:
  - Signal-logger calls get_baseline_price(bench) which can return None on transient
    Polygon API failure. None gets persisted to the signal record's baseline_benchmark_price.
  - When outcome-checker runs, score_relative(baseline_price, current_price,
    baseline_benchmark, current_benchmark) returns (None, 0.0) when ANY input is falsy.
  - Result: 399/509 signals stuck unscoreable, 450 outcomes with correct=None.

This backfill:
  1. Pulls SPY daily closes from Polygon for 2026-01-01 → today (1 API call, ~85 bars).
  2. Builds date → SPY close map.
  3. For each signal with baseline_benchmark_price=None, looks up SPY close on logged_at
     date (or nearest prior trading day) and PATCHES the signal record.
  4. For each OUTCOME with correct=None: pulls signal_id → reads now-fixed signal →
     recomputes correct + excess_return using the existing outcome.asset_price and
     outcome.benchmark_price, writes the fix back to the outcome record.

Idempotent: skips signals that already have the field set; skips outcomes already scored.
"""
import json
import os
import time
import urllib.parse
import urllib.request
from collections import Counter
from datetime import datetime, timezone, timedelta
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Attr

from ops_report import report

REGION = "us-east-1"
ddb = boto3.resource("dynamodb", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)


def get_polygon_key():
    """Source from a known Lambda's env."""
    for fn in ["justhodl-stock-screener", "justhodl-stock-analyzer", "justhodl-pnl-tracker"]:
        try:
            cfg = lam.get_function_configuration(FunctionName=fn)
            env = (cfg.get("Environment") or {}).get("Variables") or {}
            for k in ["POLYGON_KEY", "POLYGON_API_KEY"]:
                if env.get(k):
                    return env[k]
        except Exception:
            continue
    return os.environ.get("POLYGON_KEY") or "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"


def fetch_spy_history(start, end, key):
    """Fetch SPY daily closes from Polygon. Returns dict[YYYY-MM-DD] -> close (float)."""
    url = (f"https://api.polygon.io/v2/aggs/ticker/SPY/range/1/day/"
           f"{start}/{end}?adjusted=true&sort=asc&limit=5000&apiKey={key}")
    req = urllib.request.Request(url, headers={"User-Agent": "justhodl-backfill/1.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        d = json.loads(resp.read().decode())
    out = {}
    for bar in d.get("results", []):
        ts_ms = bar.get("t")
        close = bar.get("c")
        if ts_ms and close:
            dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
            out[dt.date().isoformat()] = float(close)
    return out


def closest_prior_trading_day_close(date_str, spy_map):
    """Find SPY close on date or nearest prior trading day (max 7 days back)."""
    target = datetime.fromisoformat(date_str.replace("Z", "+00:00")).date() \
        if "T" in date_str else datetime.fromisoformat(date_str).date()
    for offset in range(8):
        d = (target - timedelta(days=offset)).isoformat()
        if d in spy_map:
            return spy_map[d], d
    return None, None


def f2d(v):
    """float to Decimal (DDB)."""
    return Decimal(str(round(float(v), 6))) if v is not None else None


def main():
    with report("backfill_screener_calibration") as r:
        # ────────────────────────────────────────────────────────
        r.heading("1) Fetch SPY daily closes via Polygon")
        key = get_polygon_key()
        start = "2026-01-01"
        end = datetime.now(timezone.utc).date().isoformat()
        spy_map = fetch_spy_history(start, end, key)
        r.log(f"  fetched {len(spy_map)} SPY daily closes ({start} → {end})")
        if spy_map:
            keys = sorted(spy_map.keys())
            r.log(f"  range: {keys[0]} (${spy_map[keys[0]]}) → {keys[-1]} (${spy_map[keys[-1]]})")

        if len(spy_map) < 50:
            r.log("  ✗ too few SPY bars — abort backfill")
            return

        # ────────────────────────────────────────────────────────
        r.heading("2) Scan all screener_top_pick signals")
        sig_tbl = ddb.Table("justhodl-signals")
        signals = []
        last_key = None
        pages = 0
        while True:
            kw = {"Limit": 1000, "FilterExpression": Attr("signal_type").eq("screener_top_pick")}
            if last_key:
                kw["ExclusiveStartKey"] = last_key
            resp = sig_tbl.scan(**kw)
            signals.extend(resp.get("Items", []))
            last_key = resp.get("LastEvaluatedKey")
            pages += 1
            if not last_key or pages > 20:
                break
        r.log(f"  scanned {len(signals)} signal records ({pages} pages)")

        needs_backfill = [s for s in signals if s.get("baseline_benchmark_price") is None]
        already_set    = [s for s in signals if s.get("baseline_benchmark_price") is not None]
        r.log(f"  needs backfill: {len(needs_backfill)}")
        r.log(f"  already set:    {len(already_set)}")

        # ────────────────────────────────────────────────────────
        r.heading("3) Patch signal records — set baseline_benchmark_price")
        patched_signals = 0
        skipped_no_match = 0
        for sig in needs_backfill:
            logged_at = sig.get("logged_at") or ""
            if not logged_at:
                continue
            spy_close, matched_date = closest_prior_trading_day_close(logged_at, spy_map)
            if spy_close is None:
                skipped_no_match += 1
                continue
            try:
                sig_tbl.update_item(
                    Key={"signal_id": sig["signal_id"]},
                    UpdateExpression="SET baseline_benchmark_price = :v, backfilled_benchmark_at = :t",
                    ExpressionAttributeValues={":v": f2d(spy_close), ":t": datetime.now(timezone.utc).isoformat()},
                )
                patched_signals += 1
            except Exception as e:
                r.log(f"  ✗ patch {sig.get('signal_id')}: {e}")
        r.log(f"  ✓ patched {patched_signals} signals")
        r.log(f"  ⚠ skipped (no SPY match): {skipped_no_match}")

        # ────────────────────────────────────────────────────────
        r.heading("4) Now rescore outcomes with correct=None")
        # First, build signal_id → baseline + benchmark map (refreshed)
        signals_by_id = {}
        for s in signals:
            signals_by_id[s["signal_id"]] = s
        # Refresh patched ones
        # (DDB scan above happened before patching; for accuracy, re-pull map of patched ones)
        sig_id_to_bbp = {}
        for s in needs_backfill[:patched_signals]:
            logged_at = s.get("logged_at") or ""
            spy_close, _ = closest_prior_trading_day_close(logged_at, spy_map)
            if spy_close is not None:
                sig_id_to_bbp[s["signal_id"]] = (float(s.get("baseline_price") or 0), spy_close)
        for s in already_set:
            sig_id_to_bbp[s["signal_id"]] = (float(s.get("baseline_price") or 0), float(s.get("baseline_benchmark_price") or 0))

        out_tbl = ddb.Table("justhodl-outcomes")
        outcomes = []
        last_key = None
        pages = 0
        while True:
            kw = {"Limit": 1000, "FilterExpression": Attr("signal_type").eq("screener_top_pick")}
            if last_key:
                kw["ExclusiveStartKey"] = last_key
            resp = out_tbl.scan(**kw)
            outcomes.extend(resp.get("Items", []))
            last_key = resp.get("LastEvaluatedKey")
            pages += 1
            if not last_key or pages > 20:
                break
        r.log(f"  scanned {len(outcomes)} outcome records ({pages} pages)")

        needs_rescore = [o for o in outcomes if o.get("correct") is None]
        already_scored = [o for o in outcomes if o.get("correct") is not None]
        r.log(f"  needs rescore: {len(needs_rescore)}")
        r.log(f"  already scored: {len(already_scored)}")

        # ────────────────────────────────────────────────────────
        r.heading("5) Rescore each outcome")
        rescored = 0
        no_baseline = 0
        no_check_prices = 0
        for o in needs_rescore:
            sig_id = o.get("signal_id")
            outcome_inner = o.get("outcome") or {}
            asset_price = float(outcome_inner.get("asset_price") or 0)
            benchmark_price = float(outcome_inner.get("benchmark_price") or 0)
            if asset_price == 0 or benchmark_price == 0:
                no_check_prices += 1
                continue
            bps = sig_id_to_bbp.get(sig_id)
            if not bps:
                no_baseline += 1
                continue
            baseline_price, baseline_benchmark = bps
            if baseline_price == 0 or baseline_benchmark == 0:
                no_baseline += 1
                continue

            asset_return = ((asset_price - baseline_price) / baseline_price) * 100
            benchmark_return = ((benchmark_price - baseline_benchmark) / baseline_benchmark) * 100
            excess_return = asset_return - benchmark_return

            pred = o.get("predicted_dir", "OUTPERFORM")
            if pred == "OUTPERFORM":
                correct = excess_return > 0
            elif pred == "UNDERPERFORM":
                correct = excess_return < 0
            else:
                correct = abs(excess_return) < 1.0

            try:
                out_tbl.update_item(
                    Key={"outcome_id": o["outcome_id"]},
                    UpdateExpression=(
                        "SET #c = :c, "
                        "#o.excess_return = :er, "
                        "#o.asset_return = :ar, "
                        "#o.benchmark_return = :br, "
                        "#o.baseline_price = :bp, "
                        "#o.baseline_benchmark_price = :bbp, "
                        "#o.correct = :c, "
                        "backfilled_at = :t"
                    ),
                    ExpressionAttributeNames={"#c": "correct", "#o": "outcome"},
                    ExpressionAttributeValues={
                        ":c": correct,
                        ":er": f2d(excess_return),
                        ":ar": f2d(asset_return),
                        ":br": f2d(benchmark_return),
                        ":bp": f2d(baseline_price),
                        ":bbp": f2d(baseline_benchmark),
                        ":t": datetime.now(timezone.utc).isoformat(),
                    },
                )
                rescored += 1
            except Exception as e:
                r.log(f"  ✗ rescore {o.get('outcome_id')}: {e}")

        r.log(f"  ✓ rescored: {rescored}")
        r.log(f"  ⚠ skipped (missing baseline):    {no_baseline}")
        r.log(f"  ⚠ skipped (missing check prices): {no_check_prices}")

        # ────────────────────────────────────────────────────────
        r.heading("6) Sample 5 freshly-scored outcomes")
        # Re-pull a few
        sample = needs_rescore[:5]
        for i, o in enumerate(sample):
            try:
                fresh = out_tbl.get_item(Key={"outcome_id": o["outcome_id"]}).get("Item", {})
                inner = fresh.get("outcome", {})
                r.log(f"  [{i}] {fresh.get('outcome_id')}")
                r.log(f"      correct: {fresh.get('correct')}")
                r.log(f"      excess_return: {inner.get('excess_return')}")
                r.log(f"      asset_return:  {inner.get('asset_return')}")
                r.log(f"      benchmark_return: {inner.get('benchmark_return')}")
                r.log(f"      backfilled_at: {fresh.get('backfilled_at')}")
            except Exception as e:
                r.log(f"  ✗ {e}")

        # ────────────────────────────────────────────────────────
        r.heading("7) Distribution of newly-scored correct values")
        cnt = Counter()
        for o in needs_rescore[:rescored]:
            try:
                fresh = out_tbl.get_item(Key={"outcome_id": o["outcome_id"]}).get("Item", {})
                cnt[str(fresh.get("correct"))] += 1
            except Exception:
                pass
        for k, v in cnt.most_common():
            r.log(f"  correct={k:10s}  n={v}")
        if "True" in cnt or "False" in cnt:
            n_true = cnt.get("True", 0)
            n_false = cnt.get("False", 0)
            total = n_true + n_false
            r.log(f"")
            r.log(f"  → screener_top_pick accuracy: {n_true}/{total} = {n_true/max(total,1)*100:.1f}%")


if __name__ == "__main__":
    main()
