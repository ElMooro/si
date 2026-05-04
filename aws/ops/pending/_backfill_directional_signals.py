"""Universal directional-signal backfill.

Same bug as screener_top_pick but on the directional scoring path:
  signal-logger's get_baseline_price() failed → persisted baseline_price=None →
  outcome-checker's score_directional couldn't compute → all 30+ outcomes stuck
  with correct=None across edge_regime/carry_risk/market_phase/khalid_index/
  ml_risk/plumbing_stress/momentum_*/crypto_*.

Strategy:
  1. Identify all unscored outcomes (correct=None, is_legacy!=true).
  2. Group by source ticker (signal.measure_against): SPY, BTC-USD, GLD, USO, TLT, UUP, ...
  3. For each ticker, fetch daily closes from Polygon for 2026-01-01 → today (one call per ticker).
  4. For each affected signal:
     - Look up close on signal.logged_at date (or nearest prior trading day)
     - Patch signal.baseline_price
  5. For each affected outcome:
     - Recompute score_directional: actual_direction = UP/DOWN/NEUTRAL based on
       (price_at_check - baseline) / baseline; correct = (predicted == actual)
     - Update outcome record with correct + actual_direction + return_pct + price_at_signal

Idempotent: skips signals/outcomes already fixed.

BTC-USD is special: Polygon doesn't quote BTC-USD as a stock symbol. We use
"X:BTCUSD" for crypto, or fall back to CoinGecko historical via stable endpoint.
"""
import json
import os
import time
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from datetime import datetime, timezone, timedelta
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Attr

from ops_report import report

REGION = "us-east-1"
ddb = boto3.resource("dynamodb", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)


def get_polygon_key():
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


def fetch_polygon_history(ticker, start, end, key):
    """Fetch daily closes for a stock or ETF ticker. Returns dict[YYYY-MM-DD] -> close."""
    # Crypto needs different URL
    if ticker.upper() in ("BTC-USD", "ETH-USD", "SOL-USD", "BTC", "ETH", "SOL"):
        sym_map = {"BTC-USD": "X:BTCUSD", "BTC": "X:BTCUSD",
                   "ETH-USD": "X:ETHUSD", "ETH": "X:ETHUSD",
                   "SOL-USD": "X:SOLUSD", "SOL": "X:SOLUSD"}
        url_ticker = sym_map.get(ticker.upper(), f"X:{ticker.upper()}USD")
    else:
        url_ticker = ticker.upper()
    url = (f"https://api.polygon.io/v2/aggs/ticker/{url_ticker}/range/1/day/"
           f"{start}/{end}?adjusted=true&sort=asc&limit=5000&apiKey={key}")
    try:
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
    except Exception as e:
        print(f"[fetch] {ticker} ({url_ticker}): {e}")
        return {}


def closest_close(date_str, price_map):
    target = datetime.fromisoformat(date_str.replace("Z", "+00:00")).date() \
        if "T" in date_str else datetime.fromisoformat(date_str).date()
    for offset in range(8):
        d = (target - timedelta(days=offset)).isoformat()
        if d in price_map:
            return price_map[d], d
    return None, None


def f2d(v):
    return Decimal(str(round(float(v), 6))) if v is not None else None


def score_directional(pred, baseline, current, threshold=0.5):
    """Replica of outcome-checker logic."""
    if not all([baseline, current]):
        return None, "UNKNOWN", 0
    if baseline == 0:
        return None, "UNKNOWN", 0
    return_pct = ((current - baseline) / baseline) * 100
    if abs(return_pct) < threshold:
        actual = "NEUTRAL"
    elif return_pct > 0:
        actual = "UP"
    else:
        actual = "DOWN"
    if pred == "NEUTRAL":
        # NEUTRAL prediction: correct if absolute return_pct < 1%
        return abs(return_pct) < 1.0, actual, return_pct
    return pred == actual, actual, return_pct


def main():
    with report("backfill_directional_signals") as r:
        # ─────────────────────────────────────────────────────
        r.heading("1) Scan all unscored outcomes (correct=None, non-legacy)")
        out_tbl = ddb.Table("justhodl-outcomes")
        sig_tbl = ddb.Table("justhodl-signals")

        unscored = []
        last_key = None
        pages = 0
        while True:
            kw = {
                "Limit": 1000,
                "FilterExpression": Attr("correct").eq(None) & Attr("is_legacy").ne(True),
            }
            if last_key:
                kw["ExclusiveStartKey"] = last_key
            resp = out_tbl.scan(**kw)
            unscored.extend(resp.get("Items", []))
            last_key = resp.get("LastEvaluatedKey")
            pages += 1
            if not last_key or pages > 20:
                break
        r.log(f"  total unscored: {len(unscored)} (across {pages} pages)")

        # Group by signal_type
        by_type = Counter(o.get("signal_type", "?") for o in unscored)
        r.log(f"  unscored by signal_type:")
        for t, n in by_type.most_common():
            r.log(f"    {t:30s}  n={n}")

        # ─────────────────────────────────────────────────────
        r.heading("2) Pull source signals + group by ticker")
        # Get signal_id → outcome list mapping
        outcomes_by_sig = defaultdict(list)
        for o in unscored:
            outcomes_by_sig[o["signal_id"]].append(o)

        # Fetch all unique signals
        signals = {}
        sig_ids = list(outcomes_by_sig.keys())
        r.log(f"  unique signal_ids: {len(sig_ids)}")
        # Use BatchGetItem in chunks of 100
        for i in range(0, len(sig_ids), 100):
            batch = sig_ids[i:i+100]
            try:
                resp = ddb.batch_get_item(
                    RequestItems={"justhodl-signals": {"Keys": [{"signal_id": sid} for sid in batch]}}
                )
                for s in resp.get("Responses", {}).get("justhodl-signals", []):
                    signals[s["signal_id"]] = s
            except Exception as e:
                r.log(f"  ✗ batch get {i}: {e}")
        r.log(f"  signals loaded: {len(signals)}")

        # Group by ticker
        by_ticker = defaultdict(list)
        no_signal = 0
        no_ticker = 0
        for o in unscored:
            sid = o.get("signal_id")
            sig = signals.get(sid)
            if not sig:
                no_signal += 1
                continue
            ticker = sig.get("measure_against")
            if not ticker:
                no_ticker += 1
                continue
            by_ticker[ticker].append((o, sig))
        r.log(f"  outcomes grouped by ticker:")
        for tk, items in sorted(by_ticker.items(), key=lambda x: -len(x[1])):
            r.log(f"    {tk:15s}  n={len(items)}")
        r.log(f"  skipped — no source signal: {no_signal}")
        r.log(f"  skipped — no measure_against ticker: {no_ticker}")

        # ─────────────────────────────────────────────────────
        r.heading("3) Fetch historical price maps per ticker")
        key = get_polygon_key()
        start = "2026-01-01"
        end = datetime.now(timezone.utc).date().isoformat()
        price_maps = {}
        for tk in by_ticker.keys():
            pm = fetch_polygon_history(tk, start, end, key)
            price_maps[tk] = pm
            r.log(f"  {tk:15s}  {len(pm)} bars  range="
                  f"{(min(pm.keys()) if pm else '—')} → {(max(pm.keys()) if pm else '—')}")
            time.sleep(0.3)  # rate limit

        # ─────────────────────────────────────────────────────
        r.heading("4) Backfill signals (baseline_price) + rescore outcomes")
        now_iso = datetime.now(timezone.utc).isoformat()
        signals_patched = 0
        outcomes_rescored = 0
        skipped_no_close = 0
        skipped_baseline_set = 0
        skipped_no_check_price = 0
        results_by_type = defaultdict(lambda: Counter())

        for tk, items in by_ticker.items():
            pm = price_maps.get(tk) or {}
            if len(pm) < 30:
                r.log(f"  ⚠ {tk}: only {len(pm)} bars, skipping all {len(items)} outcomes")
                continue
            for o, sig in items:
                stype = o.get("signal_type")
                logged_at = sig.get("logged_at") or ""
                pred = sig.get("predicted_direction") or o.get("predicted_dir")
                inner = o.get("outcome") or {}
                price_at_check = float(inner.get("price_at_check") or 0)

                if price_at_check == 0:
                    skipped_no_check_price += 1
                    continue

                # Determine baseline (either already set, or backfill)
                baseline = sig.get("baseline_price")
                try:
                    baseline = float(baseline) if baseline is not None else None
                except Exception:
                    baseline = None
                if not baseline or baseline == 0:
                    bp, matched_date = closest_close(logged_at, pm)
                    if bp is None:
                        skipped_no_close += 1
                        continue
                    baseline = bp
                    # Patch source signal
                    try:
                        sig_tbl.update_item(
                            Key={"signal_id": sig["signal_id"]},
                            UpdateExpression="SET baseline_price = :v, backfilled_baseline_at = :t",
                            ExpressionAttributeValues={":v": f2d(baseline), ":t": now_iso},
                        )
                        signals_patched += 1
                    except Exception as e:
                        r.log(f"    ✗ patch sig {sig['signal_id']}: {e}")
                        continue
                else:
                    skipped_baseline_set += 1
                    # Don't double-patch but DO rescore the outcome below

                # Rescore outcome
                correct, actual_dir, return_pct = score_directional(pred, baseline, price_at_check)
                if correct is None:
                    continue
                try:
                    out_tbl.update_item(
                        Key={"outcome_id": o["outcome_id"]},
                        UpdateExpression=(
                            "SET #c = :c, "
                            "#o.correct = :c, "
                            "#o.actual_direction = :ad, "
                            "#o.return_pct = :rp, "
                            "#o.price_at_signal = :ps, "
                            "backfilled_at = :t"
                        ),
                        ExpressionAttributeNames={"#c": "correct", "#o": "outcome"},
                        ExpressionAttributeValues={
                            ":c": correct,
                            ":ad": actual_dir,
                            ":rp": f2d(return_pct),
                            ":ps": f2d(baseline),
                            ":t": now_iso,
                        },
                    )
                    outcomes_rescored += 1
                    results_by_type[stype][str(correct)] += 1
                except Exception as e:
                    r.log(f"    ✗ rescore outcome {o['outcome_id']}: {e}")

        r.log(f"")
        r.log(f"  ✓ signals patched (baseline_price set): {signals_patched}")
        r.log(f"  ✓ outcomes rescored: {outcomes_rescored}")
        r.log(f"  ⚠ skipped (no close in range): {skipped_no_close}")
        r.log(f"  ⚠ skipped (no check price): {skipped_no_check_price}")
        r.log(f"  ℹ baselines already set: {skipped_baseline_set}")

        # ─────────────────────────────────────────────────────
        r.heading("5) Per-signal accuracy of backfilled outcomes")
        for stype in sorted(results_by_type.keys()):
            cc = results_by_type[stype]
            t = cc.get("True", 0)
            f = cc.get("False", 0)
            tot = t + f
            pct = (t / tot * 100) if tot else 0
            r.log(f"    {stype:30s}  rescored={tot:>4}  correct={t:>3}  wrong={f:>3}  acc={pct:>5.1f}%")


if __name__ == "__main__":
    main()
