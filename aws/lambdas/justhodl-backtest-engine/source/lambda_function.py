"""justhodl-backtest-engine

Computes a "Calibrated Alpha Replay" from the historical signal ledger.

Methodology:
  For every scored outcome (correct ∈ {True, False}, non-legacy) we compute:

      contribution = weight × dir_sign × actual_return

    where:
      weight       = current calibration weight for this signal_type from SSM
      dir_sign     = +1 for UP/OUTPERFORM/LOAD, -1 for DOWN/UNDERPERFORM, 0 for NEUTRAL
      actual_return = excess_return_pct (for relative signals) or return_pct (directional)

  Aggregate by:
    • signal_type → top/bottom contributors with cumulative alpha
    • outcome.checked_at date (when "trade closed") → daily P&L
    • cumulative time-ordered sum → strategy NAV starting at $100,000

  This is not a full daily-rebalance simulation — those require modeling overlapping
  positions and weren't possible with the current data model. This is a pragmatic
  "what-if" estimator that answers: "If we'd weighted every realized trade by its
  current calibration weight, what would the system's cumulative alpha look like?"

  The number to compare against is SPY's return over the same time window.

Schedule: rate(6 hours) — recomputes after each calibrator run.
Reads:
  - DynamoDB justhodl-outcomes (filter: correct ∈ {True,False} & is_legacy != true)
  - SSM /justhodl/calibration/weights
Writes:
  - backtest/results.json   (full results)
  - backtest/summary.json   (slim KPIs only)
"""
import json
import os
import time
import urllib.request
from collections import Counter, defaultdict
from datetime import datetime, timezone, timedelta
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Attr

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
INITIAL_NAV = 100_000.0

# Position sizing: each scored outcome represents a hypothetical 0.5%-of-NAV trade,
# modulated by the signal's calibration weight. So a w=1.5 signal earning +10%
# contributes 0.075% to NAV. Conservative size accommodates the heavy daily
# signal counts (screener_top_pick alone fires ~10 trades/day at peak), and
# bounds gross daily exposure to a realistic ~10-20% of NAV.
POSITION_SIZE = 0.005

# v1.2 REALISTIC CONSTRAINTS — bring the unrealistically-clean v1.1 sharpe of
# ~10 down toward the honest out-of-sample range of 2-5.
SLIPPAGE_BPS_PER_LEG = 5     # 5 bps each side (entry+exit) = 10 bps round-trip per trade
CONCENTRATION_CAP = 0.40     # 40% NAV max in any single signal_type per day
GROSS_EXPOSURE_CAP = 1.00    # 100% NAV max total gross exposure per day
# (No leverage cost needed — gross is hard-capped at 100%.)

# Window preference for dedup — when the same signal_id has multiple scored
# outcomes (e.g., day_30, day_60, day_90), use the SHORTEST as the canonical one.
# Shorter windows let the strategy turn capital faster and align better with
# realistic trading horizons.
WINDOW_PREFERENCE = ["day_30", "day_60", "day_90", "day_14", "day_7", "day_3", "day_1"]

# Polygon for SPY benchmark
POLYGON_KEY = os.environ.get("POLYGON_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")
POLYGON_BASE = "https://api.polygon.io"


def fetch_spy_window(start_date, end_date):
    """Fetch SPY daily closing prices via Polygon for the [start, end] window.
    Returns dict {date_iso: close_price}.
    """
    if not start_date or not end_date:
        return {}
    url = (f"{POLYGON_BASE}/v2/aggs/ticker/SPY/range/1/day/{start_date}/{end_date}"
           f"?adjusted=true&sort=asc&limit=5000&apiKey={POLYGON_KEY}")
    try:
        with urllib.request.urlopen(url, timeout=15) as resp:
            data = json.loads(resp.read())
        prices = {}
        for r in data.get("results") or []:
            d = datetime.fromtimestamp(r["t"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
            prices[d] = r["c"]
        print(f"[backtest] fetched {len(prices)} SPY closes from {start_date} to {end_date}")
        return prices
    except Exception as e:
        print(f"[backtest] SPY fetch failed: {e}")
        return {}

S3 = boto3.client("s3", region_name=REGION)
SSM = boto3.client("ssm", region_name=REGION)
DDB = boto3.resource("dynamodb", region_name=REGION)


def to_float(v):
    if v is None:
        return None
    if isinstance(v, Decimal):
        return float(v)
    try:
        return float(v)
    except Exception:
        return None


def get_weights():
    try:
        v = SSM.get_parameter(Name="/justhodl/calibration/weights")["Parameter"]["Value"]
        d = json.loads(v)
        return {k: to_float(v) for k, v in d.items() if to_float(v) is not None}
    except Exception as e:
        print(f"[ssm] weights: {e}")
        return {}


def get_horizon_weights():
    """Fetch all per-horizon weights from /justhodl/calibration/weights/{window}.

    Returns: dict[window] -> dict[signal_type] -> weight
    """
    out = {}
    try:
        # GetParametersByPath pulls everything under the prefix in one call.
        paginator = SSM.get_paginator("get_parameters_by_path")
        for page in paginator.paginate(Path="/justhodl/calibration/weights/", Recursive=False):
            for p in page.get("Parameters", []):
                # Param name like /justhodl/calibration/weights/day_7
                window = p["Name"].split("/")[-1]
                try:
                    out[window] = {k: to_float(v) for k, v in json.loads(p["Value"]).items()
                                   if to_float(v) is not None}
                except Exception as e:
                    print(f"[ssm] parse {p['Name']}: {e}")
    except Exception as e:
        print(f"[ssm] horizon weights: {e}")
    return out


def resolve_weight(stype, window, flat_weights, horizon_weights):
    """Pick the best weight for a (signal_type, window) trade.

    Priority:
      1. Per-horizon weight if available (from per-horizon SSM)
      2. Flat aggregate weight as fallback
      3. None if neither — caller skips
    Returns: (weight, source) where source is 'horizon:{window}' or 'flat'
    """
    if window and window in horizon_weights:
        h = horizon_weights[window].get(stype)
        if h is not None:
            return h, f"horizon:{window}"
    flat = flat_weights.get(stype)
    if flat is not None:
        return flat, "flat"
    return None, None


def dir_sign(predicted_dir):
    """Map predicted direction to a numeric sign."""
    if not predicted_dir:
        return 0
    p = predicted_dir.upper()
    if p in ("UP", "OUTPERFORM", "LOAD", "LONG", "LEVER"):
        return 1
    if p in ("DOWN", "UNDERPERFORM", "EXIT", "TRIM"):
        return -1
    return 0  # NEUTRAL, HOLD, WAIT, HEDGE, UNKNOWN


def scan_scored_outcomes():
    """Pull every outcome where correct ∈ {True, False} & is_legacy != true.
    These are the only outcomes the calibrator counts."""
    tbl = DDB.Table("justhodl-outcomes")
    items = []
    last_key = None
    pages = 0
    while True:
        kw = {
            "Limit": 1000,
            "FilterExpression": (
                (Attr("correct").eq(True) | Attr("correct").eq(False))
                & Attr("is_legacy").ne(True)
            ),
        }
        if last_key:
            kw["ExclusiveStartKey"] = last_key
        resp = tbl.scan(**kw)
        items.extend(resp.get("Items", []))
        last_key = resp.get("LastEvaluatedKey")
        pages += 1
        if not last_key or pages > 30:
            break
    return items, pages


def lambda_handler(event=None, context=None):
    started = time.time()
    now = datetime.now(timezone.utc)
    print(f"[backtest] starting at {now.isoformat()}")

    # 1. Load weights
    weights = get_weights()
    horizon_weights = get_horizon_weights()
    horizon_signal_count = sum(len(v) for v in horizon_weights.values())
    print(f"[backtest] loaded {len(weights)} flat weights + {len(horizon_weights)} horizons "
          f"({horizon_signal_count} (signal,horizon) pairs)")

    # 2. Pull all scored outcomes
    outcomes, pages = scan_scored_outcomes()
    print(f"[backtest] pulled {len(outcomes)} scored outcomes ({pages} pages)")

    # 2a. Dedup by signal_id — same signal often has 2-3 outcomes (day_30/60/90).
    # Pick the shortest available window so we measure faster-turning strategies
    # and avoid triple-counting overlapping positions.
    by_sig = {}
    for o in outcomes:
        sid = o.get("signal_id")
        if not sid:
            continue
        existing = by_sig.get(sid)
        if existing is None:
            by_sig[sid] = o
            continue
        # Compare windows
        cur_window = o.get("window_key")
        prev_window = existing.get("window_key")
        try:
            cur_pref = WINDOW_PREFERENCE.index(cur_window) if cur_window in WINDOW_PREFERENCE else 99
            prev_pref = WINDOW_PREFERENCE.index(prev_window) if prev_window in WINDOW_PREFERENCE else 99
            if cur_pref < prev_pref:
                by_sig[sid] = o
        except Exception:
            pass

    deduped = list(by_sig.values())
    print(f"[backtest] after dedup by signal_id: {len(deduped)} unique trades (was {len(outcomes)})")
    outcomes = deduped

    # 3. Compute contributions
    results = []
    skipped_no_weight = 0
    skipped_no_return = 0
    n_horizon_weighted = 0
    n_flat_weighted = 0
    horizon_breakdown = defaultdict(int)  # window -> n trades scored at that horizon
    for o in outcomes:
        stype = o.get("signal_type")
        if not stype:
            continue
        window = o.get("window_key")
        # Horizon-aware: prefer per-horizon weight matching this trade's window.
        # Falls back to flat weight when (signal, horizon) has insufficient data.
        w, source = resolve_weight(stype, window, weights, horizon_weights)
        if w is None:
            skipped_no_weight += 1
            continue
        if source and source.startswith("horizon:"):
            n_horizon_weighted += 1
            horizon_breakdown[window] += 1
        else:
            n_flat_weighted += 1

        inner = o.get("outcome") or {}
        # Prefer excess_return for relative signals; fall back to return_pct for directional
        ret = to_float(inner.get("excess_return"))
        if ret is None:
            ret = to_float(inner.get("return_pct"))
        if ret is None:
            skipped_no_return += 1
            continue

        pred = o.get("predicted_dir")
        sign = dir_sign(pred)
        if sign == 0:
            # NEUTRAL — score as "correct if return small". Use bool(correct) directly.
            correct = bool(o.get("correct"))
            # Treat NEUTRAL as: positive contribution if correct, negative if wrong
            # Magnitude proxy: absolute return × position size
            contribution = POSITION_SIZE * w * (abs(ret) if correct else -abs(ret))
        else:
            # Each outcome = a 2%-of-NAV hypothetical trade, modulated by calibration weight.
            # contribution is in PERCENTAGE POINTS of NAV (e.g., 0.30 = +0.30% of NAV)
            contribution = POSITION_SIZE * w * sign * ret

        checked_at = o.get("checked_at") or inner.get("checked_at") or ""
        logged_at = o.get("logged_at") or ""
        results.append({
            "outcome_id":   o.get("outcome_id"),
            "signal_type":  stype,
            "predicted_dir": pred,
            "window_key":   window,
            "weight":       w,
            "weight_source": source,  # 'horizon:day_7' / 'horizon:day_30' / 'flat'
            "actual_return": ret,
            "correct":      bool(o.get("correct")),
            "contribution": contribution,
            "checked_at":   checked_at,
            "logged_at":    logged_at,
        })

    print(f"[backtest] kept {len(results)} contributions  "
          f"(skipped {skipped_no_weight} no_weight + {skipped_no_return} no_return)")
    print(f"[backtest] weight sources: {n_horizon_weighted} horizon-aware, {n_flat_weighted} flat fallback")
    if horizon_breakdown:
        print(f"[backtest] horizon breakdown: {dict(horizon_breakdown)}")

    # 4. Sort by checked_at — chronological order matters for NAV
    results.sort(key=lambda x: x["checked_at"] or "")

    # 5. Aggregate by signal_type
    by_signal = defaultdict(lambda: {"n": 0, "n_correct": 0, "sum_contribution": 0.0,
                                     "sum_return": 0.0, "sum_weight": 0.0,
                                     "windows_used": defaultdict(int)})
    for r in results:
        bucket = by_signal[r["signal_type"]]
        bucket["n"] += 1
        bucket["n_correct"] += 1 if r["correct"] else 0
        bucket["sum_contribution"] += r["contribution"]
        bucket["sum_return"] += r["actual_return"]
        bucket["sum_weight"] += r["weight"]
        # Track which horizons were used per signal (informational)
        wsrc = r.get("weight_source") or ""
        if wsrc.startswith("horizon:"):
            bucket["windows_used"][wsrc.split(":")[1]] += 1
        else:
            bucket["windows_used"]["flat"] += 1

    signal_summary = []
    for sig, b in by_signal.items():
        n = b["n"]
        signal_summary.append({
            "signal_type": sig,
            "weight": round(b["sum_weight"] / max(n, 1), 4),  # avg weight across trades
            "n_outcomes": n,
            "win_rate": round(b["n_correct"] / max(n, 1), 4),
            "avg_return_pct": round(b["sum_return"] / max(n, 1), 4),
            "total_contribution": round(b["sum_contribution"], 4),
            "avg_contribution": round(b["sum_contribution"] / max(n, 1), 4),
            "windows_used": dict(b["windows_used"]),  # which horizons were applied
        })
    signal_summary.sort(key=lambda x: -x["total_contribution"])

    # 6. Aggregate by date — daily contribution sum.
    # Use logged_at (trade-open date) instead of checked_at (when the outcome was scored)
    # so trades distribute across their actual firing dates instead of clustering on backfill dates.
    by_date = defaultdict(float)
    by_date_n = defaultdict(int)
    for r in results:
        d = (r["logged_at"] or r["checked_at"] or "")[:10]
        if d:
            by_date[d] += r["contribution"]
            by_date_n[d] += 1
    daily = []
    for d in sorted(by_date.keys()):
        daily.append({"date": d, "contribution": round(by_date[d], 4), "n_outcomes": by_date_n[d]})

    # 6b. v1.2 REALISTIC AGGREGATION (parallel to v1.1)
    # ─────────────────────────────────────────────────
    # Apply real-world frictions to each trade:
    #   1) Slippage: subtract 10bps round-trip per trade (5bps × 2 legs)
    #   2) Per-signal concentration cap: max 40% of NAV per signal_type per day
    #   3) Gross exposure cap: max 100% of NAV total deployed per day
    #
    # Each trade's "size" (gross %-NAV) is POSITION_SIZE × |weight|. So a w=1.5 trade
    # has size 0.75% of NAV. With 200 trades on a heavy day, gross naively = 150% NAV.
    # Caps scale all trades that day proportionally so reality is enforced.
    realistic_results = []
    by_day_trades = defaultdict(list)  # date -> list of trade indices
    for i, r in enumerate(results):
        d = (r["logged_at"] or r["checked_at"] or "")[:10]
        if d:
            by_day_trades[d].append(i)

    n_concentration_capped_days = 0
    n_gross_capped_days = 0
    total_slippage_cost = 0.0

    realistic_daily = defaultdict(float)
    realistic_daily_n = defaultdict(int)

    for d in sorted(by_day_trades.keys()):
        trade_idxs = by_day_trades[d]
        # Step 1: per-trade slippage (always charged)
        # Each trade's size in pct-NAV = POSITION_SIZE × weight (always positive for sizing)
        sizes = []
        slippage_costs = []
        for i in trade_idxs:
            r = results[i]
            size = POSITION_SIZE * abs(r["weight"])  # gross size, always positive
            slip = size * (SLIPPAGE_BPS_PER_LEG * 2) / 10000  # 10bps round-trip on size
            sizes.append(size)
            slippage_costs.append(slip)
            total_slippage_cost += slip

        # Step 2: per-signal concentration cap (per day)
        sig_gross = defaultdict(float)
        for j, i in enumerate(trade_idxs):
            sig_gross[results[i]["signal_type"]] += sizes[j]
        sig_scale = {}
        any_concentration_capped = False
        for stype, total_size in sig_gross.items():
            if total_size > CONCENTRATION_CAP:
                sig_scale[stype] = CONCENTRATION_CAP / total_size
                any_concentration_capped = True
            else:
                sig_scale[stype] = 1.0
        if any_concentration_capped:
            n_concentration_capped_days += 1

        # Apply concentration scaling
        scaled_sizes = []
        for j, i in enumerate(trade_idxs):
            scale = sig_scale[results[i]["signal_type"]]
            scaled_sizes.append(sizes[j] * scale)

        # Step 3: total gross cap (per day)
        total_gross = sum(scaled_sizes)
        gross_scale = 1.0
        if total_gross > GROSS_EXPOSURE_CAP:
            gross_scale = GROSS_EXPOSURE_CAP / total_gross
            n_gross_capped_days += 1

        # Compose final per-trade realistic contribution
        for j, i in enumerate(trade_idxs):
            r = results[i]
            sig_s = sig_scale[r["signal_type"]]
            # Original gross contribution scales by: sig_concentration_scale × gross_scale
            scaled_contribution = r["contribution"] * sig_s * gross_scale
            # Slippage scales the same way (you only pay slippage on what you actually trade)
            scaled_slip = slippage_costs[j] * sig_s * gross_scale
            net = scaled_contribution - scaled_slip * 100  # slip is fractional, contribution is in %, so multiply by 100

            realistic_daily[d] += net
            realistic_daily_n[d] += 1
            realistic_results.append({
                "outcome_id": r["outcome_id"],
                "signal_type": r["signal_type"],
                "weight": r["weight"],
                "gross_size_pct": round(sizes[j] * 100, 4),
                "scaled_size_pct": round(scaled_sizes[j] * gross_scale * 100, 4),
                "concentration_scale": round(sig_s, 4),
                "gross_scale": round(gross_scale, 4),
                "raw_contribution": round(r["contribution"], 4),
                "slippage_cost_pct": round(scaled_slip * 100, 4),
                "realistic_contribution": round(net, 4),
                "logged_at": r["logged_at"],
            })

    realistic_daily_list = []
    for d in sorted(realistic_daily.keys()):
        realistic_daily_list.append({
            "date": d,
            "contribution": round(realistic_daily[d], 4),
            "n_outcomes": realistic_daily_n[d],
        })

    # Realistic NAV curve
    realistic_nav_curve = []
    rnav = INITIAL_NAV
    rcum = 0.0
    for d in realistic_daily_list:
        rnav = rnav * (1 + d["contribution"] / 100.0)
        rcum += d["contribution"]
        realistic_nav_curve.append({
            "date": d["date"],
            "nav": round(rnav, 2),
            "daily_pct": round(d["contribution"], 4),
            "cum_pct": round(rcum, 4),
        })

    print(f"[backtest-v1.2] realistic: {len(realistic_results)} trades, "
          f"slippage_total={total_slippage_cost*100:.4f}%, "
          f"concentration_capped_days={n_concentration_capped_days}, "
          f"gross_capped_days={n_gross_capped_days}")

    # 7. Build NAV curve — start at $100k, add daily contribution as a % of NAV
    # Treat each contribution as a basis-point hit on NAV: NAV_t+1 = NAV_t * (1 + contribution_t/100)
    # since contributions are in percentage points
    nav_curve = []
    nav = INITIAL_NAV
    cum_contribution = 0.0
    for d in daily:
        ret_pct = d["contribution"] / 100.0  # contribution is in pct already
        nav = nav * (1 + ret_pct)
        cum_contribution += d["contribution"]
        nav_curve.append({
            "date": d["date"],
            "nav": round(nav, 2),
            "daily_pct": round(d["contribution"], 4),
            "cum_pct": round(cum_contribution, 4),
        })

    # 7a. Date range first
    dates = [r["logged_at"][:10] for r in results if r["logged_at"]]
    if not dates:
        dates = [r["checked_at"][:10] for r in results if r["checked_at"]]
    first_date = min(dates) if dates else None
    last_date = max(dates) if dates else None
    # Extend SPY fetch by 7 days on each side to ensure we cover trading days
    if first_date and last_date:
        try:
            sd = (datetime.fromisoformat(first_date) - timedelta(days=7)).strftime("%Y-%m-%d")
            ed = (datetime.fromisoformat(last_date) + timedelta(days=7)).strftime("%Y-%m-%d")
        except Exception:
            sd, ed = first_date, last_date
    else:
        sd, ed = first_date, last_date

    # 7b. Fetch SPY benchmark and align to nav_curve dates
    spy_prices = fetch_spy_window(sd, ed)
    spy_first_close = None
    spy_dates_sorted = sorted(spy_prices.keys())
    if spy_dates_sorted:
        spy_first_close = spy_prices[spy_dates_sorted[0]]

    # For each strategy nav_curve entry, find the closest SPY close (≤ same date or fallback prev)
    def closest_spy_close_at_or_before(d):
        # Walk backward through sorted list
        candidates = [k for k in spy_dates_sorted if k <= d]
        return spy_prices[candidates[-1]] if candidates else None

    if spy_first_close:
        for n in nav_curve:
            spy_close = closest_spy_close_at_or_before(n["date"])
            if spy_close:
                n["spy_nav"] = round(INITIAL_NAV * (spy_close / spy_first_close), 2)
                n["spy_pct"] = round((spy_close / spy_first_close - 1) * 100, 4)
            else:
                n["spy_nav"] = INITIAL_NAV
                n["spy_pct"] = 0.0

    # 8. Compute summary stats
    n_total = len(results)
    n_correct = sum(1 for r in results if r["correct"])
    total_contribution = sum(r["contribution"] for r in results)
    final_nav = nav_curve[-1]["nav"] if nav_curve else INITIAL_NAV
    nav_return_pct = (final_nav - INITIAL_NAV) / INITIAL_NAV * 100

    # SPY benchmark
    spy_final_nav = nav_curve[-1].get("spy_nav") if nav_curve else None
    spy_return_pct = nav_curve[-1].get("spy_pct") if nav_curve else None
    alpha_pct = (nav_return_pct - spy_return_pct) if spy_return_pct is not None else None

    # n_days
    n_days = (datetime.fromisoformat(last_date) - datetime.fromisoformat(first_date)).days + 1 if first_date and last_date else 0

    # Drawdown
    peak = INITIAL_NAV
    max_dd = 0.0
    for n in nav_curve:
        if n["nav"] > peak:
            peak = n["nav"]
        dd = (peak - n["nav"]) / peak * 100
        if dd > max_dd:
            max_dd = dd

    # Sharpe-ish: mean daily contribution / std × sqrt(252)
    daily_contribs = [d["contribution"] for d in daily]
    if len(daily_contribs) >= 5:
        mean_d = sum(daily_contribs) / len(daily_contribs)
        var = sum((x - mean_d) ** 2 for x in daily_contribs) / len(daily_contribs)
        std = var ** 0.5
        sharpe = (mean_d / std * (252 ** 0.5)) if std > 0 else None
    else:
        sharpe = None

    # 8b. v1.2 REALISTIC stats (post-friction)
    realistic_final_nav = realistic_nav_curve[-1]["nav"] if realistic_nav_curve else INITIAL_NAV
    realistic_return_pct = (realistic_final_nav - INITIAL_NAV) / INITIAL_NAV * 100
    realistic_alpha_pct = (realistic_return_pct - spy_return_pct) if spy_return_pct is not None else None
    # Drawdown
    rpeak = INITIAL_NAV
    realistic_max_dd = 0.0
    for n in realistic_nav_curve:
        if n["nav"] > rpeak:
            rpeak = n["nav"]
        dd = (rpeak - n["nav"]) / rpeak * 100
        if dd > realistic_max_dd:
            realistic_max_dd = dd
    # Sharpe
    rd_contribs = [d["contribution"] for d in realistic_daily_list]
    if len(rd_contribs) >= 5:
        rmean = sum(rd_contribs) / len(rd_contribs)
        rvar = sum((x - rmean) ** 2 for x in rd_contribs) / len(rd_contribs)
        rstd = rvar ** 0.5
        realistic_sharpe = (rmean / rstd * (252 ** 0.5)) if rstd > 0 else None
    else:
        realistic_sharpe = None
    # Total slippage in pct of NAV (charged across all trades, fully scaled)
    realistic_total_slippage_pct = round(
        sum(r["slippage_cost_pct"] for r in realistic_results), 4
    )

    # Attach SPY benchmark to realistic_nav_curve too
    if spy_first_close:
        for n in realistic_nav_curve:
            spy_close = closest_spy_close_at_or_before(n["date"])
            if spy_close:
                n["spy_nav"] = round(INITIAL_NAV * (spy_close / spy_first_close), 2)
                n["spy_pct"] = round((spy_close / spy_first_close - 1) * 100, 4)

    # 9. Build full results
    results_doc = {
        "v": "1.2",
        "generated_at": now.isoformat(),
        "method": "calibrated_alpha_replay_v3_horizon_aware_realistic",
        "method_description": (
            "v1.2: HORIZON-AWARE + REALISTIC. The base v1.1 model lives in `summary` (no friction). "
            "The new `realistic_summary` and `realistic_nav_curve` apply real-world frictions: "
            "(1) slippage 5bps/leg = 10bps round-trip on each trade's gross size, "
            "(2) per-signal concentration cap of 40% of NAV per day (scales down when one signal type "
            "dominates a day), (3) gross exposure cap of 100% of NAV per day (scales down when total "
            "deployed capital exceeds NAV). The v1.1 sharpe was unrealistically clean at ~10. The v1.2 "
            "realistic sharpe should land in the honest 2-5 range. Both are reported side-by-side so "
            "Khalid can see the cost of friction directly. Trades distribute across their logged_at "
            "firing dates and NAV compounds normally from $100k. Compare to SPY buy-and-hold."
        ),
        "constants": {
            "POSITION_SIZE": POSITION_SIZE,
            "SLIPPAGE_BPS_PER_LEG": SLIPPAGE_BPS_PER_LEG,
            "CONCENTRATION_CAP": CONCENTRATION_CAP,
            "GROSS_EXPOSURE_CAP": GROSS_EXPOSURE_CAP,
        },
        "summary": {
            "n_outcomes": n_total,
            "n_correct": n_correct,
            "win_rate": round(n_correct / max(n_total, 1), 4),
            "n_signals": len(by_signal),
            "first_date": first_date,
            "last_date": last_date,
            "n_days": n_days,
            "total_contribution_pct": round(total_contribution, 4),
            "initial_nav": INITIAL_NAV,
            "final_nav": round(final_nav, 2),
            "total_return_pct": round(nav_return_pct, 4),
            "max_drawdown_pct": round(max_dd, 4),
            "sharpe_proxy": round(sharpe, 4) if sharpe is not None else None,
            "spy_final_nav": round(spy_final_nav, 2) if spy_final_nav is not None else None,
            "spy_return_pct": round(spy_return_pct, 4) if spy_return_pct is not None else None,
            "alpha_vs_spy_pct": round(alpha_pct, 4) if alpha_pct is not None else None,
            "skipped_no_weight": skipped_no_weight,
            "skipped_no_return": skipped_no_return,
            # Horizon attribution (v1.1)
            "n_horizon_weighted": n_horizon_weighted,
            "n_flat_weighted": n_flat_weighted,
            "horizon_breakdown": dict(horizon_breakdown),
        },
        "realistic_summary": {
            "n_trades": len(realistic_results),
            "initial_nav": INITIAL_NAV,
            "final_nav": round(realistic_final_nav, 2),
            "total_return_pct": round(realistic_return_pct, 4),
            "max_drawdown_pct": round(realistic_max_dd, 4),
            "sharpe_proxy": round(realistic_sharpe, 4) if realistic_sharpe is not None else None,
            "alpha_vs_spy_pct": round(realistic_alpha_pct, 4) if realistic_alpha_pct is not None else None,
            "total_slippage_cost_pct": realistic_total_slippage_pct,
            "n_concentration_capped_days": n_concentration_capped_days,
            "n_gross_capped_days": n_gross_capped_days,
            "n_total_days": len(realistic_daily_list),
            "friction_drag_pct": round(nav_return_pct - realistic_return_pct, 4),
        },
        "by_signal": signal_summary,
        "daily": daily,
        "nav_curve": nav_curve,
        "realistic_daily": realistic_daily_list,
        "realistic_nav_curve": realistic_nav_curve,
    }

    # Slim summary for fast page loads
    summary_doc = {
        "v": "1.2",
        "generated_at": now.isoformat(),
        "summary": results_doc["summary"],
        "realistic_summary": results_doc["realistic_summary"],
        "constants": results_doc["constants"],
        "top_5_contributors": signal_summary[:5],
        "bottom_5_contributors": signal_summary[-5:] if len(signal_summary) >= 5 else signal_summary,
    }

    duration = round(time.time() - started, 2)
    results_doc["duration_s"] = duration
    summary_doc["duration_s"] = duration

    # 10. Write to S3
    full_body = json.dumps(results_doc, default=str).encode("utf-8")
    summary_body = json.dumps(summary_doc, default=str).encode("utf-8")
    S3.put_object(Bucket=BUCKET, Key="backtest/results.json", Body=full_body,
                  ContentType="application/json", CacheControl="public, max-age=600")
    S3.put_object(Bucket=BUCKET, Key="backtest/summary.json", Body=summary_body,
                  ContentType="application/json", CacheControl="public, max-age=600")

    print(f"[backtest] wrote backtest/results.json ({len(full_body):,}b) and summary.json ({len(summary_body):,}b)")
    print(f"[backtest] {n_total} outcomes, {n_correct} correct ({n_correct/max(n_total,1)*100:.1f}%)")
    print(f"[backtest] strategy: ${final_nav:.0f}  return={nav_return_pct:+.2f}%   SPY: ${spy_final_nav}  return={spy_return_pct}%   alpha={alpha_pct}")
    print(f"[backtest] window={first_date} → {last_date} ({n_days} days)  max_dd={max_dd:.2f}%  sharpe={sharpe}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "n_outcomes": n_total,
            "total_return_pct": round(nav_return_pct, 4),
            "final_nav": round(final_nav, 2),
            "spy_return_pct": round(spy_return_pct, 4) if spy_return_pct is not None else None,
            "alpha_vs_spy_pct": round(alpha_pct, 4) if alpha_pct is not None else None,
            "max_dd_pct": round(max_dd, 4),
            "sharpe": round(sharpe, 4) if sharpe is not None else None,
            "n_signals": len(by_signal),
            "n_horizon_weighted": n_horizon_weighted,
            "n_flat_weighted": n_flat_weighted,
            "horizon_breakdown": dict(horizon_breakdown),
            # v1.2 realistic stats
            "realistic_return_pct": round(realistic_return_pct, 4),
            "realistic_sharpe": round(realistic_sharpe, 4) if realistic_sharpe is not None else None,
            "realistic_alpha_pct": round(realistic_alpha_pct, 4) if realistic_alpha_pct is not None else None,
            "realistic_max_dd_pct": round(realistic_max_dd, 4),
            "friction_drag_pct": round(nav_return_pct - realistic_return_pct, 4),
            "n_concentration_capped_days": n_concentration_capped_days,
            "n_gross_capped_days": n_gross_capped_days,
            "duration_s": duration,
        }),
    }
