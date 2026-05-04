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

# Window preference for dedup — when the same signal_id has multiple scored
# outcomes (e.g., day_30, day_60, day_90), use the SHORTEST as the canonical one.
# Shorter windows let the strategy turn capital faster and align better with
# realistic trading horizons.
WINDOW_PREFERENCE = ["day_30", "day_60", "day_90", "day_14", "day_7", "day_3", "day_1"]

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
    print(f"[backtest] loaded {len(weights)} calibrated weights")

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
    for o in outcomes:
        stype = o.get("signal_type")
        if not stype:
            continue
        w = weights.get(stype)
        if w is None:
            skipped_no_weight += 1
            continue

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
            "window_key":   o.get("window_key"),
            "weight":       w,
            "actual_return": ret,
            "correct":      bool(o.get("correct")),
            "contribution": contribution,
            "checked_at":   checked_at,
            "logged_at":    logged_at,
        })

    print(f"[backtest] kept {len(results)} contributions  (skipped {skipped_no_weight} no_weight + {skipped_no_return} no_return)")

    # 4. Sort by checked_at — chronological order matters for NAV
    results.sort(key=lambda x: x["checked_at"] or "")

    # 5. Aggregate by signal_type
    by_signal = defaultdict(lambda: {"n": 0, "n_correct": 0, "sum_contribution": 0.0,
                                     "sum_return": 0.0, "weight": 0.0})
    for r in results:
        bucket = by_signal[r["signal_type"]]
        bucket["n"] += 1
        bucket["n_correct"] += 1 if r["correct"] else 0
        bucket["sum_contribution"] += r["contribution"]
        bucket["sum_return"] += r["actual_return"]
        bucket["weight"] = r["weight"]

    signal_summary = []
    for sig, b in by_signal.items():
        n = b["n"]
        signal_summary.append({
            "signal_type": sig,
            "weight": round(b["weight"], 4),
            "n_outcomes": n,
            "win_rate": round(b["n_correct"] / max(n, 1), 4),
            "avg_return_pct": round(b["sum_return"] / max(n, 1), 4),
            "total_contribution": round(b["sum_contribution"], 4),
            "avg_contribution": round(b["sum_contribution"] / max(n, 1), 4),
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

    # 8. Compute summary stats
    n_total = len(results)
    n_correct = sum(1 for r in results if r["correct"])
    total_contribution = sum(r["contribution"] for r in results)
    final_nav = nav_curve[-1]["nav"] if nav_curve else INITIAL_NAV
    nav_return_pct = (final_nav - INITIAL_NAV) / INITIAL_NAV * 100

    # Date range
    dates = [r["checked_at"][:10] for r in results if r["checked_at"]]
    first_date = min(dates) if dates else None
    last_date = max(dates) if dates else None
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

    # 9. Build full results
    results_doc = {
        "v": "1.0",
        "generated_at": now.isoformat(),
        "method": "calibrated_alpha_replay_v2",
        "method_description": (
            "For each scored outcome (deduped to one per signal_id, shortest window preferred), "
            "contribution = 0.005 × weight × predicted_direction_sign × actual_return. "
            "Each unique trade is sized at 0.5% of NAV modulated by the signal's calibration weight. "
            "So a w=1.5 signal earning +10% contributes 0.075% to NAV. Trades distribute across their "
            "logged_at firing dates and NAV compounds normally from $100k. This estimates how much "
            "alpha the calibrated weighting would have generated if every realized signal had been "
            "executed at this position size. Compare to SPY buy-and-hold over the same window."
        ),
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
            "skipped_no_weight": skipped_no_weight,
            "skipped_no_return": skipped_no_return,
        },
        "by_signal": signal_summary,
        "daily": daily,
        "nav_curve": nav_curve,
    }

    # Slim summary for fast page loads
    summary_doc = {
        "v": "1.0",
        "generated_at": now.isoformat(),
        "summary": results_doc["summary"],
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
    print(f"[backtest] total_contribution={total_contribution:.2f}%  final_nav=${final_nav:.0f}  return={nav_return_pct:+.2f}%")
    print(f"[backtest] window={first_date} → {last_date} ({n_days} days)  max_dd={max_dd:.2f}%  sharpe={sharpe}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "n_outcomes": n_total,
            "total_return_pct": round(nav_return_pct, 4),
            "final_nav": round(final_nav, 2),
            "max_dd_pct": round(max_dd, 4),
            "sharpe": round(sharpe, 4) if sharpe is not None else None,
            "n_signals": len(by_signal),
            "duration_s": duration,
        }),
    }
