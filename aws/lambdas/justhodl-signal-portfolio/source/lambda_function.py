"""
justhodl-signal-portfolio — Per-signal paper portfolio with PnL tracking.

Reads daily signals from asymmetric-scorer + cot-extremes + divergence-scanner +
short-interest + earnings-tracker. For each signal, simulates a paper trade
with entry/target/stop and tracks PnL.

State persists in S3 across runs. Each day:
  1. Load existing open positions from S3
  2. Update marks-to-market using latest Polygon prices
  3. Close positions that hit target or stop
  4. Open NEW positions for fresh signals (deduplicated against open positions)
  5. Compute aggregate stats: win rate, avg win/loss, Sharpe, max DD, profit factor
  6. Write state + append to history

Signal source ranking (lowest signal_id wins on dup):
  1. asymmetric-scorer (most rigorous: 5-dim QARP + risk-sized)
  2. cot-extremes-scanner (smart-money positioning extremes)
  3. divergence-scanner (12 cross-asset OLS residual extremes)
  4. earnings-tracker PEAD (post-earnings drift)
  5. short-interest squeeze risk

Output:
  - portfolio/signal-portfolio-state.json   (open + recently-closed positions)
  - portfolio/signal-portfolio-history.json (rolling 365-day trade ledger)
"""
import json
import os
import time
import urllib.request
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from statistics import mean, stdev
import boto3
import hashlib

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
STATE_KEY = "portfolio/signal-portfolio-state.json"
HISTORY_KEY = "portfolio/signal-portfolio-history.json"

POLYGON_KEY = os.environ.get("POLYGON_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")

# Position parameters (aggressive enough to prove alpha quickly)
DEFAULT_HOLD_DAYS = 21       # 1-month max hold
DEFAULT_TARGET_PCT = 0.075   # 7.5% take-profit
DEFAULT_STOP_PCT = 0.04      # 4% stop-loss → ~1.9 reward/risk
INITIAL_NAV = 100_000.0      # paper portfolio starting equity
PER_TRADE_RISK_PCT = 0.01    # 1% portfolio risk per trade
MAX_OPEN_POSITIONS = 25
HISTORY_RETAIN_DAYS = 365


def fetch_s3_json(key, default=None):
    try:
        obj = S3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except S3.exceptions.NoSuchKey:
        return default if default is not None else {}
    except Exception as e:
        print(f"[s3] read {key} failed: {e}")
        return default if default is not None else {}


def put_s3_json(key, payload, cache_seconds=300):
    body = json.dumps(payload, default=str).encode("utf-8")
    S3.put_object(
        Bucket=BUCKET, Key=key, Body=body,
        ContentType="application/json",
        CacheControl=f"public, max-age={cache_seconds}",
    )
    return len(body)


def polygon_last_close(ticker, retries=2):
    """Fetch most recent daily close from Polygon."""
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/prev?adjusted=true&apiKey={POLYGON_KEY}"
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "justhodl-signal-portfolio/1.0"})
            with urllib.request.urlopen(req, timeout=10) as r:
                data = json.loads(r.read().decode())
            if data.get("results"):
                bar = data["results"][0]
                return {
                    "ticker": ticker,
                    "close": float(bar["c"]),
                    "high": float(bar["h"]),
                    "low": float(bar["l"]),
                    "open": float(bar["o"]),
                    "ts": bar.get("t"),
                }
        except Exception as e:
            if attempt == retries - 1:
                print(f"[polygon] {ticker} failed: {e}")
            time.sleep(0.4)
    return None


def make_signal_id(source, ticker, direction, signal_type, generated_at_date=None):
    """Deterministic id: hash of source+ticker+direction+date so we don't dupe within the same day."""
    date_str = (generated_at_date or datetime.now(timezone.utc).date().isoformat())[:10]
    raw = f"{source}|{ticker}|{direction}|{signal_type}|{date_str}"
    return hashlib.sha1(raw.encode()).hexdigest()[:16]


def harvest_signals():
    """Pull current signals from across the system and normalize to common schema."""
    signals = []
    today = datetime.now(timezone.utc).date().isoformat()

    # 1. Asymmetric scorer — top setups (highest priority)
    asym = fetch_s3_json("data/asymmetric-setups.json")
    for s in (asym.get("setups") or [])[:15]:
        ticker = s.get("ticker")
        if not ticker:
            continue
        direction = "LONG" if s.get("score", 0) >= 50 else "SHORT"
        signals.append({
            "signal_id": make_signal_id("asymmetric", ticker, direction, "qarp", today),
            "source": "asymmetric_scorer",
            "ticker": ticker,
            "direction": direction,
            "signal_type": s.get("setup_type", "qarp"),
            "score": s.get("score"),
            "rationale": s.get("rationale", ""),
            "priority": 1,
        })

    # 2. COT extremes
    cot = fetch_s3_json("data/cot-extremes.json")
    for s in (cot.get("extremes") or [])[:10]:
        # COT signals: speculator net at extreme percentile → mean revert
        contract = s.get("contract", "")
        # Map to underlying ticker if possible
        ticker_map = {
            "GOLD": "GLD", "SILVER": "SLV", "CRUDE": "USO", "NATGAS": "UNG",
            "S&P": "SPY", "NASDAQ": "QQQ", "RUSSELL": "IWM", "DOW": "DIA",
            "EURO": "FXE", "10YR": "IEF", "30YR": "TLT", "5YR": "IEF",
            "BITCOIN": "IBIT", "ETHEREUM": "ETHA",
        }
        ticker = None
        for k, v in ticker_map.items():
            if k in contract.upper():
                ticker = v
                break
        if not ticker:
            continue
        # If specs at extreme net long → contrarian short, and vice versa
        pctile = s.get("percentile", 50)
        if pctile >= 90:
            direction = "SHORT"
        elif pctile <= 10:
            direction = "LONG"
        else:
            continue
        signals.append({
            "signal_id": make_signal_id("cot", ticker, direction, "extreme", today),
            "source": "cot_extremes",
            "ticker": ticker,
            "direction": direction,
            "signal_type": "speculator_extreme",
            "score": pctile if direction == "SHORT" else (100 - pctile),
            "rationale": f"COT speculator net at p{pctile} ({contract})",
            "priority": 2,
        })

    # 3. Divergence scanner
    div = fetch_s3_json("data/divergence-current.json") or fetch_s3_json("data/divergence/current.json")
    for d in (div.get("divergences") or [])[:8]:
        # Each divergence: pair + direction
        ticker = d.get("rich_ticker") or d.get("ticker_a")
        cheap_ticker = d.get("cheap_ticker") or d.get("ticker_b")
        if ticker and cheap_ticker:
            # Pair trade: short rich, long cheap. We track the long leg (cheap_ticker).
            signals.append({
                "signal_id": make_signal_id("divergence", cheap_ticker, "LONG", "pair_cheap", today),
                "source": "divergence",
                "ticker": cheap_ticker,
                "direction": "LONG",
                "signal_type": "cross_asset_cheap",
                "score": abs(d.get("z_score", 0)) * 10,
                "rationale": f"{cheap_ticker} cheap vs {ticker} (z={d.get('z_score'):+.2f})" if d.get('z_score') is not None else f"{cheap_ticker} cheap vs {ticker}",
                "priority": 3,
            })

    # 4. Earnings tracker — STRONG_POSITIVE_DRIFT only
    earn = fetch_s3_json("data/earnings-tracker.json")
    for r in (earn.get("pead_signals") or [])[:8]:
        if r.get("pead_label") == "STRONG_POSITIVE_DRIFT":
            ticker = r.get("ticker")
            if not ticker:
                continue
            signals.append({
                "signal_id": make_signal_id("earnings", ticker, "LONG", "pead", today),
                "source": "earnings_pead",
                "ticker": ticker,
                "direction": "LONG",
                "signal_type": "post_earnings_drift",
                "score": r.get("pead_score", 80),
                "rationale": f"PEAD: beat {r.get('eps_surprise_pct', 0):+.1f}%, 1d {r.get('return_1d_pct', 0):+.1f}%",
                "priority": 4,
            })

    # 5. Short interest — squeeze risk only (the highest-conviction class)
    si = fetch_s3_json("data/short-interest.json")
    for r in (si.get("top_squeeze_risk") or [])[:6]:
        ticker = r.get("ticker")
        if not ticker:
            continue
        signals.append({
            "signal_id": make_signal_id("short_squeeze", ticker, "LONG", "squeeze", today),
            "source": "short_squeeze",
            "ticker": ticker,
            "direction": "LONG",
            "signal_type": "squeeze_risk",
            "score": r.get("score", 80),
            "rationale": f"Squeeze risk: dtc={r.get('days_to_cover', 0):.1f}, falling short vol",
            "priority": 5,
        })

    # Dedupe by ticker+direction (keep highest priority)
    seen = {}
    for s in sorted(signals, key=lambda x: x["priority"]):
        key = (s["ticker"], s["direction"])
        if key not in seen:
            seen[key] = s
    deduped = list(seen.values())

    print(f"[harvest] raw={len(signals)} deduped={len(deduped)}")
    return deduped


def open_position(signal, entry_price, nav):
    """Construct a new paper position from a signal."""
    risk_dollars = nav * PER_TRADE_RISK_PCT
    stop_pct = DEFAULT_STOP_PCT
    target_pct = DEFAULT_TARGET_PCT
    direction = signal["direction"]

    if direction == "LONG":
        stop_price = entry_price * (1 - stop_pct)
        target_price = entry_price * (1 + target_pct)
        risk_per_share = entry_price - stop_price
    else:
        stop_price = entry_price * (1 + stop_pct)
        target_price = entry_price * (1 - target_pct)
        risk_per_share = stop_price - entry_price

    qty = max(1, int(risk_dollars / max(risk_per_share, 0.01)))
    notional = qty * entry_price

    return {
        "signal_id": signal["signal_id"],
        "source": signal["source"],
        "ticker": signal["ticker"],
        "direction": direction,
        "signal_type": signal["signal_type"],
        "rationale": signal.get("rationale", ""),
        "score": signal.get("score"),
        "entry_date": datetime.now(timezone.utc).date().isoformat(),
        "entry_price": round(entry_price, 4),
        "stop_price": round(stop_price, 4),
        "target_price": round(target_price, 4),
        "qty": qty,
        "notional_at_entry": round(notional, 2),
        "max_hold_days": DEFAULT_HOLD_DAYS,
        "status": "OPEN",
        "current_price": round(entry_price, 4),
        "current_pnl_pct": 0.0,
        "current_pnl_dollars": 0.0,
        "high_water_mark_price": round(entry_price, 4),
        "low_water_mark_price": round(entry_price, 4),
        "exit_date": None,
        "exit_price": None,
        "exit_reason": None,
        "realized_pnl_pct": None,
        "realized_pnl_dollars": None,
        "days_held": 0,
    }


def update_position(pos, current_price):
    """Mark to market and check for target/stop/time exit."""
    pos["current_price"] = round(current_price, 4)

    direction = pos["direction"]
    entry = pos["entry_price"]
    if direction == "LONG":
        pnl_pct = (current_price / entry - 1) * 100
    else:
        pnl_pct = (entry / current_price - 1) * 100
    pnl_dollars = pos["qty"] * (current_price - entry) * (1 if direction == "LONG" else -1)
    pos["current_pnl_pct"] = round(pnl_pct, 3)
    pos["current_pnl_dollars"] = round(pnl_dollars, 2)
    pos["high_water_mark_price"] = round(max(pos.get("high_water_mark_price", entry), current_price), 4)
    pos["low_water_mark_price"] = round(min(pos.get("low_water_mark_price", entry), current_price), 4)

    # Days held
    entry_dt = datetime.fromisoformat(pos["entry_date"]).date()
    today = datetime.now(timezone.utc).date()
    pos["days_held"] = (today - entry_dt).days

    # Exit checks
    exit_reason = None
    if direction == "LONG":
        if current_price <= pos["stop_price"]:
            exit_reason = "STOP_HIT"
        elif current_price >= pos["target_price"]:
            exit_reason = "TARGET_HIT"
    else:
        if current_price >= pos["stop_price"]:
            exit_reason = "STOP_HIT"
        elif current_price <= pos["target_price"]:
            exit_reason = "TARGET_HIT"
    if exit_reason is None and pos["days_held"] >= pos["max_hold_days"]:
        exit_reason = "TIME_EXIT"

    if exit_reason:
        pos["status"] = "CLOSED"
        pos["exit_date"] = datetime.now(timezone.utc).date().isoformat()
        pos["exit_price"] = round(current_price, 4)
        pos["exit_reason"] = exit_reason
        pos["realized_pnl_pct"] = pos["current_pnl_pct"]
        pos["realized_pnl_dollars"] = pos["current_pnl_dollars"]
    return pos


def compute_aggregate_stats(closed_positions, open_positions, initial_nav):
    """Compute win rate, Sharpe, max drawdown, profit factor, etc."""
    if not closed_positions:
        return {
            "n_closed": 0,
            "n_open": len(open_positions),
            "win_rate": None,
            "avg_win_pct": None,
            "avg_loss_pct": None,
            "profit_factor": None,
            "total_realized_pnl": 0.0,
            "total_realized_pnl_pct": 0.0,
            "sharpe_proxy": None,
            "max_drawdown_pct": None,
            "expectancy_pct": None,
            "best_trade_pct": None,
            "worst_trade_pct": None,
            "avg_hold_days": None,
            "by_source": {},
        }

    wins = [p for p in closed_positions if p["realized_pnl_pct"] > 0]
    losses = [p for p in closed_positions if p["realized_pnl_pct"] <= 0]
    n = len(closed_positions)
    win_rate = round(len(wins) / n * 100, 1)
    avg_win_pct = round(mean([p["realized_pnl_pct"] for p in wins]), 2) if wins else 0.0
    avg_loss_pct = round(mean([p["realized_pnl_pct"] for p in losses]), 2) if losses else 0.0

    gross_wins = sum(p["realized_pnl_dollars"] for p in wins)
    gross_losses = abs(sum(p["realized_pnl_dollars"] for p in losses))
    profit_factor = round(gross_wins / gross_losses, 2) if gross_losses > 0 else None

    total_pnl = round(sum(p["realized_pnl_dollars"] for p in closed_positions), 2)
    total_pnl_pct = round(total_pnl / initial_nav * 100, 2)

    pnl_pcts = [p["realized_pnl_pct"] for p in closed_positions]
    if len(pnl_pcts) >= 2 and stdev(pnl_pcts) > 0:
        sharpe_proxy = round(mean(pnl_pcts) / stdev(pnl_pcts), 3)
    else:
        sharpe_proxy = None

    # Max drawdown via running equity curve from realized pnl chronologically
    sorted_closed = sorted(closed_positions, key=lambda p: p["exit_date"] or "")
    eq = initial_nav
    peak = eq
    max_dd_pct = 0.0
    for p in sorted_closed:
        eq += p["realized_pnl_dollars"]
        peak = max(peak, eq)
        dd = (eq - peak) / peak * 100
        if dd < max_dd_pct:
            max_dd_pct = dd
    max_dd_pct = round(max_dd_pct, 2)

    expectancy_pct = round(mean(pnl_pcts), 3)
    best_trade = max(closed_positions, key=lambda p: p["realized_pnl_pct"])
    worst_trade = min(closed_positions, key=lambda p: p["realized_pnl_pct"])
    avg_hold_days = round(mean([p["days_held"] for p in closed_positions]), 1)

    # By-source stats
    by_source = {}
    for p in closed_positions:
        src = p.get("source", "unknown")
        if src not in by_source:
            by_source[src] = {"n": 0, "wins": 0, "total_pnl_dollars": 0.0, "pnl_pcts": []}
        by_source[src]["n"] += 1
        if p["realized_pnl_pct"] > 0:
            by_source[src]["wins"] += 1
        by_source[src]["total_pnl_dollars"] += p["realized_pnl_dollars"]
        by_source[src]["pnl_pcts"].append(p["realized_pnl_pct"])
    for src, v in by_source.items():
        v["win_rate"] = round(v["wins"] / v["n"] * 100, 1) if v["n"] > 0 else None
        v["avg_pnl_pct"] = round(mean(v["pnl_pcts"]), 2) if v["pnl_pcts"] else None
        v["total_pnl_dollars"] = round(v["total_pnl_dollars"], 2)
        del v["pnl_pcts"]

    return {
        "n_closed": n,
        "n_open": len(open_positions),
        "win_rate": win_rate,
        "avg_win_pct": avg_win_pct,
        "avg_loss_pct": avg_loss_pct,
        "profit_factor": profit_factor,
        "total_realized_pnl": total_pnl,
        "total_realized_pnl_pct": total_pnl_pct,
        "sharpe_proxy": sharpe_proxy,
        "max_drawdown_pct": max_dd_pct,
        "expectancy_pct": expectancy_pct,
        "best_trade_pct": round(best_trade["realized_pnl_pct"], 2),
        "best_trade_ticker": best_trade["ticker"],
        "worst_trade_pct": round(worst_trade["realized_pnl_pct"], 2),
        "worst_trade_ticker": worst_trade["ticker"],
        "avg_hold_days": avg_hold_days,
        "by_source": by_source,
    }


def lambda_handler(event=None, context=None):
    started = time.time()
    today = datetime.now(timezone.utc).date().isoformat()

    state = fetch_s3_json(STATE_KEY, {
        "version": "1.0",
        "initial_nav": INITIAL_NAV,
        "open_positions": [],
        "recently_closed": [],
        "all_closed_positions": [],
        "first_seen": today,
        "last_run_date": None,
    })
    history = fetch_s3_json(HISTORY_KEY, {"daily_snapshots": []})

    open_positions = state.get("open_positions", [])
    all_closed = state.get("all_closed_positions", [])
    initial_nav = state.get("initial_nav", INITIAL_NAV)
    print(f"[start] open={len(open_positions)} all_closed={len(all_closed)}")

    # 1. Mark-to-market all open positions
    open_tickers = list(set(p["ticker"] for p in open_positions))
    print(f"[mtm] fetching prices for {len(open_tickers)} tickers")
    prices = {}
    if open_tickers:
        with ThreadPoolExecutor(max_workers=8) as ex:
            futs = {ex.submit(polygon_last_close, t): t for t in open_tickers}
            for f in as_completed(futs):
                bar = f.result()
                if bar:
                    prices[bar["ticker"]] = bar["close"]

    closed_today = []
    still_open = []
    for pos in open_positions:
        price = prices.get(pos["ticker"])
        if price is None:
            still_open.append(pos)
            continue
        updated = update_position(pos, price)
        if updated["status"] == "CLOSED":
            closed_today.append(updated)
            all_closed.append(updated)
        else:
            still_open.append(updated)

    print(f"[mtm] still_open={len(still_open)} closed_today={len(closed_today)}")

    # 2. Harvest fresh signals and open new positions
    signals = harvest_signals()
    open_signal_ids = {p["signal_id"] for p in still_open}
    open_ticker_directions = {(p["ticker"], p["direction"]) for p in still_open}

    new_positions = []
    nav = initial_nav + sum(p.get("realized_pnl_dollars", 0) for p in all_closed)
    for sig in signals:
        if len(still_open) + len(new_positions) >= MAX_OPEN_POSITIONS:
            break
        if sig["signal_id"] in open_signal_ids:
            continue
        if (sig["ticker"], sig["direction"]) in open_ticker_directions:
            continue
        # Need entry price
        bar = polygon_last_close(sig["ticker"])
        if not bar:
            continue
        pos = open_position(sig, bar["close"], nav)
        new_positions.append(pos)
        open_ticker_directions.add((sig["ticker"], sig["direction"]))

    print(f"[open] {len(new_positions)} new positions")

    # Combine open list
    final_open = still_open + new_positions

    # 3. Compute aggregate stats
    stats = compute_aggregate_stats(all_closed, final_open, initial_nav)

    # 4. Open positions current value
    sum_open_pnl = round(sum(p.get("current_pnl_dollars", 0) for p in final_open), 2)
    current_nav = round(nav + sum_open_pnl, 2)

    # 5. Save state
    new_state = {
        "version": "1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "as_of_date": today,
        "first_seen": state.get("first_seen", today),
        "last_run_date": today,
        "initial_nav": initial_nav,
        "current_nav": current_nav,
        "current_nav_pct_chg": round((current_nav - initial_nav) / initial_nav * 100, 2),
        "unrealized_pnl_dollars": sum_open_pnl,
        "open_positions": final_open,
        "recently_closed": closed_today,
        "all_closed_positions": all_closed[-200:],  # cap to last 200 closed
        "stats": stats,
        "duration_s": round(time.time() - started, 2),
    }
    state_size = put_s3_json(STATE_KEY, new_state)

    # 6. Append to history
    snap = {
        "date": today,
        "n_open": len(final_open),
        "n_closed_today": len(closed_today),
        "n_closed_total": len(all_closed),
        "current_nav": current_nav,
        "current_nav_pct_chg": new_state["current_nav_pct_chg"],
        "unrealized_pnl": sum_open_pnl,
        "win_rate": stats.get("win_rate"),
        "total_realized_pnl_pct": stats.get("total_realized_pnl_pct"),
    }
    daily = history.get("daily_snapshots") or []
    # Replace today if exists, else append
    daily = [s for s in daily if s.get("date") != today]
    daily.append(snap)
    # Trim
    cutoff = (datetime.now(timezone.utc).date() - timedelta(days=HISTORY_RETAIN_DAYS)).isoformat()
    daily = [s for s in daily if s.get("date", "") >= cutoff]
    history_size = put_s3_json(HISTORY_KEY, {"daily_snapshots": daily})

    print(f"[done] open={len(final_open)} closed_today={len(closed_today)} all_closed={len(all_closed)}")
    print(f"[done] current_nav=${current_nav:,.2f} ({new_state['current_nav_pct_chg']:+.2f}%) win_rate={stats.get('win_rate')}%")
    print(f"[done] state={state_size:,}b history={history_size:,}b in {new_state['duration_s']}s")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "n_open": len(final_open),
            "n_new_today": len(new_positions),
            "n_closed_today": len(closed_today),
            "n_closed_total": len(all_closed),
            "current_nav": current_nav,
            "current_nav_pct_chg": new_state["current_nav_pct_chg"],
            "win_rate": stats.get("win_rate"),
            "duration_s": new_state["duration_s"],
        }),
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler({}, None), indent=2, default=str))
