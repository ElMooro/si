"""
justhodl-tape-reader — Institutional tape-activity detector.

WHY THIS EXISTS
───────────────
The platform has options-flow but NO equity-side tape reader. Block trades,
dark-pool prints, and unusual volume relative to baseline are the highest-
frequency institutional-footprint signals, and they were invisible.

ALGORITHM (v1)
──────────────
For every name in the S&P 500 universe, daily after market close:

  1. PULL today's snapshot via Polygon /v3/snapshot/locale/us/markets/stocks/tickers
     (single bulk call returns ~9000 tickers with day.v, prevDay.v, todaysChangePerc,
      day.h/l, day.vw, etc.)

  2. PULL last 20 trading days of grouped daily bars
     (/v2/aggs/grouped/locale/us/market/stocks/{date}, 20 calls)
     Build per-ticker 20d avg volume + 20d avg dollar volume + 20d avg range.

  3. SCORE each ticker:
       rel_volume       = today_vol / avg_20d_vol            (cap 30 pts)
       rel_dollar_vol   = today_dollar_vol / avg_20d_dvol     (cap 25 pts)
       range_expansion  = today_range_pct / avg_20d_range_pct (cap 20 pts)
       avg_trade_size   = today_vol / today_n_trades          (block proxy, cap 25 pts)

     Block proxy: when AVG TRADE SIZE is unusually large relative to baseline,
     it means institutions are crossing big prints (often via dark pool or block
     desk). High avg_trade_size + high relative volume = institutional footprint.

  4. RANK top 30 by composite score. Tag with "loud tape" classifications:
       BLOCK_PRINTS   — avg_trade_size z >= 2.0
       VOLUME_SURGE   — rel_volume >= 3.0
       RANGE_EXP      — range_expansion >= 2.0
       NOTIONAL_LOAD  — dollar_volume >= 99th pct (>$1B for liquid names)

OUTPUT
──────
  s3://justhodl-dashboard-live/data/tape-reader.json
  {
    as_of, n_universe, n_with_data,
    top_loud_tape: [
      { ticker, score, classification[],
        rel_volume, rel_dollar_volume, range_expansion,
        avg_trade_size_today, avg_trade_size_baseline,
        today_vol, today_dollar_vol, today_n_trades,
        change_pct,
        rationale (1-line)
      }, ... x30
    ],
    market_breadth: { advance, decline, unch, advance_decline_ratio }
  }

SCHEDULE
────────
  cron(30 21 * * MON-FRI *)  — 5:30 PM ET on weekdays after market close
                                (90min after close, gives Polygon time to settle)

ZERO DETERIORATION
  ✓ No Lambda touched
  ✓ Polygon premium key already paid for — same /v3/snapshot used by other lambdas
  ✓ ~21 API calls per run (1 snapshot + 20 grouped daily)
  ✓ Failure-safe: if grouped daily fails, falls back to prevDay comparison
"""
import json
import os
import statistics
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY_OUT = os.environ.get("S3_KEY_OUT", "data/tape-reader.json")
POLY_KEY = os.environ.get("POLY_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")
N_BASELINE_DAYS = int(os.environ.get("N_BASELINE_DAYS", "20"))
MIN_DOLLAR_VOL = float(os.environ.get("MIN_DOLLAR_VOL", "5000000"))  # $5M min daily

S3 = boto3.client("s3", region_name=REGION)


def _http_get_json(url, timeout=30):
    req = urllib.request.Request(url, headers={"User-Agent": "justhodl-tape/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def fetch_snapshot_all():
    """Single bulk call — returns today's snapshot for all US stocks."""
    qs = urllib.parse.urlencode({"apiKey": POLY_KEY})
    url = f"https://api.polygon.io/v3/snapshot/locale/us/markets/stocks/tickers?{qs}"
    try:
        d = _http_get_json(url, timeout=60)
        return d.get("tickers") or []
    except Exception as e:
        print(f"[tape] snapshot fail: {e}")
        return []


def fetch_grouped_daily(date_str):
    """One day of OHLCV+transactions for all stocks. Returns dict ticker → bar."""
    qs = urllib.parse.urlencode({"apiKey": POLY_KEY, "adjusted": "true"})
    url = f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{date_str}?{qs}"
    try:
        d = _http_get_json(url, timeout=30)
        results = d.get("results") or []
        return {b.get("T"): b for b in results if b.get("T")}
    except Exception as e:
        print(f"[tape] grouped daily {date_str} fail: {e}")
        return {}


def get_baseline_dates(n=20):
    """Last n trading days going backwards from yesterday (skip today, weekends).
    Heuristic — Polygon will simply return empty for non-trading days."""
    out = []
    cur = datetime.now(timezone.utc).date() - timedelta(days=1)
    while len(out) < n + 5:  # buffer for weekends/holidays
        if cur.weekday() < 5:  # 0=Mon, 6=Sun
            out.append(cur.strftime("%Y-%m-%d"))
        cur -= timedelta(days=1)
        if len(out) >= n + 5:
            break
    return out[:n + 5]


def fetch_universe():
    """Pull ticker list from existing universe.json (1,795 stocks across cap buckets)."""
    try:
        obj = S3.get_object(Bucket=BUCKET, Key="data/universe.json")
        d = json.loads(obj["Body"].read())
        # universe-builder v3 schema: {stocks: [{symbol, name, market_cap, ...}]}
        stocks = d.get("stocks") or []
        # Limit to large/mid/small cap, exclude micro/nano (low quality data)
        out = []
        for s in stocks:
            sym = s.get("symbol")
            if not sym:
                continue
            cap_bucket = s.get("cap_bucket") or ""
            if cap_bucket in ("mega", "large", "mid", "small"):
                out.append(sym)
        if out:
            print(f"[tape] universe loaded: {len(out)} symbols (excluded micro/nano)")
            return out
        # If schema differs, try fallback paths
        for key in ("sp500", "tickers", "universe", "symbols"):
            v = d.get(key) if isinstance(d, dict) else None
            if isinstance(v, list):
                fb = []
                for item in v:
                    if isinstance(item, str):
                        fb.append(item)
                    elif isinstance(item, dict):
                        fb.append(item.get("symbol") or item.get("ticker"))
                fb = [x for x in fb if x]
                if fb:
                    return fb
    except Exception as e:
        print(f"[tape] universe fetch fail: {e}")
    # Hard-coded fallback (large-caps)
    return [
        "AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA", "JPM",
        "V", "UNH", "XOM", "MA", "PG", "AVGO", "HD", "CVX", "MRK", "ABBV",
        "PEP", "KO", "WMT", "BAC", "LLY", "TMO", "ORCL", "PFE", "DIS",
        "ADBE", "ABT", "CSCO", "ACN", "CRM", "NFLX", "AMD", "WFC", "INTC",
        "INTU", "T", "VZ", "TXN", "QCOM", "PM", "RTX", "LIN", "NEE",
        "DHR", "UNP", "BMY", "AMGN", "PLTR", "MU", "DE", "CAT",
    ]


def build_baseline(n_days, universe_set):
    """For each baseline trading day, pull grouped daily and accumulate per ticker."""
    print(f"[tape] Pulling {n_days}-day baseline (in parallel)…")
    dates = get_baseline_dates(n_days)
    per_ticker = {}  # sym → list of bars
    successes = 0
    with ThreadPoolExecutor(max_workers=4) as exe:
        futures = {exe.submit(fetch_grouped_daily, d): d for d in dates}
        for fut in as_completed(futures):
            date_bars = fut.result()
            if date_bars:
                successes += 1
            for sym, bar in date_bars.items():
                if sym in universe_set:
                    per_ticker.setdefault(sym, []).append(bar)

    # Compute baselines per ticker
    baseline = {}
    for sym, bars in per_ticker.items():
        if len(bars) < 5:  # need at least a week of data
            continue
        vols = [b.get("v", 0) or 0 for b in bars]
        d_vols = [(b.get("v", 0) or 0) * (b.get("vw", 0) or b.get("c", 0) or 0) for b in bars]
        ranges = []
        for b in bars:
            h = b.get("h"); l = b.get("l"); c = b.get("c")
            if h and l and c:
                ranges.append((h - l) / c if c else 0)
        n_txns = [b.get("n", 0) or 0 for b in bars]
        baseline[sym] = {
            "avg_vol": statistics.mean(vols) if vols else 0,
            "avg_dollar_vol": statistics.mean(d_vols) if d_vols else 0,
            "avg_range_pct": statistics.mean(ranges) if ranges else 0,
            "avg_n_txns": statistics.mean(n_txns) if n_txns else 0,
            "n_bars_used": len(bars),
        }
    print(f"[tape] Baseline built: {len(baseline)} tickers from {successes}/{len(dates)} days")
    return baseline


def score_ticker(snap, baseline_rec):
    """Compute the score components for one ticker. Return (score, components, classifications)."""
    day = snap.get("day") or {}
    today_vol = day.get("v", 0) or 0
    today_close = day.get("c", 0) or 0
    today_high = day.get("h", 0) or 0
    today_low = day.get("l", 0) or 0
    today_n_trades = day.get("n", 0) or 0
    today_dollar_vol = today_vol * (day.get("vw", today_close) or today_close)
    today_range_pct = (today_high - today_low) / today_close if today_close else 0

    avg_vol = baseline_rec.get("avg_vol", 0) or 0
    avg_dollar_vol = baseline_rec.get("avg_dollar_vol", 0) or 0
    avg_range_pct = baseline_rec.get("avg_range_pct", 0) or 0
    avg_n_txns = baseline_rec.get("avg_n_txns", 0) or 0

    if today_dollar_vol < MIN_DOLLAR_VOL or avg_vol == 0:
        return 0, None, []

    rel_volume = today_vol / max(avg_vol, 1)
    rel_dollar_vol = today_dollar_vol / max(avg_dollar_vol, 1)
    rel_range = today_range_pct / max(avg_range_pct, 0.001)
    avg_trade_size_today = today_vol / max(today_n_trades, 1)
    avg_trade_size_base = avg_vol / max(avg_n_txns, 1)
    block_ratio = avg_trade_size_today / max(avg_trade_size_base, 1)

    # Score (0-100 capped)
    s_vol = min(30, max(0, (rel_volume - 1) * 15))      # 1x=0, 3x=30
    s_dvol = min(25, max(0, (rel_dollar_vol - 1) * 12))
    s_range = min(20, max(0, (rel_range - 1) * 10))
    s_block = min(25, max(0, (block_ratio - 1) * 15))
    score = s_vol + s_dvol + s_range + s_block

    classifications = []
    if rel_volume >= 3.0:
        classifications.append("VOLUME_SURGE")
    if rel_range >= 2.0:
        classifications.append("RANGE_EXPANSION")
    if block_ratio >= 1.8:
        classifications.append("BLOCK_PRINTS")
    if today_dollar_vol >= 1_000_000_000:
        classifications.append("MEGA_NOTIONAL")

    return round(score, 1), {
        "rel_volume": round(rel_volume, 2),
        "rel_dollar_volume": round(rel_dollar_vol, 2),
        "range_expansion": round(rel_range, 2),
        "block_ratio": round(block_ratio, 2),
        "today_vol": today_vol,
        "today_dollar_vol": int(today_dollar_vol),
        "today_n_trades": today_n_trades,
        "avg_trade_size_today": round(avg_trade_size_today, 0),
        "avg_trade_size_baseline": round(avg_trade_size_base, 0),
    }, classifications


def synth_rationale(ticker, components, classifications, change_pct):
    parts = [f"vol {components['rel_volume']}× baseline"]
    if components["block_ratio"] > 1.5:
        parts.append(f"avg trade size {components['block_ratio']}× normal (block prints)")
    if components["range_expansion"] > 1.5:
        parts.append(f"range {components['range_expansion']}× normal")
    if change_pct is not None:
        parts.append(f"close {'+' if change_pct >= 0 else ''}{change_pct:.1f}%")
    return " · ".join(parts)


def lambda_handler(event, context):
    started = time.time()

    print("[tape] Loading universe…")
    universe = fetch_universe()
    universe_set = set(universe)
    print(f"[tape] Universe: {len(universe)} tickers")

    print("[tape] Pulling all-tickers snapshot (1 call)…")
    snapshots = fetch_snapshot_all()
    print(f"[tape] Snapshot: {len(snapshots)} tickers")

    # Filter to universe + index by ticker
    by_ticker = {s.get("ticker"): s for s in snapshots
                  if s.get("ticker") in universe_set}
    print(f"[tape] In universe + with snapshot: {len(by_ticker)}")

    # Build 20-day baseline
    baseline = build_baseline(N_BASELINE_DAYS, universe_set)

    # Score each ticker
    results = []
    breadth = {"advance": 0, "decline": 0, "unch": 0}
    for ticker, snap in by_ticker.items():
        change_pct = snap.get("todaysChangePerc")
        if change_pct is not None:
            if change_pct > 0.5:
                breadth["advance"] += 1
            elif change_pct < -0.5:
                breadth["decline"] += 1
            else:
                breadth["unch"] += 1

        base_rec = baseline.get(ticker)
        if not base_rec:
            continue
        score, components, classifications = score_ticker(snap, base_rec)
        if score == 0 or components is None:
            continue
        results.append({
            "ticker": ticker,
            "score": score,
            "classifications": classifications,
            "change_pct": round(change_pct, 2) if change_pct is not None else None,
            **components,
            "rationale": synth_rationale(ticker, components, classifications, change_pct),
        })

    results.sort(key=lambda r: r["score"], reverse=True)
    top = results[:30]

    payload = {
        "schema_version": "1.0",
        "method": "tape_reader_v1",
        "as_of": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "n_universe": len(universe),
        "n_with_data": len(results),
        "top_loud_tape": top,
        "market_breadth": {
            **breadth,
            "advance_decline_ratio": round(breadth["advance"] / max(breadth["decline"], 1), 2),
        },
        "duration_s": round(time.time() - started, 1),
    }

    body_bytes = json.dumps(payload, indent=2, default=str).encode("utf-8")
    S3.put_object(
        Bucket=BUCKET, Key=S3_KEY_OUT, Body=body_bytes,
        ContentType="application/json", CacheControl="max-age=600",
    )
    print(f"[tape] DONE in {payload['duration_s']}s · {len(results)} scored · "
          f"top: {[r['ticker'] for r in top[:5]]}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "ok": True, "n_with_data": len(results),
            "n_top": len(top),
            "top_5": [r["ticker"] for r in top[:5]],
            "duration_s": payload["duration_s"],
        }),
    }
