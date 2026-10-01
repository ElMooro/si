"""Daily aggregate activity statistics, not participant or trade-direction evidence.

Existing grouped-daily source, universe, baseline window and request cadence are
unchanged. Relative volume (30), relative dollar volume (25), range expansion
(20), and valid relative average transaction size (25) supply the score. The
last term is omitted when either size operand is unavailable, without rescaling
other terms. Average size describes volume / transaction count, not block prints,
institutional identity, venue activity or signed order flow.
"""
import json
import math
import os
import statistics
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
from managed_secret import managed_secret  # audit 2026-09-08 INST-06: no literal credentials

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY_OUT = os.environ.get("S3_KEY_OUT", "data/tape-reader.json")
POLY_KEY = managed_secret(('POLY_KEY', 'POLYGON_API_KEY', 'POLYGON_KEY'), ("/justhodl/polygon/api-key",))
N_BASELINE_DAYS = int(os.environ.get("N_BASELINE_DAYS", "20"))
MIN_DOLLAR_VOL = float(os.environ.get("MIN_DOLLAR_VOL", "5000000"))  # $5M min daily

S3 = boto3.client("s3", region_name=REGION)


def _http_get_json(url, timeout=30):
    req = urllib.request.Request(url, headers={"User-Agent": "justhodl-tape/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def fetch_grouped_daily(date_str):
    """One day of OHLCV+transactions for all stocks. Returns dict ticker → bar.
    Bar: {T, v, vw, o, c, h, l, t, n}"""
    qs = urllib.parse.urlencode({"apiKey": POLY_KEY, "adjusted": "true"})
    url = f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{date_str}?{qs}"
    try:
        d = _http_get_json(url, timeout=30)
        results = d.get("results") or []
        return {b.get("T"): b for b in results if b.get("T")}
    except Exception as e:
        print(f"[tape] grouped daily {date_str} fail: {e}")
        return {}


def fetch_most_recent_today():
    """Find the most recent trading day with data via grouped daily.
    Walks backwards from today up to 5 days until we find one with results.
    Returns (date_str, dict ticker → bar) or (None, {})."""
    cur = datetime.now(timezone.utc).date()
    for _ in range(7):
        ds = cur.strftime("%Y-%m-%d")
        bars = fetch_grouped_daily(ds)
        if bars and len(bars) > 100:
            print(f"[tape] today's data found: {ds} ({len(bars)} tickers)")
            return ds, bars
        cur -= timedelta(days=1)
    return None, {}


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


def finite_number(value):
    """JSON numbers only; booleans, strings and nonfinite values are unavailable."""
    if type(value) not in (int, float):
        return None
    try:
        return value if math.isfinite(value) else None
    except OverflowError:
        return None


def transaction_count(value):
    value = finite_number(value)
    return value if value is not None and value >= 0 and value == int(value) else None


def average_trade_size(volume, count):
    volume = finite_number(volume)
    count = transaction_count(count)
    if volume is None or volume < 0 or count is None or count <= 0:
        return None
    return finite_number(volume / count)


def build_baseline(n_days, universe_set, exclude_date=None):
    """For each baseline trading day, pull grouped daily and accumulate per ticker.
    Excludes exclude_date (typically today_date) so baseline is strictly historical."""
    print(f"[tape] Pulling {n_days}-day baseline (in parallel)…")
    dates = get_baseline_dates(n_days)
    if exclude_date and exclude_date in dates:
        dates = [d for d in dates if d != exclude_date]
    per_ticker = {}
    successes = 0
    with ThreadPoolExecutor(max_workers=4) as exe:
        futures = {exe.submit(fetch_grouped_daily, d): d for d in dates}
        for fut in as_completed(futures):
            date_bars = fut.result()
            if date_bars and len(date_bars) > 100:
                successes += 1
            for sym, bar in date_bars.items():
                if sym in universe_set:
                    per_ticker.setdefault(sym, []).append(bar)

    baseline = {}
    for sym, bars in per_ticker.items():
        if len(bars) < 5:
            continue
        vols = [b.get("v", 0) or 0 for b in bars]
        d_vols = [(b.get("v", 0) or 0) * (b.get("vw", 0) or b.get("c", 0) or 0) for b in bars]
        ranges = []
        for b in bars:
            h = b.get("h"); l = b.get("l"); c = b.get("c")
            if h and l and c:
                ranges.append((h - l) / c if c else 0)
        n_txns = [transaction_count(b.get("n")) for b in bars]
        size_complete = all(average_trade_size(b.get("v"), b.get("n")) is not None for b in bars)
        baseline[sym] = {
            "avg_vol": statistics.mean(vols) if vols else 0,
            "avg_dollar_vol": statistics.mean(d_vols) if d_vols else 0,
            "avg_range_pct": statistics.mean(ranges) if ranges else 0,
            "avg_n_txns": statistics.mean(n_txns) if all(n is not None for n in n_txns) else None,
            "avg_trade_size_baseline": average_trade_size(sum(b["v"] for b in bars), sum(n_txns)) if size_complete else None,
            "n_bars_used": len(bars),
        }
    print(f"[tape] Baseline built: {len(baseline)} tickers from {successes}/{len(dates)} days")
    return baseline


def score_ticker(today_bar, baseline_rec):
    """Compute score components from a grouped-daily bar.
    today_bar fields: T (ticker), v (volume), vw (vwap), o, c, h, l, n (n_trades)
    """
    today_vol = finite_number(today_bar.get("v"))
    if today_vol is None or today_vol < 0:
        return 0, None, [], None
    today_close = today_bar.get("c", 0) or 0
    today_high = today_bar.get("h", 0) or 0
    today_low = today_bar.get("l", 0) or 0
    today_open = today_bar.get("o", 0) or 0
    today_n_trades = transaction_count(today_bar.get("n"))
    today_vwap = today_bar.get("vw", today_close) or today_close
    today_dollar_vol = today_vol * today_vwap
    today_range_pct = (today_high - today_low) / today_close if today_close else 0
    change_pct = ((today_close - today_open) / today_open * 100) if today_open else 0

    avg_vol = baseline_rec.get("avg_vol", 0) or 0
    avg_dollar_vol = baseline_rec.get("avg_dollar_vol", 0) or 0
    avg_range_pct = baseline_rec.get("avg_range_pct", 0) or 0

    if today_dollar_vol < MIN_DOLLAR_VOL or avg_vol == 0:
        return 0, None, [], change_pct

    rel_volume = today_vol / max(avg_vol, 1)
    rel_dollar_vol = today_dollar_vol / max(avg_dollar_vol, 1)
    rel_range = today_range_pct / max(avg_range_pct, 0.001)
    avg_trade_size_today = average_trade_size(today_vol, today_n_trades)
    avg_trade_size_base = finite_number(baseline_rec.get("avg_trade_size_baseline"))
    if avg_trade_size_base is not None and avg_trade_size_base < 0:
        avg_trade_size_base = None
    # Preserve the legacy denominator floor for valid data in this bounded fix.
    block_ratio = (finite_number(avg_trade_size_today / max(avg_trade_size_base, 1))
                   if avg_trade_size_today is not None and avg_trade_size_base is not None
                   and avg_trade_size_base > 0 else None)

    s_vol = min(30, max(0, (rel_volume - 1) * 15))
    s_dvol = min(25, max(0, (rel_dollar_vol - 1) * 12))
    s_range = min(20, max(0, (rel_range - 1) * 10))
    s_block = min(25, max(0, (block_ratio - 1) * 15)) if block_ratio is not None else 0
    score = s_vol + s_dvol + s_range + s_block

    classifications = []
    if rel_volume >= 3.0:
        classifications.append("VOLUME_SURGE")
    if rel_range >= 2.0:
        classifications.append("RANGE_EXPANSION")
    if block_ratio is not None and block_ratio >= 1.8:
        classifications.append("LARGE_AVG_TRADE_SIZE")
    if today_dollar_vol >= 1_000_000_000:
        classifications.append("MEGA_NOTIONAL")

    return round(score, 1), {
        "rel_volume": round(rel_volume, 2),
        "rel_dollar_volume": round(rel_dollar_vol, 2),
        "range_expansion": round(rel_range, 2),
        "block_ratio": round(block_ratio, 2) if block_ratio is not None else None,
        "trade_size_status": "available" if block_ratio is not None else "unavailable",
        "trade_size_score": round(s_block, 1),
        "trade_count_status": ("unavailable" if today_n_trades is None else
                               "reported_zero" if today_n_trades == 0 else "reported"),
        "today_vol": int(today_vol),
        "today_dollar_vol": int(today_dollar_vol),
        "today_n_trades": today_n_trades,
        "avg_trade_size_today": round(avg_trade_size_today, 0) if avg_trade_size_today is not None else None,
        "avg_trade_size_baseline": round(avg_trade_size_base, 0) if avg_trade_size_base is not None else None,
    }, classifications, round(change_pct, 2)


def synth_rationale(ticker, components, classifications, change_pct):
    parts = [f"vol {components['rel_volume']}× baseline"]
    if components["block_ratio"] is None:
        parts.append("average-size comparison unavailable; size score omitted")
    elif components["block_ratio"] > 1.5:
        parts.append(f"aggregate avg trade size {components['block_ratio']}× baseline")
    if components["range_expansion"] > 1.5:
        parts.append(f"range {components['range_expansion']}× normal")
    if change_pct is not None:
        parts.append(f"open-to-close {'+' if change_pct >= 0 else ''}{change_pct:.1f}%")
    return " · ".join(parts)


def lambda_handler(event, context):
    started = time.time()

    print("[tape] Loading universe…")
    universe = fetch_universe()
    universe_set = set(universe)
    print(f"[tape] Universe: {len(universe)} tickers")

    print("[tape] Finding most recent trading day with data…")
    today_date, today_bars = fetch_most_recent_today()
    if not today_bars:
        return {"statusCode": 200, "body": json.dumps({"ok": False, "err": "no recent grouped data"})}
    print(f"[tape] Today = {today_date} ({len(today_bars)} tickers in market)")

    # Filter to universe
    today_universe = {t: b for t, b in today_bars.items() if t in universe_set}
    print(f"[tape] In universe + with today data: {len(today_universe)}")

    # Build 20-day baseline (excluding today_date)
    baseline = build_baseline(N_BASELINE_DAYS, universe_set, exclude_date=today_date)

    # Score each ticker
    results = []
    breadth = {"advance": 0, "decline": 0, "unch": 0}
    for ticker, bar in today_universe.items():
        base_rec = baseline.get(ticker)
        if not base_rec:
            continue
        score, components, classifications, change_pct = score_ticker(bar, base_rec)

        if change_pct is not None:
            if change_pct > 0.5:
                breadth["advance"] += 1
            elif change_pct < -0.5:
                breadth["decline"] += 1
            else:
                breadth["unch"] += 1

        if score == 0 or components is None:
            continue
        results.append({
            "ticker": ticker,
            "score": score,
            "classifications": classifications,
            "change_pct": change_pct,
            **components,
            "rationale": synth_rationale(ticker, components, classifications, change_pct),
        })

    results.sort(key=lambda r: r["score"], reverse=True)
    top = results[:30]

    payload = {
        "schema_version": "1.2",
        "measurement_contract": "tape-reader-activity.v2",
        "meaning": "Unsigned daily aggregate activity; no participant identity, block-trade detection or trade direction.",
        "score_scope": "30 volume + 25 dollar volume + 20 range + up to 25 valid average-size points; unavailable size contributes no points, with no rescaling.",
        "units": {"avg_trade_size_today": "shares per transaction", "avg_trade_size_baseline": "shares per transaction", "block_ratio": "relative average size (legacy field name; denominator floored at one share)", "today_n_trades": "transactions"},
        "method": "tape_reader_v1_grouped_daily",
        "as_of": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "today_date": today_date,
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
            "ok": True,
            "today_date": today_date,
            "n_with_data": len(results),
            "n_top": len(top),
            "top_5": [r["ticker"] for r in top[:5]],
            "duration_s": payload["duration_s"],
        }),
    }
