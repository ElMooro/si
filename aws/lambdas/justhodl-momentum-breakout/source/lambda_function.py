"""
justhodl-momentum-breakout — early momentum detector for catching pumps before
they go parabolic.

This is the system's 6th hunter, designed specifically to catch the names that
fundamental hunters miss because the pump precedes the fundamentals.

DETECTION STRATEGY:

For each stock in unified universe:
  1. Pull 90-day daily price + volume history (FMP /historical-price-eod)
  2. Compute breakout signals:
     a. Price relative strength: stock vs SPY 20-day, 60-day
     b. New 20d/60d high (price > max of last N closes)
     c. Volume surge: today's volume / 20-day avg
     d. % gain over last 5/10/30 days
     e. Volume-weighted accumulation index
  3. Score each signal 0-100, combine into MOMENTUM_SCORE (0-100)
  4. Flag tier-A if MOMENTUM >= 70

WHO THIS CATCHES:
  • Stocks 5-15% off recent high but volume building (early breakout)
  • Stocks at new 60-day highs with volume surge (just broken out)
  • Stocks outpacing SPY by >15% over 30 days with rising RS
  • Quietly accumulating names where smart money is loading

WHO IT DOESN'T:
  • Already-pumped names (>+50% in 30d) — these get a 'PARABOLIC' flag instead
  • Microcaps with no liquidity (avg daily $ volume < $5M)

OUTPUT: data/momentum-breakout.json
"""
import io, json, os, time, urllib.request, urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = os.environ.get("S3_KEY", "data/momentum-breakout.json")
FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
N_WORKERS = int(os.environ.get("N_WORKERS", "12"))
MIN_DOLLAR_VOL = float(os.environ.get("MIN_DOLLAR_VOL", "5000000"))  # $5M/day
MAX_TICKERS = int(os.environ.get("MAX_TICKERS", "600"))
TIMEOUT_BUDGET_S = int(os.environ.get("TIMEOUT_BUDGET_S", "260"))

S3 = boto3.client("s3", region_name=REGION)


def _http_get_json(url, timeout=15):
    req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-Momentum/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def get_universe():
    """Read unified universe.json — the shared seed pool."""
    try:
        obj = S3.get_object(Bucket=BUCKET, Key="data/universe.json")
        d = json.loads(obj["Body"].read())
        stocks = d.get("stocks", []) or []
        tickers = []
        for s in stocks:
            sym = (s.get("symbol") or "").upper().strip()
            if sym:
                tickers.append(sym)
        print(f"[momentum] universe: {len(tickers)} from data/universe.json")
        return tickers[:MAX_TICKERS]
    except Exception as e:
        print(f"[momentum] WARN — universe load: {e}")
        return []


def fetch_history(symbol, days=90):
    """Return list of {date, close, volume} sorted oldest first, or None."""
    url = f"https://financialmodelingprep.com/stable/historical-price-eod/full?symbol={symbol}&apikey={FMP_KEY}"
    try:
        d = _http_get_json(url, timeout=15)
        if not d or not isinstance(d, list):
            return None
        # Take last N
        out = []
        for r in d[:days]:
            close = r.get("close")
            vol = r.get("volume")
            date = r.get("date")
            if close is not None and date:
                out.append({"date": date, "close": float(close), "volume": float(vol or 0)})
        out.sort(key=lambda x: x["date"])
        return out
    except Exception:
        return None


def fetch_spy_history():
    """Cached SPY 90-day history — single fetch shared across workers."""
    return fetch_history("SPY", days=90)


def compute_signals(symbol, history, spy_returns):
    """Compute all momentum signals + score. Returns dict or None."""
    if not history or len(history) < 30:
        return None

    closes = [h["close"] for h in history]
    volumes = [h["volume"] for h in history]
    dates = [h["date"] for h in history]
    n = len(closes)
    today_close = closes[-1]
    today_vol = volumes[-1]

    # Avg dollar volume — liquidity filter
    avg_dollar_vol = sum(c * v for c, v in zip(closes[-20:], volumes[-20:])) / min(20, n)
    if avg_dollar_vol < MIN_DOLLAR_VOL:
        return None

    # Returns over various windows
    ret_5d = (today_close / closes[-6] - 1) * 100 if n >= 6 else None
    ret_10d = (today_close / closes[-11] - 1) * 100 if n >= 11 else None
    ret_20d = (today_close / closes[-21] - 1) * 100 if n >= 21 else None
    ret_60d = (today_close / closes[-61] - 1) * 100 if n >= 61 else None

    # Highs over windows
    max_20d = max(closes[-20:]) if n >= 20 else None
    max_60d = max(closes[-60:]) if n >= 60 else max_20d
    pct_from_20d_high = (today_close / max_20d - 1) * 100 if max_20d else 0
    pct_from_60d_high = (today_close / max_60d - 1) * 100 if max_60d else 0
    is_at_20d_high = abs(pct_from_20d_high) < 0.5
    is_at_60d_high = abs(pct_from_60d_high) < 0.5

    # Volume surge — today vs 20d avg
    avg_vol_20 = sum(volumes[-20:]) / min(20, n) if n >= 20 else 0
    vol_ratio = today_vol / avg_vol_20 if avg_vol_20 > 0 else 0

    # Relative strength vs SPY
    rs_20d = None
    rs_60d = None
    if spy_returns and n >= 21:
        rs_20d = ret_20d - spy_returns.get("20d", 0)
    if spy_returns and n >= 61:
        rs_60d = ret_60d - spy_returns.get("60d", 0)

    # PARABOLIC filter — already up >50% in 30d means we missed it
    is_parabolic = (ret_20d or 0) > 50 or (ret_10d or 0) > 35

    # Volume-accumulation index — count days where volume > avg AND close > prev close
    last_20 = list(zip(closes[-20:], volumes[-20:]))
    accum_days = 0
    for i in range(1, len(last_20)):
        if last_20[i][1] > avg_vol_20 and last_20[i][0] > last_20[i-1][0]:
            accum_days += 1
    accum_pct = accum_days / 19 * 100 if len(last_20) > 1 else 0

    # SCORING
    score = 0
    flags = []

    # 1. Price gain over 60d (10pts max)
    if ret_60d is not None:
        score += min(max(ret_60d / 30, 0), 1) * 10  # +30% in 60d caps

    # 2. Price gain over 20d but not parabolic (15pts)
    if ret_20d is not None and not is_parabolic:
        if ret_20d > 5:
            score += min(ret_20d / 25, 1) * 15  # +25% in 20d caps
            flags.append("RISING_20D")

    # 3. New highs (20pts)
    if is_at_60d_high:
        score += 20
        flags.append("AT_60D_HIGH")
    elif is_at_20d_high:
        score += 12
        flags.append("AT_20D_HIGH")
    elif pct_from_60d_high > -5:
        score += 8
        flags.append("NEAR_60D_HIGH")

    # 4. Volume surge (15pts)
    if vol_ratio > 2.5:
        score += 15
        flags.append("VOL_SURGE_2.5X")
    elif vol_ratio > 1.8:
        score += 10
        flags.append("VOL_SURGE_1.8X")
    elif vol_ratio > 1.3:
        score += 5

    # 5. Volume accumulation pattern (15pts)
    if accum_pct > 60:
        score += 15
        flags.append("STRONG_ACCUM")
    elif accum_pct > 40:
        score += 8
        flags.append("MOD_ACCUM")

    # 6. Relative strength vs SPY (15pts)
    if rs_20d is not None and rs_20d > 5:
        score += min(rs_20d / 15, 1) * 15  # +15% RS caps
        flags.append(f"RS_20D_+{rs_20d:.0f}")
    if rs_60d is not None and rs_60d > 10:
        score += 5  # bonus for sustained outperformance
        flags.append(f"RS_60D_+{rs_60d:.0f}")

    # 7. Parabolic penalty
    if is_parabolic:
        score = score * 0.5
        flags.append("PARABOLIC_DISCOUNT")

    score = min(score, 100)

    # Tier
    if score >= 75:
        tier = "TIER_A_BREAKOUT"
    elif score >= 60:
        tier = "TIER_B_MOMENTUM"
    elif score >= 45:
        tier = "WATCH"
    else:
        tier = "MARGINAL"

    return {
        "symbol": symbol,
        "score": round(score, 1),
        "tier": tier,
        "flags": flags,
        "is_parabolic": is_parabolic,
        "metrics": {
            "today_close": round(today_close, 2),
            "ret_5d_pct": round(ret_5d, 1) if ret_5d is not None else None,
            "ret_10d_pct": round(ret_10d, 1) if ret_10d is not None else None,
            "ret_20d_pct": round(ret_20d, 1) if ret_20d is not None else None,
            "ret_60d_pct": round(ret_60d, 1) if ret_60d is not None else None,
            "pct_from_60d_high": round(pct_from_60d_high, 1),
            "pct_from_20d_high": round(pct_from_20d_high, 1),
            "vol_ratio_today": round(vol_ratio, 2),
            "rs_vs_spy_20d_pct": round(rs_20d, 1) if rs_20d is not None else None,
            "rs_vs_spy_60d_pct": round(rs_60d, 1) if rs_60d is not None else None,
            "accum_pct": round(accum_pct, 1),
            "avg_dollar_vol_20d": round(avg_dollar_vol, 0),
        },
    }


def lambda_handler(event=None, context=None):
    started = time.time()
    deadline_at = started + TIMEOUT_BUDGET_S
    print(f"[momentum] starting v1.0, max_tickers={MAX_TICKERS}, min_dollar_vol=${MIN_DOLLAR_VOL/1e6:.1f}M")

    universe = get_universe()
    if not universe:
        return {"statusCode": 200, "body": json.dumps({"n": 0, "reason": "no universe"})}

    print(f"[momentum] computing SPY benchmark returns...")
    spy = fetch_spy_history()
    spy_returns = {}
    if spy and len(spy) >= 61:
        spy_close = spy[-1]["close"]
        spy_returns["20d"] = (spy_close / spy[-21]["close"] - 1) * 100
        spy_returns["60d"] = (spy_close / spy[-61]["close"] - 1) * 100
        print(f"[momentum] SPY returns: 20d={spy_returns['20d']:.2f}%, 60d={spy_returns['60d']:.2f}%")

    results = []
    n_no_history = 0
    n_no_signal = 0

    def evaluate(sym):
        if time.time() > deadline_at:
            return None
        h = fetch_history(sym, days=90)
        if not h:
            return ("no_history", sym)
        sig = compute_signals(sym, h, spy_returns)
        if not sig:
            return ("no_signal", sym)
        return ("ok", sig)

    with ThreadPoolExecutor(max_workers=N_WORKERS) as pool:
        futures = {pool.submit(evaluate, s): s for s in universe}
        for f in as_completed(futures):
            try:
                tag = f.result()
            except Exception:
                continue
            if not tag:
                continue
            kind, payload = tag
            if kind == "ok":
                results.append(payload)
            elif kind == "no_history":
                n_no_history += 1
            elif kind == "no_signal":
                n_no_signal += 1

    print(f"[momentum] OK: {len(results)}, no_history: {n_no_history}, no_signal: {n_no_signal}")
    results.sort(key=lambda x: x["score"], reverse=True)

    tier_a = [r for r in results if r["tier"] == "TIER_A_BREAKOUT"]
    tier_b = [r for r in results if r["tier"] == "TIER_B_MOMENTUM"]
    parabolic = [r for r in results if r.get("is_parabolic")]

    out = {
        "schema_version": 1,
        "method": "momentum_breakout_v1",
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime()),
        "duration_s": round(time.time() - started, 1),
        "spy_returns": spy_returns,
        "stats": {
            "n_universe": len(universe),
            "n_evaluated": len(results),
            "n_tier_a": len(tier_a),
            "n_tier_b": len(tier_b),
            "n_parabolic": len(parabolic),
            "n_no_history": n_no_history,
            "n_no_signal": n_no_signal,
        },
        "summary": {
            "top_25_overall": [
                {
                    "symbol": r["symbol"],
                    "score": r["score"],
                    "tier": r["tier"],
                    "flags": r["flags"],
                    "ret_20d": r["metrics"]["ret_20d_pct"],
                    "ret_60d": r["metrics"]["ret_60d_pct"],
                    "pct_from_60d_high": r["metrics"]["pct_from_60d_high"],
                    "vol_ratio": r["metrics"]["vol_ratio_today"],
                    "rs_vs_spy_20d": r["metrics"]["rs_vs_spy_20d_pct"],
                }
                for r in results[:25]
            ],
            "tier_a": [r["symbol"] for r in tier_a],
            "parabolic": [r["symbol"] for r in parabolic[:15]],
        },
        "all_qualifying": results,
    }

    body = json.dumps(out, default=str).encode()
    S3.put_object(Bucket=BUCKET, Key=S3_KEY, Body=body, ContentType="application/json")
    print(f"[momentum] wrote {len(body):,}b to {S3_KEY}")
    print(f"[momentum] tier_a={len(tier_a)} tier_b={len(tier_b)} parabolic={len(parabolic)}")
    if results[:8]:
        print(f"[momentum] TOP: {[(r['symbol'], r['score'], r['tier']) for r in results[:8]]}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "n_evaluated": len(results),
            "n_tier_a": len(tier_a),
            "n_tier_b": len(tier_b),
            "duration_s": out["duration_s"],
        }),
    }
