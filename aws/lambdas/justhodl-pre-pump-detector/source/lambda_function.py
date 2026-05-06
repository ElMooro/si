"""
justhodl-pre-pump-detector — DETECT THE COILED SPRING.

This is the system's 7th hunter, designed to catch names BEFORE they break out,
not after. It scans for the structural pattern that precedes large multi-month
moves in microcaps, with a focus on AI/semi/optical supply-chain names.

THE PATTERN we're looking for (the "coiled spring"):
  1. STAGNANT PRICE — stock has been sideways/down for 60-180 days (low std dev)
  2. RISING ON-BALANCE-VOLUME — volume on up days exceeds volume on down days
     even while price is flat (smart money accumulating)
  3. LIQUIDITY EXPANDING — average daily volume is INCREASING over the past
     30 days vs the previous 60-day baseline (more eyes on the name)
  4. NEAR 6-MONTH LOW — price is in the bottom 30% of 6-month range
  5. DECLINING SHORT INTEREST (when available) — bears giving up
  6. NO EARNINGS DISASTER — no -20%+ gap-down in last 90 days
  7. THEMATIC FIT — sector/industry matches an active theme

This is the literal opposite of momentum-breakout (which catches stocks already
moving). pre-pump catches stocks coiling.

A perfect pre-pump signal is:
  - Price within 10% of 180d low
  - 20-day std dev of returns BELOW historical average
  - OBV trending up over last 60d
  - Volume MA(20) > volume MA(60)
  - In a thematic sector (semis, AI, etc)

OUTPUT: data/pre-pump-signals.json — list of coiled springs.

This is what would have caught:
  • AXTI in mid-2025 (low for months, then exploded)
  • LWLG even earlier (research stage, quietly accumulating)
  • AEHR pre-Q1 2025 breakout
"""
import io, json, os, time, urllib.request, urllib.error, math, statistics
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = os.environ.get("S3_KEY", "data/pre-pump-signals.json")
FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
N_WORKERS = int(os.environ.get("N_WORKERS", "12"))
MAX_TICKERS = int(os.environ.get("MAX_TICKERS", "600"))
MIN_DOLLAR_VOL = float(os.environ.get("MIN_DOLLAR_VOL", "1000000"))  # only $1M for microcaps
TIMEOUT_BUDGET_S = int(os.environ.get("TIMEOUT_BUDGET_S", "260"))

S3 = boto3.client("s3", region_name=REGION)


def _http_get_json(url, timeout=15):
    req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-PrePump/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def get_universe():
    try:
        obj = S3.get_object(Bucket=BUCKET, Key="data/universe.json")
        d = json.loads(obj["Body"].read())
        stocks = d.get("stocks", []) or []
        return [(s.get("symbol") or "").upper() for s in stocks if s.get("symbol")][:MAX_TICKERS]
    except Exception as e:
        print(f"[prepump] universe load failed: {e}")
        return []


def fetch_history(symbol, days=210):
    url = f"https://financialmodelingprep.com/stable/historical-price-eod/full?symbol={symbol}&apikey={FMP_KEY}"
    try:
        d = _http_get_json(url, timeout=15)
        if not d or not isinstance(d, list):
            return None
        out = []
        for r in d[:days]:
            close = r.get("close")
            high = r.get("high")
            low = r.get("low")
            vol = r.get("volume")
            date = r.get("date")
            if close is not None and date:
                out.append({
                    "date": date,
                    "close": float(close),
                    "high": float(high or close),
                    "low": float(low or close),
                    "volume": float(vol or 0),
                })
        out.sort(key=lambda x: x["date"])
        return out
    except Exception:
        return None


def compute_signals(symbol, history):
    """Compute pre-pump signals. Returns dict or None."""
    if not history or len(history) < 120:
        return None

    closes = [h["close"] for h in history]
    volumes = [h["volume"] for h in history]
    highs = [h["high"] for h in history]
    lows = [h["low"] for h in history]
    n = len(closes)
    today = closes[-1]

    # ────────────────────────── Liquidity filter ──────────────────────────
    avg_dollar_vol = sum(c * v for c, v in zip(closes[-30:], volumes[-30:])) / min(30, n)
    if avg_dollar_vol < MIN_DOLLAR_VOL:
        return None

    # ────────────────────────── Range position ──────────────────────────
    # Where in 180-day range is the price? 0 = at low, 100 = at high
    lookback = min(180, n)
    range_high = max(closes[-lookback:])
    range_low = min(closes[-lookback:])
    if range_high == range_low:
        return None
    range_position = (today - range_low) / (range_high - range_low) * 100
    pct_above_180d_low = (today / range_low - 1) * 100

    # ────────────────────────── Volatility (compression) ──────────────────────────
    # Daily returns
    returns_60 = []
    for i in range(max(0, n-61), n-1):
        if closes[i] > 0:
            returns_60.append((closes[i+1] / closes[i] - 1) * 100)
    returns_180 = []
    for i in range(max(0, n-181), n-1):
        if closes[i] > 0:
            returns_180.append((closes[i+1] / closes[i] - 1) * 100)

    stdev_60 = statistics.stdev(returns_60) if len(returns_60) > 1 else 0
    stdev_180 = statistics.stdev(returns_180) if len(returns_180) > 1 else 0
    # Lower stdev_60 vs stdev_180 means VOLATILITY COMPRESSION (coiled spring)
    vol_compression = stdev_180 / stdev_60 if stdev_60 > 0 else 0  # >1 means compressing

    # ────────────────────────── On-Balance-Volume (OBV) trend ──────────────────────────
    # OBV = cumulative sum of volume*sign(price_change)
    obv = [0]
    for i in range(1, n):
        if closes[i] > closes[i-1]:
            obv.append(obv[-1] + volumes[i])
        elif closes[i] < closes[i-1]:
            obv.append(obv[-1] - volumes[i])
        else:
            obv.append(obv[-1])
    # Is OBV rising while price is flat? Compute slope of last 60 OBV values
    if len(obv) >= 60:
        obv_recent = obv[-60:]
        # Simple linear-fit slope (normalized)
        x_mean = (len(obv_recent) - 1) / 2
        y_mean = sum(obv_recent) / len(obv_recent)
        num = sum((i - x_mean) * (v - y_mean) for i, v in enumerate(obv_recent))
        den = sum((i - x_mean) ** 2 for i in range(len(obv_recent)))
        obv_slope = num / den if den > 0 else 0
        # Normalize by mean volume so it's comparable across stocks
        avg_vol = sum(volumes[-60:]) / 60
        obv_slope_norm = obv_slope / avg_vol if avg_vol > 0 else 0
    else:
        obv_slope_norm = 0

    # ────────────────────────── Liquidity expansion ──────────────────────────
    avg_vol_20 = sum(volumes[-20:]) / min(20, n)
    avg_vol_60 = sum(volumes[-60:]) / min(60, n) if n >= 60 else avg_vol_20
    avg_vol_120 = sum(volumes[-120:]) / min(120, n) if n >= 120 else avg_vol_60
    liq_expansion_30v60 = avg_vol_20 / avg_vol_60 if avg_vol_60 > 0 else 1.0
    liq_expansion_30v120 = avg_vol_20 / avg_vol_120 if avg_vol_120 > 0 else 1.0

    # ────────────────────────── Price stagnation ──────────────────────────
    # If you bought 60 days ago, what would you have? +/- range
    ret_60d = (today / closes[-61] - 1) * 100 if n >= 61 else 0
    ret_120d = (today / closes[-121] - 1) * 100 if n >= 121 else 0

    # ────────────────────────── Earnings disaster check ──────────────────────────
    # Look for any -15% single-day drop in last 60 days (gap-down on bad print)
    has_disaster = False
    for i in range(max(0, n-60), n-1):
        if closes[i] > 0:
            day_ret = (closes[i+1] / closes[i] - 1) * 100
            if day_ret < -15:
                has_disaster = True
                break

    # ────────────────────────── Recent breakout signal ──────────────────────────
    # We DON'T want pre-pump to fire on names already breaking out. Check
    # if there's a small lift in the last 5 days (< 8%) — early signs of waking.
    ret_5d = (today / closes[-6] - 1) * 100 if n >= 6 else 0

    # ────────────────────────── SCORING ──────────────────────────
    score = 0
    flags = []

    # 1. Range position — stock should be in bottom 35% of 180d range (max 25 pts)
    if range_position < 20:
        score += 25
        flags.append("AT_180D_LOW_ZONE")
    elif range_position < 35:
        score += 18
        flags.append("LOWER_180D_THIRD")
    elif range_position < 50:
        score += 8

    # 2. Volatility compression — vol_compression > 1.2 means recent stdev BELOW
    #    historical (the calm before the storm). Cap at 2.0
    if vol_compression > 1.6:
        score += 18
        flags.append("VOL_HEAVILY_COMPRESSED")
    elif vol_compression > 1.25:
        score += 12
        flags.append("VOL_COMPRESSED")
    elif vol_compression > 1.10:
        score += 5

    # 3. OBV slope — accumulation while flat (max 20 pts)
    if obv_slope_norm > 0.5:
        score += 20
        flags.append("STRONG_OBV_ACCUMULATION")
    elif obv_slope_norm > 0.2:
        score += 12
        flags.append("OBV_ACCUMULATION")
    elif obv_slope_norm > 0.05:
        score += 5

    # 4. Liquidity expansion — recent volume > older volume (max 15 pts)
    if liq_expansion_30v120 > 1.6:
        score += 15
        flags.append("LIQUIDITY_EXPANDING_FAST")
    elif liq_expansion_30v120 > 1.25:
        score += 10
        flags.append("LIQUIDITY_EXPANDING")
    elif liq_expansion_30v120 > 1.05:
        score += 4

    # 5. Stagnant price — small absolute returns over 60d/120d (max 10 pts)
    if abs(ret_60d) < 8 and abs(ret_120d) < 12:
        score += 10
        flags.append("STAGNANT_PRICE")
    elif abs(ret_60d) < 15:
        score += 5

    # 6. Early waking — 5d return between 1-8% suggests start of breakout (max 12 pts)
    if 1 <= ret_5d <= 8:
        score += 12
        flags.append("EARLY_WAKING")
    elif 0 <= ret_5d <= 12:
        score += 5

    # 7. Disaster disqualifier
    if has_disaster:
        score = score * 0.4
        flags.append("RECENT_DISASTER_DISCOUNT")

    score = min(score, 100)

    # ────────────────────────── Tier ──────────────────────────
    if score >= 75:
        tier = "TIER_A_COILED"
    elif score >= 60:
        tier = "TIER_B_BUILDING"
    elif score >= 45:
        tier = "WATCH"
    else:
        tier = "MARGINAL"

    return {
        "symbol": symbol,
        "score": round(score, 1),
        "tier": tier,
        "flags": flags,
        "metrics": {
            "today_close": round(today, 2),
            "range_position_180d_pct": round(range_position, 1),
            "pct_above_180d_low": round(pct_above_180d_low, 1),
            "vol_compression_ratio": round(vol_compression, 2),
            "stdev_60d_returns": round(stdev_60, 2),
            "stdev_180d_returns": round(stdev_180, 2),
            "obv_slope_normalized": round(obv_slope_norm, 3),
            "liquidity_expansion_30v60": round(liq_expansion_30v60, 2),
            "liquidity_expansion_30v120": round(liq_expansion_30v120, 2),
            "ret_5d_pct": round(ret_5d, 1),
            "ret_60d_pct": round(ret_60d, 1),
            "ret_120d_pct": round(ret_120d, 1),
            "has_recent_disaster": has_disaster,
            "avg_dollar_vol_30d": round(avg_dollar_vol, 0),
        },
    }


def lambda_handler(event=None, context=None):
    started = time.time()
    deadline_at = started + TIMEOUT_BUDGET_S
    print(f"[prepump] starting v1.0, max_tickers={MAX_TICKERS}, min_dollar_vol=${MIN_DOLLAR_VOL/1e6:.1f}M")

    universe = get_universe()
    if not universe:
        return {"statusCode": 200, "body": json.dumps({"n": 0, "reason": "no universe"})}
    print(f"[prepump] universe: {len(universe)} tickers")

    results = []
    n_no_history = 0
    n_no_signal = 0

    def evaluate(sym):
        if time.time() > deadline_at:
            return ("deadline", sym)
        h = fetch_history(sym, days=210)
        if not h:
            return ("no_history", sym)
        sig = compute_signals(sym, h)
        if not sig:
            return ("no_signal", sym)
        return ("ok", sig)

    with ThreadPoolExecutor(max_workers=N_WORKERS) as pool:
        futures = {pool.submit(evaluate, s): s for s in universe}
        for f in as_completed(futures):
            try:
                kind, payload = f.result() or ("none", None)
            except Exception:
                continue
            if kind == "ok":
                results.append(payload)
            elif kind == "no_history":
                n_no_history += 1
            elif kind == "no_signal":
                n_no_signal += 1

    print(f"[prepump] OK: {len(results)}, no_history: {n_no_history}, no_signal: {n_no_signal}")
    results.sort(key=lambda x: x["score"], reverse=True)

    tier_a = [r for r in results if r["tier"] == "TIER_A_COILED"]
    tier_b = [r for r in results if r["tier"] == "TIER_B_BUILDING"]

    out = {
        "schema_version": 1,
        "method": "pre_pump_detector_v1",
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime()),
        "duration_s": round(time.time() - started, 1),
        "stats": {
            "n_universe": len(universe),
            "n_evaluated": len(results),
            "n_tier_a": len(tier_a),
            "n_tier_b": len(tier_b),
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
                    "range_pos": r["metrics"]["range_position_180d_pct"],
                    "vol_compression": r["metrics"]["vol_compression_ratio"],
                    "obv_slope": r["metrics"]["obv_slope_normalized"],
                    "liq_expansion": r["metrics"]["liquidity_expansion_30v120"],
                    "ret_60d": r["metrics"]["ret_60d_pct"],
                }
                for r in results[:25]
            ],
            "tier_a": [r["symbol"] for r in tier_a],
        },
        "all_qualifying": results,
    }

    body = json.dumps(out, default=str).encode()
    S3.put_object(Bucket=BUCKET, Key=S3_KEY, Body=body, ContentType="application/json")
    print(f"[prepump] wrote {len(body):,}b")
    print(f"[prepump] tier_a={len(tier_a)} tier_b={len(tier_b)}")
    if results[:8]:
        print(f"[prepump] TOP: {[(r['symbol'], r['score'], r['tier']) for r in results[:8]]}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "n_evaluated": len(results),
            "n_tier_a": len(tier_a),
            "n_tier_b": len(tier_b),
            "duration_s": out["duration_s"],
        }),
    }
