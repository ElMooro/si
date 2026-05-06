"""
justhodl-pre-pump-detector v2 — DATA-CALIBRATED edition.

v1 was too theoretical (hunting for "perfectly stagnant" coiled springs).
v2 is calibrated against the actual pump-list winners (ICHR, INTC, LITE, CRDO):

The real "imminent breakout" pattern measured from 9 historical winners:
  • Volatility compression: median 1.43, p25 1.35    → threshold > 1.22
  • OBV accumulation: median +0.18, max +0.41         → threshold > 0.127
  • Liquidity expansion 30v120: median 1.06, p75 1.40 → threshold > 0.90
  • Range position: variable (25%-100%) — NOT a hard filter
  • ret_60d: -13% to +140% — NOT a hard filter, allow up to +70%
  • Most importantly: confirmed UPTREND already in place (ret_30d +26% median)

The new score combines:
  - Strong OBV accumulation (the consistent winner signal)
  - Volatility compression (rest before second leg)
  - Liquidity expansion (more eyes on the name)
  - Established uptrend not yet parabolic (ret_60d 5-70%)
  - Recent consolidation (ret_5d in -10% to +20% — pause before next leg)

OUTPUT: data/pre-pump-signals.json
"""
import io, json, os, time, urllib.request, urllib.error, statistics
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = os.environ.get("S3_KEY", "data/pre-pump-signals.json")
FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
N_WORKERS = int(os.environ.get("N_WORKERS", "12"))
MAX_TICKERS = int(os.environ.get("MAX_TICKERS", "600"))
MIN_DOLLAR_VOL = float(os.environ.get("MIN_DOLLAR_VOL", "1000000"))
TIMEOUT_BUDGET_S = int(os.environ.get("TIMEOUT_BUDGET_S", "260"))

S3 = boto3.client("s3", region_name=REGION)


def _http_get_json(url, timeout=15):
    req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-PrePump/2.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def get_universe():
    try:
        obj = S3.get_object(Bucket=BUCKET, Key="data/universe.json")
        d = json.loads(obj["Body"].read())
        return [(s.get("symbol") or "").upper() for s in d.get("stocks", []) if s.get("symbol")][:MAX_TICKERS]
    except Exception as e:
        print("[prepump-v2] universe load failed: " + str(e))
        return []


def fetch_history(symbol, days=210):
    url = "https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=" + symbol + "&apikey=" + FMP_KEY
    try:
        d = _http_get_json(url, timeout=15)
        if not isinstance(d, list):
            return None
        out = []
        for r in d[:days]:
            if r.get("close") and r.get("date"):
                out.append({
                    "date": r.get("date"),
                    "close": float(r.get("close")),
                    "volume": float(r.get("volume") or 0),
                })
        out.sort(key=lambda x: x["date"])
        return out
    except Exception:
        return None


def compute_signals(symbol, history):
    if not history or len(history) < 120:
        return None

    closes = [h["close"] for h in history]
    volumes = [h["volume"] for h in history]
    n = len(closes)
    today = closes[-1]

    avg_dollar_vol = sum(c * v for c, v in zip(closes[-30:], volumes[-30:])) / min(30, n)
    if avg_dollar_vol < MIN_DOLLAR_VOL:
        return None

    # Range position
    lookback = min(180, n)
    range_high = max(closes[-lookback:])
    range_low = min(closes[-lookback:])
    range_position = (today - range_low) / (range_high - range_low) * 100 if range_high > range_low else 50

    # Volatility compression
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
    vol_compression = stdev_180 / stdev_60 if stdev_60 > 0 else 0

    # OBV slope
    obv = [0]
    for i in range(1, n):
        if closes[i] > closes[i-1]:
            obv.append(obv[-1] + volumes[i])
        elif closes[i] < closes[i-1]:
            obv.append(obv[-1] - volumes[i])
        else:
            obv.append(obv[-1])
    if len(obv) >= 60:
        obv_recent = obv[-60:]
        x_mean = (len(obv_recent) - 1) / 2
        y_mean = sum(obv_recent) / len(obv_recent)
        num = sum((i - x_mean) * (v - y_mean) for i, v in enumerate(obv_recent))
        den = sum((i - x_mean) ** 2 for i in range(len(obv_recent)))
        obv_slope = num / den if den > 0 else 0
        avg_vol = sum(volumes[-60:]) / 60
        obv_slope_norm = obv_slope / avg_vol if avg_vol > 0 else 0
    else:
        obv_slope_norm = 0

    # Liquidity expansion
    avg_vol_20 = sum(volumes[-20:]) / min(20, n)
    avg_vol_60 = sum(volumes[-60:]) / min(60, n) if n >= 60 else avg_vol_20
    avg_vol_120 = sum(volumes[-120:]) / min(120, n) if n >= 120 else avg_vol_60
    liq_30v60 = avg_vol_20 / avg_vol_60 if avg_vol_60 > 0 else 1.0
    liq_30v120 = avg_vol_20 / avg_vol_120 if avg_vol_120 > 0 else 1.0

    # Returns
    ret_5d = (today / closes[-6] - 1) * 100 if n >= 6 else 0
    ret_30d = (today / closes[-31] - 1) * 100 if n >= 31 else 0
    ret_60d = (today / closes[-61] - 1) * 100 if n >= 61 else 0
    ret_120d = (today / closes[-121] - 1) * 100 if n >= 121 else 0

    # Disaster check
    has_disaster = False
    for i in range(max(0, n-60), n-1):
        if closes[i] > 0 and (closes[i+1] / closes[i] - 1) * 100 < -15:
            has_disaster = True
            break

    # ─────────── v2 SCORING ───────────
    score = 0.0
    flags = []

    # 1. OBV accumulation — most important winner signal (max 30 pts)
    if obv_slope_norm >= 0.40:
        score += 30
        flags.append("OBV_STRONG_ACCUM")
    elif obv_slope_norm >= 0.20:
        score += 22
        flags.append("OBV_ACCUM")
    elif obv_slope_norm >= 0.13:
        score += 14
        flags.append("OBV_RISING")
    elif obv_slope_norm >= 0.05:
        score += 6

    # 2. Volatility compression — rest before second leg (max 20 pts)
    if vol_compression >= 1.55:
        score += 20
        flags.append("VOL_COMP_STRONG")
    elif vol_compression >= 1.35:
        score += 14
        flags.append("VOL_COMP_MOD")
    elif vol_compression >= 1.22:
        score += 8

    # 3. Liquidity expansion — more eyes (max 15 pts)
    if liq_30v120 >= 1.40:
        score += 15
        flags.append("LIQ_EXPANDING_FAST")
    elif liq_30v120 >= 1.15:
        score += 10
        flags.append("LIQ_EXPANDING")
    elif liq_30v120 >= 0.95:
        score += 5

    # 4. Established trend without being parabolic (max 20 pts)
    # Sweet spot: ret_60d between +5 and +60 (uptrend confirmed, not blow-off)
    if 5 <= ret_60d <= 60:
        score += 20
        flags.append("UPTREND_NOT_PARABOLIC")
    elif 60 < ret_60d <= 100:
        score += 10
        flags.append("UPTREND_LATE")
    elif 0 <= ret_60d < 5:
        score += 12
        flags.append("FLAT_TURNING_UP")
    elif -15 <= ret_60d < 0:
        score += 8
        flags.append("BASE_FORMING")

    # 5. Recent consolidation/pause — ret_5d should be in calm range (max 10 pts)
    if -3 <= ret_5d <= 8:
        score += 10
        flags.append("CALM_RECENT")
    elif 8 < ret_5d <= 15:
        score += 6
        flags.append("EARLY_THRUST")

    # 6. Range position — not too extreme (max 5 pts)
    if 20 <= range_position <= 85:
        score += 5

    # Disaster penalty
    if has_disaster:
        score = score * 0.4
        flags.append("DISASTER_DISCOUNT")

    # Parabolic disqualifier — already too far
    if ret_60d > 100 or (ret_30d > 60 and ret_5d > 15):
        score = score * 0.5
        flags.append("LATE_DISCOUNT")

    score = min(score, 100)

    if score >= 70:
        tier = "TIER_A_BREAKING"
    elif score >= 55:
        tier = "TIER_B_BUILDING"
    elif score >= 40:
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
            "vol_compression": round(vol_compression, 2),
            "obv_slope_norm": round(obv_slope_norm, 3),
            "liq_30v60": round(liq_30v60, 2),
            "liq_30v120": round(liq_30v120, 2),
            "ret_5d_pct": round(ret_5d, 1),
            "ret_30d_pct": round(ret_30d, 1),
            "ret_60d_pct": round(ret_60d, 1),
            "ret_120d_pct": round(ret_120d, 1),
            "has_disaster": has_disaster,
            "avg_dollar_vol_30d": round(avg_dollar_vol, 0),
        },
    }


def lambda_handler(event=None, context=None):
    started = time.time()
    deadline_at = started + TIMEOUT_BUDGET_S
    print("[prepump-v2] starting v2.0 (calibrated thresholds)")

    universe = get_universe()
    if not universe:
        return {"statusCode": 200, "body": json.dumps({"n": 0, "reason": "no universe"})}
    print("[prepump-v2] universe: " + str(len(universe)) + " tickers")

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

    print("[prepump-v2] OK: " + str(len(results)) + ", no_history: " + str(n_no_history))
    results.sort(key=lambda x: x["score"], reverse=True)

    tier_a = [r for r in results if r["tier"] == "TIER_A_BREAKING"]
    tier_b = [r for r in results if r["tier"] == "TIER_B_BUILDING"]

    out = {
        "schema_version": 2,
        "method": "pre_pump_detector_v2_calibrated",
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
                    "obv_slope": r["metrics"]["obv_slope_norm"],
                    "vol_comp": r["metrics"]["vol_compression"],
                    "liq_expand": r["metrics"]["liq_30v120"],
                    "ret_60d": r["metrics"]["ret_60d_pct"],
                    "ret_30d": r["metrics"]["ret_30d_pct"],
                    "ret_5d": r["metrics"]["ret_5d_pct"],
                }
                for r in results[:25]
            ],
            "tier_a": [r["symbol"] for r in tier_a],
        },
        "all_qualifying": results,
    }

    body = json.dumps(out, default=str).encode()
    S3.put_object(Bucket=BUCKET, Key=S3_KEY, Body=body, ContentType="application/json")
    print("[prepump-v2] wrote " + str(len(body)) + "b")
    print("[prepump-v2] tier_a=" + str(len(tier_a)) + " tier_b=" + str(len(tier_b)))
    if results[:8]:
        print("[prepump-v2] TOP: " + str([(r["symbol"], r["score"], r["tier"]) for r in results[:8]]))

    return {
        "statusCode": 200,
        "body": json.dumps({
            "n_evaluated": len(results),
            "n_tier_a": len(tier_a),
            "n_tier_b": len(tier_b),
            "duration_s": out["duration_s"],
        }),
    }
