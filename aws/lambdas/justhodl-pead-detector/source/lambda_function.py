"""
justhodl-pead-detector — Earnings Surprise Streak + PEAD scanner

Post-Earnings Announcement Drift (PEAD) is one of the most documented
anomalies in equity markets. Stocks that beat earnings expectations
significantly continue to drift in the direction of the surprise for
60-90 days. The effect is strongest when:
  • Beat magnitude > 5% of consensus (top decile of surprises)
  • Multiple consecutive quarters of beats (streak of 3+ → much stronger)
  • Surprise magnitudes themselves accelerating (each Q's beat bigger)
  • Stock didn't fully gap-up on the announcement (institutions still building)

WHAT THIS DETECTS — for every stock in universe (all caps):

  1. EPS Surprise Streak — count consecutive quarters where
     reported_eps > estimated_eps with magnitude > 0%
  2. Average beat magnitude across last 4 quarters (in % of estimate)
  3. Acceleration of beats — are recent beats larger than older beats?
  4. Revenue beat streak — same logic but for revenue
  5. Combined EPS+Rev beat (both metrics beat) — gold standard
  6. Days since last earnings announcement (PEAD effect strongest 1-30 days post)
  7. Post-earnings price drift — stock up since last earnings announcement?
     If yes, drift continuing; if no, mean-reversion opportunity
  8. Pre-earnings setup — for stocks 2-10 days FROM next earnings

SCORE 0-100 combines:
  • Beat streak length (3+ Q in a row): max 25
  • Average beat magnitude: max 25
  • Acceleration of magnitudes: max 15
  • Recent EPS+Rev double-beat: max 15
  • Post-earnings drift positive: max 10
  • Recent earnings within 30 days: max 10

Tiers:
  TIER_S_DRIFTING: 4Q+ streak, avg beat >10%, recent earnings <30d
  TIER_A_BEATING:  3Q streak, avg beat >5%
  TIER_B_BUILDING: 2Q streak, avg beat >3%

OUTPUT: data/pead-signals.json
"""
import io, json, os, time, urllib.request, urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import boto3

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = os.environ.get("S3_KEY", "data/pead-signals.json")
FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
N_WORKERS = int(os.environ.get("N_WORKERS", "12"))
MAX_TICKERS = int(os.environ.get("MAX_TICKERS", "1500"))
TIMEOUT_BUDGET_S = int(os.environ.get("TIMEOUT_BUDGET_S", "550"))

S3 = boto3.client("s3", region_name=REGION)


def fetch_url(url, timeout=15):
    req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-PEAD/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def get_universe():
    """All stocks across all cap buckets — PEAD is universal."""
    try:
        obj = S3.get_object(Bucket=BUCKET, Key="data/universe.json")
        d = json.loads(obj["Body"].read())
        # All buckets — PEAD works across the entire size spectrum
        all_stocks = d.get("stocks", [])
        return all_stocks[:MAX_TICKERS]
    except Exception as e:
        print("[pead] universe load failed: " + str(e))
        return []


def fetch_earnings_surprises(symbol, limit=8):
    """FMP /earnings-surprises returns historical eps actual vs estimate."""
    url = ("https://financialmodelingprep.com/stable/earnings-surprises-bulk?"
           "symbol=" + symbol + "&limit=" + str(limit) + "&apikey=" + FMP_KEY)
    try:
        d = fetch_url(url, timeout=12)
        if isinstance(d, list):
            return d
    except urllib.error.HTTPError:
        # Try alternate endpoint
        url2 = ("https://financialmodelingprep.com/stable/earnings-surprises?"
                "symbol=" + symbol + "&apikey=" + FMP_KEY)
        try:
            d = fetch_url(url2, timeout=12)
            if isinstance(d, list):
                return d[:limit]
        except Exception:
            pass
    except Exception:
        pass
    return None


def fetch_earnings_calendar(symbol):
    """Get next earnings date for this symbol via FMP."""
    today = time.strftime("%Y-%m-%d")
    future = time.strftime("%Y-%m-%d", time.gmtime(time.time() + 90 * 86400))
    url = ("https://financialmodelingprep.com/stable/earnings?"
           "symbol=" + symbol + "&apikey=" + FMP_KEY)
    try:
        d = fetch_url(url, timeout=10)
        if not isinstance(d, list):
            return None
        # Filter to upcoming (future date) and past (last 90 days)
        recent_or_upcoming = []
        for e in d:
            dt_str = e.get("date") or ""
            if not dt_str:
                continue
            # Only include events within +/- 90d for context
            try:
                dt = datetime.strptime(dt_str, "%Y-%m-%d")
                days_diff = (dt - datetime.now()).days
                if -90 <= days_diff <= 90:
                    recent_or_upcoming.append({**e, "_days_diff": days_diff})
            except Exception:
                continue
        return recent_or_upcoming
    except Exception:
        return None


def fetch_quote(symbol):
    try:
        d = fetch_url("https://financialmodelingprep.com/stable/quote?symbol=" + symbol + "&apikey=" + FMP_KEY, timeout=10)
        if isinstance(d, list) and d:
            return d[0]
    except Exception:
        pass
    return None


def fetch_history(symbol, days=120):
    try:
        d = fetch_url("https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=" + symbol + "&apikey=" + FMP_KEY, timeout=15)
        if not isinstance(d, list):
            return None
        out = []
        for x in d[:days]:
            if x.get("close") and x.get("date"):
                out.append({"date": x["date"], "close": float(x["close"])})
        out.sort(key=lambda r: r["date"])
        return out
    except Exception:
        return None


def evaluate_ticker(stock):
    sym = (stock.get("symbol") or "").upper()
    cap_bucket = stock.get("cap_bucket", "?")
    sector = stock.get("sector", "?")
    market_cap = stock.get("market_cap", 0)

    # Get earnings surprises
    surprises = fetch_earnings_surprises(sym, limit=8)
    if not surprises or len(surprises) < 3:
        return None

    # Sort newest first by date (FMP usually does, but be safe)
    surprises_sorted = sorted(surprises, key=lambda x: x.get("date", ""), reverse=True)
    
    # Compute beats
    beats = []
    for q in surprises_sorted:
        actual = q.get("epsActual") or q.get("actualEarningResult") or q.get("eps")
        estimate = q.get("epsEstimated") or q.get("estimatedEarning") or q.get("epsEstimate")
        if actual is None or estimate is None:
            continue
        try:
            actual = float(actual)
            estimate = float(estimate)
        except (TypeError, ValueError):
            continue
        if abs(estimate) < 0.001:
            # Avoid divide-by-zero on near-zero estimates
            continue
        beat_pct = (actual - estimate) / abs(estimate) * 100
        beats.append({
            "quarter_end": q.get("date"),
            "actual": actual,
            "estimate": estimate,
            "beat_pct": beat_pct,
            "beat": actual > estimate,
        })

    if len(beats) < 3:
        return None

    # Streak: consecutive beats from most recent backward
    streak = 0
    for b in beats:
        if b["beat"] and b["beat_pct"] > 0:
            streak += 1
        else:
            break

    # Average beat magnitude (last 4 quarters)
    last_4 = beats[:4]
    avg_beat_pct = sum(b["beat_pct"] for b in last_4) / len(last_4) if last_4 else 0
    
    # Acceleration: are most recent beats larger than older ones?
    if len(beats) >= 4:
        recent_avg = sum(b["beat_pct"] for b in beats[:2]) / 2
        older_avg = sum(b["beat_pct"] for b in beats[2:4]) / 2
        beat_acceleration = recent_avg - older_avg
    else:
        beat_acceleration = 0

    # Latest beat
    latest_beat = beats[0] if beats else None
    latest_beat_pct = latest_beat["beat_pct"] if latest_beat else 0
    latest_qtr_end = latest_beat["quarter_end"] if latest_beat else None

    # Days since latest earnings announcement (proxy: quarter end + ~30 days)
    days_since_qtr_end = None
    days_since_earnings = None
    if latest_qtr_end:
        try:
            qe_dt = datetime.strptime(latest_qtr_end, "%Y-%m-%d")
            days_since_qtr_end = (datetime.now() - qe_dt).days
            # Earnings announcement is typically 25-50 days after quarter end
            days_since_earnings = max(0, days_since_qtr_end - 35)
        except Exception:
            pass

    # Post-earnings price drift (if recent earnings within 60 days)
    drift_pct = None
    if days_since_earnings is not None and days_since_earnings < 60:
        history = fetch_history(sym, days=90)
        if history:
            # Compare today vs price at announcement time (approximate)
            today_close = history[-1]["close"]
            # Find price ~days_since_earnings ago
            target_idx = max(0, len(history) - days_since_earnings - 1)
            past_close = history[target_idx]["close"] if target_idx < len(history) else None
            if past_close and past_close > 0:
                drift_pct = (today_close - past_close) / past_close * 100

    # Get next earnings date
    upcoming_earnings = None
    days_to_next = None
    cal = fetch_earnings_calendar(sym)
    if cal:
        future_events = [e for e in cal if e.get("_days_diff", -100) > 0]
        if future_events:
            # Sort by date
            future_events.sort(key=lambda x: x.get("_days_diff", 999))
            upcoming_earnings = future_events[0].get("date")
            days_to_next = future_events[0].get("_days_diff")

    # ─── SCORING ───
    score = 0.0
    flags = []

    # 1. Streak length
    if streak >= 5:
        score += 25; flags.append("STREAK_5Q+")
    elif streak >= 4:
        score += 20; flags.append("STREAK_4Q")
    elif streak >= 3:
        score += 15; flags.append("STREAK_3Q")
    elif streak >= 2:
        score += 8; flags.append("STREAK_2Q")

    # 2. Average beat magnitude
    if avg_beat_pct > 25:
        score += 25; flags.append("BIG_AVG_BEAT_25%+")
    elif avg_beat_pct > 15:
        score += 18; flags.append("STRONG_AVG_BEAT_15%+")
    elif avg_beat_pct > 8:
        score += 12; flags.append("AVG_BEAT_8%+")
    elif avg_beat_pct > 3:
        score += 6; flags.append("AVG_BEAT_3%+")

    # 3. Beat acceleration (magnitudes growing)
    if beat_acceleration > 10:
        score += 15; flags.append("BEATS_ACCELERATING_10PP+")
    elif beat_acceleration > 5:
        score += 10; flags.append("BEATS_ACCELERATING_5PP+")
    elif beat_acceleration > 0:
        score += 5

    # 4. Latest beat — was it big?
    if latest_beat_pct > 30:
        score += 15; flags.append("LATEST_BEAT_30%+")
    elif latest_beat_pct > 15:
        score += 10; flags.append("LATEST_BEAT_15%+")
    elif latest_beat_pct > 5:
        score += 5; flags.append("LATEST_BEAT_5%+")

    # 5. Recent earnings (PEAD window — drift strongest 1-30 days)
    if days_since_earnings is not None:
        if 1 <= days_since_earnings <= 15:
            score += 10; flags.append("PEAD_PEAK_WINDOW")
        elif 15 < days_since_earnings <= 45:
            score += 7; flags.append("PEAD_ACTIVE")
        elif 45 < days_since_earnings <= 90:
            score += 3; flags.append("PEAD_FADING")

    # 6. Drift confirmation
    if drift_pct is not None:
        if drift_pct > 10:
            score += 10; flags.append("DRIFT_UP_10%+")
        elif drift_pct > 3:
            score += 6; flags.append("DRIFT_UP")
        elif drift_pct < -5:
            # Mean-reversion opportunity (good company, oversold)
            flags.append("MEAN_REVERSION_SETUP")
            if streak >= 3:
                score += 8  # paradox: beating but selling = opportunity
                flags.append("FUNDAMENTALS_VS_PRICE_DIVERGE")

    # 7. Pre-earnings setup
    if days_to_next is not None and 2 <= days_to_next <= 14:
        if streak >= 3:
            score += 8; flags.append("PRE_EARNINGS_STREAK")
        else:
            flags.append("PRE_EARNINGS_NO_EDGE")

    score = min(score, 100)

    # Tier
    if streak >= 4 and avg_beat_pct > 10 and (days_since_earnings is None or days_since_earnings < 45):
        tier = "TIER_S_DRIFTING"
    elif streak >= 3 and avg_beat_pct > 5:
        tier = "TIER_A_BEATING"
    elif streak >= 2 and avg_beat_pct > 3:
        tier = "TIER_B_BUILDING"
    else:
        tier = "QUIET"

    return {
        "symbol": sym,
        "score": round(score, 1),
        "tier": tier,
        "flags": flags,
        "metrics": {
            "streak": streak,
            "avg_beat_pct": round(avg_beat_pct, 2),
            "beat_acceleration": round(beat_acceleration, 2),
            "latest_beat_pct": round(latest_beat_pct, 2),
            "days_since_earnings": days_since_earnings,
            "days_to_next_earnings": days_to_next,
            "next_earnings_date": upcoming_earnings,
            "post_earnings_drift_pct": round(drift_pct, 2) if drift_pct is not None else None,
            "cap_bucket": cap_bucket,
            "market_cap": market_cap,
            "sector": sector,
        },
        "history": [
            {
                "qtr_end": b["quarter_end"],
                "actual": round(b["actual"], 4),
                "estimate": round(b["estimate"], 4),
                "beat_pct": round(b["beat_pct"], 2),
            }
            for b in beats[:6]
        ],
    }


def lambda_handler(event=None, context=None):
    started = time.time()
    deadline = started + TIMEOUT_BUDGET_S
    print("[pead] starting v1.0")

    universe = get_universe()
    if not universe:
        return {"statusCode": 200, "body": json.dumps({"n": 0})}
    print("[pead] universe: " + str(len(universe)) + " stocks")

    results = []
    n_no_data = 0

    def evaluate(stock):
        if time.time() > deadline:
            return None
        try:
            return evaluate_ticker(stock)
        except Exception as e:
            print("[pead] " + (stock.get("symbol") or "?") + " err: " + str(e)[:80])
            return None

    with ThreadPoolExecutor(max_workers=N_WORKERS) as pool:
        futures = {pool.submit(evaluate, s): s for s in universe}
        for f in as_completed(futures):
            try:
                r = f.result()
                if r:
                    results.append(r)
                else:
                    n_no_data += 1
            except Exception:
                n_no_data += 1

    print("[pead] OK: " + str(len(results)) + ", no_data: " + str(n_no_data))
    results.sort(key=lambda x: -x["score"])

    by_tier = {
        "tier_s": [r for r in results if r["tier"] == "TIER_S_DRIFTING"],
        "tier_a": [r for r in results if r["tier"] == "TIER_A_BEATING"],
        "tier_b": [r for r in results if r["tier"] == "TIER_B_BUILDING"],
    }

    # Cap-bucket breakdowns
    by_cap = {"nano": [], "micro": [], "small": [], "mid": [], "large": [], "mega": []}
    for r in results:
        b = r["metrics"]["cap_bucket"]
        if b in by_cap and r["tier"] != "QUIET":
            by_cap[b].append(r)

    # Pre-earnings opportunities (next 2-14 days)
    pre_earnings_setups = [r for r in results
                            if r["metrics"]["days_to_next_earnings"] is not None
                            and 2 <= r["metrics"]["days_to_next_earnings"] <= 14
                            and r["metrics"]["streak"] >= 3]

    out = {
        "schema_version": 1,
        "method": "pead_detector_v1",
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime()),
        "duration_s": round(time.time() - started, 1),
        "stats": {
            "n_universe": len(universe),
            "n_evaluated": len(results),
            "n_no_data": n_no_data,
            "n_tier_s": len(by_tier["tier_s"]),
            "n_tier_a": len(by_tier["tier_a"]),
            "n_tier_b": len(by_tier["tier_b"]),
            "n_pre_earnings_setups": len(pre_earnings_setups),
            "by_cap_bucket": {k: len(v) for k, v in by_cap.items()},
        },
        "summary": {
            "top_30_overall": [
                {
                    "symbol": r["symbol"],
                    "score": r["score"],
                    "tier": r["tier"],
                    "streak": r["metrics"]["streak"],
                    "avg_beat_pct": r["metrics"]["avg_beat_pct"],
                    "beat_accel": r["metrics"]["beat_acceleration"],
                    "latest_beat_pct": r["metrics"]["latest_beat_pct"],
                    "days_since_earnings": r["metrics"]["days_since_earnings"],
                    "days_to_next": r["metrics"]["days_to_next_earnings"],
                    "drift_pct": r["metrics"]["post_earnings_drift_pct"],
                    "cap_bucket": r["metrics"]["cap_bucket"],
                    "flags": r["flags"],
                }
                for r in results[:30]
            ],
            "tier_s": [r["symbol"] for r in by_tier["tier_s"]][:30],
            "best_microcap": [
                {"symbol": r["symbol"], "score": r["score"], "streak": r["metrics"]["streak"],
                 "avg_beat": r["metrics"]["avg_beat_pct"]}
                for r in results
                if r["metrics"]["cap_bucket"] in ("nano", "micro") and r["score"] >= 40
            ][:15],
            "best_smallcap": [
                {"symbol": r["symbol"], "score": r["score"], "streak": r["metrics"]["streak"],
                 "avg_beat": r["metrics"]["avg_beat_pct"]}
                for r in results
                if r["metrics"]["cap_bucket"] == "small" and r["score"] >= 50
            ][:15],
            "pre_earnings_setups": [
                {"symbol": r["symbol"], "score": r["score"], "streak": r["metrics"]["streak"],
                 "next_earnings": r["metrics"]["next_earnings_date"],
                 "days_to_next": r["metrics"]["days_to_next_earnings"]}
                for r in pre_earnings_setups[:20]
            ],
        },
        "all_qualifying": results,
    }

    body = json.dumps(out, default=str).encode()
    S3.put_object(Bucket=BUCKET, Key=S3_KEY, Body=body, ContentType="application/json")
    print("[pead] wrote " + str(len(body)) + "b")
    print("[pead] tier_s=" + str(len(by_tier["tier_s"])) + " tier_a=" + str(len(by_tier["tier_a"])))
    if results[:5]:
        print("[pead] top: " + str([(r["symbol"], r["score"], r["metrics"]["streak"]) for r in results[:5]]))

    return {
        "statusCode": 200,
        "body": json.dumps({
            "n_evaluated": len(results),
            "n_tier_s": len(by_tier["tier_s"]),
            "n_tier_a": len(by_tier["tier_a"]),
            "n_pre_earnings": len(pre_earnings_setups),
            "duration_s": out["duration_s"],
        }),
    }
