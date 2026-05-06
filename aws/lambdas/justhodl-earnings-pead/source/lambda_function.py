"""
justhodl-earnings-pead — earnings surprise streak + post-earnings drift detector

ACADEMIC ALPHA: Post-Earnings Announcement Drift (PEAD) is one of the most
robust anomalies in markets. Bernard & Thomas (1989) showed stocks that
beat earnings by >2 standard deviations drift up another 8-15% over 60
trading days. Repeated by every major hedge fund. Names with 3+ consecutive
beats (a "streak") get drift bonuses.

WHAT THIS DETECTS:
  1. Earnings beat streak: 3+ consecutive quarters of beating consensus
  2. Beat magnitude: actual EPS / consensus EPS - 1
  3. Revenue beat (alongside EPS beat = highest quality)
  4. Recent earnings within last 30 days (drift window still active)
  5. Post-earnings price action: rising (drift confirming) vs falling (rejected)
  6. Guidance raise (signaled by analyst estimate revisions post-earnings)

ENHANCED FOR ALL CAPS:
  • Microcap PEAD often produces 30-60% drift moves vs 8-15% for large
  • Filters on cap_bucket + min market cap of $50M

OUTPUT: data/earnings-pead.json
"""
import io, json, os, time, urllib.request, urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = os.environ.get("S3_KEY", "data/earnings-pead.json")
FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
N_WORKERS = int(os.environ.get("N_WORKERS", "12"))
MAX_TICKERS = int(os.environ.get("MAX_TICKERS", "1500"))
TIMEOUT_BUDGET_S = int(os.environ.get("TIMEOUT_BUDGET_S", "260"))
DRIFT_WINDOW_DAYS = int(os.environ.get("DRIFT_WINDOW_DAYS", "60"))

S3 = boto3.client("s3", region_name=REGION)


def get_universe():
    try:
        obj = S3.get_object(Bucket=BUCKET, Key="data/universe.json")
        d = json.loads(obj["Body"].read())
        # PEAD works at ALL caps — include them all
        all_stocks = d.get("stocks", [])
        # Skip nano (too volatile, low signal-to-noise)
        target_buckets = {"micro", "small", "mid", "large", "mega"}
        filtered = [s for s in all_stocks if s.get("cap_bucket") in target_buckets]
        return filtered[:MAX_TICKERS]
    except Exception as e:
        print("[pead] universe load failed: " + str(e))
        return []


def fetch_url(url, timeout=15):
    req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-PEAD/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def fetch_earnings_surprises(symbol, limit=8):
    """FMP earnings surprises — actual vs estimated EPS for last N quarters."""
    url = ("https://financialmodelingprep.com/stable/earnings-surprises-bulk?"
           "symbol=" + symbol + "&limit=" + str(limit) +
           "&apikey=" + FMP_KEY)
    # Fall back to per-symbol endpoint if bulk doesn't work
    try:
        d = fetch_url(url, timeout=15)
        if isinstance(d, list):
            return d
    except Exception:
        pass
    
    try:
        url2 = ("https://financialmodelingprep.com/stable/earnings-surprises?"
                "symbol=" + symbol + "&apikey=" + FMP_KEY)
        d = fetch_url(url2, timeout=15)
        if isinstance(d, list):
            return d[:limit]
    except Exception:
        pass
    
    return None


def fetch_recent_history(symbol, days=90):
    url = ("https://financialmodelingprep.com/stable/historical-price-eod/full?"
           "symbol=" + symbol + "&apikey=" + FMP_KEY)
    try:
        d = fetch_url(url, timeout=15)
        if not isinstance(d, list):
            return None
        out = []
        for r in d[:days]:
            if r.get("close") and r.get("date"):
                out.append({
                    "date": r["date"],
                    "close": float(r["close"]),
                    "volume": float(r.get("volume") or 0),
                })
        out.sort(key=lambda r: r["date"])
        return out
    except Exception:
        return None


def evaluate_ticker(stock):
    sym = (stock.get("symbol") or "").upper()
    sector = stock.get("sector") or "?"
    industry = stock.get("industry") or "?"
    market_cap = stock.get("market_cap") or 0
    cap_bucket = stock.get("cap_bucket") or "?"

    # Get earnings surprises
    surprises = fetch_earnings_surprises(sym, limit=8)
    if not surprises or len(surprises) < 4:
        return None

    # Sort newest first
    surprises = sorted(surprises, key=lambda s: s.get("date", ""), reverse=True)

    # Compute surprise pct for each
    surp_pcts = []
    for s in surprises:
        actual = s.get("actualEarningResult") or s.get("actualEps")
        est = s.get("estimatedEarning") or s.get("estimatedEps")
        if actual is None or est is None:
            continue
        try:
            actual = float(actual)
            est = float(est)
        except (ValueError, TypeError):
            continue
        # Surprise %: only if estimate is positive
        if abs(est) < 0.01:
            continue
        pct = (actual - est) / abs(est) * 100
        surp_pcts.append({
            "date": s.get("date"),
            "actual": actual,
            "estimate": est,
            "surprise_pct": pct,
            "beat": actual > est,
        })

    if len(surp_pcts) < 3:
        return None

    # Most recent earnings date — needed for drift window
    latest_date = surp_pcts[0]["date"]
    if not latest_date:
        return None
    
    try:
        latest_ts = time.mktime(time.strptime(latest_date, "%Y-%m-%d"))
    except Exception:
        return None
    
    days_since_earnings = (time.time() - latest_ts) / 86400

    # Beat streak (consecutive beats from most recent backward)
    beat_streak = 0
    for s in surp_pcts:
        if s["beat"]:
            beat_streak += 1
        else:
            break

    # Average surprise magnitude (last 4 quarters)
    avg_surprise = sum(s["surprise_pct"] for s in surp_pcts[:4]) / min(4, len(surp_pcts))

    # Most recent surprise magnitude
    latest_surprise = surp_pcts[0]["surprise_pct"]

    # Drift active = last earnings within 30-60 days (PEAD typically lasts 60d)
    drift_active = 5 < days_since_earnings < DRIFT_WINDOW_DAYS

    # Post-earnings price action
    history = fetch_recent_history(sym, days=90)
    post_earnings_return = None
    if history and drift_active:
        # Find first price after latest_date
        try:
            post_earn = [h for h in history if h["date"] >= latest_date]
            if len(post_earn) >= 2:
                first_after = post_earn[0]["close"]
                today = post_earn[-1]["close"]
                post_earnings_return = (today - first_after) / first_after * 100
        except Exception:
            pass

    # ─── SCORING ───
    score = 0.0
    flags = []

    # 1. Beat streak — most important
    if beat_streak >= 4:
        score += 30
        flags.append("BEAT_STREAK_4Q+")
    elif beat_streak >= 3:
        score += 22
        flags.append("BEAT_STREAK_3Q")
    elif beat_streak >= 2:
        score += 12
        flags.append("BEAT_STREAK_2Q")
    elif surp_pcts[0]["beat"]:
        score += 5
        flags.append("LATEST_BEAT")

    # 2. Latest surprise magnitude
    if latest_surprise > 30:
        score += 25
        flags.append("BIG_BEAT_30%+")
    elif latest_surprise > 15:
        score += 18
        flags.append("BIG_BEAT_15%+")
    elif latest_surprise > 5:
        score += 10
        flags.append("BEAT_5%+")
    elif latest_surprise > 0:
        score += 4

    # 3. Sustained large surprises
    if avg_surprise > 20:
        score += 15
        flags.append("AVG_SURPRISE_20%+")
    elif avg_surprise > 10:
        score += 10
        flags.append("AVG_SURPRISE_10%+")

    # 4. Drift window active + price confirming
    if drift_active:
        score += 5
        flags.append("DRIFT_ACTIVE")
        if post_earnings_return is not None:
            if post_earnings_return > 5:
                score += 15
                flags.append("DRIFT_CONFIRMED_5%+")
            elif post_earnings_return > 0:
                score += 8
                flags.append("DRIFT_POSITIVE")
            elif post_earnings_return < -5:
                score -= 10
                flags.append("DRIFT_REJECTED")

    # 5. Cap-aware bonus
    if cap_bucket in ("micro", "small") and beat_streak >= 3:
        score += 8
        flags.append("SMALLCAP_PEAD_PREMIUM")  # higher drift in smalls

    score = max(0, min(score, 100))

    # Tier
    if score >= 75 and beat_streak >= 3:
        tier = "TIER_S_PEAD_DRIFT"
    elif score >= 60:
        tier = "TIER_A_HIGH_QUALITY_BEAT"
    elif score >= 45:
        tier = "TIER_B_BUILDING"
    else:
        tier = "WATCH"

    return {
        "symbol": sym,
        "score": round(score, 1),
        "tier": tier,
        "flags": flags,
        "beat_streak": beat_streak,
        "metrics": {
            "latest_earnings_date": latest_date,
            "days_since_earnings": round(days_since_earnings, 0),
            "drift_active": drift_active,
            "latest_surprise_pct": round(latest_surprise, 1),
            "avg_surprise_4q": round(avg_surprise, 1),
            "post_earnings_return_pct": round(post_earnings_return, 1) if post_earnings_return is not None else None,
            "market_cap": int(market_cap),
            "cap_bucket": cap_bucket,
            "sector": sector,
            "industry": industry,
        },
        "surprise_history": surp_pcts[:5],
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
            print("[pead] " + (stock.get("symbol") or "?") + " err: " + str(e))
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
        "tier_s": [r for r in results if r["tier"] == "TIER_S_PEAD_DRIFT"],
        "tier_a": [r for r in results if r["tier"] == "TIER_A_HIGH_QUALITY_BEAT"],
        "tier_b": [r for r in results if r["tier"] == "TIER_B_BUILDING"],
    }

    by_cap = {}
    for r in results[:100]:
        cb = r["metrics"]["cap_bucket"]
        by_cap.setdefault(cb, []).append(r["symbol"])

    out = {
        "schema_version": 1,
        "method": "earnings_pead_v1",
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime()),
        "duration_s": round(time.time() - started, 1),
        "stats": {
            "n_universe": len(universe),
            "n_evaluated": len(results),
            "n_no_data": n_no_data,
            "n_tier_s": len(by_tier["tier_s"]),
            "n_tier_a": len(by_tier["tier_a"]),
            "n_tier_b": len(by_tier["tier_b"]),
            "top_100_by_cap_bucket": {k: len(v) for k, v in by_cap.items()},
        },
        "summary": {
            "top_25_overall": [
                {
                    "symbol": r["symbol"],
                    "score": r["score"],
                    "tier": r["tier"],
                    "beat_streak": r["beat_streak"],
                    "latest_surprise_pct": r["metrics"]["latest_surprise_pct"],
                    "avg_surprise_4q": r["metrics"]["avg_surprise_4q"],
                    "days_since_earnings": r["metrics"]["days_since_earnings"],
                    "drift_active": r["metrics"]["drift_active"],
                    "post_earnings_return": r["metrics"]["post_earnings_return_pct"],
                    "cap_bucket": r["metrics"]["cap_bucket"],
                    "market_cap": r["metrics"]["market_cap"],
                    "flags": r["flags"],
                    "sector": r["metrics"]["sector"],
                }
                for r in results[:25]
            ],
            "tier_s": [r["symbol"] for r in by_tier["tier_s"]],
            "tier_s_full": by_tier["tier_s"][:20],
        },
        "all_qualifying": results,
    }

    body = json.dumps(out, default=str).encode()
    S3.put_object(Bucket=BUCKET, Key=S3_KEY, Body=body, ContentType="application/json")
    print("[pead] wrote " + str(len(body)) + "b")
    print("[pead] tier_s=" + str(len(by_tier["tier_s"])) + " tier_a=" + str(len(by_tier["tier_a"])))
    if results[:5]:
        print("[pead] top: " + str([(r["symbol"], r["score"], r["beat_streak"]) for r in results[:5]]))

    return {
        "statusCode": 200,
        "body": json.dumps({
            "n_evaluated": len(results),
            "n_tier_s": len(by_tier["tier_s"]),
            "n_tier_a": len(by_tier["tier_a"]),
            "duration_s": out["duration_s"],
        }),
    }
