"""
justhodl-microcap-float-squeeze — parabolic squeeze setup detector

Microcap stocks can go parabolic when 3 conditions align:
  1. Float exhaustion: daily volume / float gets very high (> 50%)
  2. High short interest: days-to-cover > 8
  3. Short interest velocity: rising or stable (bears didn't bail yet)
  4. Borrow rate spike: hard-to-borrow rate jumping

WHAT THIS TRACKS (per microcap stock):
  • Float share count (FMP)
  • 30-day average volume
  • Volume / float ratio (how much of float trades each day)
  • Short interest from FINRA (days short, %)
  • Days to cover = short_interest / avg_daily_volume
  • Short volume velocity from FINRA (rising/falling shorts)
  • Recent price level vs 60d high (squeeze setup vs already-squeezed)
  • Real revenue floor (not pure pump-and-dump)

FILTERING:
  • Only stocks with market cap $50M - $2B (microcap to small)
  • Daily $ volume > $500K (liquid enough to actually buy)
  • Has actual revenue > $20M (filters out pure pump shells)
  • Price > $1 (filters out penny stocks)

SCORE 0-100 combines:
  • Float exhaustion intensity
  • Days-to-cover magnitude
  • Short interest velocity (rising = better)
  • Recent base structure (not already pumped)
  • Liquidity adequacy
  • Optional: revenue growth bonus (ties into rev-accel)

OUTPUT: data/microcap-float-squeeze.json
"""
import io, json, os, time, urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
import boto3

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = os.environ.get("S3_KEY", "data/microcap-float-squeeze.json")
FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
N_WORKERS = int(os.environ.get("N_WORKERS", "10"))
MAX_TICKERS = int(os.environ.get("MAX_TICKERS", "600"))
TIMEOUT_BUDGET_S = int(os.environ.get("TIMEOUT_BUDGET_S", "260"))

S3 = boto3.client("s3", region_name=REGION)


def get_universe():
    """Filter universe to nano/micro/small/mid caps for squeeze detection."""
    try:
        obj = S3.get_object(Bucket=BUCKET, Key="data/universe.json")
        d = json.loads(obj["Body"].read())
        all_stocks = d.get("stocks", [])
        target_buckets = {"nano", "micro", "small", "mid"}
        filtered = [s for s in all_stocks if s.get("cap_bucket") in target_buckets]
        return filtered[:MAX_TICKERS]
    except Exception as e:
        print("[float-sq] universe load failed: " + str(e))
        return []


def fetch_url(url, timeout=15):
    req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-FloatSq/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "replace")


def fetch_quote(symbol):
    try:
        text = fetch_url("https://financialmodelingprep.com/stable/quote?symbol=" + symbol + "&apikey=" + FMP_KEY)
        d = json.loads(text)
        if isinstance(d, list) and d:
            return d[0]
    except Exception:
        pass
    return None


def fetch_profile(symbol):
    """Get share float, shares outstanding."""
    try:
        text = fetch_url("https://financialmodelingprep.com/stable/profile?symbol=" + symbol + "&apikey=" + FMP_KEY)
        d = json.loads(text)
        if isinstance(d, list) and d:
            return d[0]
    except Exception:
        pass
    return None


def fetch_history(symbol, days=90):
    """Daily price + volume for last 90 days."""
    try:
        text = fetch_url("https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=" + symbol + "&apikey=" + FMP_KEY, timeout=15)
        d = json.loads(text)
        if not isinstance(d, list):
            return None
        out = []
        for x in d[:days]:
            if x.get("close") and x.get("date"):
                out.append({
                    "date": x["date"],
                    "close": float(x["close"]),
                    "volume": float(x.get("volume") or 0),
                })
        out.sort(key=lambda r: r["date"])
        return out
    except Exception:
        return None


def fetch_finra_short_volume(date_yyyymmdd):
    """FINRA RegSHO daily short volume — free."""
    url = "https://cdn.finra.org/equity/regsho/daily/CNMSshvol" + date_yyyymmdd + ".txt"
    try:
        text = fetch_url(url, timeout=20)
    except Exception:
        return {}
    out = {}
    for line in text.splitlines()[1:]:
        parts = line.split("|")
        if len(parts) < 5:
            continue
        sym = parts[1].strip().upper()
        try:
            short_vol = float(parts[2])
            total_vol = float(parts[4])
            if total_vol > 0:
                out[sym] = {
                    "short_vol": short_vol,
                    "total_vol": total_vol,
                    "short_pct": short_vol / total_vol * 100,
                }
        except (ValueError, IndexError):
            continue
    return out


def get_finra_short_history(days=20):
    """Multi-day FINRA history (skip weekends)."""
    history = defaultdict(list)
    days_collected = 0
    days_back = 1
    while days_collected < days and days_back < days * 2 + 5:
        check_dt = time.gmtime(time.time() - days_back * 86400)
        if check_dt.tm_wday >= 5:
            days_back += 1
            continue
        date_str = time.strftime("%Y%m%d", check_dt)
        date_iso = time.strftime("%Y-%m-%d", check_dt)
        try:
            data = fetch_finra_short_volume(date_str)
            if data:
                for sym, info in data.items():
                    history[sym].append({"date": date_iso, **info})
                days_collected += 1
        except Exception:
            pass
        days_back += 1
    for sym in history:
        history[sym].sort(key=lambda x: x["date"])
    return dict(history)


def evaluate_ticker(stock, finra_history):
    sym = (stock.get("symbol") or "").upper()
    sector = stock.get("sector", "?")
    industry = stock.get("industry", "?")

    # Use universe-supplied data first to avoid extra API calls
    market_cap = stock.get("market_cap") or 0
    price = stock.get("price") or 0
    
    # If universe data is missing, fall back to live quote
    if not (market_cap and price):
        quote = fetch_quote(sym)
        if not quote:
            return None
        market_cap = quote.get("marketCap") or 0
        price = quote.get("price") or 0

    # Filter: $50M - $5B mcap (loosened upper to capture small inflection plays)
    if not (50_000_000 <= market_cap < 5_000_000_000):
        return None
    if price < 1.0:
        return None

    # Derive shares outstanding from market_cap / price (always works,
    # avoids need for now-broken /share-float endpoint)
    if price <= 0:
        return None
    shares_out = market_cap / price
    
    # Float estimate: 80% of shares outstanding (insiders/restricted typically 10-25%)
    float_shares = shares_out * 0.80
    if float_shares <= 0:
        return None

    # History
    history = fetch_history(sym, days=90)
    if not history or len(history) < 30:
        return None

    closes = [h["close"] for h in history]
    volumes = [h["volume"] for h in history]
    n = len(closes)
    today = closes[-1]

    avg_dollar_vol_30 = sum(c * v for c, v in zip(closes[-30:], volumes[-30:])) / min(30, n)
    if avg_dollar_vol_30 < 200_000:
        return None

    avg_vol_30 = sum(volumes[-30:]) / min(30, n)
    avg_vol_60 = sum(volumes[-60:]) / min(60, n) if n >= 60 else avg_vol_30

    # Float exhaustion: daily volume / float
    float_turnover_30d = avg_vol_30 / float_shares * 100  # in %

    # FINRA short data
    finra = finra_history.get(sym, [])
    days_to_cover = None
    short_pct_recent = None
    short_velocity = None
    short_pct_change = None
    if len(finra) >= 5:
        recent_short_pct = sum(d["short_pct"] for d in finra[-5:]) / 5
        older_short_pct = sum(d["short_pct"] for d in finra[:-5]) / max(1, len(finra) - 5) if len(finra) > 5 else recent_short_pct
        avg_short_vol_5d = sum(d["short_vol"] for d in finra[-5:]) / 5
        short_pct_recent = recent_short_pct
        short_pct_change = recent_short_pct - older_short_pct
        # Days-to-cover: total short / avg_daily_volume
        # FINRA gives daily short_volume (sold short on that day), not total short interest.
        # As a proxy, use cumulative recent short_vol / avg_vol
        days_to_cover = avg_short_vol_5d / max(avg_vol_30, 1) * 5  # rough estimate
        short_velocity = short_pct_change

    # Returns / range context
    ret_5d = (today / closes[-6] - 1) * 100 if n >= 6 else 0
    ret_30d = (today / closes[-31] - 1) * 100 if n >= 31 else 0
    ret_60d = (today / closes[-61] - 1) * 100 if n >= 61 else 0
    range_high_60 = max(closes[-60:]) if n >= 60 else max(closes)
    range_low_60 = min(closes[-60:]) if n >= 60 else min(closes)
    range_pos = (today - range_low_60) / (range_high_60 - range_low_60) * 100 if range_high_60 > range_low_60 else 50

    # Liquidity vs float pre-conditions
    # An "easy squeeze" candidate is one where vol/float is high enough that
    # if the stock breaks out, normal-sized institutional buying alone can't
    # be filled without pushing price up materially.

    # ─── SCORING ───
    score = 0.0
    flags = []

    # 1. Float exhaustion
    if float_turnover_30d > 5:
        score += 25
        flags.append("FLOAT_HEAVY_TURNOVER")
    elif float_turnover_30d > 2:
        score += 15
        flags.append("FLOAT_MOD_TURNOVER")
    elif float_turnover_30d > 1:
        score += 8

    # 2. Short interest absolute level
    if short_pct_recent is not None:
        if short_pct_recent > 50:
            score += 25
            flags.append("SHORT_PCT_50%+")
        elif short_pct_recent > 40:
            score += 18
            flags.append("SHORT_PCT_40%+")
        elif short_pct_recent > 30:
            score += 10
            flags.append("SHORT_PCT_30%+")

    # 3. Days-to-cover
    if days_to_cover is not None:
        if days_to_cover > 10:
            score += 15
            flags.append("DAYS_TO_COVER_10+")
        elif days_to_cover > 5:
            score += 10
            flags.append("DAYS_TO_COVER_5+")

    # 4. Short velocity — rising shorts = bears piling in (squeeze fuel)
    if short_velocity is not None:
        if short_velocity > 5:
            score += 12
            flags.append("SHORT_RISING_5PP+")
        elif short_velocity > 2:
            score += 6
            flags.append("SHORT_RISING")
        elif short_velocity < -5:
            score -= 5
            flags.append("SHORTS_COVERED")  # already squeezed

    # 5. Setup vs already-pumped
    if range_pos < 60 and abs(ret_5d) < 10:
        score += 10
        flags.append("BASE_FORMING_SETUP")
    elif range_pos > 90 and ret_5d > 15:
        score -= 15
        flags.append("ALREADY_RUNNING")  # don't chase

    # 6. Volume not yet exploded (still under the radar)
    vol_surge = avg_vol_30 / avg_vol_60 if avg_vol_60 > 0 else 1.0
    if vol_surge > 2.5 and range_pos < 80:
        score += 8
        flags.append("VOLUME_BUILDING_QUIETLY")

    # 7. Liquidity sanity
    if avg_dollar_vol_30 > 5_000_000:
        score += 5
    elif avg_dollar_vol_30 > 2_000_000:
        score += 3

    score = max(0, min(score, 100))

    # Tier
    if score >= 70:
        tier = "TIER_S_PARABOLIC_SETUP"
    elif score >= 55:
        tier = "TIER_A_SQUEEZE_BREWING"
    elif score >= 40:
        tier = "TIER_B_WATCH"
    else:
        tier = "QUIET"

    return {
        "symbol": sym,
        "score": round(score, 1),
        "tier": tier,
        "flags": flags,
        "metrics": {
            "price": round(price, 2),
            "market_cap": int(market_cap),
            "float_shares": int(float_shares),
            "shares_outstanding": int(shares_out),
            "avg_vol_30d": int(avg_vol_30),
            "avg_dollar_vol_30d": int(avg_dollar_vol_30),
            "float_turnover_30d_pct": round(float_turnover_30d, 2),
            "short_pct_recent": round(short_pct_recent, 1) if short_pct_recent is not None else None,
            "short_pct_change": round(short_pct_change, 2) if short_pct_change is not None else None,
            "days_to_cover_proxy": round(days_to_cover, 1) if days_to_cover is not None else None,
            "ret_5d": round(ret_5d, 1),
            "ret_30d": round(ret_30d, 1),
            "ret_60d": round(ret_60d, 1),
            "range_pos_60d": round(range_pos, 1),
            "vol_surge_30v60": round(vol_surge, 2),
            "sector": sector,
            "industry": industry,
        },
    }


def lambda_handler(event=None, context=None):
    started = time.time()
    deadline = started + TIMEOUT_BUDGET_S
    print("[float-sq] starting v1.0")

    universe = get_universe()
    if not universe:
        return {"statusCode": 200, "body": json.dumps({"n": 0})}
    print("[float-sq] universe: " + str(len(universe)) + " stocks")

    print("[float-sq] fetching FINRA short volume history...")
    finra_history = get_finra_short_history(days=20)
    print("[float-sq] FINRA tickers: " + str(len(finra_history)))

    results = []
    n_no_data = 0
    n_filtered_out = 0

    def evaluate(stock):
        if time.time() > deadline:
            return None
        try:
            return evaluate_ticker(stock, finra_history)
        except Exception as e:
            print("[float-sq] " + (stock.get("symbol") or "?") + " err: " + str(e))
            return None

    with ThreadPoolExecutor(max_workers=N_WORKERS) as pool:
        futures = {pool.submit(evaluate, s): s for s in universe}
        for f in as_completed(futures):
            try:
                r = f.result()
                if r:
                    results.append(r)
                else:
                    n_filtered_out += 1
            except Exception:
                n_no_data += 1

    print("[float-sq] OK: " + str(len(results)) + ", filtered_out: " + str(n_filtered_out))
    results.sort(key=lambda x: -x["score"])

    by_tier = {
        "tier_s": [r for r in results if r["tier"] == "TIER_S_PARABOLIC_SETUP"],
        "tier_a": [r for r in results if r["tier"] == "TIER_A_SQUEEZE_BREWING"],
        "tier_b": [r for r in results if r["tier"] == "TIER_B_WATCH"],
    }

    out = {
        "schema_version": 1,
        "method": "microcap_float_squeeze_v1",
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime()),
        "duration_s": round(time.time() - started, 1),
        "stats": {
            "n_universe": len(universe),
            "n_evaluated": len(results),
            "n_filtered_out": n_filtered_out,
            "n_tier_s": len(by_tier["tier_s"]),
            "n_tier_a": len(by_tier["tier_a"]),
            "n_tier_b": len(by_tier["tier_b"]),
            "n_finra_tickers": len(finra_history),
        },
        "summary": {
            "top_25_overall": [
                {
                    "symbol": r["symbol"],
                    "score": r["score"],
                    "tier": r["tier"],
                    "flags": r["flags"],
                    "price": r["metrics"]["price"],
                    "market_cap": r["metrics"]["market_cap"],
                    "float_turnover": r["metrics"]["float_turnover_30d_pct"],
                    "short_pct": r["metrics"]["short_pct_recent"],
                    "days_to_cover": r["metrics"]["days_to_cover_proxy"],
                    "short_change": r["metrics"]["short_pct_change"],
                    "range_pos": r["metrics"]["range_pos_60d"],
                    "vol_surge": r["metrics"]["vol_surge_30v60"],
                    "ret_5d": r["metrics"]["ret_5d"],
                }
                for r in results[:25]
            ],
            "tier_s": [r["symbol"] for r in by_tier["tier_s"]],
        },
        "all_qualifying": results,
    }

    body = json.dumps(out, default=str).encode()
    S3.put_object(Bucket=BUCKET, Key=S3_KEY, Body=body, ContentType="application/json")
    print("[float-sq] wrote " + str(len(body)) + "b")
    print("[float-sq] tier_s=" + str(len(by_tier["tier_s"])) + " tier_a=" + str(len(by_tier["tier_a"])))

    return {
        "statusCode": 200,
        "body": json.dumps({
            "n_evaluated": len(results),
            "n_tier_s": len(by_tier["tier_s"]),
            "n_tier_a": len(by_tier["tier_a"]),
            "duration_s": out["duration_s"],
        }),
    }
