"""justhodl-buzz-velocity — institutional pre-pump attention tracker.

THESIS
══════
Retail money is drawn by attention. By the time something is on CNBC,
it's already pumped. The alpha is in the VELOCITY of attention — when
mention counts grow 3-5x in a week BEFORE price moves materially.

Sources used (free, reliable):
  1. REDDIT  — JSON endpoints, r/wallstreetbets + r/stocks + r/investing
              + r/options + r/SecurityAnalysis  
  2. NEWS    — NewsAPI (we have the key); recent article mention counts
  3. GOOGLE  — Skipped for now (pytrends has rate-limit issues for Lambda;
              can add via separate lambda later)

WHAT WE COMPUTE per ticker:
  - Reddit mentions: today vs 30d rolling avg
  - News mentions: today vs 30d rolling avg
  - Composite velocity: weighted combination
  - "Stealth" filter: high velocity + low PRICE move = pre-pump alpha

OUTPUT (data/buzz-velocity.json)
═══════════════════════════════════
Top 30 tickers by buzz velocity, with subscores + headlines.
Emits event buzz.spike for tickers with velocity > 3.0 + low price move.
"""

import json
import re
import time
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from statistics import mean

import boto3

from _sentry_lite import track_errors

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
OUTPUT_KEY = "data/buzz-velocity.json"

NEWS_API_KEY = "17d36cdd13c44e139853b3a6876cf940"
FMP_KEY = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
HTTP_TIMEOUT = 12
USER_AGENT = "JustHodlBuzzVelocity/1.0"

s3 = boto3.client("s3", region_name=REGION)

# Subreddits to scan (each has different mention patterns)
SUBREDDITS = ["wallstreetbets", "stocks", "investing", "options", "SecurityAnalysis"]
SUBREDDIT_WEIGHTS = {
    "wallstreetbets":   0.40,  # highest retail signal
    "stocks":           0.25,
    "investing":        0.15,
    "options":          0.15,
    "SecurityAnalysis": 0.05,
}

# Universe: top ~150 US large/mid caps + meme-prone names
UNIVERSE_LIMIT = 150
MEME_CANDIDATES = [
    "GME", "AMC", "BB", "BBBY", "PLTR", "AMD", "NVDA", "TSLA", "AAPL",
    "RIVN", "LCID", "F", "BYND", "DJT", "RDDT", "ROOT", "MARA", "RIOT",
    "COIN", "HOOD", "SOFI", "OPEN", "WBD", "CHWY", "DUOL", "RBLX", "SMCI",
    "ARM", "CRWD", "PANW", "SNOW", "DDOG", "NET", "MELI", "SHOP", "MDB",
    "ELF", "BITX", "MSTR", "IBIT", "FBTC", "ETHW",
]


def _get_json(url, headers=None, timeout=HTTP_TIMEOUT):
    h = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    if headers:
        h.update(headers)
    try:
        req = urllib.request.Request(url, headers=h)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception as e:
        return {"_err": str(e)[:200]}


# ─── Universe ────────────────────────────────────────────────────────────

def get_universe():
    """Top US large/mid caps + the meme candidates list (union).
    
    Filters out foreign-listed dual-tickers (e.g. LMT.BA = Buenos Aires
    listing of Lockheed Martin) which were leaking through with just
    country=US since FMP returns those as 'US-headquartered' anyway.
    """
    url = (
        f"https://financialmodelingprep.com/stable/company-screener"
        f"?marketCapMoreThan=1000000000&isActivelyTrading=true"
        f"&country=US&exchange=NYSE,NASDAQ"
        f"&limit={UNIVERSE_LIMIT}&apikey={FMP_KEY}"
    )
    data = _get_json(url)
    tickers = []
    if isinstance(data, list):
        for r in data:
            t = r.get("symbol")
            # Post-filter: any ticker with a dot is a foreign listing
            if t and "." not in t:
                tickers.append({"symbol": t, "name": r.get("companyName", "")})
    # Union with meme candidates
    have = {t["symbol"] for t in tickers}
    for m in MEME_CANDIDATES:
        if m not in have and "." not in m:
            tickers.append({"symbol": m, "name": m})
    return tickers


# ─── Price history (for "stealth" filter) ────────────────────────────────

def get_recent_price_perf(ticker: str, days: int = 7) -> float:
    """% return over last N days. Used to detect 'stealth' buzz
    (high mentions but price hasn't moved)."""
    url = f"https://financialmodelingprep.com/stable/historical-price-eod/full?symbol={ticker}&apikey={FMP_KEY}"
    data = _get_json(url)
    if isinstance(data, dict) and "_err" in data:
        return None
    items = data if isinstance(data, list) else data.get("historical", [])
    if len(items) < days + 1:
        return None
    try:
        latest = float(items[0]["close"])
        prior = float(items[days]["close"])
        if prior <= 0:
            return None
        return (latest - prior) / prior * 100
    except Exception:
        return None


# ─── Reddit mentions ─────────────────────────────────────────────────────

def count_reddit_mentions(ticker: str, name: str, subreddit: str,
                           time_filter: str = "week") -> dict:
    """Use Reddit's search.json endpoint. Counts posts matching ticker
    or name in given subreddit + time window.
    time_filter: hour, day, week, month, year, all"""
    # Build query: $TICKER (most common in WSB) OR ticker by itself
    # Use exact-match quotes to avoid noise
    queries = [f'${ticker}']
    # For company name, only if name has 2+ words (avoid false positives)
    if name and len(name.split()) >= 2:
        queries.append(f'"{name}"')
    
    n_posts = 0
    n_comments = 0  # we'll estimate from posts
    sample = []
    
    for q in queries[:1]:  # use just the $TICKER for cleanliness
        url = (
            f"https://www.reddit.com/r/{subreddit}/search.json"
            f"?q={urllib.parse.quote(q)}&restrict_sr=1&t={time_filter}&limit=25&sort=new"
        )
        data = _get_json(url, headers={"User-Agent": USER_AGENT})
        if not isinstance(data, dict) or "_err" in data:
            continue
        listing = data.get("data", {}).get("children") or []
        for child in listing:
            d = child.get("data") or {}
            title = d.get("title", "")
            # Filter: must actually contain the ticker as a word (Reddit search is loose)
            if not re.search(rf"\b\$?{re.escape(ticker)}\b", title, re.IGNORECASE):
                continue
            n_posts += 1
            n_comments += d.get("num_comments", 0)
            if len(sample) < 3:
                sample.append({
                    "title":    title[:120],
                    "score":    d.get("score", 0),
                    "comments": d.get("num_comments", 0),
                    "created":  d.get("created_utc"),
                })
    
    return {
        "n_posts":    n_posts,
        "n_comments": n_comments,
        "sample":     sample,
    }


# ─── News mentions ───────────────────────────────────────────────────────

def count_news_mentions(ticker: str, name: str, days: int = 7) -> dict:
    """NewsAPI mention count for ticker/name in last N days."""
    # Compound query: ticker OR name (if distinctive)
    q_parts = [f'"{ticker}"']
    if name and name.lower() != ticker.lower() and len(name.split()) >= 2:
        q_parts.append(f'"{name}"')
    q = " OR ".join(q_parts)
    
    since = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    qp = urllib.parse.urlencode({
        "q":         q,
        "from":      since,
        "language":  "en",
        "sortBy":    "publishedAt",
        "pageSize":  20,
        "apiKey":    NEWS_API_KEY,
    })
    data = _get_json(f"https://newsapi.org/v2/everything?{qp}")
    if not isinstance(data, dict) or data.get("status") != "ok":
        return {"total": 0, "sample": []}
    
    total = data.get("totalResults") or 0
    sample = []
    for art in (data.get("articles") or [])[:3]:
        sample.append({
            "title":  (art.get("title") or "")[:120],
            "source": (art.get("source") or {}).get("name"),
            "publishedAt": art.get("publishedAt"),
        })
    return {"total": total, "sample": sample}


# ─── Velocity computation ────────────────────────────────────────────────

def compute_velocity(short_window_count, long_window_count, window_ratio: float) -> dict:
    """short_window_count = mentions in last 7d
       long_window_count  = mentions in last 30d
       window_ratio       = 7/30 (expected proportion if flat)
       
    Velocity = (short / long_baseline_per_unit_time)
             = (short / (long * window_ratio))
    
    Velocity 1.0 = flat; 2.0 = 2x normal; 3.0+ = surge."""
    if long_window_count <= 0:
        # If long window is 0 but short isn't, that's also a "new" signal
        if short_window_count > 0:
            return {"velocity": 5.0, "interpretation": "FROM_ZERO", "n_short": short_window_count}
        return {"velocity": 1.0, "interpretation": "NO_DATA", "n_short": 0}
    
    expected_short = long_window_count * window_ratio
    if expected_short <= 0.5:
        velocity = short_window_count + 1  # cap at high val if expected very low
    else:
        velocity = short_window_count / expected_short
    
    if velocity >= 5:
        interp = "EXTREME_SURGE"
    elif velocity >= 3:
        interp = "SPIKE"
    elif velocity >= 2:
        interp = "ACCELERATING"
    elif velocity >= 1.3:
        interp = "RISING"
    elif velocity >= 0.7:
        interp = "FLAT"
    else:
        interp = "FADING"
    
    return {
        "velocity":       round(velocity, 2),
        "interpretation": interp,
        "n_short":        short_window_count,
        "n_long":         long_window_count,
        "expected_short": round(expected_short, 1),
    }


# v3 — sentiment scoring on a sample of titles
# Lightweight rule-based since we can't deploy ML models in Lambda easily.
# Returns score in [-1, 1] where +1 = bullish, -1 = bearish.
_BULLISH_WORDS = {
    "moon", "rocket", "🚀", "bullish", "buy", "long", "calls", "yolo", "diamond",
    "hands", "💎", "loaded", "loading", "accumulating", "breakout", "rally",
    "squeeze", "🟢", "gem", "undervalued", "moass", "tendies", "winning",
    "soaring", "surging", "pump", "explode", "ath", "epic", "massive", "monster",
}
_BEARISH_WORDS = {
    "puts", "short", "bearish", "🩸", "rug", "rugpull", "dump", "crash",
    "tanking", "tanked", "drilling", "🔴", "guh", "bagholder", "fud", "rekt",
    "exit", "selling", "dumping", "down", "drop", "dropping", "loss", "losses",
    "scam", "fraud", "warning", "avoid", "trash", "garbage", "bankruptcy",
    "delisting", "investigation",
}

def lightweight_sentiment(titles: list) -> dict:
    """Rule-based sentiment scoring across a sample of titles.
    Returns {score, n_bullish, n_bearish, n_neutral}."""
    if not titles:
        return {"score": 0, "n_bullish": 0, "n_bearish": 0, "n_neutral": 0, "n_total": 0}
    
    n_bullish = n_bearish = n_neutral = 0
    for title in titles:
        if not title:
            continue
        words = title.lower().split()
        b_hits = sum(1 for w in words if any(bull in w for bull in _BULLISH_WORDS))
        s_hits = sum(1 for w in words if any(bear in w for bear in _BEARISH_WORDS))
        if b_hits > s_hits and b_hits > 0:
            n_bullish += 1
        elif s_hits > b_hits and s_hits > 0:
            n_bearish += 1
        else:
            n_neutral += 1
    
    total = n_bullish + n_bearish + n_neutral
    if total == 0:
        return {"score": 0, "n_bullish": 0, "n_bearish": 0, "n_neutral": 0, "n_total": 0}
    
    # Net sentiment: -1 to +1
    score = (n_bullish - n_bearish) / max(total, 1)
    return {
        "score":     round(score, 3),
        "n_bullish": n_bullish,
        "n_bearish": n_bearish,
        "n_neutral": n_neutral,
        "n_total":   total,
    }


def analyze_ticker(stock: dict) -> dict:
    ticker = stock["symbol"]
    name = stock.get("name") or ""
    
    # ── Reddit: scan each subreddit at "week" (short) and "month" (long)
    reddit_short = {}
    reddit_long = {}
    for sr in SUBREDDITS:
        try:
            short = count_reddit_mentions(ticker, name, sr, time_filter="week")
            long_ = count_reddit_mentions(ticker, name, sr, time_filter="month")
            reddit_short[sr] = short
            reddit_long[sr] = long_
        except Exception as e:
            reddit_short[sr] = {"n_posts": 0, "_err": str(e)[:100]}
            reddit_long[sr] = {"n_posts": 0}
        time.sleep(0.2)  # be polite to Reddit
    
    # Weighted total
    reddit_short_weighted = sum(
        reddit_short.get(sr, {}).get("n_posts", 0) * SUBREDDIT_WEIGHTS[sr]
        for sr in SUBREDDITS
    )
    reddit_long_weighted = sum(
        reddit_long.get(sr, {}).get("n_posts", 0) * SUBREDDIT_WEIGHTS[sr]
        for sr in SUBREDDITS
    )
    reddit_velocity = compute_velocity(reddit_short_weighted,
                                          reddit_long_weighted,
                                          window_ratio=7/30)
    
    # ── News: 7d vs 30d
    try:
        news_7d = count_news_mentions(ticker, name, days=7)
        news_30d = count_news_mentions(ticker, name, days=30)
    except Exception:
        news_7d = {"total": 0, "sample": []}
        news_30d = {"total": 0, "sample": []}
    news_velocity = compute_velocity(news_7d.get("total", 0),
                                       news_30d.get("total", 0),
                                       window_ratio=7/30)
    
    # ── Composite velocity (weighted)
    composite_velocity = round(
        reddit_velocity["velocity"] * 0.6 + news_velocity["velocity"] * 0.4, 2
    )
    
    # ── v3 NEW: Sentiment scoring on sample titles
    # Pool Reddit + News titles
    all_titles = []
    for sr in SUBREDDITS:
        for sample in reddit_short.get(sr, {}).get("sample", []) or []:
            if sample.get("title"):
                all_titles.append(sample["title"])
    for sample in (news_7d.get("sample") or []):
        if sample.get("title"):
            all_titles.append(sample["title"])
    sentiment = lightweight_sentiment(all_titles)
    
    # ── "Stealth" check: high velocity + LOW price move = pre-pump alpha
    try:
        price_perf_7d = get_recent_price_perf(ticker, days=7)
    except Exception:
        price_perf_7d = None
    
    stealth = (composite_velocity >= 2.5 and price_perf_7d is not None
                 and abs(price_perf_7d) < 5.0)
    
    # ── v3 NEW: DIVERGENCE — buzz rising but price falling (bearish drift or contrarian)
    divergence = None
    if composite_velocity >= 1.8 and price_perf_7d is not None:
        if price_perf_7d <= -8:
            divergence = "negative_divergence"  # attention up, price tanking (bad news)
        elif price_perf_7d >= 12 and sentiment["score"] < -0.2:
            divergence = "positive_divergence"  # price up but sentiment turning sour
    
    # ── Score: 0-100
    score = min(100, composite_velocity * 25)
    if stealth:
        score = min(100, score + 20)  # bonus for pre-pump positioning
    # Sentiment-direction adjustment: if strongly bullish, boost; bearish, reduce
    if sentiment["score"] >= 0.4:
        score = min(100, score + 8)
    elif sentiment["score"] <= -0.4:
        score = max(0, score - 8)
    
    return {
        "ticker":             ticker,
        "name":               name,
        "score":              round(score, 1),
        "composite_velocity": composite_velocity,
        "reddit_velocity":    reddit_velocity,
        "news_velocity":      news_velocity,
        "sentiment":          sentiment,
        "price_perf_7d_pct":  round(price_perf_7d, 2) if price_perf_7d is not None else None,
        "stealth_signal":     stealth,
        "divergence":         divergence,
        "reddit_breakdown_7d": {
            sr: reddit_short.get(sr, {}).get("n_posts", 0) for sr in SUBREDDITS
        },
        "news_7d_count":      news_7d.get("total"),
        "news_30d_count":     news_30d.get("total"),
        "sample_headlines":   (news_7d.get("sample") or [])[:2],
        "thesis":             _thesis(composite_velocity, stealth, price_perf_7d,
                                         reddit_velocity, news_velocity, sentiment,
                                         divergence),
    }


def _thesis(velocity, stealth, price_perf, reddit_v, news_v, sentiment=None, divergence=None):
    bits = []
    if velocity >= 3:
        bits.append(f"buzz velocity {velocity}x baseline")
    elif velocity >= 2:
        bits.append(f"accelerating buzz ({velocity}x)")
    if reddit_v.get("interpretation") in ("EXTREME_SURGE", "SPIKE", "FROM_ZERO"):
        bits.append(f"Reddit: {reddit_v['interpretation'].lower().replace('_',' ')}")
    if news_v.get("interpretation") in ("EXTREME_SURGE", "SPIKE"):
        bits.append("news spiking")
    if sentiment and abs(sentiment.get("score", 0)) >= 0.3:
        tone = "bullish" if sentiment["score"] > 0 else "bearish"
        bits.append(f"sentiment {tone} ({sentiment['score']:+.2f})")
    if divergence == "negative_divergence":
        bits.append("⚠ NEG DIVERGENCE — attention up, price tanking")
    elif divergence == "positive_divergence":
        bits.append("⚠ POS DIVERGENCE — price up, sentiment turning")
    if stealth:
        bits.append(f"STEALTH (price only {price_perf:+.1f}% in 7d)")
    return " · ".join(bits) if bits else "Moderate buzz"


@track_errors
def handler(event, context):
    started = datetime.now(timezone.utc)
    
    universe = get_universe()
    # Dedupe by symbol
    seen = set()
    universe = [u for u in universe if not (u["symbol"] in seen or seen.add(u["symbol"]))]
    print(f"[buzz] universe size: {len(universe)}")
    
    results = []
    for i, stock in enumerate(universe):
        try:
            r = analyze_ticker(stock)
            if r and (r["score"] >= 25 or r["stealth_signal"]):
                results.append(r)
        except Exception as e:
            print(f"[buzz] err on {stock['symbol']}: {e}")
        if (i + 1) % 15 == 0:
            elapsed = (datetime.now(timezone.utc) - started).total_seconds()
            print(f"[buzz] processed {i+1}/{len(universe)}  found {len(results)}  "
                  f"elapsed {elapsed:.0f}s")
        # Hard time budget: each ticker has ~10 HTTP calls
        if (datetime.now(timezone.utc) - started).total_seconds() > 750:
            print("[buzz] time budget exhausted, stopping early")
            break
    
    results.sort(key=lambda r: -r["score"])
    
    # ─── Output ─────────────────────────────────────────────────────────
    out = {
        "schema_version": "1.0",
        "method":         "buzz_velocity_v1",
        "generated_at":   datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "duration_s":     round((datetime.now(timezone.utc) - started).total_seconds(), 1),
        "n_universe":     len(universe),
        "n_results":      len(results),
        "weights": {
            "reddit_in_composite":   0.6,
            "news_in_composite":     0.4,
            "stealth_bonus":         20,
            "subreddit_weights":     SUBREDDIT_WEIGHTS,
        },
        "top_30":         results[:30],
        "stealth_picks":  [r for r in results if r["stealth_signal"]][:12],
        "all_results":    results,
        "notes": (
            "Velocity = short_window_count / (long_window_count * 7/30). "
            "STEALTH signal = composite velocity >= 2.5 + |7d price perf| < 5%. "
            "These are pre-pump alpha candidates: attention rising while "
            "price hasn't moved yet."
        ),
    }
    
    body = json.dumps(out, default=str, separators=(",", ":")).encode("utf-8")
    s3.put_object(Bucket=BUCKET, Key=OUTPUT_KEY, Body=body,
                  ContentType="application/json",
                  CacheControl="public, max-age=3600")
    print(f"[buzz] wrote {len(body):,}B  top: {results[0]['ticker'] if results else 'none'}")
    
    # ─── Emit events ────────────────────────────────────────────────────
    try:
        from system_events import publish_many
        spikes = [r for r in results if r["composite_velocity"] >= 3.0][:5]
        events_to_pub = [
            ("buzz.spike", {
                "ticker":            r["ticker"],
                "composite_velocity": r["composite_velocity"],
                "reddit_interp":     r["reddit_velocity"].get("interpretation"),
                "news_interp":       r["news_velocity"].get("interpretation"),
                "stealth":           r["stealth_signal"],
                "price_7d_pct":      r["price_perf_7d_pct"],
            })
            for r in spikes
        ]
        for i in range(0, len(events_to_pub), 10):
            publish_many(events_to_pub[i:i+10])
    except Exception as e:
        print(f"[buzz] event publish failed: {e}")
    
    return {"statusCode": 200, "body": json.dumps({
        "ok": True,
        "n_results": len(results),
        "n_stealth": len(out["stealth_picks"]),
        "top_ticker": results[0]["ticker"] if results else None,
        "top_score": results[0]["score"] if results else None,
        "duration_s": out["duration_s"],
    })}


lambda_handler = handler
