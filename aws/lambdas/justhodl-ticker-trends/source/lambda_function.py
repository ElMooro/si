"""justhodl-ticker-trends — per-ticker Google search interest velocity.

ROLE
════
4th forward-looking signal source for the future-intelligence composite.
Complementary to justhodl-google-trends (which tracks macro INDICES like
'melt_up_attention' and 'ai_hype') — this one tracks per-TICKER search
interest, surfacing names where retail is googling but price hasn't
moved yet.

WHY NOT PYTRENDS?
═════════════════
pytrends pulls in pandas + numpy = ~80MB of deps that don't deploy
cleanly into Lambda, plus the library's explore-token step has high
failure rate from cloud IP ranges. We use the bare Google Trends widget
endpoints directly with urllib + json. Zero external deps.

ARCHITECTURE
════════════
For each ticker:
  Step 1: GET trends.google.com/trends/api/explore  → widget tokens
  Step 2: GET trends.google.com/trends/api/widgetdata/multiline
          (with token) → daily interest series [0-100]
  Step 3: Compute velocity = avg(last_7d) / avg(prior_23d)
  Step 4: STEALTH detection: velocity ≥ 2 AND |price_7d| < 5%

RATE LIMITING
═════════════
Google Trends 429s aggressively under load. Mitigations:
  - Cap at 80 tickers per run (~11 min runtime)
  - Sleep SLEEP_BETWEEN_S between tickers (default 8s)
  - Single retry with 30s backoff on 429
  - Skip-on-error (don't bring down whole run)
  - Schedule 2x daily so transient blocks self-heal

OUTPUT
══════
  data/ticker-trends.json — per-ticker velocity + interest level + raw series
  Emits ticker_trends.spike event for velocity >= 3.0 (filtered by
  coordinator for STEALTH or extreme spikes only)
"""
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone
from statistics import mean

import boto3

from _sentry_lite import track_errors

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
OUTPUT_KEY = "data/ticker-trends.json"

FMP_KEY = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"

# Tunable via env
MAX_TICKERS    = int(os.environ.get("MAX_TICKERS", "80"))
SLEEP_BETWEEN  = float(os.environ.get("SLEEP_BETWEEN_S", "8.0"))
HTTP_TIMEOUT   = 15

# Realistic browser UA — Google Trends rejects bare User-Agent strings
USER_AGENT = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/126.0.0.0 Safari/537.36")

# Universe focus list — names where Google Trends has the strongest signal
# (large-cap tech + recently-trending tickers + meme bench). Avoids dilute
# coverage of small-caps where search interest is too noisy.
FOCUS_TICKERS = [
    # Mega-cap tech
    "AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "AVGO", "TSLA", "ORCL",
    # AI/semis
    "AMD", "INTC", "TSM", "ASML", "MU", "QCOM", "ARM", "SMCI",
    # Power for AI chain
    "VST", "GEV", "CEG", "VRT", "ETN",
    # Cyber
    "PANW", "CRWD", "ZS", "NET", "S",
    # Quantum
    "IONQ", "RGTI", "QBTS",
    # Fintech / Crypto-adjacent
    "COIN", "HOOD", "SOFI", "MSTR", "PYPL",
    # EV / battery
    "RIVN", "LCID", "ALB", "LAC", "PLL", "ALTM",
    # Defense
    "LMT", "RTX", "NOC", "GD", "AVAV", "RKLB",
    # Biotech (newly-added rotation chain)
    "LLY", "NVO", "REGN", "VRTX", "MRNA", "BIIB",
    # Commodities (newly-added chains)
    "FCX", "SCCO", "CCJ", "DNN", "UEC",
    # Datacenter REIT
    "EQIX", "DLR",
    # Meme
    "GME", "AMC", "BB", "PLTR", "DJT", "RDDT", "DUOL", "RBLX",
    # Speculative (quantum / SMR)
    "MP", "USAR", "OKLO", "SMR", "NNE",
]

s3 = boto3.client("s3", region_name=REGION)


# ─── HTTP with retry + 429 handling ──────────────────────────────────────

def _http_get(url, retries=2, backoff_s=30):
    h = {
        "User-Agent":      USER_AGENT,
        "Accept":          "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer":         "https://trends.google.com/trends/",
    }
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers=h)
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
                return r.read().decode("utf-8")
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < retries:
                print(f"[ticker-trends] 429, sleeping {backoff_s}s then retry…")
                time.sleep(backoff_s)
                continue
            return None
        except Exception:
            if attempt < retries:
                time.sleep(2)
                continue
            return None
    return None


def _strip_prefix_json(s):
    """Google Trends responses are JSON with a 4-5 char prefix designed
    to defeat JSON-hijacking. Find first '{' and parse from there."""
    if not s:
        return None
    i = s.find("{")
    if i < 0:
        return None
    try:
        return json.loads(s[i:])
    except json.JSONDecodeError:
        return None


# ─── Google Trends widget protocol ───────────────────────────────────────

def get_trends_series(keyword, geo="US", timeframe="today 1-m"):
    """Two-call dance:
      1. /api/explore  → widget tokens
      2. /api/widgetdata/multiline  → daily 0-100 interest values"""
    # Step 1: explore
    explore_req = {
        "comparisonItem": [
            {"keyword": keyword, "geo": geo, "time": timeframe}
        ],
        "category": 0,
        "property": "",
    }
    params1 = urllib.parse.urlencode({
        "hl":  "en-US",
        "tz":  "300",
        "req": json.dumps(explore_req, separators=(",", ":")),
    })
    url1 = f"https://trends.google.com/trends/api/explore?{params1}"
    body1 = _http_get(url1)
    if not body1:
        return {"err": "explore_failed"}
    data1 = _strip_prefix_json(body1)
    if not data1 or "widgets" not in data1:
        return {"err": "explore_no_widgets"}
    
    timeseries_widget = None
    for w in data1["widgets"]:
        if w.get("id") == "TIMESERIES":
            timeseries_widget = w
            break
    if not timeseries_widget:
        return {"err": "no_timeseries_widget"}
    
    token = timeseries_widget.get("token")
    req2 = timeseries_widget.get("request")
    if not token or not req2:
        return {"err": "no_token"}
    
    # Step 2: timeseries data
    params2 = urllib.parse.urlencode({
        "hl":    "en-US",
        "tz":    "300",
        "token": token,
        "req":   json.dumps(req2, separators=(",", ":")),
    })
    url2 = f"https://trends.google.com/trends/api/widgetdata/multiline?{params2}"
    body2 = _http_get(url2)
    if not body2:
        return {"err": "multiline_failed"}
    data2 = _strip_prefix_json(body2)
    if not data2 or "default" not in data2:
        return {"err": "multiline_no_default"}
    
    timeline = data2.get("default", {}).get("timelineData") or []
    series = []
    for pt in timeline:
        val = pt.get("value") or []
        if val:
            try:
                series.append(int(val[0]))
            except (ValueError, TypeError):
                continue
    
    return {"series": series, "n": len(series)}


# ─── Velocity scoring ────────────────────────────────────────────────────

def compute_trends_velocity(series):
    """Daily Google Trends values 0-100 → velocity (7d avg vs prior period)."""
    if not series or len(series) < 14:
        return {"velocity": None, "level": None, "interp": "NO_DATA"}
    
    recent = series[-7:]
    prior  = series[-30:-7] if len(series) >= 30 else series[:-7]
    
    recent_avg = mean(recent)
    prior_avg  = mean(prior) if prior else 0.01
    
    if prior_avg < 1:
        velocity = recent_avg + 1  # floor case
    else:
        velocity = recent_avg / prior_avg
    
    if velocity >= 4:     interp = "EXTREME_SURGE"
    elif velocity >= 2.5: interp = "SPIKE"
    elif velocity >= 1.8: interp = "ACCELERATING"
    elif velocity >= 1.3: interp = "RISING"
    elif velocity >= 0.7: interp = "FLAT"
    else: interp = "FADING"
    
    return {
        "velocity":     round(velocity, 2),
        "level":        round(recent_avg, 1),
        "prior_level":  round(prior_avg, 1),
        "max_in_range": max(series),
        "interp":       interp,
    }


def get_recent_price_perf(ticker, days=7):
    """Used for STEALTH detection."""
    url = (f"https://financialmodelingprep.com/stable/historical-price-eod/full"
            f"?symbol={ticker}&apikey={FMP_KEY}")
    body = _http_get(url, retries=0)
    if not body:
        return None
    try:
        data = json.loads(body)
    except Exception:
        return None
    items = data if isinstance(data, list) else data.get("historical", [])
    if len(items) < days + 1:
        return None
    try:
        latest = float(items[0]["close"])
        prior  = float(items[days]["close"])
        if prior <= 0:
            return None
        return (latest - prior) / prior * 100
    except Exception:
        return None


# ─── Per-ticker analysis ─────────────────────────────────────────────────

def analyze_ticker(ticker):
    trends_result = get_trends_series(ticker, geo="US", timeframe="today 1-m")
    if "err" in trends_result:
        return {"ticker": ticker, "err": trends_result["err"]}
    
    series = trends_result.get("series") or []
    velocity_info = compute_trends_velocity(series)
    
    if velocity_info.get("velocity") is None:
        return {"ticker": ticker, "err": "insufficient_data", "series_n": len(series)}
    
    price_7d = get_recent_price_perf(ticker, days=7)
    velocity = velocity_info["velocity"]
    stealth = (velocity >= 2.0 and price_7d is not None and abs(price_7d) < 5.0)
    
    # Score 0-100
    score = min(100, velocity * 25)
    if stealth:
        score = min(100, score + 20)
    if velocity_info["level"] >= 50:
        score = min(100, score + 5)
    
    thesis_bits = []
    if velocity >= 2.5:
        thesis_bits.append(f"Google search {velocity}x baseline")
    elif velocity >= 1.5:
        thesis_bits.append(f"rising search interest ({velocity}x)")
    if stealth:
        thesis_bits.append(f"STEALTH (price only {price_7d:+.1f}% in 7d)")
    if velocity_info["max_in_range"] >= 80 and velocity_info["level"] < 50:
        thesis_bits.append("fading from peak")
    
    return {
        "ticker":        ticker,
        "score":         round(score, 1),
        "velocity":      velocity,
        "current_level": velocity_info["level"],
        "prior_level":   velocity_info["prior_level"],
        "max_in_range":  velocity_info["max_in_range"],
        "interp":        velocity_info["interp"],
        "price_7d_pct":  round(price_7d, 2) if price_7d is not None else None,
        "stealth":       stealth,
        "series":        series[-30:],
        "thesis":        " · ".join(thesis_bits) if thesis_bits else
                            f"Search {velocity_info['interp'].lower().replace('_',' ')}",
    }


@track_errors
def handler(event, context):
    started = datetime.now(timezone.utc)
    
    universe = list(dict.fromkeys(FOCUS_TICKERS))[:MAX_TICKERS]
    expected_min = SLEEP_BETWEEN * len(universe) / 60
    print(f"[ticker-trends] universe: {len(universe)} tickers, "
          f"~{expected_min:.0f}min expected runtime")
    
    results = []
    errors  = defaultdict(int)
    
    for i, ticker in enumerate(universe):
        try:
            r = analyze_ticker(ticker)
            if "err" in r:
                errors[r["err"]] += 1
            else:
                results.append(r)
        except Exception as e:
            errors["exception"] += 1
            print(f"[ticker-trends] err on {ticker}: {e}")
        
        if i < len(universe) - 1:
            time.sleep(SLEEP_BETWEEN)
        
        if (i + 1) % 10 == 0:
            elapsed = (datetime.now(timezone.utc) - started).total_seconds()
            print(f"[ticker-trends] {i+1}/{len(universe)}  ok={len(results)}  "
                  f"err={dict(errors)}  elapsed={elapsed:.0f}s")
        
        # Hard time budget
        if (datetime.now(timezone.utc) - started).total_seconds() > 780:
            print("[ticker-trends] time budget exhausted, stopping early")
            break
    
    results.sort(key=lambda r: -r["score"])
    
    out = {
        "schema_version": "1.0",
        "method":         "ticker_trends_v1",
        "generated_at":   datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "duration_s":     round((datetime.now(timezone.utc) - started).total_seconds(), 1),
        "n_processed":    len(universe),
        "n_ok":           len(results),
        "errors":         dict(errors),
        "config": {
            "max_tickers":   MAX_TICKERS,
            "sleep_between": SLEEP_BETWEEN,
        },
        "top_20":         results[:20],
        "stealth_picks":  [r for r in results if r["stealth"]][:10],
        "all_results":    results,
        "notes": (
            "Per-ticker Google search interest velocity = last_7d_avg / "
            "prior_23d_avg. STEALTH = velocity >= 2.0 + |7d price perf| < 5% "
            "(retail attention rising but price hasn't moved — pre-pump alpha)."
        ),
    }
    
    body = json.dumps(out, default=str, separators=(",", ":")).encode("utf-8")
    s3.put_object(Bucket=BUCKET, Key=OUTPUT_KEY, Body=body,
                  ContentType="application/json",
                  CacheControl="public, max-age=14400")
    print(f"[ticker-trends] wrote {len(body):,}B  top: "
          f"{results[0]['ticker'] if results else 'none'}")
    
    # Emit events for spikes
    try:
        from system_events import publish_many
        spikes = [r for r in results if r["velocity"] >= 3.0][:5]
        events_to_pub = [
            ("ticker_trends.spike", {
                "ticker":       r["ticker"],
                "velocity":     r["velocity"],
                "level":        r["current_level"],
                "interp":       r["interp"],
                "stealth":      r["stealth"],
                "price_7d_pct": r["price_7d_pct"],
            }) for r in spikes
        ]
        if events_to_pub:
            publish_many(events_to_pub)
    except Exception as e:
        print(f"[ticker-trends] event publish failed: {e}")
    
    return {"statusCode": 200, "body": json.dumps({
        "ok":           True,
        "n_ok":         len(results),
        "n_stealth":    len(out["stealth_picks"]),
        "top_ticker":   results[0]["ticker"] if results else None,
        "top_velocity": results[0]["velocity"] if results else None,
        "duration_s":   out["duration_s"],
    })}


lambda_handler = handler
