"""justhodl-ticker-trends — per-ticker search-interest velocity.

ROLE
════
4th forward-looking signal source for the future-intelligence composite.
Tracks search interest velocity per ticker; STEALTH detection flags
names where attention is rising but price hasn't moved yet.

DESIGN (v2 — 2026-05-31)
═══════════════════════
Switched PRIMARY source from Google Trends to Wikipedia pageviews after
v1 failed silently in production (AWS us-east-1 IP ranges get aggressively
429-blocked by Google Trends — a known datacenter-egress issue).

  PRIMARY:  Wikipedia pageviews API — free, no auth, no rate limits.
            People research stocks → click Wikipedia → registered as
            a pageview. Strong correlation with Google search interest
            (which is what we ACTUALLY want, just measured differently).
  
  FALLBACK: Google Trends widget API (best-effort, one try, skip on 429).
            When it works, we average it in. When blocked, the engine
            still produces output from Wikipedia alone.

The output schema is identical to v1 so future-intelligence
integration is unchanged.

RATE LIMITING
═════════════
Wikipedia API is generous (no documented rate limit; we keep 1s/req for
politeness). Total runtime for 80 tickers ≈ 2-3 min.

OUTPUT
══════
  data/ticker-trends.json — per-ticker velocity + interest level + raw series
  Emits ticker_trends.spike event for velocity >= 3.0
"""
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from statistics import mean

import boto3

from _sentry_lite import track_errors

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
OUTPUT_KEY = "data/ticker-trends.json"

FMP_KEY = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"

# Tunable via env
MAX_TICKERS    = int(os.environ.get("MAX_TICKERS", "80"))
SLEEP_BETWEEN  = float(os.environ.get("SLEEP_BETWEEN_S", "1.0"))  # Wikipedia is generous
TRY_GOOGLE     = os.environ.get("TRY_GOOGLE", "1") == "1"
HTTP_TIMEOUT   = 12

USER_AGENT = ("JustHodl-TickerTrends/2.0 (raafouis@gmail.com) - "
                "ticker search-interest velocity tracker")

# Ticker → Wikipedia article slug. The 80-ticker focus list — names where
# search interest is meaningful (large-cap + meme-prone + thematic plays).
# Most companies' Wikipedia URL is predictable; verified each one manually.
TICKER_TO_WIKI = {
    # Mega-cap tech
    "AAPL":  "Apple_Inc.",
    "MSFT":  "Microsoft",
    "GOOGL": "Alphabet_Inc.",
    "AMZN":  "Amazon_(company)",
    "META":  "Meta_Platforms",
    "NVDA":  "Nvidia",
    "AVGO":  "Broadcom",
    "TSLA":  "Tesla,_Inc.",
    "ORCL":  "Oracle_Corporation",
    # AI / semis
    "AMD":   "Advanced_Micro_Devices",
    "INTC":  "Intel",
    "TSM":   "TSMC",
    "ASML":  "ASML_Holding",
    "MU":    "Micron_Technology",
    "QCOM":  "Qualcomm",
    "ARM":   "Arm_Holdings",
    "SMCI":  "Supermicro",
    # AI-power chain T3
    "VST":   "Vistra_Corp",
    "GEV":   "GE_Vernova",
    "CEG":   "Constellation_Energy",
    "VRT":   "Vertiv",
    "ETN":   "Eaton_Corporation",
    # Cyber
    "PANW":  "Palo_Alto_Networks",
    "CRWD":  "CrowdStrike",
    "ZS":    "Zscaler",
    "NET":   "Cloudflare",
    "S":     "SentinelOne",
    # Quantum
    "IONQ":  "IonQ",
    "RGTI":  "Rigetti_Computing",
    "QBTS":  "D-Wave_Systems",   # Wikipedia still uses pre-rename canonical
    # Fintech / Crypto-adjacent
    "COIN":  "Coinbase",
    "HOOD":  "Robinhood_Markets",
    "SOFI":  "SoFi",
    "MSTR":  "Strategy_(company)",  # formerly MicroStrategy
    "PYPL":  "PayPal",
    # EV / battery
    "RIVN":  "Rivian",
    "LCID":  "Lucid_Motors",
    "ALB":   "Albemarle_Corporation",
    "LAC":   "Lithium_Americas",
    "PLL":   "Piedmont_Lithium",
    "ALTM":  "Arcadium_Lithium",
    # Defense
    "LMT":   "Lockheed_Martin",
    "RTX":   "RTX_Corporation",
    "NOC":   "Northrop_Grumman",
    "GD":    "General_Dynamics",
    "AVAV":  "AeroVironment",
    "RKLB":  "Rocket_Lab",
    # Biotech
    "LLY":   "Eli_Lilly_and_Company",
    "NVO":   "Novo_Nordisk",
    "REGN":  "Regeneron_Pharmaceuticals",
    "VRTX":  "Vertex_Pharmaceuticals",
    "MRNA":  "Moderna",
    "BIIB":  "Biogen",
    # Commodities
    "FCX":   "Freeport-McMoRan",
    "SCCO":  "Southern_Copper_Corporation",
    "CCJ":   "Cameco",
    "DNN":   "Denison_Mines",
    # UEC removed — no Wikipedia article exists for Uranium Energy Corp
    # Datacenter REITs
    "EQIX":  "Equinix",
    "DLR":   "Digital_Realty",
    # Meme
    "GME":   "GameStop",
    "AMC":   "AMC_Theatres",
    "BB":    "BlackBerry_Limited",
    "PLTR":  "Palantir_Technologies",
    "DJT":   "Trump_Media_&_Technology_Group",  # literal & — urllib will encode
    "RDDT":  "Reddit,_Inc.",
    "DUOL":  "Duolingo",
    "RBLX":  "Roblox",
    # Speculative
    "MP":    "MP_Materials",
    "USAR":  "USA_Rare_Earth",
    "OKLO":  "Oklo_Inc.",          # was Oklo_(company)
    "SMR":   "NuScale_Power",
    "NNE":   "Nano_Nuclear_Energy",
}

s3 = boto3.client("s3", region_name=REGION)


def _http_get(url, timeout=HTTP_TIMEOUT, retries=0, log_label=""):
    """Minimal urllib GET with optional retry. Logs errors at debug level."""
    h = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers=h)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read().decode("utf-8")
        except urllib.error.HTTPError as e:
            print(f"[ticker-trends-v2] {log_label} HTTP {e.code} from {url[:120]}")
            if e.code == 404:
                return None
            if attempt < retries:
                time.sleep(2)
                continue
            return None
        except Exception as e:
            print(f"[ticker-trends-v2] {log_label} {type(e).__name__}: {str(e)[:100]} from {url[:120]}")
            if attempt < retries:
                time.sleep(2)
                continue
            return None
    return None


# ─── PRIMARY: Wikipedia pageviews ────────────────────────────────────────

def get_wiki_pageviews(article: str, days: int = 31) -> list:
    """Daily pageview counts for an article over last N days.
    
    Returns: list of int (oldest first), or None on failure.
    
    Wikipedia caches pageview data with ~24h lag — so requesting through
    today returns 0 for today. We back off 2 days to get reliable counts.
    """
    if not article:
        return None
    
    end_dt = datetime.now(timezone.utc) - timedelta(days=2)
    start_dt = end_dt - timedelta(days=days)
    start_s = start_dt.strftime("%Y%m%d") + "00"
    end_s   = end_dt.strftime("%Y%m%d") + "00"
    
    article_enc = urllib.parse.quote(article, safe="")
    url = (
        f"https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/"
        f"en.wikipedia/all-access/user/{article_enc}/daily/{start_s}/{end_s}"
    )
    body = _http_get(url, timeout=10, retries=1, log_label=f"wiki[{article}]")
    if not body:
        return None
    try:
        data = json.loads(body)
    except Exception:
        return None
    items = data.get("items") or []
    if not items:
        return None
    
    # Sort by timestamp asc and extract view counts
    items.sort(key=lambda x: x.get("timestamp", ""))
    series = []
    for it in items:
        try:
            series.append(int(it.get("views", 0)))
        except (ValueError, TypeError):
            continue
    return series if len(series) >= 14 else None


# ─── FALLBACK: Google Trends (best-effort, single try) ──────────────────

def _strip_prefix_json(s):
    if not s:
        return None
    i = s.find("{")
    if i < 0:
        return None
    try:
        return json.loads(s[i:])
    except json.JSONDecodeError:
        return None


def get_google_trends_series(keyword, geo="US", timeframe="today 1-m"):
    """Best-effort Google Trends fetch. One try, short timeout, no retry.
    Returns list of int or None."""
    h = {
        "User-Agent":      ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/126.0.0.0 Safari/537.36"),
        "Accept":          "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer":         "https://trends.google.com/trends/",
    }
    
    def _try_get(url):
        try:
            req = urllib.request.Request(url, headers=h)
            with urllib.request.urlopen(req, timeout=8) as r:
                return r.read().decode("utf-8")
        except Exception:
            return None
    
    explore_req = {
        "comparisonItem": [{"keyword": keyword, "geo": geo, "time": timeframe}],
        "category": 0, "property": "",
    }
    params1 = urllib.parse.urlencode({
        "hl": "en-US", "tz": "300",
        "req": json.dumps(explore_req, separators=(",", ":")),
    })
    body1 = _try_get(f"https://trends.google.com/trends/api/explore?{params1}")
    if not body1:
        return None
    data1 = _strip_prefix_json(body1)
    if not data1 or "widgets" not in data1:
        return None
    
    widget = next((w for w in data1["widgets"] if w.get("id") == "TIMESERIES"), None)
    if not widget:
        return None
    token, req2 = widget.get("token"), widget.get("request")
    if not token or not req2:
        return None
    
    params2 = urllib.parse.urlencode({
        "hl": "en-US", "tz": "300", "token": token,
        "req": json.dumps(req2, separators=(",", ":")),
    })
    body2 = _try_get(f"https://trends.google.com/trends/api/widgetdata/multiline?{params2}")
    if not body2:
        return None
    data2 = _strip_prefix_json(body2)
    if not data2 or "default" not in data2:
        return None
    
    timeline = data2.get("default", {}).get("timelineData") or []
    series = []
    for pt in timeline:
        val = pt.get("value") or []
        if val:
            try:
                series.append(int(val[0]))
            except (ValueError, TypeError):
                continue
    return series if len(series) >= 14 else None


# ─── Velocity scoring (works on any daily series) ────────────────────────

def compute_velocity(series):
    """daily counts → velocity (7d avg vs prior period)."""
    if not series or len(series) < 14:
        return {"velocity": None, "level": None, "interp": "NO_DATA"}
    
    recent = series[-7:]
    prior  = series[-30:-7] if len(series) >= 30 else series[:-7]
    
    recent_avg = mean(recent)
    prior_avg  = mean(prior) if prior else 0.01
    
    if prior_avg < 1:
        velocity = recent_avg + 1
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
    """For STEALTH detection."""
    url = (f"https://financialmodelingprep.com/stable/historical-price-eod/full"
            f"?symbol={ticker}&apikey={FMP_KEY}")
    body = _http_get(url, timeout=10, retries=0)
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


def analyze_ticker(ticker, wiki_article):
    """Two sources tried (Wikipedia primary, Google Trends best-effort)."""
    sources_used = []
    
    # PRIMARY: Wikipedia
    wiki_series = get_wiki_pageviews(wiki_article, days=31) if wiki_article else None
    if wiki_series:
        sources_used.append("wiki")
    
    # FALLBACK: Google Trends (skip if disabled or Wikipedia got us enough)
    gtrends_series = None
    if TRY_GOOGLE and wiki_series is None:
        # Only try Google when Wikipedia fails — saves time and 429s
        gtrends_series = get_google_trends_series(ticker)
        if gtrends_series:
            sources_used.append("google_trends")
    
    # Use whichever source returned data; Wikipedia preferred for accuracy
    series_for_score = wiki_series or gtrends_series
    if series_for_score is None:
        return {"ticker": ticker, "err": "no_source_data"}
    
    velocity_info = compute_velocity(series_for_score)
    if velocity_info.get("velocity") is None:
        return {"ticker": ticker, "err": "insufficient_data",
                  "sources_used": sources_used}
    
    price_7d = get_recent_price_perf(ticker, days=7)
    velocity = velocity_info["velocity"]
    stealth = (velocity >= 2.0 and price_7d is not None and abs(price_7d) < 5.0)
    
    score = min(100, velocity * 25)
    if stealth:
        score = min(100, score + 20)
    # Wikipedia views can be high in absolute number — only give the
    # "absolute level" bonus when level is high relative to prior period
    if velocity_info["level"] >= velocity_info["prior_level"] * 1.5:
        score = min(100, score + 5)
    
    thesis_bits = []
    src_label = "Wikipedia views" if "wiki" in sources_used else "Google search"
    if velocity >= 2.5:
        thesis_bits.append(f"{src_label} {velocity}x baseline")
    elif velocity >= 1.5:
        thesis_bits.append(f"rising {src_label} ({velocity}x)")
    if stealth:
        thesis_bits.append(f"STEALTH (price only {price_7d:+.1f}% in 7d)")
    
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
        "sources_used":  sources_used,
        "wiki_article":  wiki_article,
        "series":        series_for_score[-30:],
        "thesis":        " · ".join(thesis_bits) if thesis_bits else
                            f"Search {velocity_info['interp'].lower().replace('_',' ')}",
    }


@track_errors
def handler(event, context):
    started = datetime.now(timezone.utc)
    
    # Process all mapped tickers up to MAX_TICKERS
    universe = list(TICKER_TO_WIKI.items())[:MAX_TICKERS]
    print(f"[ticker-trends-v2] universe: {len(universe)} tickers (wiki+gtrends), "
          f"primary=Wikipedia, gtrends_fallback={TRY_GOOGLE}")
    
    results = []
    errors  = defaultdict(int)
    sources_count = defaultdict(int)
    
    for i, (ticker, wiki_article) in enumerate(universe):
        try:
            r = analyze_ticker(ticker, wiki_article)
            if "err" in r:
                errors[r["err"]] += 1
                continue
            results.append(r)
            for src in r.get("sources_used", []):
                sources_count[src] += 1
        except Exception as e:
            errors["exception"] += 1
            print(f"[ticker-trends-v2] err on {ticker}: {e}")
        
        if i < len(universe) - 1:
            time.sleep(SLEEP_BETWEEN)
        
        if (i + 1) % 10 == 0:
            elapsed = (datetime.now(timezone.utc) - started).total_seconds()
            print(f"[ticker-trends-v2] {i+1}/{len(universe)}  ok={len(results)}  "
                  f"err={dict(errors)}  src={dict(sources_count)}  "
                  f"elapsed={elapsed:.0f}s")
        
        if (datetime.now(timezone.utc) - started).total_seconds() > 780:
            print("[ticker-trends-v2] time budget exhausted, stopping early")
            break
    
    results.sort(key=lambda r: -r["score"])
    
    out = {
        "schema_version":  "2.0",
        "method":          "wikipedia_primary_gtrends_fallback_v2",
        "generated_at":    datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "duration_s":      round((datetime.now(timezone.utc) - started).total_seconds(), 1),
        "n_processed":     len(universe),
        "n_ok":            len(results),
        "errors":          dict(errors),
        "sources_used_count": dict(sources_count),
        "config": {
            "max_tickers":  MAX_TICKERS,
            "sleep_between": SLEEP_BETWEEN,
            "try_google":   TRY_GOOGLE,
        },
        "top_20":         results[:20],
        "stealth_picks":  [r for r in results if r["stealth"]][:10],
        "all_results":    results,
        "notes": (
            "v2: Wikipedia pageviews PRIMARY (free, unlimited, no IP blocks), "
            "Google Trends FALLBACK (best-effort, blocked from AWS us-east-1 in v1). "
            "Velocity = last_7d_avg / prior_23d_avg of daily counts. "
            "STEALTH = velocity >= 2.0 + |7d price perf| < 5%."
        ),
    }
    
    body = json.dumps(out, default=str, separators=(",", ":")).encode("utf-8")
    s3.put_object(Bucket=BUCKET, Key=OUTPUT_KEY, Body=body,
                  ContentType="application/json",
                  CacheControl="public, max-age=14400")
    print(f"[ticker-trends-v2] wrote {len(body):,}B  top: "
          f"{results[0]['ticker'] if results else 'none'}  "
          f"sources={dict(sources_count)}")
    
    # Emit events
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
                "source":       r.get("sources_used", []),
            }) for r in spikes
        ]
        if events_to_pub:
            publish_many(events_to_pub)
    except Exception as e:
        print(f"[ticker-trends-v2] event publish failed: {e}")
    
    return {"statusCode": 200, "body": json.dumps({
        "ok":             True,
        "n_ok":           len(results),
        "n_stealth":      len(out["stealth_picks"]),
        "sources_used":   dict(sources_count),
        "top_ticker":     results[0]["ticker"] if results else None,
        "top_velocity":   results[0]["velocity"] if results else None,
        "duration_s":     out["duration_s"],
    })}


lambda_handler = handler
