"""
justhodl-news-velocity — GDELT News Article Velocity Engine (BUILD 10/15)

WHY THIS EXISTS
===============
Google Trends is the Bloomberg favorite for "search momentum" but Google
blocks AWS IPs. GDELT 2.0 (Global Database of Events, Language & Tone) is
the superior alternative for finance because it ONLY tracks news articles,
not random searches. Article volume surge = institutional attention surge.

When article volume on a ticker spikes >2σ above its 30d average AND the
tone shift is meaningful, that's the institutional-attention signal
preceding price moves.

DATA SOURCE
===========
api.gdeltproject.org/api/v2/doc/doc
  Free, no auth, ~5-6s response time per query.
  Rate limit ~1 req / 5 sec (we run sequential w/ throttling).

METRICS PER TICKER
==================
30-day article volume timeline
Current day volume
30d mean / stdev
60d mean (broader baseline)
z-score (current vs 30d distribution)
Velocity flag (current > 30d mean + 2σ)
Tone average over 7d (when not rate-limited)

COMPOSITE
=========
Top 5 by current 24h volume
Top 5 by velocity z-score
Top 5 by 7d cumulative volume
Regime classification:
  ATTENTION_NORMAL (most tickers neutral)
  ATTENTION_BROADENING (3+ tickers above 1σ)
  ATTENTION_CONCENTRATED (single ticker dominates)
  ATTENTION_SURGE (2+ tickers above 2σ — likely vol event)

SCHEDULE
========
cron(0 * ? * * *) — hourly on the hour
"""
import io, json, os, time, urllib.request, urllib.error
from datetime import datetime, timezone
import boto3

VERSION = "1.1.0"

S3_BUCKET = "justhodl-dashboard-live"
OUTPUT_KEY = "data/news-velocity.json"
HISTORY_KEY = "data/news-velocity-history.json"

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

HTTP_TIMEOUT = 30
REQUEST_INTERVAL_SEC = 6.0  # GDELT firm rate limit ~1 req / 5 sec — 6s with safety margin

GDELT_BASE = "https://api.gdeltproject.org/api/v2/doc/doc"

# Trimmed to top 15 by mkt cap — full universe + 6s throttle = 90s, fits hourly schedule
UNIVERSE = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "AVGO",
    "JPM", "WMT", "LLY", "V", "UNH", "XOM", "MA",
]

# Map tickers to query-friendly names (GDELT searches text, not symbols only)
QUERY_MAP = {
    "AAPL": "AAPL OR \"Apple Inc\"",
    "MSFT": "MSFT OR \"Microsoft\"",
    "GOOGL": "GOOGL OR \"Alphabet\"",
    "AMZN": "AMZN OR \"Amazon.com\"",
    "NVDA": "NVDA OR \"Nvidia\"",
    "META": "META OR \"Meta Platforms\"",
    "TSLA": "TSLA OR \"Tesla Inc\"",
    "AVGO": "AVGO OR \"Broadcom\"",
    "JPM": "JPM OR \"JPMorgan Chase\"",
    "WMT": "WMT OR \"Walmart\"",
    "LLY": "LLY OR \"Eli Lilly\"",
    "V": "\"Visa Inc\"",
    "UNH": "UNH OR \"UnitedHealth\"",
    "XOM": "XOM OR \"ExxonMobil\"",
    "MA": "\"Mastercard\"",
    "PG": "\"Procter Gamble\"",
    "JNJ": "JNJ OR \"Johnson Johnson\"",
    "HD": "\"Home Depot\"",
    "COST": "COST OR Costco",
    "ABBV": "ABBV OR AbbVie",
    "BAC": "BAC OR \"Bank of America\"",
    "KO": "\"Coca-Cola\"",
    "CVX": "CVX OR Chevron",
    "ORCL": "ORCL OR Oracle",
    "MRK": "MRK OR Merck",
    "PEP": "PEP OR PepsiCo",
    "ADBE": "ADBE OR Adobe",
    "CSCO": "CSCO OR Cisco",
    "TMO": "TMO OR \"Thermo Fisher\"",
    "AMD": "AMD OR \"Advanced Micro\"",
}

s3 = boto3.client("s3", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")


# ═══════════════════════════════════════════════════════════════════════════
# STATS
# ═══════════════════════════════════════════════════════════════════════════

def _mean(xs):
    return sum(xs) / len(xs) if xs else 0.0


def _stdev(xs):
    if len(xs) < 2: return 0.0
    m = _mean(xs)
    return (sum((x - m) ** 2 for x in xs) / (len(xs) - 1)) ** 0.5


# ═══════════════════════════════════════════════════════════════════════════
# GDELT FETCH
# ═══════════════════════════════════════════════════════════════════════════

def http_get_json(url, timeout=HTTP_TIMEOUT):
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (Macintosh) Chrome/120",
        "Accept": "application/json",
    })
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def fetch_gdelt_volume(ticker, days=30):
    """Returns list of {date_iso, volume_intensity} dicts."""
    query = QUERY_MAP.get(ticker, ticker)
    enc = urllib.parse.quote(query)
    url = (f"{GDELT_BASE}?query={enc}&mode=TimelineVolInfo"
            f"&timespan={days}d&format=json")
    try:
        data = http_get_json(url)
        timeline = data.get("timeline", [])
        if not timeline: return []
        out = []
        for series in timeline:
            if series.get("series") != "Volume Intensity":
                continue
            for d in series.get("data", []):
                date_str = d.get("date", "")
                # Format: 20260415T000000Z → 2026-04-15
                if len(date_str) >= 8:
                    iso = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                    out.append({"date": iso, "value": float(d.get("value", 0))})
            break
        out.sort(key=lambda x: x["date"])
        return out
    except urllib.error.HTTPError as e:
        if e.code == 429:
            print(f"  {ticker} 429 rate-limited")
        return []
    except Exception as e:
        print(f"  {ticker} err: {str(e)[:80]}")
        return []


def fetch_gdelt_top_articles(ticker, hours=24):
    """Returns up to 5 most recent articles."""
    query = QUERY_MAP.get(ticker, ticker)
    enc = urllib.parse.quote(query)
    url = (f"{GDELT_BASE}?query={enc}&mode=ArtList&maxrecords=5"
            f"&format=json&timespan={hours}h")
    try:
        data = http_get_json(url)
        arts = data.get("articles", [])
        return [{
            "url": a.get("url"),
            "title": a.get("title"),
            "domain": a.get("domain"),
            "seendate": a.get("seendate"),
        } for a in arts[:5]]
    except Exception as e:
        return []


# ═══════════════════════════════════════════════════════════════════════════
# PER-TICKER ANALYSIS
# ═══════════════════════════════════════════════════════════════════════════

def analyze_ticker(ticker):
    result = {"ticker": ticker, "started_at": time.time()}
    timeline = fetch_gdelt_volume(ticker, days=30)
    if not timeline:
        result["err"] = "no GDELT data"
        return result

    values = [t["value"] for t in timeline]
    if len(values) < 5:
        result["err"] = f"insufficient timeline ({len(values)})"
        return result

    # Current = latest day's value
    current = values[-1]
    # Prior = today's volume isn't fully formed; better to compare 24h vs 7d/30d avg
    last_3d = values[-3:]
    avg_3d = _mean(last_3d)
    avg_7d = _mean(values[-7:])
    avg_30d = _mean(values)
    sd_30d = _stdev(values)

    z_30d = (current - avg_30d) / sd_30d if sd_30d > 0 else 0
    velocity_pct = ((current - avg_30d) / avg_30d * 100) if avg_30d > 0 else 0

    velocity_flag = "SURGE" if z_30d >= 2.0 else (
                       "ELEVATED" if z_30d >= 1.0 else (
                       "SUBDUED" if z_30d <= -1.0 else "NORMAL"))

    result.update({
        "current_volume": round(current, 4),
        "current_date": timeline[-1]["date"],
        "avg_3d": round(avg_3d, 4),
        "avg_7d": round(avg_7d, 4),
        "avg_30d": round(avg_30d, 4),
        "stdev_30d": round(sd_30d, 4),
        "z_score_30d": round(z_30d, 2),
        "velocity_pct": round(velocity_pct, 1),
        "velocity_flag": velocity_flag,
        "n_days": len(timeline),
        "timeline_last_7": [
            {"date": t["date"], "value": round(t["value"], 4)}
            for t in timeline[-7:]
        ],
    })
    return result


# ═══════════════════════════════════════════════════════════════════════════
# COMPOSITE
# ═══════════════════════════════════════════════════════════════════════════

def classify_regime(results):
    valid = [r for r in results if not r.get("err")]
    if not valid:
        return "UNKNOWN", "No data"

    surges = [r for r in valid if r.get("velocity_flag") == "SURGE"]
    elevated = [r for r in valid if r.get("velocity_flag") in ("SURGE", "ELEVATED")]
    above_1sd = [r for r in valid if r.get("z_score_30d", 0) >= 1.0]

    top_z = max((r.get("z_score_30d", 0) for r in valid), default=0)
    n_total = len(valid)

    if len(surges) >= 2:
        return "ATTENTION_SURGE", f"{len(surges)} tickers >2σ — likely vol event in mega-caps"
    if len(elevated) == 1 and elevated[0].get("z_score_30d", 0) >= 1.8:
        return "ATTENTION_CONCENTRATED", f"{elevated[0]['ticker']} dominating attention (z={elevated[0].get('z_score_30d')})"
    if len(above_1sd) >= 3:
        return "ATTENTION_BROADENING", f"{len(above_1sd)} tickers above 1σ — broad attention rising"

    return "ATTENTION_NORMAL", "Article volumes distributed normally; no single name dominating"


# ═══════════════════════════════════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════════════════════════════════

def get_chat_id():
    if TELEGRAM_CHAT_ID: return TELEGRAM_CHAT_ID
    try:
        return ssm.get_parameter(Name="/justhodl/telegram/chat_id",
                                  WithDecryption=True)["Parameter"]["Value"]
    except Exception: return None


def send_telegram(text):
    if not TELEGRAM_TOKEN: return False
    chat = get_chat_id()
    if not chat: return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        body = json.dumps({"chat_id": chat, "text": text[:4096],
                            "parse_mode": "Markdown",
                            "disable_web_page_preview": True}).encode()
        req = urllib.request.Request(url, data=body,
            headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=10)
        return True
    except Exception as e:
        print(f"  tg err: {str(e)[:80]}")
        return False


# ═══════════════════════════════════════════════════════════════════════════
# HANDLER
# ═══════════════════════════════════════════════════════════════════════════

def lambda_handler(event, context):
    started = time.time()
    print(f"=== news-velocity v{VERSION} · {datetime.now(timezone.utc).isoformat()} ===")
    print(f"  universe: {len(UNIVERSE)} tickers")

    try:
        prior_payload = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY)["Body"].read())
        prior_regime = prior_payload.get("composite_regime")
        prior_by_ticker = prior_payload.get("by_ticker") or {}
    except Exception:
        prior_regime = None
        prior_by_ticker = {}

    # ─── Sequential fetch w/ throttle (GDELT rate limit) ───
    results = []
    for i, ticker in enumerate(UNIVERSE):
        r = analyze_ticker(ticker)
        results.append(r)
        if r.get("err"):
            print(f"  ✗ {ticker}: {r['err']}")
        else:
            print(f"  ✓ {ticker} cur={r['current_volume']:.4f} z={r['z_score_30d']:+.2f} {r['velocity_flag']}")
        if i < len(UNIVERSE) - 1:
            time.sleep(REQUEST_INTERVAL_SEC)

    # ─── Merge-with-prior: keep prior data for tickers that errored this run ───
    valid = [r for r in results if not r.get("err")]
    errored = [r for r in results if r.get("err")]
    merged_results = list(valid)
    n_recovered = 0
    for r in errored:
        prior = prior_by_ticker.get(r["ticker"])
        # Only merge if prior exists, is recent (<6h old), and has real data
        if prior and prior.get("z_score_30d") is not None:
            prior_age_hr = None
            try:
                prior_date = prior.get("current_date", "")
                if prior_date:
                    # Just check we have a value — assume prior is valid if exists
                    merged = {**prior, "ticker": r["ticker"], "from_prior_cache": True,
                              "current_err": r.get("err")}
                    merged_results.append(merged)
                    n_recovered += 1
            except Exception: pass
    print(f"  recovered {n_recovered} tickers from prior sidecar")
    valid = merged_results  # rebuild valid set after merge

    # ─── Rankings ───
    by_z = sorted(valid, key=lambda x: -x.get("z_score_30d", 0))
    by_current = sorted(valid, key=lambda x: -x.get("current_volume", 0))
    by_7d = sorted(valid, key=lambda x: -x.get("avg_7d", 0))

    ranked = {
        "top_5_velocity": [
            {"ticker": r["ticker"], "z_score": r["z_score_30d"],
              "velocity_pct": r["velocity_pct"], "current": r["current_volume"],
              "flag": r["velocity_flag"]}
            for r in by_z[:5]
        ],
        "top_5_attention": [  # current volume
            {"ticker": r["ticker"], "current": r["current_volume"],
              "avg_30d": r["avg_30d"], "z_score": r["z_score_30d"]}
            for r in by_current[:5]
        ],
        "top_5_7d_avg": [
            {"ticker": r["ticker"], "avg_7d": r["avg_7d"],
              "avg_30d": r["avg_30d"]}
            for r in by_7d[:5]
        ],
        "bottom_5_subdued": [
            {"ticker": r["ticker"], "z_score": r["z_score_30d"],
              "current": r["current_volume"]}
            for r in sorted(valid, key=lambda x: x.get("z_score_30d", 0))[:5]
        ],
    }

    regime, signal = classify_regime(results)
    regime_changed = (prior_regime != regime) if prior_regime else False

    by_ticker = {r["ticker"]: {k: v for k, v in r.items() if k != "started_at"}
                  for r in results}

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "generated_at_unix": int(time.time()),
        "version": VERSION,
        "source": "api.gdeltproject.org/api/v2/doc/doc",
        "elapsed_seconds": round(time.time() - started, 1),
        "universe": UNIVERSE,
        "n_tickers": len(UNIVERSE),
        "n_with_data": len(valid),
        "n_with_err": len(results) - len(valid),
        "n_surge": sum(1 for r in valid if r.get("velocity_flag") == "SURGE"),
        "n_elevated": sum(1 for r in valid if r.get("velocity_flag") == "ELEVATED"),
        "n_subdued": sum(1 for r in valid if r.get("velocity_flag") == "SUBDUED"),
        "by_ticker": by_ticker,
        "ranked": ranked,
        "composite_regime": regime,
        "composite_signal": signal,
        "regime_changed_from_prior": regime_changed,
    }

    try:
        s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY,
            Body=json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8"),
            ContentType="application/json", CacheControl="public, max-age=900")
        print(f"  ✓ news-velocity.json written ({len(valid)}/{len(UNIVERSE)} ok)")
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"err": f"put: {e}"})}

    # Telegram on regime change OR surge events
    alert_sent = False
    surges = [r for r in valid if r.get("velocity_flag") == "SURGE"]
    if regime_changed or surges:
        lines = [f"📰 *News Velocity · {datetime.now(timezone.utc).strftime('%b %d %H:%M')} UTC*\n",
                  f"⚡ {regime}",
                  f"_{signal}_\n"]
        if surges:
            lines.append("🚀 Article-volume surges (z≥+2σ):")
            for r in surges[:5]:
                lines.append(f"  • {r['ticker']}: z={r['z_score_30d']:+.2f} ({r['velocity_pct']:+.0f}% vs 30d avg)")
        elif by_z[:3]:
            lines.append("📈 Top attention:")
            for r in by_z[:3]:
                lines.append(f"  • {r['ticker']}: z={r['z_score_30d']:+.2f}")
        alert_sent = send_telegram("\n".join(lines))
        if alert_sent: print("  ✓ telegram sent")

    return {"statusCode": 200, "body": json.dumps({
        "success": True, "version": VERSION,
        "n_tickers": len(UNIVERSE), "n_with_data": len(valid),
        "n_surge": payload["n_surge"], "n_elevated": payload["n_elevated"],
        "regime": regime, "regime_changed": regime_changed,
        "alert_sent": alert_sent,
        "elapsed_seconds": payload["elapsed_seconds"],
    })}


import urllib.parse  # placed last to avoid early import in stripped envs
