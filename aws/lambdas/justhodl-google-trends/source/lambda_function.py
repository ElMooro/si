"""
justhodl-google-trends — Real Google Trends Lambda (BUILD 10/15)

WHY THIS REPLACES google-trends-agent
=====================================
The legacy google-trends-agent Lambda used random.randint() to fabricate
per-term search interest scores. Khalid's rule is NO FAKE DATA — every
number we surface must come from a real source. This module uses the
unofficial Google Trends API directly.

DATA SOURCES (all unofficial, no auth)
======================================
1. trends.google.com/trends/api/dailytrends
   → Returns real top-20 daily trending searches in US (titles + traffic).
2. trends.google.com/trends/explore?q=TERM&geo=US
   → Bootstrap widget metadata (token + req payload for step 3).
3. trends.google.com/trends/api/widgetdata/multiline?req=...&token=...
   → Real per-term interest_over_time series (0-100 normalized).

We use approach #1 (works without bootstrap) for the daily trending list,
and a curated set of financial-anchor terms (macro fear, recession, etc.)
via approach #2+#3 for the indices.

INDICES COMPUTED FROM REAL VALUES
==================================
crypto_fear_index = mean of last-7d interest for:
  "bitcoin crash", "crypto crash", "cryptocurrency scam"
employment_stress_index = mean for:
  "layoffs", "unemployment", "looking for a job"
recession_fear_index = mean for:
  "recession", "financial crisis", "stock market crash"
melt_up_index = mean for:
  "all time high", "buy stocks", "trending stocks"

Each index has:
  current_value (raw 0-100 from Google)
  prior_value (7 days ago)
  delta_pp (the alpha — change in retail attention)
  regime (TRANQUIL / WATCHING / ELEVATED / SPIKE)

REGIME
======
composite_market_attention based on which indices are spiking:
  CRISIS_ATTENTION    recession+crypto_fear both spiking
  STRESS_BUILDING      one index +30pp from baseline
  COMPLACENCY          all indices below median
  NORMAL               otherwise
  MELT_UP_ATTENTION    melt_up_index spiking with low fear

OUTPUT
======
data/google-trends.json — current values + deltas + indices + trending list

SCHEDULE
========
cron(0 */4 * * ? *) — every 4 hours (Google rate-limits aggressive fetches)
"""
import io, json, os, time, urllib.request, urllib.error, urllib.parse
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3

VERSION = "1.0.0"

S3_BUCKET = "justhodl-dashboard-live"
OUTPUT_KEY = "data/google-trends.json"

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# Curated index terms (real query strings sent to Google)
INDEX_TERMS = {
    "crypto_fear": ["bitcoin crash", "crypto crash", "cryptocurrency scam"],
    "employment_stress": ["layoffs", "unemployment", "looking for a job"],
    "recession_fear": ["recession", "financial crisis", "stock market crash"],
    "melt_up_attention": ["buy stocks", "all time high", "trending stocks"],
    "fed_attention": ["fed rate hike", "fomc meeting", "powell speech"],
    "ai_hype": ["ai stocks", "nvidia stock", "openai"],
}

HTTP_TIMEOUT = 15
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
}

s3 = boto3.client("s3", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")


# ═══════════════════════════════════════════════════════════════════════════
# GOOGLE TRENDS ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════════

def _strip_jsonp(text):
    """Google prefixes responses with `)]}'` to prevent JSON hijacking."""
    if text.startswith(")]}'") or text.startswith(")]}',"):
        return text[5:].lstrip(",").strip()
    if text.startswith(")]}'"):
        return text[4:].strip()
    # Find first { or [
    for i, ch in enumerate(text):
        if ch in "[{":
            return text[i:]
    return text


def fetch_daily_trends(geo="US"):
    """Returns top trending searches today as list of {title, traffic, related}."""
    url = (f"https://trends.google.com/trends/api/dailytrends"
            f"?hl=en-US&tz=-300&geo={geo}&ns=15")
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
            raw = r.read().decode("utf-8", "replace")
        data = json.loads(_strip_jsonp(raw))
        out = []
        days = (data.get("default") or {}).get("trendingSearchesDays") or []
        if not days: return []
        first = days[0]
        for s in first.get("trendingSearches", [])[:25]:
            title = (s.get("title") or {}).get("query", "")
            traffic = s.get("formattedTraffic", "")
            related = [a.get("title") for a in (s.get("articles") or [])[:3]]
            related_titles = [r for r in related if r]
            shareUrl = s.get("shareUrl", "")
            out.append({
                "title": title,
                "traffic_estimate": traffic,
                "related_articles": related_titles,
                "share_url": shareUrl,
            })
        return out
    except Exception as e:
        print(f"  daily trends err: {str(e)[:120]}")
        return []


def fetch_explore_widget(term, geo="US", tf="now 7-d"):
    """Step 1 of per-term interest fetch: get widget bootstrap (token + req)."""
    payload = {
        "hl": "en-US", "tz": -300,
        "req": json.dumps({
            "comparisonItem": [{"keyword": term, "geo": geo, "time": tf}],
            "category": 0, "property": "",
        }),
    }
    url = "https://trends.google.com/trends/api/explore?" + urllib.parse.urlencode(payload)
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
            raw = r.read().decode("utf-8", "replace")
        data = json.loads(_strip_jsonp(raw))
        widgets = data.get("widgets", [])
        for w in widgets:
            if w.get("id") == "TIMESERIES":
                return {"token": w.get("token"), "req": w.get("request")}
        return None
    except urllib.error.HTTPError as e:
        if e.code == 429:
            print(f"  {term} rate-limited (429)")
        return None
    except Exception as e:
        print(f"  {term} explore err: {str(e)[:80]}")
        return None


def fetch_interest_over_time(term, geo="US", tf="now 7-d"):
    """Returns list of {time, value} for the term over last 7 days.
    Returns None on failure."""
    w = fetch_explore_widget(term, geo, tf)
    if not w or not w.get("token") or not w.get("req"):
        return None
    payload = {
        "hl": "en-US", "tz": -300,
        "req": json.dumps(w["req"]),
        "token": w["token"],
    }
    url = "https://trends.google.com/trends/api/widgetdata/multiline?" + urllib.parse.urlencode(payload)
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
            raw = r.read().decode("utf-8", "replace")
        data = json.loads(_strip_jsonp(raw))
        timeline = (data.get("default") or {}).get("timelineData") or []
        out = []
        for t in timeline:
            val = (t.get("value") or [None])[0]
            if val is None: continue
            out.append({"time": t.get("formattedTime"), "value": int(val)})
        return out
    except urllib.error.HTTPError as e:
        if e.code == 429:
            print(f"  {term} timeline rate-limited")
        return None
    except Exception as e:
        print(f"  {term} timeline err: {str(e)[:80]}")
        return None


# ═══════════════════════════════════════════════════════════════════════════
# INDEX COMPUTATION
# ═══════════════════════════════════════════════════════════════════════════

def fetch_index_data(index_name, terms):
    """For each term in the index, fetch 7-day timeline. Average the latest
    value (current) and 7-day-ago value (prior) across terms."""
    term_data = {}
    for term in terms:
        timeline = fetch_interest_over_time(term, geo="US", tf="now 7-d")
        if timeline:
            term_data[term] = timeline
        # Rate-limit: small delay between terms
        time.sleep(0.5)
    if not term_data:
        return {"err": "all terms failed", "n_terms_loaded": 0}

    # Compute means: latest hour value, 24h-ago value
    current_means = []
    prior_means = []
    for term, tl in term_data.items():
        if not tl: continue
        # Google returns ~168 hourly points for 7d
        latest = tl[-1]["value"]
        # 7 days ago = first point
        prior = tl[0]["value"] if len(tl) > 0 else latest
        current_means.append(latest)
        prior_means.append(prior)

    if not current_means:
        return {"err": "no valid timelines", "n_terms_loaded": 0}

    cur = sum(current_means) / len(current_means)
    prior = sum(prior_means) / len(prior_means)
    delta = cur - prior

    # Regime
    if cur >= 75 or delta >= 30:
        regime = "SPIKE"
    elif cur >= 55 or delta >= 15:
        regime = "ELEVATED"
    elif cur >= 30:
        regime = "WATCHING"
    else:
        regime = "TRANQUIL"

    return {
        "current": round(cur, 1),
        "prior_7d": round(prior, 1),
        "delta_pp": round(delta, 1),
        "regime": regime,
        "n_terms_loaded": len(current_means),
        "per_term_current": {t: tl[-1]["value"] for t, tl in term_data.items() if tl},
    }


def composite_regime(indices):
    """Build a market-attention composite from individual indices."""
    crypto = (indices.get("crypto_fear") or {}).get("regime", "TRANQUIL")
    rec = (indices.get("recession_fear") or {}).get("regime", "TRANQUIL")
    melt = (indices.get("melt_up_attention") or {}).get("regime", "TRANQUIL")
    emp = (indices.get("employment_stress") or {}).get("regime", "TRANQUIL")

    n_spike = sum(1 for r in (crypto, rec, melt, emp) if r == "SPIKE")
    n_elev = sum(1 for r in (crypto, rec, melt, emp) if r == "ELEVATED")

    if crypto in ("SPIKE", "ELEVATED") and rec in ("SPIKE", "ELEVATED"):
        return "CRISIS_ATTENTION", "Multiple fear indices spiking — retail fear engaged"
    if melt == "SPIKE" and crypto in ("TRANQUIL", "WATCHING"):
        return "MELT_UP_ATTENTION", "Melt-up curiosity dominant; fear indices muted — euphoria risk"
    if n_spike >= 1 or n_elev >= 2:
        return "STRESS_BUILDING", "One+ fear indices elevated; widening retail concern"
    if all(r == "TRANQUIL" for r in (crypto, rec, melt, emp)):
        return "COMPLACENCY", "All retail attention indices tranquil — complacency or pre-event quiet"
    return "NORMAL", "Mixed retail attention; no clear signal"


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
    print(f"=== google-trends v{VERSION} · {datetime.now(timezone.utc).isoformat()} ===")

    # Load prior state for regime change detection
    try:
        prior_payload = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY)["Body"].read())
        prior_regime = prior_payload.get("composite_regime")
    except Exception:
        prior_regime = None

    # ─── Fetch daily trending searches ───
    print("  fetching daily trending searches...")
    daily = fetch_daily_trends("US")
    print(f"  ✓ daily trending: {len(daily)} items")

    # ─── Fetch per-index interest data (sequential to avoid rate limiting) ───
    indices = {}
    for name, terms in INDEX_TERMS.items():
        print(f"  fetching index: {name} ({len(terms)} terms)")
        idx = fetch_index_data(name, terms)
        indices[name] = idx
        if not idx.get("err"):
            print(f"    {name}: cur={idx.get('current')} prior={idx.get('prior_7d')} Δ={idx.get('delta_pp'):+}pp {idx.get('regime')}")

    # Composite regime
    comp_regime, comp_signal = composite_regime(indices)

    # Build market_fear_index from real values (weighted average of fear-related indices)
    crypto_v = (indices.get("crypto_fear") or {}).get("current") or 0
    rec_v = (indices.get("recession_fear") or {}).get("current") or 0
    emp_v = (indices.get("employment_stress") or {}).get("current") or 0
    melt_v = (indices.get("melt_up_attention") or {}).get("current") or 0
    market_fear_index = round(crypto_v * 0.3 + rec_v * 0.4 + emp_v * 0.3, 1)
    # Bull/bear pulse: melt_up minus mean fear
    bull_bear_pulse = round(melt_v - (crypto_v + rec_v) / 2, 1)

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "generated_at_unix": int(time.time()),
        "version": VERSION,
        "source": "trends.google.com unofficial API · REAL data (no random/fabricated values)",
        "elapsed_seconds": round(time.time() - started, 1),
        "daily_trending_us": daily,
        "n_daily_trending": len(daily),
        "indices": indices,
        "market_fear_index": market_fear_index,
        "bull_bear_pulse": bull_bear_pulse,
        "composite_regime": comp_regime,
        "composite_signal": comp_signal,
        "regime_changed_from_prior": (prior_regime != comp_regime) if prior_regime else False,
        "thresholds": {
            "spike_value": 75, "spike_delta": 30,
            "elevated_value": 55, "elevated_delta": 15,
            "watching_value": 30,
        },
        "n_indices_loaded": sum(1 for idx in indices.values() if not idx.get("err")),
    }

    s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY,
        Body=json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8"),
        ContentType="application/json", CacheControl="public, max-age=1800")
    print(f"  ✓ data/google-trends.json written")

    # Telegram alert on regime change OR extreme readings
    alert_sent = False
    if prior_regime and prior_regime != comp_regime:
        lines = [f"🔎 *Google Trends · {datetime.now(timezone.utc).strftime('%b %d')}*\n",
                  f"⚡ {comp_regime}",
                  f"_(was {prior_regime})_",
                  f"_{comp_signal}_\n",
                  f"📊 Crypto Fear: {crypto_v}  · Recession: {rec_v}",
                  f"📊 Employment Stress: {emp_v}  · Melt-Up: {melt_v}",
                  f"📊 Market Fear Index: {market_fear_index}  · B/B Pulse: {bull_bear_pulse:+}"]
        if daily:
            lines.append(f"\n🔥 Top trending: {', '.join(d['title'][:25] for d in daily[:5])}")
        alert_sent = send_telegram("\n".join(lines))
        if alert_sent: print("  ✓ telegram sent")

    return {"statusCode": 200, "body": json.dumps({
        "success": True,
        "version": VERSION,
        "n_daily_trending": len(daily),
        "n_indices_loaded": payload["n_indices_loaded"],
        "composite_regime": comp_regime,
        "market_fear_index": market_fear_index,
        "bull_bear_pulse": bull_bear_pulse,
        "regime_changed": prior_regime != comp_regime if prior_regime else False,
        "alert_sent": alert_sent,
        "elapsed_seconds": payload["elapsed_seconds"],
    })}
