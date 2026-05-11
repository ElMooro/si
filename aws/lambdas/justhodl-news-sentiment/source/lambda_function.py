"""
justhodl-news-sentiment v2 — FMP edition (was: broken NewsAPI edition)

═══════════════════════════════════════════════════════════════════════
WHY THIS WAS REWRITTEN
─────────────────────
v1 (NewsAPI) failed on every invocation. NewsAPI.org's free tier has tight
rate limits; with 26 parallel calls in <1 minute, every call returned
"Max retries exceeded". Result: 0/503 stocks got news, 503/503 scored
neutral, $0.04/day wasted on Claude calls with empty input.

v2 uses FMP's /stable/news/stock?symbols=X endpoint — same API the
screener already uses successfully, paid tier with no rate-limit issues.

═══════════════════════════════════════════════════════════════════════
PIPELINE
────────
1. Read screener/data.json from S3 to get the S&P 500 universe AND
   pre-computed newsCount7d (so we only score stocks with real news flow).
2. Filter to stocks with newsCount7d >= 1 → typically 200-300 stocks.
3. For each, fetch FMP news/stock with limit=8 (parallel, 12 workers).
4. Batch 8 stocks per Claude haiku call with all their headlines inline.
5. Claude returns array of {symbol, score, signal, reason}.
6. Write sidecar at sentiment/data.json — same schema v1 used so the
   screener page (which already reads this key) continues to work.

EXPECTED METRICS
  - News fetch:       ~25s parallelized (200-300 calls × 12 workers)
  - Claude scoring:   ~50s for 30-40 batches with 4 parallel workers
  - Total runtime:    75-90 seconds
  - Cost per run:     ~$0.05 (haiku is cheap)
  - Output size:      80-120 KB JSON
"""
import json
import os
import time
import urllib.request
import urllib.error
import urllib.parse
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3

FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
FMP_BASE = "https://financialmodelingprep.com/stable"
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_KEY", "")
S3_BUCKET = "justhodl-dashboard-live"
SCREENER_KEY = "screener/data.json"
SENTIMENT_KEY = "sentiment/data.json"

CLAUDE_MODEL = "claude-haiku-4-5-20251001"
CLAUDE_BATCH = 8           # stocks per Claude call
HEADLINES_PER_STOCK = 8     # max headlines to include per stock
NEWS_FETCH_WORKERS = 12     # parallel FMP news calls
CLAUDE_WORKERS = 4           # parallel Claude calls
MIN_NEWS_COUNT_7D = 1        # only score stocks with >=1 article last 7 days
CACHE_TTL_HOURS = 5          # avoid double-runs

s3 = boto3.client("s3", region_name="us-east-1")


def fmp_news(symbol):
    """Fetch up to HEADLINES_PER_STOCK headlines for one symbol via FMP."""
    url = f"{FMP_BASE}/news/stock?symbols={symbol}&limit={HEADLINES_PER_STOCK}&apikey={FMP_KEY}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-NS/2.0"})
        with urllib.request.urlopen(req, timeout=12) as r:
            data = json.loads(r.read().decode("utf-8"))
        if not isinstance(data, list):
            return symbol, []
        headlines = []
        for a in data[:HEADLINES_PER_STOCK]:
            t = (a.get("title") or "").strip()
            d = (a.get("publishedDate") or "")[:10]
            if t and len(t) > 8:
                headlines.append(f"[{d}] {t}")
        return symbol, headlines
    except Exception as e:
        print(f"  fmp_news {symbol} err: {str(e)[:80]}")
        return symbol, []


def claude_score_batch(batch):
    """Send a batch of {symbol: [headlines]} to Claude haiku.
    Returns list of {symbol, score, signal, reason}."""
    if not batch:
        return []
    lines = []
    for sym, headlines in batch.items():
        if not headlines: continue
        hlines = " || ".join(h[:160] for h in headlines[:HEADLINES_PER_STOCK])
        lines.append(f"\n{sym}:\n  {hlines}")
    if not lines:
        return []

    prompt = f"""You are an equity research analyst scoring market sentiment from recent news.

For each stock below, return ONE JSON object with: symbol, score (-1.0 to +1.0),
signal (bullish/bearish/neutral), reason (one sentence max).

Score guidance:
  +0.7 to +1.0: clearly bullish (earnings beat, upgrades, major contract wins)
  +0.3 to +0.7: positive (modest beats, analyst optimism, product launches)
  -0.3 to +0.3: neutral or mixed
  -0.3 to -0.7: negative (downgrades, missed earnings, regulatory issues)
  -0.7 to -1.0: clearly bearish (fraud, lawsuits, CEO departures, plunges)

Be DECISIVE. Many news items are mildly negative or positive — pick a side.
Truly neutral only if news is purely informational (e.g. dividend announcements).

STOCKS:
{chr(10).join(lines)}

Return ONLY a JSON array. No markdown, no explanation. Exact format:
[{{"symbol":"AAPL","score":0.45,"signal":"bullish","reason":"Strong iPhone 17 demand and services beat"}}]"""

    body = json.dumps({
        "model": CLAUDE_MODEL,
        "max_tokens": 1600,
        "messages": [{"role": "user", "content": prompt}],
    }).encode("utf-8")

    try:
        req = urllib.request.Request(
            "https://api.anthropic.com/v1/messages",
            data=body,
            headers={
                "Content-Type": "application/json",
                "x-api-key": ANTHROPIC_KEY,
                "anthropic-version": "2023-06-01",
            },
            method="POST")
        with urllib.request.urlopen(req, timeout=45) as r:
            resp = json.loads(r.read().decode("utf-8"))
        text = resp["content"][0]["text"].strip()
        if "```" in text:
            parts = text.split("```")
            text = parts[1] if len(parts) > 1 else parts[0]
            if text.startswith("json"):
                text = text[4:].strip()
        return json.loads(text.strip())
    except Exception as e:
        print(f"  claude err: {str(e)[:200]}")
        return []


def lambda_handler(event, context):
    headers = {"Content-Type": "application/json",
               "Access-Control-Allow-Origin": "*"}
    started = time.time()

    force = isinstance(event, dict) and event.get("force") is True
    if not force:
        try:
            head = s3.head_object(Bucket=S3_BUCKET, Key=SENTIMENT_KEY)
            age_h = (time.time() - head["LastModified"].timestamp()) / 3600
            if age_h < CACHE_TTL_HOURS:
                return {"statusCode": 200, "headers": headers,
                        "body": json.dumps({"from_cache": True,
                                              "age_hours": round(age_h, 2)})}
        except Exception:
            pass

    print(f"=== NEWS SENTIMENT v2 START · {datetime.now(timezone.utc).isoformat()} ===")

    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=SCREENER_KEY)
        screener = json.loads(obj["Body"].read())
        stocks = screener.get("stocks") or []
    except Exception as e:
        return {"statusCode": 500, "headers": headers,
                "body": json.dumps({"error": f"screener read: {e}"})}

    candidates = [s for s in stocks
                  if (s.get("newsCount7d") or 0) >= MIN_NEWS_COUNT_7D]
    print(f"  total: {len(stocks)} | with news 7d: {len(candidates)}")

    fetch_started = time.time()
    headlines_by_sym = {}
    with ThreadPoolExecutor(max_workers=NEWS_FETCH_WORKERS) as ex:
        futures = [ex.submit(fmp_news, s["symbol"]) for s in candidates]
        for f in as_completed(futures):
            sym, hl = f.result()
            if hl: headlines_by_sym[sym] = hl
    print(f"  fetched headlines for {len(headlines_by_sym)} in {time.time()-fetch_started:.1f}s")

    symbols = list(headlines_by_sym.keys())
    batches = [{s: headlines_by_sym[s] for s in symbols[i:i+CLAUDE_BATCH]}
               for i in range(0, len(symbols), CLAUDE_BATCH)]
    print(f"  {len(batches)} Claude batches × {CLAUDE_BATCH}")

    score_started = time.time()
    all_scores = {}
    with ThreadPoolExecutor(max_workers=CLAUDE_WORKERS) as ex:
        futures = [ex.submit(claude_score_batch, b) for b in batches]
        for f in as_completed(futures):
            res = f.result()
            for item in (res or []):
                sym = item.get("symbol")
                if sym:
                    all_scores[sym] = {
                        "sentimentScore": round(float(item.get("score", 0)), 3),
                        "sentimentSignal": item.get("signal", "neutral"),
                        "sentimentReason": (item.get("reason") or "")[:200],
                    }
    print(f"  scored {len(all_scores)} in {time.time()-score_started:.1f}s")

    sentiment_list = []
    sym_to_name = {s["symbol"]: s.get("name", s["symbol"]) for s in stocks}
    for s in stocks:
        sym = s["symbol"]
        scored = all_scores.get(sym)
        sentiment_list.append({
            "symbol": sym, "name": sym_to_name.get(sym, sym),
            "sentimentScore": scored["sentimentScore"] if scored else 0.0,
            "sentimentSignal": scored["sentimentSignal"] if scored else "neutral",
            "sentimentReason": scored["sentimentReason"] if scored else "",
            "headlines": headlines_by_sym.get(sym, []),
            "hasNews": bool(headlines_by_sym.get(sym)),
        })

    bullish = sum(1 for s in sentiment_list if s["sentimentSignal"] == "bullish")
    bearish = sum(1 for s in sentiment_list if s["sentimentSignal"] == "bearish")
    neutral = sum(1 for s in sentiment_list if s["sentimentSignal"] == "neutral")

    elapsed = time.time() - started
    print(f"=== DONE · B:{bullish} Bear:{bearish} N:{neutral} · {elapsed:.1f}s ===")

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "generated_at_unix": int(time.time()),
        "elapsed_seconds": round(elapsed, 1),
        "count": len(sentiment_list),
        "stocks_with_news": len(headlines_by_sym),
        "stocks_scored": len(all_scores),
        "bullish_count": bullish,
        "bearish_count": bearish,
        "neutral_count": neutral,
        "model": CLAUDE_MODEL,
        "source": "fmp",
        "sentiment": sentiment_list,
    }

    try:
        s3.put_object(
            Bucket=S3_BUCKET, Key=SENTIMENT_KEY,
            Body=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
            ContentType="application/json",
            CacheControl="public, max-age=3600")
        print(f"  wrote {round(len(json.dumps(payload))/1024,1)} KB")
    except Exception as e:
        print(f"  s3 put err: {e}")

    return {"statusCode": 200, "headers": headers,
            "body": json.dumps({"success": True,
                                  "stocks_with_news": len(headlines_by_sym),
                                  "stocks_scored": len(all_scores),
                                  "bullish": bullish, "bearish": bearish, "neutral": neutral,
                                  "elapsed_seconds": round(elapsed, 1)})}
