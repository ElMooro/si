import json
import os
import time
import urllib3
import boto3
from datetime import datetime, timezone, timedelta

NEWSAPI_KEY   = "17d36cdd13c44e139853b3a6876cf940"
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_KEY", "")
S3_BUCKET     = "justhodl-dashboard-live"
SCREENER_KEY  = "screener/data.json"
SENTIMENT_KEY = "sentiment/data.json"
CACHE_TTL     = 20 * 3600
BATCH_TICKERS = 20
BATCH_CLAUDE  = 40

http = urllib3.PoolManager(
    maxsize=20,
    retries=urllib3.Retry(3, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503])
)
s3 = boto3.client("s3", region_name="us-east-1")

def newsapi_get(endpoint, params):
    qs = "&".join(f"{k}={v}" for k,v in params.items())
    url = f"https://newsapi.org/v2/{endpoint}?{qs}&apiKey={NEWSAPI_KEY}"
    try:
        r = http.request("GET", url, timeout=urllib3.Timeout(connect=5, read=15))
        if r.status == 200:
            return json.loads(r.data.decode("utf-8"))
        print(f"  NewsAPI {r.status}")
    except Exception as e:
        print(f"  NewsAPI ERR: {e}")
    return None

def claude_score(batch_text):
    payload = {
        "model": "claude-haiku-4-5-20251001",
        "max_tokens": 2048,
        "messages": [{"role": "user", "content": f"""You are a financial sentiment analyst. For each stock below, analyze the headlines and return a JSON array.

STOCKS AND HEADLINES:
{batch_text}

Return ONLY a valid JSON array (no markdown, no explanation) with this exact format:
[{{"symbol":"TICKER","score":0.65,"signal":"bullish","reason":"one sentence max"}}]

Rules:
- score: float from -1.0 (very bearish) to +1.0 (very bullish), 0.0 = neutral
- signal: exactly one of: bullish, bearish, neutral
- If no headlines, score=0.0, signal=neutral
- Base ONLY on provided headlines"""}]
    }
    try:
        r = http.request(
            "POST",
            "https://api.anthropic.com/v1/messages",
            body=json.dumps(payload).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "x-api-key": ANTHROPIC_KEY,
                "anthropic-version": "2023-06-01"
            },
            timeout=urllib3.Timeout(connect=10, read=45)
        )
        if r.status == 200:
            data = json.loads(r.data.decode("utf-8"))
            text = data["content"][0]["text"].strip()
            if "```" in text:
                text = text.split("```")[1]
                if text.startswith("json"):
                    text = text[4:]
            return json.loads(text.strip())
        else:
            print(f"  Claude {r.status}: {r.data.decode()[:200]}")
    except Exception as e:
        print(f"  Claude ERR: {e}")
    return []

def get_stock_list():
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=SCREENER_KEY)
        data = json.loads(obj["Body"].read())
        stocks = data.get("stocks", [])
        return [(s["symbol"], s.get("name", s["symbol"])) for s in stocks]
    except Exception as e:
        print(f"  Screener cache miss: {e}")
    try:
        r = http.request("GET",
            "https://financialmodelingprep.com/stable/sp500-constituent?apikey=wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb",
            timeout=urllib3.Timeout(connect=5, read=15))
        if r.status == 200:
            sp500 = json.loads(r.data.decode("utf-8"))
            return [(s["symbol"], s.get("name", s["symbol"])) for s in sp500]
    except:
        pass
    return []

def fetch_news_batch(symbols_names):
    tickers = [sn[0] for sn in symbols_names]
    query = " OR ".join(f'"{t}"' for t in tickers)
    since = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")
    data = newsapi_get("everything", {
        "q": query, "language": "en",
        "sortBy": "publishedAt", "pageSize": "100", "from": since
    })
    symbol_headlines = {t: [] for t in tickers}
    if data and data.get("articles"):
        for article in data["articles"]:
            title = article.get("title", "") or ""
            desc  = article.get("description", "") or ""
            text  = f"{title}. {desc}"
            for ticker in tickers:
                name = next((n for s,n in symbols_names if s == ticker), ticker)
                name_first = name.split()[0] if name else ""
                if (f" {ticker} " in f" {text} " or
                    (name_first and len(name_first) > 3 and name_first.lower() in text.lower())):
                    if len(symbol_headlines[ticker]) < 5:
                        symbol_headlines[ticker].append(title[:150])
    return symbol_headlines

def score_batch_with_claude(symbol_headlines_batch):
    lines = []
    for symbol, headlines in symbol_headlines_batch.items():
        if headlines:
            joined = " | ".join(headlines[:5])
            lines.append(f"{symbol}: {joined}")
        else:
            lines.append(f"{symbol}: [no recent news]")
    results = claude_score("\n".join(lines))
    scored = {}
    if isinstance(results, list):
        for item in results:
            sym = item.get("symbol", "")
            if sym:
                scored[sym] = {
                    "sentimentScore":  round(float(item.get("score", 0)), 3),
                    "sentimentSignal": item.get("signal", "neutral"),
                    "sentimentReason": item.get("reason", ""),
                }
    for symbol in symbol_headlines_batch:
        if symbol not in scored:
            scored[symbol] = {"sentimentScore": 0.0, "sentimentSignal": "neutral", "sentimentReason": "No data"}
        scored[symbol]["headlines"] = symbol_headlines_batch[symbol]
    return scored

def lambda_handler(event, context):
    headers = {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}
    force = isinstance(event, dict) and event.get("force", False)

    if not force:
        try:
            obj = s3.get_object(Bucket=S3_BUCKET, Key=SENTIMENT_KEY)
            cached = json.loads(obj["Body"].read())
            age = time.time() - cached.get("generated_at_unix", 0)
            if age < CACHE_TTL:
                return {"statusCode": 200, "headers": headers,
                        "body": json.dumps({"from_cache": True, "age_hours": round(age/3600,2),
                                            "count": cached.get("count",0)})}
        except:
            pass

    t0 = time.time()
    print("=== SENTIMENT START ===")

    stocks = get_stock_list()
    if not stocks:
        return {"statusCode": 500, "headers": headers, "body": json.dumps({"error": "No stocks"})}
    print(f"  {len(stocks)} stocks | {time.time()-t0:.1f}s")

    all_headlines = {}
    batches = [stocks[i:i+BATCH_TICKERS] for i in range(0, len(stocks), BATCH_TICKERS)]
    for i, batch in enumerate(batches):
        headlines = fetch_news_batch(batch)
        all_headlines.update(headlines)
        if i % 5 == 0:
            print(f"  News {i+1}/{len(batches)} | {time.time()-t0:.1f}s")
        time.sleep(0.3)

    found = sum(1 for h in all_headlines.values() if h)
    print(f"  {found}/{len(stocks)} stocks have news | {time.time()-t0:.1f}s")

    all_sentiment = {}
    symbols = list(all_headlines.keys())
    claude_batches = [{s: all_headlines[s] for s in symbols[i:i+BATCH_CLAUDE]}
                      for i in range(0, len(symbols), BATCH_CLAUDE)]
    for i, batch in enumerate(claude_batches):
        scored = score_batch_with_claude(batch)
        all_sentiment.update(scored)
        print(f"  Claude {i+1}/{len(claude_batches)} | {time.time()-t0:.1f}s")
        time.sleep(1)

    sentiment_list = []
    for symbol, name in stocks:
        s = all_sentiment.get(symbol, {"sentimentScore": 0.0, "sentimentSignal": "neutral",
                                        "sentimentReason": "No data", "headlines": []})
        sentiment_list.append({
            "symbol": symbol, "name": name,
            "sentimentScore":  s.get("sentimentScore", 0.0),
            "sentimentSignal": s.get("sentimentSignal", "neutral"),
            "sentimentReason": s.get("sentimentReason", ""),
            "headlines":       s.get("headlines", []),
            "hasNews":         len(s.get("headlines", [])) > 0
        })

    elapsed = time.time() - t0
    bullish = sum(1 for s in sentiment_list if s["sentimentSignal"] == "bullish")
    bearish = sum(1 for s in sentiment_list if s["sentimentSignal"] == "bearish")
    neutral = sum(1 for s in sentiment_list if s["sentimentSignal"] == "neutral")
    print(f"=== DONE: B:{bullish} Bear:{bearish} N:{neutral} | {elapsed:.1f}s ===")

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "generated_at_unix": int(time.time()),
        "elapsed_seconds": round(elapsed, 1),
        "count": len(sentiment_list),
        "bullish_count": bullish, "bearish_count": bearish, "neutral_count": neutral,
        "sentiment": sentiment_list
    }

    s3.put_object(Bucket=S3_BUCKET, Key=SENTIMENT_KEY,
                  Body=json.dumps(payload, separators=(",",":")),
                  ContentType="application/json", CacheControl="max-age=72000")

    return {"statusCode": 200, "headers": headers,
            "body": json.dumps({"success": True, "count": len(sentiment_list),
                                "bullish": bullish, "bearish": bearish,
                                "elapsed_seconds": round(elapsed,1)})}
