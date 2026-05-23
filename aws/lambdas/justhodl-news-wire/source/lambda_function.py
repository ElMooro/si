"""
justhodl-news-wire — Real-Time News with Portfolio-Impact Scoring
====================================================================

Polls NewsAPI + FMP general/financial news, hash-dedupes, Claude batch
classifies (tickers/themes/sentiment/urgency), scores per-headline impact
on YOUR specific portfolio. Bloomberg TOP-style feed.

Output: data/news-wire.json + data/news-wire-state.json
Schedule: rate(15 minutes)
"""
import os, json, time, urllib.request, urllib.parse, hashlib, re
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3

VERSION = "1.0.0"
REGION = os.environ.get('AWS_REGION', 'us-east-1')
BUCKET = os.environ.get('S3_BUCKET', 'justhodl-dashboard-live')
OUT_KEY = "data/news-wire.json"
STATE_KEY = "data/news-wire-state.json"
NEWSAPI_KEY = os.environ.get('NEWSAPI_KEY', '')
FMP_KEY = os.environ.get('FMP_KEY', '')
ANTHROPIC_KEY = os.environ.get('ANTHROPIC_API_KEY', '')
TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID', '')
MAX_NEW_TO_SCORE = int(os.environ.get('MAX_NEW_TO_SCORE', '20'))
MODEL = "claude-haiku-4-5-20251001"

s3 = boto3.client('s3', region_name=REGION)


def http_get_json(url, timeout=12):
    req = urllib.request.Request(url, headers={'User-Agent': 'JustHodl/NewsWire'})
    return json.loads(urllib.request.urlopen(req, timeout=timeout).read())


def normalize_title(t):
    t = (t or '').lower()
    t = re.sub(r'[^a-z0-9 ]', '', t)
    return re.sub(r'\s+', ' ', t).strip()


def hash_title(t):
    return hashlib.md5(normalize_title(t).encode()).hexdigest()[:16]


def fetch_newsapi():
    if not NEWSAPI_KEY:
        return []
    try:
        url = (f"https://newsapi.org/v2/top-headlines?country=us&category=business"
               f"&pageSize=50&apiKey={NEWSAPI_KEY}")
        data = http_get_json(url, timeout=10)
        return [{
            'source': 'newsapi',
            'title': a.get('title', ''),
            'description': (a.get('description') or '')[:300],
            'url': a.get('url'),
            'published_at': a.get('publishedAt'),
            'origin': (a.get('source') or {}).get('name', 'unknown'),
        } for a in data.get('articles', []) if a.get('title')]
    except Exception as e:
        print(f"[newsapi] {e}")
        return []


def fetch_fmp_news():
    if not FMP_KEY:
        return []
    items = []
    for endpoint, src_tag in [('general-news', 'fmp-general'),
                               ('fmp-articles', 'fmp-articles')]:
        try:
            url = f"https://financialmodelingprep.com/stable/{endpoint}?page=0&size=40&apikey={FMP_KEY}"
            data = http_get_json(url, timeout=10)
            if not isinstance(data, list):
                continue
            for a in data:
                items.append({
                    'source': src_tag,
                    'title': a.get('title', '') or a.get('headline', ''),
                    'description': (a.get('text') or a.get('description') or '')[:300],
                    'url': a.get('url') or a.get('link'),
                    'published_at': a.get('publishedDate') or a.get('date'),
                    'origin': a.get('site') or a.get('publisher') or 'FMP',
                    'sentiment_raw': a.get('sentiment'),
                })
        except Exception as e:
            print(f"[fmp {endpoint}] {e}")
    return items


def classify_batch(headlines):
    if not ANTHROPIC_KEY or not headlines:
        return [None] * len(headlines)
    items_text = "\n".join([f"{i+1}. {h['title']}" for i, h in enumerate(headlines)])
    prompt = f"""You are a financial news classifier. For each headline below, classify it.

Headlines:
{items_text}

Themes (pick best-fitting): fed_policy, inflation, recession_risk, employment, china, geopolitics, energy, technology, ai, crypto, earnings, M&A, regulation, election, supply_chain, banking, real_estate

Return ONLY a JSON array (no preamble, no code fences):
[{{"i":1,"tickers":["..."],"themes":["..."],"sentiment":<-2..2>,"urgency":<1..5>,"summary":"<≤15w>"}},...]

Definitions:
- sentiment: -2 = very bearish for markets, 0 = neutral, +2 = very bullish
- urgency: 1 = background/historical, 5 = market-moving NOW"""
    try:
        req = urllib.request.Request(
            "https://api.anthropic.com/v1/messages",
            data=json.dumps({"model": MODEL, "max_tokens": 2500,
                             "messages": [{"role": "user", "content": prompt}]}).encode(),
            headers={"Content-Type": "application/json",
                     "x-api-key": ANTHROPIC_KEY,
                     "anthropic-version": "2023-06-01"},
            method='POST')
        resp = json.loads(urllib.request.urlopen(req, timeout=60).read())
        text = resp['content'][0]['text']
        text = re.sub(r'^```(?:json)?\s*', '', text.strip())
        text = re.sub(r'\s*```$', '', text)
        arr = json.loads(text)
        results = [None] * len(headlines)
        for item in arr:
            i = item.get('i', 0) - 1
            if 0 <= i < len(headlines):
                results[i] = item
        return results
    except Exception as e:
        print(f"[anthropic batch] {e}")
        return [None] * len(headlines)


def get_portfolio_tickers():
    try:
        obj = s3.get_object(Bucket=BUCKET, Key='data/portfolio.json')
        raw = json.loads(obj['Body'].read())
        positions = raw if isinstance(raw, list) else raw.get('positions', [])
        return set((p.get('symbol') or p.get('ticker') or '').upper()
                   for p in positions if isinstance(p, dict))
    except Exception:
        return set()


def score_portfolio_impact(headline, my_tickers):
    c = headline.get('classification') or {}
    sentiment = c.get('sentiment', 0) or 0
    urgency = c.get('urgency', 1) or 1
    score = sentiment * urgency
    tickers = set([(t or '').upper() for t in c.get('tickers', [])])
    if my_tickers & tickers:
        score *= 3
    return round(float(score), 1)


def load_state():
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=STATE_KEY)
        return json.loads(obj['Body'].read())
    except Exception:
        return {'seen_hashes': [], 'scored_headlines': []}


def send_telegram(msg):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        data = urllib.parse.urlencode({
            'chat_id': TELEGRAM_CHAT_ID, 'text': msg[:4000],
            'parse_mode': 'Markdown',
            'disable_web_page_preview': 'true',
        }).encode()
        urllib.request.urlopen(urllib.request.Request(url, data=data, method='POST'), timeout=10)
        return True
    except Exception:
        return False


def lambda_handler(event=None, context=None):
    started = time.time()
    state = load_state()
    seen = set(state.get('seen_hashes', []))
    scored = state.get('scored_headlines', [])
    
    items = []
    with ThreadPoolExecutor(max_workers=3) as ex:
        futures = [ex.submit(fetch_newsapi), ex.submit(fetch_fmp_news)]
        for f in as_completed(futures):
            try:
                items.extend(f.result())
            except Exception:
                pass
    
    new_items = []
    for h in items:
        title = h.get('title') or ''
        if not title:
            continue
        h_hash = hash_title(title)
        if h_hash in seen:
            continue
        h['hash'] = h_hash
        new_items.append(h)
        seen.add(h_hash)
    new_items = new_items[:MAX_NEW_TO_SCORE]
    print(f"[news-wire] {len(new_items)} new headlines")
    
    if new_items:
        classifications = classify_batch(new_items)
        for h, c in zip(new_items, classifications):
            h['classification'] = c
            h['scored_at'] = datetime.now(timezone.utc).isoformat()
    
    my_tickers = get_portfolio_tickers()
    for h in new_items:
        h['portfolio_impact_score'] = score_portfolio_impact(h, my_tickers)
    
    scored.extend(new_items)
    scored = scored[-200:]
    
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    recent = [h for h in scored
              if (h.get('scored_at') or '') > cutoff and h.get('classification')]
    recent.sort(key=lambda h: abs(h.get('portfolio_impact_score', 0)), reverse=True)
    top_10 = recent[:10]
    
    state_save = {'seen_hashes': list(seen)[-2000:], 'scored_headlines': scored}
    s3.put_object(Bucket=BUCKET, Key=STATE_KEY,
                  Body=json.dumps(state_save, default=str).encode(),
                  ContentType='application/json')
    
    payload = {
        'version': VERSION,
        'generated_at': datetime.now(timezone.utc).isoformat(),
        'n_new_this_run': len(new_items),
        'n_scored_total': len(scored),
        'portfolio_tickers_tracked': len(my_tickers),
        'top_10_24h_by_impact': top_10,
        'recent_30': scored[-30:][::-1],
        'elapsed_s': round(time.time() - started, 1),
    }
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(payload, default=str, indent=2).encode(),
                  ContentType='application/json', CacheControl='max-age=600')
    
    high_impact = [h for h in new_items
                   if (h.get('classification') or {}).get('urgency', 0) >= 4
                   and abs(h.get('portfolio_impact_score', 0)) >= 6]
    if high_impact:
        lines = [f"*🔥 HIGH-IMPACT NEWS*"]
        for h in high_impact[:5]:
            c = h.get('classification', {})
            lines.append(f"\n*{h.get('title','')[:120]}*")
            lines.append(f"  impact: {h.get('portfolio_impact_score')}  "
                         f"urgency: {c.get('urgency')}  sentiment: {c.get('sentiment')}")
            if c.get('tickers'):
                lines.append(f"  tickers: {', '.join(c['tickers'][:5])}")
            if c.get('themes'):
                lines.append(f"  themes: {', '.join(c['themes'][:3])}")
        send_telegram("\n".join(lines))
    
    print(f"[news-wire] done · new={len(new_items)} · top24h={len(top_10)} · "
          f"high-impact={len(high_impact)} · elapsed={payload['elapsed_s']}s")
    return {
        'statusCode': 200,
        'body': json.dumps({'ok': True, 'n_new': len(new_items),
                            'n_high_impact': len(high_impact),
                            'elapsed_s': payload['elapsed_s']}),
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), indent=2))
