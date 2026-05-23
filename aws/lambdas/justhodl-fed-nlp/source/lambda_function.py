"""
justhodl-fed-nlp — Federal Reserve Communications NLP Engine
==============================================================

Polls Fed press releases + speeches RSS, scores via Claude on 6
hawkish/dovish dimensions, tracks rolling-5 drift with σ-based shift
classification. Documented edge in macro literature (Renaissance-style
language analysis on FOMC corpus).

Output: data/fed-nlp.json
Schedule: rate(6 hours) — Fed publishes irregularly; 6h catches new docs
"""
import os, json, time, urllib.request, urllib.parse, re, math
from datetime import datetime, timezone
from xml.etree import ElementTree as ET
import boto3

VERSION = "1.0.0"
REGION = os.environ.get('AWS_REGION', 'us-east-1')
BUCKET = os.environ.get('S3_BUCKET', 'justhodl-dashboard-live')
OUT_KEY = "data/fed-nlp.json"
ANTHROPIC_KEY = os.environ.get('ANTHROPIC_API_KEY', '')
TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID', '')
MAX_NEW_DOCS = int(os.environ.get('MAX_NEW_DOCS', '5'))
MODEL = "claude-haiku-4-5-20251001"

s3 = boto3.client('s3', region_name=REGION)

FED_FEED_PRESS = "https://www.federalreserve.gov/feeds/press_monetary.xml"
FED_FEED_SPEECHES = "https://www.federalreserve.gov/feeds/speeches.xml"


def http_get(url, timeout=15):
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0 JustHodl/FedNLP'})
    return urllib.request.urlopen(req, timeout=timeout).read()


def parse_rss(xml_bytes):
    try:
        root = ET.fromstring(xml_bytes)
        items = []
        for item in root.iter('item'):
            items.append({
                'title': (item.findtext('title') or '').strip(),
                'link': (item.findtext('link') or '').strip(),
                'pub_date': (item.findtext('pubDate') or '').strip(),
                'description': (item.findtext('description') or '').strip()[:500],
            })
        return items
    except Exception as e:
        print(f"[rss] {e}")
        return []


def fetch_doc_text(url):
    try:
        html = http_get(url, timeout=20).decode('utf-8', errors='replace')
        html = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
        html = re.sub(r'<style[^>]*>.*?</style>', '', html, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<[^>]+>', ' ', html)
        text = re.sub(r'\s+', ' ', text).strip()
        return text[:12000]
    except Exception as e:
        print(f"[fetch] {url[:80]}: {e}")
        return None


def anthropic_score(doc_text, doc_title):
    if not ANTHROPIC_KEY:
        return None
    prompt = f"""You are scoring a Federal Reserve communication on hawkish-dovish dimensions.

Title: {doc_title}

Body:
---
{doc_text}
---

Return ONLY a JSON object (no preamble, no code fences):

{{
  "rates_direction": <0-10>,
  "inflation_framing": <0-10>,
  "labor_framing": <0-10>,
  "financial_conditions": <0-10>,
  "balance_sheet": <0-10>,
  "overall_hawkish": <0-10>,
  "key_topics": ["topic1","topic2","topic3"],
  "one_line_summary": "<one sentence>"
}}

Scale: 0 = strongly dovish, 10 = strongly hawkish.
- rates_direction: 10 = hints at hikes, 0 = hints at cuts
- inflation_framing: 10 = persistent, 0 = transitory
- labor_framing: 10 = overheated, 0 = softening
- financial_conditions: 10 = needs more tightening, 0 = already too tight
- balance_sheet: 10 = QT continues, 0 = pause/QE
- overall_hawkish: holistic 0-10"""
    try:
        req = urllib.request.Request(
            "https://api.anthropic.com/v1/messages",
            data=json.dumps({
                "model": MODEL, "max_tokens": 600,
                "messages": [{"role": "user", "content": prompt}],
            }).encode(),
            headers={"Content-Type": "application/json",
                     "x-api-key": ANTHROPIC_KEY,
                     "anthropic-version": "2023-06-01"},
            method='POST')
        resp = json.loads(urllib.request.urlopen(req, timeout=60).read())
        text = resp['content'][0]['text']
        text = re.sub(r'^```(?:json)?\s*', '', text.strip())
        text = re.sub(r'\s*```$', '', text)
        scores = json.loads(text)
        scores['composite'] = round(float(scores.get('overall_hawkish', 5)) * 10, 1)
        return scores
    except Exception as e:
        print(f"[anthropic] {e}")
        return None


def load_state():
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=OUT_KEY)
        return json.loads(obj['Body'].read())
    except Exception:
        return {'processed_links': [], 'documents': []}


def send_telegram(msg):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        data = urllib.parse.urlencode({
            'chat_id': TELEGRAM_CHAT_ID, 'text': msg[:4000],
            'parse_mode': 'Markdown',
        }).encode()
        urllib.request.urlopen(urllib.request.Request(url, data=data, method='POST'), timeout=10)
        return True
    except Exception:
        return False


def compute_drift(documents):
    composites = [d.get('scores', {}).get('composite') for d in documents
                  if isinstance(d.get('scores'), dict) and d['scores'].get('composite') is not None]
    if len(composites) < 2:
        return {'insufficient_data': True, 'n_composites': len(composites)}
    recent_5 = composites[-5:] if len(composites) >= 5 else composites
    prior_5 = composites[-10:-5] if len(composites) >= 10 else composites[:-len(recent_5)]
    recent_avg = sum(recent_5) / len(recent_5)
    prior_avg = sum(prior_5) / len(prior_5) if prior_5 else recent_avg
    drift = recent_avg - prior_avg
    z = 0
    if len(composites) >= 6:
        m = sum(composites) / len(composites)
        v = sum((c - m)**2 for c in composites) / max(len(composites)-1, 1)
        sd = math.sqrt(v) if v > 0 else 1.0
        z = drift / sd if sd > 0 else 0
    classification = ('HAWKISH_SHIFT' if z > 1.0 else
                      'DOVISH_SHIFT' if z < -1.0 else 'STABLE')
    return {
        'recent_5_avg': round(recent_avg, 1),
        'prior_5_avg': round(prior_avg, 1),
        'drift_points': round(drift, 1),
        'drift_z': round(z, 2),
        'classification': classification,
        'all_composites_last15': composites[-15:],
        'n_composites': len(composites),
    }


def lambda_handler(event=None, context=None):
    started = time.time()
    state = load_state()
    processed = set(state.get('processed_links', []))
    documents = state.get('documents', [])
    
    new_docs = []
    for feed_url in [FED_FEED_PRESS, FED_FEED_SPEECHES]:
        try:
            xml = http_get(feed_url, timeout=15)
            for item in parse_rss(xml)[:15]:
                if item['link'] and item['link'] not in processed:
                    new_docs.append(item)
        except Exception as e:
            print(f"[feed] {e}")
    new_docs = new_docs[:MAX_NEW_DOCS]
    print(f"[fed-nlp] {len(new_docs)} new docs to score")
    
    for doc in new_docs:
        text = fetch_doc_text(doc['link'])
        if not text:
            continue
        scores = anthropic_score(text, doc['title'])
        if not scores:
            continue
        documents.append({
            'title': doc['title'],
            'link': doc['link'],
            'pub_date': doc['pub_date'],
            'fetched_at': datetime.now(timezone.utc).isoformat(),
            'scores': scores,
        })
        processed.add(doc['link'])
    
    documents = documents[-50:]
    drift = compute_drift(documents)
    
    payload = {
        'version': VERSION,
        'generated_at': datetime.now(timezone.utc).isoformat(),
        'n_total_documents': len(documents),
        'n_new_this_run': len(new_docs),
        'drift': drift,
        'latest_5_documents': documents[-5:],
        'processed_links': list(processed)[-300:],
        'documents': documents,
        'elapsed_s': round(time.time() - started, 1),
    }
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(payload, default=str, indent=2).encode(),
                  ContentType='application/json', CacheControl='max-age=3600')
    
    if drift.get('classification') in ('HAWKISH_SHIFT', 'DOVISH_SHIFT') and new_docs:
        lines = [f"*🏦 FED LANGUAGE: {drift['classification']}*"]
        lines.append(f"Drift: {drift.get('drift_points')} pts ({drift.get('drift_z')}σ)")
        lines.append(f"Recent 5 avg: {drift.get('recent_5_avg')}/100  vs prior 5: {drift.get('prior_5_avg')}/100")
        if documents:
            latest = documents[-1]
            lines.append(f"\n_{latest.get('title','')[:100]}_")
            lines.append(f"Composite: {latest.get('scores',{}).get('composite')}/100")
            summ = latest.get('scores', {}).get('one_line_summary', '')
            if summ:
                lines.append(f"_{summ[:200]}_")
        send_telegram("\n".join(lines))
    
    print(f"[fed-nlp] done · new={len(new_docs)} · drift={drift.get('classification')} · elapsed={payload['elapsed_s']}s")
    return {
        'statusCode': 200,
        'body': json.dumps({'ok': True, 'new': len(new_docs), 'total': len(documents),
                            'drift': drift.get('classification'), 'elapsed_s': payload['elapsed_s']}),
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), indent=2))
