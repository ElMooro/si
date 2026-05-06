import json
import urllib.request
import urllib.parse
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

# Bundle api_auth.py alongside lambda_function.py
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from api_auth import authorize

FRED_KEY = '2f057499936072679d8843d7fce99989'
FRED_BASE = 'https://api.stlouisfed.org/fred/series/observations'

# Allowed origins for Origin-bypass mode — justhodl.ai frontend pages
# (carry.html, fred.html) call this Lambda directly. Adding strict
# auth would break them; Origin allowlist lets browser pages through
# while requiring API keys for external (curl, scripts) callers.
ALLOWED_ORIGINS = [
    "https://justhodl.ai",
    "https://www.justhodl.ai",
]

def fetch_one(series_id, limit=365):
    params = urllib.parse.urlencode({'series_id': series_id, 'api_key': FRED_KEY, 'file_type': 'json', 'limit': limit, 'sort_order': 'desc'})
    url = f"{FRED_BASE}?{params}"
    req = urllib.request.Request(url, headers={'User-Agent': 'JustHodl/1.0'})
    with urllib.request.urlopen(req, timeout=10) as resp:
        data = json.loads(resp.read())
    obs = [{'date': o['date'], 'value': float(o['value'])} for o in data.get('observations', []) if o.get('value', '.') != '.']
    if not obs:
        return {'series_id': series_id, 'error': 'No data'}
    return {'series_id': series_id, 'value': obs[0]['value'], 'date': obs[0]['date'], 'observations': obs[:60], 'count': len(obs)}

def lambda_handler(event, context):
    # ─── Auth gate (Origin-bypass mode) ──────────────────────────────
    # justhodl.ai frontend pages (carry.html, fred.html) call this
    # without any key — Origin matches → allowed through as ENTERPRISE.
    # External callers (curl, third-party apps) need a valid jhd_<key>.
    key_meta, err = authorize(event, allowed_origins=ALLOWED_ORIGINS)
    if err:
        return err

    params = event.get('queryStringParameters') or {}
    series = params.get('series', '')
    if not series:
        return {'statusCode': 200, 'body': json.dumps({'status': 'ok', 'service': 'JustHodl FRED Proxy'})}
    series_list = [s.strip() for s in series.split(',') if s.strip()][:20]
    if len(series_list) == 1:
        try:
            result = fetch_one(series_list[0])
            return {'statusCode': 200, 'body': json.dumps(result)}
        except Exception as e:
            return {'statusCode': 200, 'body': json.dumps({'series_id': series_list[0], 'error': str(e)})}
    results = {}
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_one, sid): sid for sid in series_list}
        for future in as_completed(futures):
            sid = futures[future]
            try:
                results[sid] = future.result()
            except Exception as e:
                results[sid] = {'series_id': sid, 'error': str(e)}
    return {'statusCode': 200, 'body': json.dumps({'batch': True, 'count': len(results), 'data': results})}
