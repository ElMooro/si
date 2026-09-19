"""Bounded provider collection and retained original-response validation."""
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
import json
import time
from urllib.parse import urlencode, quote
import urllib.request
import urllib.error

from daily_market_model import CONTRACT, ET
from evidence_store import capture, read_verified, public_source_url

MAX_BYTES = 4 * 1024 * 1024


def fetch(url, headers, deadline):
    # Use a normal TLS-verifying opener, independent of legacy HTTP shims.
    for attempt in range(3):
        remaining = deadline - time.monotonic()
        if remaining < 3: raise TimeoutError('market acquisition budget exhausted')
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'JustHodl-SourceEvidence/1.0', 'Accept': 'application/json', **headers})
            with urllib.request.build_opener().open(req, timeout=min(20, remaining)) as response:
                raw = response.read(MAX_BYTES+1)
                if len(raw) > MAX_BYTES: raise ValueError('provider response exceeds bound')
                return raw, datetime.now(timezone.utc)
        except urllib.error.HTTPError as exc:
            if exc.code not in (429, 500, 502, 503, 504) or attempt == 2: raise
        except (TimeoutError, urllib.error.URLError):
            if attempt == 2: raise
        delay = 2*(attempt+1)
        if time.monotonic()+delay >= deadline: raise TimeoutError('market acquisition budget exhausted')
        time.sleep(delay)


def source(client, bucket, provider, url, request, headers, deadline):
    raw, received = fetch(url, headers, deadline)
    evidence = capture(client, bucket, provider, url, raw, received)
    # A successful put alone does not prove retained bytes. Read before use.
    if read_verified(client, bucket, evidence) != raw:
        raise ValueError('retained source readback differs')
    return {'request': request, 'response': json.loads(raw), 'evidence': evidence,
            'acquired_at': received.isoformat()}


def collect(client, bucket, symbols, names, polygon_key, budget_seconds=400):
    deadline = time.monotonic()+budget_seconds
    today = datetime.now(timezone.utc).astimezone(ET).date()
    end = today-timedelta(days=1)
    start = end-timedelta(days=400)
    universe = list(dict.fromkeys(symbols))
    out = {'contract': CONTRACT, 'universe': universe, 'equities': {}, 'crypto': None, 'errors': {}}
    def stock(symbol):
        request = {'symbol': symbol, 'start': start.isoformat(), 'end': end.isoformat(), 'multiplier': 1,
                   'timespan': 'day', 'adjusted': True, 'sort': 'desc', 'limit': 50000}
        url = 'https://api.polygon.io/v2/aggs/ticker/'+quote(symbol, safe='')+'/range/1/day/'+start.isoformat()+'/'+end.isoformat()+'?adjusted=true&sort=desc&limit=50000'
        result = source(client, bucket, 'polygon', url, request, {'Authorization': 'Bearer '+polygon_key}, deadline)
        result['name'] = names.get(symbol, symbol)
        return result
    def safe_error(exc):
        return 'HTTP_'+str(exc.code) if isinstance(exc, urllib.error.HTTPError) else type(exc).__name__
    if polygon_key:
        with ThreadPoolExecutor(max_workers=3) as pool:
            futures = {pool.submit(stock, symbol): symbol for symbol in universe}
            for future in as_completed(futures):
                symbol = futures[future]
                try: out['equities'][symbol] = future.result()
                except Exception as exc: out['errors'][symbol] = safe_error(exc)
    else:
        out['errors'].update({symbol: 'MISSING_PROVIDER_CREDENTIAL' for symbol in universe})
    request = {'vs_currency': 'usd', 'order': 'market_cap_desc', 'per_page': 25,
               'page': 1, 'sparkline': False, 'price_change_percentage': '1h,24h,7d,30d'}
    query = {**request, 'sparkline': 'false'}
    url = 'https://api.coingecko.com/api/v3/coins/markets?'+urlencode(query)
    try: out['crypto'] = source(client, bucket, 'coingecko', url, request, {}, deadline)
    except Exception as exc: out['errors']['crypto'] = safe_error(exc)
    # Sorting fixes manifest order; do not pretend failed symbols disappeared
    # from the requested universe or carry an old quote as a new acquisition.
    out['equities'] = dict(sorted(out['equities'].items()))
    out['errors'] = dict(sorted(out['errors'].items()))
    return out


def verify_sources(sources, read):
    """`read` returns original decompressed bytes; shared by public replay."""
    import hashlib
    if sources.get('contract') != CONTRACT: raise ValueError('market source contract differs')
    originals = list(sources.get('equities', {}).values())
    if sources.get('crypto'): originals.append(sources['crypto'])
    for item in originals:
        evidence = item['evidence']
        raw = read(evidence['key'])
        if len(raw) != evidence['bytes'] or hashlib.sha256(raw).hexdigest() != evidence['sha256']:
            raise ValueError('market original bytes differ')
        if json.loads(raw) != item['response']: raise ValueError('market response differs from original')
        request = item['request']
        if evidence['provider'] == 'polygon':
            expected = 'https://api.polygon.io/v2/aggs/ticker/'+quote(request['symbol'], safe='')+'/range/1/day/'+request['start']+'/'+request['end']+'?adjusted=true&sort=desc&limit=50000'
        elif evidence['provider'] == 'coingecko':
            expected = 'https://api.coingecko.com/api/v3/coins/markets?'+urlencode({**request, 'sparkline': 'false'})
        else: raise ValueError('market provider differs')
        if evidence['source_url'] != public_source_url(expected): raise ValueError('market request identity differs')
        request_sha = hashlib.sha256(evidence['source_url'].encode()).hexdigest()
        if evidence['key'] != 'data/evidence/'+evidence['provider']+'/'+request_sha+'/'+evidence['sha256']+'.bin.gz':
            raise ValueError('market evidence key identity differs')
    return len(originals)
