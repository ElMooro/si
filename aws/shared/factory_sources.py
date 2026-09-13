"""Approved outside-world observations. No arbitrary URL fetches or private notes."""
import gzip
import hashlib
import io
import json
import urllib.request
from datetime import datetime, time, timedelta

from factory_core import Invalid, NY, UTC, canonical, finite, iso, retain_fact, timestamp

TAPE = {'SPY': 'AMEX:SPY', 'QQQ': 'NASDAQ:QQQ', 'IWM': 'AMEX:IWM', 'TLT': 'NASDAQ:TLT', 'GLD': 'AMEX:GLD', 'BTC': 'COINBASE:BTCUSD'}
OSS = ('huggingface/open-r1', 'python/cpython')
LICENSES = {'Apache-2.0', 'MIT', 'BSD-3-Clause', 'BSD-2-Clause', 'PSF-2.0', 'Python-2.0'}


def warehouse_document(store, key):
    response = store.s3.get_object(Bucket=store.public, Key=key)
    raw = response['Body'].read(1024 * 1024 + 1)
    if len(raw) > 1024 * 1024:
        raise Invalid('warehouse_object_too_large')
    if key.endswith('.gz'):
        with gzip.GzipFile(fileobj=io.BytesIO(raw)) as stream:
            raw = stream.read(4 * 1024 * 1024 + 1)
        if len(raw) > 4 * 1024 * 1024:
            raise Invalid('warehouse_expansion_too_large')
    doc = json.loads(raw)
    if not isinstance(doc, dict):
        raise Invalid('warehouse_document_required')
    return doc, hashlib.sha256(raw).hexdigest()


def funding(store, previous, now):
    result = {}
    try:
        doc, raw_hash = warehouse_document(store, 'data/ofr-funding.json')
    except Exception:
        doc, raw_hash = {}, None
    for field in ('sofr', 'triparty_rate', 'dvp_rate', 'gcf_rate', 'triparty_volume'):
        item = doc.get(field) or {}
        observed = item.get('as_of') or item.get('observed')
        if isinstance(observed, str) and len(observed) == 10:
            observed += 'T00:00:00+00:00'
        fact = {'value': item.get('value'), 'observed_at': observed, 'source': item.get('source_url') or item.get('source'),
                'unit': item.get('unit'), 'data_unavailable': item.get('data_unavailable'),
                'raw_sha256': raw_hash, 'warehouse_key': 'data/ofr-funding.json'}
        result[field] = retain_fact(previous.get(field), fact, now, max_age_seconds=4 * 86400)
    return result


def bars(store, symbol, now):
    tv = TAPE[symbol]
    key = 'data/warm/tv-bars/universe/' + tv.replace(':', '__') + '.json.gz'
    doc, raw_hash = warehouse_document(store, key)
    if doc.get('tv_symbol') != tv or not isinstance(doc.get('source'), str):
        raise Invalid('warehouse_symbol_or_source_mismatch')
    data = doc.get('bars')
    if not isinstance(data, list) or not data or len(data) > 30000:
        raise Invalid('warehouse_bars_missing')
    normalized = []
    for row in data[-260:]:
        if not isinstance(row, list) or len(row) < 5:
            raise Invalid('warehouse_bar_shape')
        epoch, opening, high, low, close = [finite(v) for v in row[:5]]
        if min(opening, high, low, close) <= 0 or low > min(opening, close) or high < max(opening, close):
            raise Invalid('warehouse_bar_prices')
        observed = datetime.fromtimestamp(epoch, UTC)
        # Only completed prior-day bars; a current intraday daily candle cannot leak into a Monday entry.
        day = observed.date() if observed.hour == 0 else observed.astimezone(NY).date()
        if day >= now.astimezone(NY).date():
            continue
        normalized.append({'date': day.isoformat(), 'open': opening, 'close': close})
    if len(normalized) < 6 or len({r['date'] for r in normalized}) != len(normalized):
        raise Invalid('warehouse_bars_insufficient_or_duplicate')
    normalized.sort(key=lambda row: row['date'])
    last = normalized[-1]
    observed = datetime.combine(datetime.fromisoformat(last['date']).date(), time(16), NY)
    return {'value': last['close'], 'observed_at': iso(observed), 'source': doc['source'], 'unit': 'USD',
        'raw_sha256': raw_hash, 'warehouse_key': key, 'bars': normalized[-21:],
        'research_only': True, 'official_grading_eligible': False, 'data_unavailable': False}


def oss_metadata(previous, now):
    results = {}
    for repo in OSS:
        prior = previous.get(repo, {})
        if prior.get('checked_at') and (now - timestamp(prior['checked_at'])).total_seconds() < 86400:
            results[repo] = prior
            continue
        try:
            request = urllib.request.Request('https://api.github.com/repos/' + repo,
                headers={'User-Agent': 'JustHodl-Factory-GearA', 'Accept': 'application/vnd.github+json'})
            # Metadata only. Redirects cannot turn an allowlisted request into an arbitrary fetch.
            class NoRedirect(urllib.request.HTTPRedirectHandler):
                def redirect_request(self, *args, **kwargs):
                    raise Invalid('source_redirect_not_allowed')
            with urllib.request.build_opener(NoRedirect).open(request, timeout=4) as response:
                raw = response.read(65537)
            if len(raw) > 65536:
                raise Invalid('oss_metadata_too_large')
            doc = json.loads(raw)
            if doc.get('full_name', '').lower() != repo.lower():
                raise Invalid('oss_repository_mismatch')
            license_id = (doc.get('license') or {}).get('spdx_id')
            results[repo] = {'repository': repo, 'license': license_id, 'license_review_required': license_id not in LICENSES,
                'pushed_at': doc.get('pushed_at'), 'default_branch': doc.get('default_branch'),
                'source': 'https://github.com/' + repo, 'raw_sha256': hashlib.sha256(raw).hexdigest(),
                'checked_at': iso(now), 'metadata_only': True, 'training_content_downloaded': False, 'status': 'observed'}
        except Exception as exc:
            results[repo] = {**prior, 'repository': repo, 'checked_at': iso(now), 'status': 'retained' if prior else 'unavailable',
                             'error': type(exc).__name__, 'training_content_downloaded': False}
    return results


def sense(store, previous, now):
    facts = funding(store, previous.get('funding', {}), now)
    tape = {}
    for symbol in TAPE:
        try:
            candidate = bars(store, symbol, now)
        except Exception:
            candidate = None
        tape[symbol] = retain_fact(previous.get('tape', {}).get(symbol), candidate, now, max_age_seconds=4 * 86400)
    oss = oss_metadata(previous.get('oss', {}), now)
    snapshot = {'observed_at': iso(now), 'funding': facts, 'tape': tape, 'oss': oss}
    snapshot['snapshot_hash'] = hashlib.sha256(canonical(snapshot)).hexdigest()
    store.immutable(store.private, 'factory/sources/' + now.strftime('%Y-%m-%dT%H-%M') + '.json', snapshot)
    return snapshot
