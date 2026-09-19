"""Original FRED responses and deterministic research packet; no paid AI calls."""
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import threading
import time
import urllib.error
import urllib.request
from urllib.parse import urlencode

from evidence_store import capture, read_verified
from report_observations import build, digest, encoded

MAX_BYTES = 4 * 1024 * 1024
CURRENT = 'data/report-measurements.json'
_rate_lock = threading.Lock()
_last_request = 0.0
OBSERVATION_LIMIT = 4000
HISTORY_POLICY = 'native-levels-four-thousand-observations.v1'


def now():
    return datetime.now(timezone.utc).isoformat()


def code(exc):
    return str(getattr(exc, 'response', {}).get('Error', {}).get('Code', ''))


def error_label(exc):
    # HTTP status is operationally useful; exception text can contain the
    # authenticated request URL and must never reach public data or logs.
    return 'HTTP_'+str(exc.code) if isinstance(exc, urllib.error.HTTPError) else type(exc).__name__


def get(client, bucket, key, limit=32*1024*1024):
    try:
        obj = client.get_object(Bucket=bucket, Key=key)
    except Exception as exc:
        if code(exc) in ('404', 'NoSuchKey'): return None, None
        raise
    blob = obj['Body'].read(limit+1)
    if len(blob) > limit: raise ValueError('object exceeds explicit bound')
    return json.loads(blob), obj['ETag']


def age(stamp):
    parsed = datetime.fromisoformat(stamp.replace('Z', '+00:00'))
    if parsed.tzinfo is None: raise ValueError('source clock lacks timezone')
    seconds = (datetime.now(timezone.utc)-parsed).total_seconds()
    if seconds < -5: raise ValueError('future source clock')
    return max(0, seconds)


def original(client, bucket, sid, part, key, deadline):
    global _last_request
    params = {'series_id': sid, 'api_key': key, 'file_type': 'json'}
    path = 'series'
    if part == 'observations':
        path += '/observations'
        params.update(sort_order='desc',limit=OBSERVATION_LIMIT,units='lin')
    url = 'https://api.stlouisfed.org/fred/'+path+'?'+urlencode(params)
    # build_opener avoids the legacy urllib.urlopen cache shim. Cached synthetic
    # FRED-shaped JSON must never be relabeled as an original HTTP response.
    opener = urllib.request.build_opener()
    for attempt in range(3):
        if time.monotonic() > deadline-25: raise TimeoutError('acquisition budget exhausted')
        with _rate_lock:
            pause = max(0, .8-(time.monotonic()-_last_request))
            if pause: time.sleep(pause)
            _last_request = time.monotonic()
        try:
            request = urllib.request.Request(url, headers={'User-Agent': 'JustHodl source-backed research admin@justhodl.ai'})
            with opener.open(request, timeout=20) as response:
                raw = response.read(MAX_BYTES+1)
            acquired = now()
            if len(raw) > MAX_BYTES: raise ValueError('response exceeds capture bound')
            document = json.loads(raw)
            if 'error_code' in document: raise ValueError('provider rejected query')
            receipt = capture(client, bucket, 'fred', url, raw)
            if read_verified(client, bucket, receipt) != raw: raise ValueError('original readback differs')
            return document, receipt, acquired
        except urllib.error.HTTPError as exc:
            if exc.code not in (429, 500, 502, 503, 504) or attempt == 2: raise
            time.sleep(3*(attempt+1))
    raise RuntimeError('provider retry exhausted')


def load_input(client, bucket, descriptor):
    return {'definition': json.loads(read_verified(client, bucket, descriptor['evidence']['definition'])),
            'observations': json.loads(read_verified(client, bucket, descriptor['evidence']['observations'])),
            'evidence': descriptor['evidence'], 'acquired_at': descriptor['acquired_at']}


def acquire(client, bucket, sid, key, deadline):
    if not re.fullmatch('[A-Za-z0-9_]+', sid): raise ValueError('invalid FRED identity')
    cache_key = 'data/report-research/cache/'+sid+'.json'
    cached, etag = get(client, bucket, cache_key, 128*1024)
    try:
        if (cached and cached.get('contract') == 'report-source-cache.v1'
                and cached.get('history_policy') == HISTORY_POLICY and age(cached['acquired_at']) <= 3600):
            return load_input(client, bucket, cached), None
        definition = receipt = definition_at = None
        if cached and cached.get('contract') == 'report-source-cache.v1' and age(cached['definition_acquired_at']) <= 86400:
            receipt = cached['evidence']['definition']; definition_at = cached['definition_acquired_at']
            definition = json.loads(read_verified(client, bucket, receipt))
        if definition is None:
            definition, receipt, definition_at = original(client, bucket, sid, 'definition', key, deadline)
        observations, obs_receipt, acquired = original(client, bucket, sid, 'observations', key, deadline)
        descriptor = {'contract': 'report-source-cache.v1', 'history_policy':HISTORY_POLICY,
                      'series_id': sid, 'acquired_at': acquired,
                      'definition_acquired_at': definition_at,
                      'evidence': {'definition': receipt, 'observations': obs_receipt}}
        # Do not overwrite another concurrent acquisition. The run keeps its own
        # input receipts regardless of which cache writer won.
        try:
            client.put_object(Bucket=bucket, Key=cache_key, Body=encoded(descriptor), ContentType='application/json',
                              **({'IfMatch': etag} if etag else {'IfNoneMatch': '*'}))
        except Exception as exc:
            if code(exc) not in ('412', 'PreconditionFailed', '409', 'ConditionalRequestConflict'): raise
        return {'definition': definition, 'observations': observations, 'evidence': descriptor['evidence'], 'acquired_at': acquired}, None
    except Exception as exc:
        # Retain the actual old acquisition clock. A retry does not renew it.
        if cached and cached.get('contract') == 'report-source-cache.v1':
            return load_input(client, bucket, cached), error_label(exc)
        raise


def immutable(client, bucket, key, body, content_type):
    try:
        client.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type, IfNoneMatch='*')
    except Exception as exc:
        if code(exc) not in ('412', 'PreconditionFailed', '409', 'ConditionalRequestConflict'): raise
        if client.get_object(Bucket=bucket, Key=key)['Body'].read(len(body)+1) != body:
            raise ValueError('immutable run differs') from exc


def publish(client, bucket, catalog, inputs, errors):
    import report_observations
    stamp = now(); output = build(catalog, inputs, stamp, errors)
    source = Path(report_observations.__file__).read_bytes()
    compiler_sha = hashlib.sha256(source).hexdigest()
    compiler_key = 'data/report-research/compilers/'+compiler_sha+'.py'
    immutable(client, bucket, compiler_key, source, 'text/plain')
    manifest = {'contract': 'report-research-replay.v1', 'generated_at': stamp, 'catalog': catalog,
                'inputs': {sid: {'evidence': item['evidence'], 'acquired_at': item['acquired_at']} for sid, item in inputs.items()},
                'errors': errors, 'output_sha256': digest(output), 'compiler': {'key': compiler_key, 'sha256': compiler_sha},
                'scope': output['scope'], 'sizing_eligible': False, 'publication_time_verified': False}
    manifest_key = 'data/report-research/runs/'+digest(manifest)+'.json'
    immutable(client, bucket, manifest_key, encoded(manifest), 'application/json')
    class RetainedInputs:
        # Re-read one original pair at a time. A second complete expanded
        # response graph would unnecessarily double the collector's peak RAM.
        def get(self, sid):
            item = manifest['inputs'].get(sid)
            return load_input(client, bucket, item) if item else None
    if digest(build(catalog, RetainedInputs(), stamp, errors)) != manifest['output_sha256']:
        raise ValueError('retained original replay differs')
    output['replay'] = {'manifest_key': manifest_key, 'output_sha256': manifest['output_sha256'], 'compiler_sha256': compiler_sha}
    for _ in range(4):
        previous, etag = get(client, bucket, CURRENT)
        if previous and previous.get('generated_at', '') > stamp:
            return {'published': False, 'reason': 'newer concurrent run already current', 'replay': output['replay']}
        if previous and previous.get('contract') == output['contract']:
            for sid, row in output['measurements'].items():
                old = previous.get('measurements', {}).get(sid)
                if old and datetime.fromisoformat(old['acquired_at'].replace('Z', '+00:00')) > datetime.fromisoformat(row['acquired_at'].replace('Z', '+00:00')):
                    # A slow run can finish later with earlier observations. Its
                    # new generated_at must not overwrite a newer acquisition.
                    return {'published': False, 'reason': 'current source acquisition is newer: '+sid, 'replay': output['replay']}
        try:
            client.put_object(Bucket=bucket, Key=CURRENT, Body=encoded(output), ContentType='application/json', CacheControl='no-cache',
                              **({'IfMatch': etag} if etag else {'IfNoneMatch': '*'}))
            return {'published': True, 'generated_at': stamp, 'quality': output['quality'], 'replay': output['replay']}
        except Exception as exc:
            if code(exc) not in ('412', 'PreconditionFailed', '409', 'ConditionalRequestConflict'): raise
    raise RuntimeError('concurrent publication retry bound exceeded')


def run(client, bucket, catalog, key, budget_seconds=650):
    if not key: raise ValueError('configured FRED credential unavailable')
    deadline = time.monotonic()+budget_seconds
    inputs = {}; errors = {}
    # Two workers and an explicit 0.8 s minimum request interval. Existing legacy
    # producers remain separate callers; 429 responses are handled explicitly.
    with ThreadPoolExecutor(max_workers=2) as pool:
        pending = {pool.submit(acquire, client, bucket, sid, key, deadline): sid for sid in catalog}
        for future in as_completed(pending):
            sid = pending[future]
            try:
                item, error = future.result(); inputs[sid] = item
                if error: errors[sid] = error
            except Exception as exc:
                errors[sid] = error_label(exc)
    return publish(client, bucket, catalog, inputs, errors)
