"""Bounded public-source collection, original-byte replay and conditional publication."""
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import re
import time
import urllib.request
import urllib.error
import urllib.parse
import regime_model as model
import pd_fails_context

MAX_BYTES = 4*1024*1024
PRIVATE = 'audit-private/20260909-originals/risk-regime/'
COMPILERS = (model, pd_fails_context)
CONTEXT_KEYS = ('bank-stress', 'breadth-divergence', 'capital-inflows', 'cb-injection', 'cds-monitor',
    'china-liquidity', 'correlation-breaks', 'credit-equity-divergence', 'crisis-canaries', 'cross-asset-confirm',
    'crypto-liquidity', 'dollar-radar', 'global-liquidity', 'global-sovereign', 'hot-money', 'liquidity-inflection',
    'market-internals', 'plumbing-stress', 'polygon-fx-regime', 'repo-lending', 'repo-market', 'systemic-stress',
    'tail-risk', 'vix-backwardation-trigger', 'vix9d-vix-inversion', 'vol-target-unwind', 'vvix-vov-regime')


def now(): return datetime.now(timezone.utc).isoformat()
def code(exc): return str(getattr(exc, 'response', {}).get('Error', {}).get('Code', ''))
def conflict(exc): return code(exc) in ('412', '409', 'PreconditionFailed', 'ConditionalRequestConflict')
def missing(exc): return code(exc) in ('404', 'NoSuchKey')


def bounded(stream):
    try: raw = stream.read(MAX_BYTES+1)
    finally: stream.close()
    if len(raw) > MAX_BYTES: raise ValueError('research object exceeds byte bound')
    return raw


def immutable(client, bucket, key, raw, kind='application/json', private=False):
    if private and not key.startswith(PRIVATE): raise ValueError('protected prefix required')
    try:
        client.put_object(Bucket=bucket, Key=key, Body=raw, ContentType=kind, IfNoneMatch='*',
            CacheControl='no-store' if private else 'public, max-age=31536000, immutable')
    except Exception as exc:
        if not conflict(exc): raise
    if bounded(client.get_object(Bucket=bucket, Key=key)['Body']) != raw: raise ValueError('immutable artifact differs')


def reader(client, bucket):
    def read(key):
        if not isinstance(key, str) or not re.fullmatch(r'data/[A-Za-z0-9_./-]+', key) or '..' in key:
            raise ValueError('public artifact key required')
        return bounded(client.get_object(Bucket=bucket, Key=key)['Body'])
    return read


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl): return None


class Collector:
    def __init__(self, client, bucket, fred_key, massive_key, transport=None, deadline=None):
        self.client, self.bucket = client, bucket
        self.fred_key, self.massive_key = fred_key, massive_key
        self.transport = transport or urllib.request.build_opener(NoRedirect()).open
        self.deadline = deadline or time.monotonic()+150
        self.calls = 0; self.sources = {}; self.bodies = {}

    def fetch(self, name, url, provider):
        try:
            if self.calls >= 40 or time.monotonic() >= self.deadline: raise TimeoutError('collection budget exhausted')
            self.calls += 1
            parsed = urllib.parse.urlsplit(url)
            model.safe_url(url, parsed.path, 'api.stlouisfed.org' if provider == 'fred' else 'api.massive.com')
            key = self.fred_key if provider == 'fred' else self.massive_key
            if not key: raise ValueError('provider credential unavailable')
            authenticated = url + '&api_key='+urllib.parse.quote(key, safe='') if provider == 'fred' else url
            headers = {'User-Agent': 'JustHodl-risk-research/2.0', 'Accept': 'application/json'}
            if provider != 'fred': headers['Authorization'] = 'Bearer '+key
            with self.transport(urllib.request.Request(authenticated, headers=headers), timeout=max(1, min(15, self.deadline-time.monotonic()))) as response:
                raw = bounded(response)
            for secret in (self.fred_key, self.massive_key):
                if secret and secret.encode() in raw: raise ValueError('credential reflected in provider response')
            doc = json.loads(raw)
            if not isinstance(doc, dict): raise ValueError('provider object required')
            stamp = now(); sha = hashlib.sha256(raw).hexdigest(); request_sha = hashlib.sha256(url.encode()).hexdigest()
            target = model.PREFIX+'sources/'+request_sha+'/'+sha+'.json'
            immutable(self.client, self.bucket, target, raw)
            ref = {'status': 'captured', 'provider': provider, 'request_url': url, 'request_sha256': request_sha,
                'key': target, 'sha256': sha, 'bytes': len(raw), 'acquired_at': stamp}
            self.sources[name] = ref; self.bodies[name] = raw
            return doc
        except Exception as exc:
            # Exception strings from HTTP libraries can contain authenticated URLs.
            self.sources[name] = {'status': 'unavailable', 'reason': type(exc).__name__, 'provider': provider}
            return None


def collect(client, bucket, fred_key, massive_key, transport=None):
    collector = Collector(client, bucket, fred_key, massive_key, transport)
    evaluation = datetime.now(timezone.utc).date(); option_pages = {}
    for sid in model.SERIES:
        q = dict(series_id=sid, file_type='json', realtime_start=str(evaluation), realtime_end=str(evaluation))
        collector.fetch('definition:'+sid, 'https://api.stlouisfed.org/fred/series?'+urllib.parse.urlencode(q), 'fred')
        q.update(observation_start=str(evaluation-timedelta(days=800)), observation_end=str(evaluation), units='lin',
                 sort_order='asc', limit=10000, offset=0, output_type=1)
        collector.fetch('observations:'+sid, 'https://api.stlouisfed.org/fred/series/observations?'+urllib.parse.urlencode(q), 'fred')
    for symbol in model.SYMBOLS:
        doc = collector.fetch('previous:'+symbol, 'https://api.massive.com/v2/aggs/ticker/'+symbol+'/prev?adjusted=true', 'massive')
        rows = (doc or {}).get('results') or []
        spot = model.decimal(rows[0].get('c')) if len(rows) == 1 else None
        if spot is None or spot <= 0: continue
        from decimal import Decimal
        q = {'strike_price.gte': f'{spot*Decimal("0.88"):.2f}', 'strike_price.lte': f'{spot*Decimal("1.12"):.2f}',
             'expiration_date.gte': str(evaluation+timedelta(days=21)), 'expiration_date.lte': str(evaluation+timedelta(days=45)),
             'sort': 'ticker', 'order': 'asc', 'limit': 250}
        path = '/v3/snapshot/options/'+symbol
        url = 'https://api.massive.com'+path+'?'+urllib.parse.urlencode(q); pages = []
        for index in range(12):
            try:
                q = model.safe_url(url, path)
                if index and set(q) != {'cursor'}: raise ValueError('unexpected continuation')
            except ValueError: break
            name = f'options:{symbol}:{index+1}'
            doc = collector.fetch(name, url, 'massive')
            if doc is None: break
            pages.append(name); url = doc.get('next_url')
            if not url: break
        option_pages[symbol] = pages
    collector.fetch('fx:AUDJPY', 'https://api.massive.com/v2/aggs/ticker/C:AUDJPY/range/1/day/'+
        f'{evaluation-timedelta(days=65)}/{evaluation-timedelta(days=1)}?adjusted=true&sort=asc&limit=50000', 'massive')
    context = {}; read = reader(client, bucket)
    for name in CONTEXT_KEYS:
        key = 'data/'+name+'.json'
        try:
            raw = read(key); doc = json.loads(raw)
            if not isinstance(doc, dict): raise ValueError('context document required')
            # Only typed public coverage metadata is retained. No personal/watchlist/account projection.
            stamp = doc.get('generated_at')
            if stamp is not None: model.clock(stamp)
            version = doc.get('version')
            if not isinstance(version, (str, int, float)): version = None
            status = (doc.get('quality') or {}).get('status')
            if status not in ('fresh', 'partial', 'stale', 'unavailable', 'degraded', 'research_only', 'unverified'): status = 'unverified'
            context[name] = {'source': key, 'source_generated_at': stamp, 'source_packet_sha256': hashlib.sha256(raw).hexdigest(),
                'version': version, 'declared_quality': status, 'role': 'linked_context_only', 'original_provider_verified': False, **model.PERMISSIONS}
        except Exception:
            context[name] = {'source': key, 'source_generated_at': None, 'declared_quality': 'unavailable',
                'role': 'linked_context_only', 'original_provider_verified': False, **model.PERMISSIONS}
    try:
        raw = read(pd_fails_context.KEY); doc = json.loads(raw)
        # Exact public settlement source retained separately, never mixed into native-vote counts.
        target = model.PREFIX+'settlement-inputs/'+hashlib.sha256(raw).hexdigest()+'.json'
        immutable(client, bucket, target, raw)
        settlement = {'key': target, 'sha256': hashlib.sha256(raw).hexdigest(), 'bytes': len(raw)}
    except Exception: settlement = None
    return {'contract': 'risk-regime-inputs.v1', 'generated_at': now(), 'evaluation_date': str(evaluation),
        'sources': collector.sources, 'option_pages': option_pages, 'context': context, 'settlement': settlement,
        'provider_request_count': collector.calls}, collector.bodies


def compile_output(inputs, bodies, read):
    output = model.build(inputs, bodies); ref = inputs.get('settlement')
    doc = {}
    if ref:
        raw = checked(ref, read, 'settlement-inputs'); doc = json.loads(raw)
    output['pd_settlement_fails'] = pd_fails_context.project(doc, model.clock(inputs['generated_at']))
    return output


def checked(ref, read, category):
    sha = ref.get('sha256', '')
    if not re.fullmatch('[a-f0-9]{64}', sha) or ref.get('key') != model.PREFIX+category+'/'+sha+'.json':
        raise ValueError('artifact identity differs')
    raw = read(ref['key'])
    if len(raw) != ref['bytes'] or hashlib.sha256(raw).hexdigest() != sha: raise ValueError('artifact bytes differ')
    return raw


def source_bodies(inputs, read):
    bodies = {}
    for name, ref in inputs['sources'].items():
        if ref.get('status') != 'captured': continue
        url = ref['request_url']; request_sha = hashlib.sha256(url.encode()).hexdigest(); sha = ref['sha256']
        if not re.fullmatch('[a-f0-9]{64}', sha) or ref.get('request_sha256') != request_sha or ref['key'] != model.PREFIX+'sources/'+request_sha+'/'+sha+'.json':
            raise ValueError('original request identity differs')
        raw = read(ref['key'])
        if len(raw) != ref['bytes'] or hashlib.sha256(raw).hexdigest() != sha: raise ValueError('original response differs')
        bodies[name] = raw
    return bodies


def replay(ref, read):
    key = ref.get('manifest_key', '')
    if not re.fullmatch(re.escape(model.PREFIX)+r'runs/[a-f0-9]{64}\.json', key): raise ValueError('run identity missing')
    raw = read(key)
    if key != model.PREFIX+'runs/'+hashlib.sha256(raw).hexdigest()+'.json': raise ValueError('run bytes differ')
    manifest = json.loads(raw)
    if manifest.get('contract') != 'risk-regime-replay.v1' or manifest.get('output_sha256') != ref.get('output_sha256'):
        raise ValueError('run contract/output differs')
    if set(manifest['compilers']) != {m.__name__ for m in COMPILERS}: raise ValueError('compiler set differs')
    for module in COMPILERS:
        body = Path(module.__file__).read_bytes(); sha = hashlib.sha256(body).hexdigest(); cr = manifest['compilers'][module.__name__]
        if cr != {'key': model.PREFIX+'compilers/'+sha+'.py', 'sha256': sha} or read(cr['key']) != body:
            raise ValueError('reviewed compiler differs; use matching release checkout')
    inputs = json.loads(checked(manifest['input'], read, 'inputs'))
    output = compile_output(inputs, source_bodies(inputs, read), read)
    retained = json.loads(checked(manifest['output'], read, 'outputs'))
    if output != retained or model.digest(output) != manifest['output_sha256'] or output['generated_at'] != manifest['generated_at']:
        raise ValueError('original-source replay differs')
    return output


def retain(client, bucket, inputs, output):
    refs = {}
    for name, doc in [('input', inputs), ('output', output)]:
        raw = model.encoded(doc); sha = hashlib.sha256(raw).hexdigest(); key = model.PREFIX+name+'s/'+sha+'.json'
        immutable(client, bucket, key, raw); refs[name] = {'key': key, 'sha256': sha, 'bytes': len(raw)}
    compilers = {}
    for module in COMPILERS:
        raw = Path(module.__file__).read_bytes(); sha = hashlib.sha256(raw).hexdigest(); key = model.PREFIX+'compilers/'+sha+'.py'
        immutable(client, bucket, key, raw, 'text/x-python'); compilers[module.__name__] = {'key': key, 'sha256': sha}
    manifest = {'contract': 'risk-regime-replay.v1', 'generated_at': inputs['generated_at'], **refs,
        'compilers': compilers, 'output_sha256': model.digest(output),
        'scope': 'native provider bytes + request bindings + typed public context + exact compiler; no point-in-time backtest claim'}
    key = model.PREFIX+'runs/'+model.digest(manifest)+'.json'; immutable(client, bucket, key, model.encoded(manifest))
    ref = {'manifest_key': key, 'output_sha256': manifest['output_sha256']}
    if replay(ref, reader(client, bucket)) != output: raise ValueError('retained replay differs')
    return ref


def publish(client, bucket, packet):
    stamp = model.clock(packet['generated_at'])
    for _ in range(4):
        try:
            obj = client.get_object(Bucket=bucket, Key=model.CURRENT); raw = bounded(obj['Body']); old = json.loads(raw)
            if old.get('generated_at'):
                old_stamp = model.clock(old['generated_at'])
                if old_stamp > stamp: return False
                if old_stamp == stamp and old != packet: raise ValueError('conflicting snapshot clock')
            immutable(client, bucket, PRIVATE+hashlib.sha256(raw).hexdigest()+'.bin', raw, 'application/octet-stream', True)
            condition = {'IfMatch': obj['ETag']}
        except Exception as exc:
            if not missing(exc): raise
            condition = {'IfNoneMatch': '*'}
        try:
            client.put_object(Bucket=bucket, Key=model.CURRENT, Body=model.encoded(packet), ContentType='application/json',
                CacheControl='no-store', **condition)
            if reader(client, bucket)(model.CURRENT) != model.encoded(packet):
                # A newer writer may have won; this run's immutable evidence remains available.
                live = json.loads(reader(client, bucket)(model.CURRENT))
                if model.clock(live['generated_at']) <= stamp: raise ValueError('current publication readback differs')
            return True
        except Exception as exc:
            if not conflict(exc): raise
    raise RuntimeError('publication race exhausted; immutable run retained')


def run(client, bucket, fred_key, massive_key, validation_only=False):
    inputs, bodies = collect(client, bucket, fred_key, massive_key)
    output = compile_output(inputs, bodies, reader(client, bucket)); ref = retain(client, bucket, inputs, output)
    published = False if validation_only else publish(client, bucket, {**output, 'replay': ref})
    return {'published': published, 'validation_only': validation_only, 'generated_at': output['generated_at'],
        'quality': output['quality'], 'replay': ref, 'provider_request_count': inputs['provider_request_count'],
        'private_account_reads': 0, 'paid_ai_calls': 0, 'notifications_sent': 0, 'portfolio_writes': 0}
