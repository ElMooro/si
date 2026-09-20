"""Public native FMP/FRED capture, original-byte replay and compare-and-swap publication."""
from datetime import datetime, timezone, timedelta
import hashlib,json,re,time,urllib.request,urllib.error,urllib.parse
from pathlib import Path
import global_research_model as model
MAX_BYTES=16*1024*1024
PRIVATE='audit-private/20260909-originals/global-stress/'
COMPILERS=(model,)
CONTEXT_KEYS=('jsi','sovereign-stress','ciss-stress','risk-regime','global-sovereign','liquidity-flow','settlement-fails','euro-fragmentation','eurodollar-stress')


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
    def __init__(self, client, bucket, fred_key, fmp_key, transport=None, deadline=None):
        self.client, self.bucket = client, bucket
        self.fred_key, self.fmp_key = fred_key, fmp_key
        self.transport = transport or urllib.request.build_opener(NoRedirect()).open
        self.deadline = deadline or time.monotonic()+180
        self.calls = 0; self.sources = {}; self.bodies = {}

    def fetch(self, name, url, provider):
        try:
            if self.calls >= 62 or time.monotonic() >= self.deadline: raise TimeoutError('collection budget exhausted')
            self.calls += 1
            parsed = urllib.parse.urlsplit(url)
            model.safe_url(url, parsed.path)
            key = self.fred_key if provider == 'fred' else self.fmp_key
            if not key: raise ValueError('provider credential unavailable')
            authenticated = url + '&api_key='+urllib.parse.quote(key, safe='') if provider == 'fred' else url
            headers = {'User-Agent': 'JustHodl-global-stress-research/2.0', 'Accept': 'application/json'}
            if provider != 'fred': headers['apikey'] = key
            with self.transport(urllib.request.Request(authenticated, headers=headers), timeout=max(1, min(15, self.deadline-time.monotonic()))) as response:
                raw = bounded(response)
            for secret in (self.fred_key, self.fmp_key):
                if secret and secret.encode() in raw: raise ValueError('credential reflected in provider response')
            doc = json.loads(raw)
            if not isinstance(doc, (dict,list)): raise ValueError('provider JSON object or array required')
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
    if manifest.get('contract') != 'global-stress-replay.v1' or manifest.get('output_sha256') != ref.get('output_sha256'):
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
    manifest = {'contract': 'global-stress-replay.v1', 'generated_at': inputs['generated_at'], **refs,
        'compilers': compilers, 'output_sha256': model.digest(output),
        'scope': 'native provider bytes + request bindings + typed public context + exact compiler; no point-in-time backtest claim'}
    key = model.PREFIX+'runs/'+model.digest(manifest)+'.json'; immutable(client, bucket, key, model.encoded(manifest))
    ref = {'manifest_key': key, 'output_sha256': manifest['output_sha256']}
    if replay(ref, reader(client, bucket)) != output: raise ValueError('retained replay differs')
    return ref


def publish(client, bucket, packet, key=model.CURRENT):
    if key not in (model.CURRENT,'data/global-stress-history.json'):raise ValueError('unsupported mutable publication')
    stamp = model.clock(packet['generated_at'])
    for _ in range(4):
        try:
            obj = client.get_object(Bucket=bucket, Key=key); raw = bounded(obj['Body']); old = json.loads(raw)
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
            client.put_object(Bucket=bucket, Key=key, Body=model.encoded(packet), ContentType='application/json',
                CacheControl='no-store', **condition)
            if reader(client, bucket)(key) != model.encoded(packet):
                # A newer writer may have won; this run's immutable evidence remains available.
                live = json.loads(reader(client, bucket)(key))
                if model.clock(live['generated_at']) <= stamp: raise ValueError('current publication readback differs')
            return True
        except Exception as exc:
            if not conflict(exc): raise
    raise RuntimeError('publication race exhausted; immutable run retained')

def collect(client,bucket,fred_key,fmp_key,transport=None):
    collector=Collector(client,bucket,fred_key,fmp_key,transport)
    evaluation=datetime.now(timezone.utc).date();start=str(evaluation-timedelta(days=800));end=str(evaluation-timedelta(days=1))
    for symbol in model.INSTRUMENTS:
        for kind,path in [('profile','profile'),('full','historical-price-eod/full'),('dividend-adjusted','historical-price-eod/dividend-adjusted')]:
            q={'symbol':symbol}
            if kind!='profile':q.update({'from':start,'to':end})
            collector.fetch(kind+':'+symbol,'https://financialmodelingprep.com/stable/'+path+'?'+urllib.parse.urlencode(q),'fmp')
    for sid in model.SERIES:
        q=dict(series_id=sid,file_type='json',realtime_start=str(evaluation),realtime_end=str(evaluation))
        collector.fetch('definition:'+sid,'https://api.stlouisfed.org/fred/series?'+urllib.parse.urlencode(q),'fred')
        q.update(observation_start='1990-01-01' if sid in model.LONG_HISTORY else start,observation_end=str(evaluation),units='lin',sort_order='asc',limit=20000 if sid in model.LONG_HISTORY else 10000,offset=0,output_type=1)
        collector.fetch('observations:'+sid,'https://api.stlouisfed.org/fred/series/observations?'+urllib.parse.urlencode(q),'fred')
    context={};read=reader(client,bucket)
    for name in CONTEXT_KEYS:
        key='data/'+name+'.json'
        try:
            raw=read(key);doc=json.loads(raw)
            if not isinstance(doc,dict):raise ValueError('public context required')
            stamp=doc.get('generated_at')
            if stamp is not None:model.clock(stamp)
            quality=doc.get('quality')
            status=quality.get('status') if isinstance(quality,dict) else None
            if status not in ('fresh','partial','stale','unavailable','degraded','research_only','unverified'):status='unverified'
            context[name]={'source':key,'source_generated_at':stamp,'source_packet_sha256':hashlib.sha256(raw).hexdigest(),
                'declared_quality':status,'role':'linked_context_only','original_provider_verified':False,**model.PERMISSIONS}
        except Exception:
            context[name]={'source':key,'source_generated_at':None,'declared_quality':'unavailable','role':'linked_context_only',
                'original_provider_verified':False,**model.PERMISSIONS}
    return {'contract':'global-stress-inputs.v1','generated_at':now(),'evaluation_date':str(evaluation),'sources':collector.sources,
        'history_start':start,'price_end':end,'context':context,'provider_request_count':collector.calls},collector.bodies


def compile_output(inputs,bodies,read):return model.build(inputs,bodies)


def run(client,bucket,fred_key,fmp_key,validation_only=False):
    inputs,bodies=collect(client,bucket,fred_key,fmp_key)
    output=compile_output(inputs,bodies,reader(client,bucket));ref=retain(client,bucket,inputs,output)
    published=False if validation_only else publish(client,bucket,{**output,'replay':ref})
    if published:
        publish(client,bucket,{'contract':'global-stress-history-pointer.v1','generated_at':output['generated_at'],
            'snapshots':[],'native_history_run':ref,'source_packet':'data/global-stress.json',
            'note':'The legacy synthetic composite series is unqualified. Native-frequency histories are retained in the reproducible run output.',
            **model.PERMISSIONS},'data/global-stress-history.json')
    return {'published':published,'validation_only':validation_only,'generated_at':output['generated_at'],'quality':output['quality'],
        'replay':ref,'provider_request_count':inputs['provider_request_count'],'private_account_reads':0,
        'paid_ai_calls':0,'notifications_sent':0,'portfolio_writes':0,'signals_emitted':0,'ssm_weight_writes':0}
