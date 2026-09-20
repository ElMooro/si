"""Bounded original-source collection, protected retention, replay and CAS publication.

Public artifacts contain derived survey observations and source identities. Whole
publisher HTML stays in the existing protected AWS archive, replayable with IAM.
No private account data, consumer invocation, LLM, notification or trading path.
"""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
import json
import re
import sys
import time
import urllib.error
import urllib.request

import aaii_research_model as model

PREFIX = 'data/aaii-research/'
CURRENT = 'data/aaii-sentiment.json'
PRIVATE = 'audit-private/20260909-originals/aaii-sentiment/'
MAX = 2*1024*1024
COMPILERS = (model, sys.modules[__name__])


def now(): return datetime.now(timezone.utc).isoformat()


def bounded(stream):
    try: raw = stream.read(MAX+1)
    finally: stream.close()
    if len(raw) > MAX: raise ValueError('artifact exceeds byte bound')
    return raw


def error_code(exc): return str(getattr(exc, 'response', {}).get('Error', {}).get('Code', ''))
def missing(exc): return error_code(exc) in ('404', 'NoSuchKey')
def conflict(exc): return error_code(exc) in ('409', '412', 'PreconditionFailed', 'ConditionalRequestConflict')


def allowed(key):
    return isinstance(key, str) and (key == CURRENT or bool(re.fullmatch(
        re.escape(PRIVATE)+r'[a-f0-9]{64}\.bin|'+re.escape(PREFIX)+r'(?:inputs|outputs|runs|compilers)/[a-f0-9]{64}\.(?:json|py)', key)))


def reader(client, bucket):
    def read(key):
        if not allowed(key): raise ValueError('unapproved research artifact path')
        return bounded(client.get_object(Bucket=bucket, Key=key)['Body'])
    return read


def immutable(client, bucket, key, raw, kind='application/json'):
    if not allowed(key) or key == CURRENT or not isinstance(raw, bytes) or len(raw) > MAX:
        raise ValueError('bounded immutable research artifact required')
    if key.rsplit('/', 1)[-1].split('.')[0] != model.sha(raw): raise ValueError('content address differs')
    try:
        client.put_object(Bucket=bucket, Key=key, Body=raw, ContentType=kind, IfNoneMatch='*',
            CacheControl='no-store' if key.startswith(PRIVATE) else 'public, max-age=31536000, immutable')
    except Exception as exc:
        if not conflict(exc): raise
    if reader(client, bucket)(key) != raw: raise ValueError('immutable readback differs')


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise ValueError('provider redirect refused')


def source_url(name):
    if name not in model.URLS: raise ValueError('reviewed AAII source name required')
    return model.URLS[name]


def acquire(name, deadline, opener=None):
    if time.monotonic() >= deadline: return {'error': 'collection_budget_exhausted'}
    request = urllib.request.Request(source_url(name), headers={
        'User-Agent': 'Mozilla/5.0 (compatible; JustHodl Research/2.0; +https://justhodl.ai/about.html)',
        'Accept': 'text/html', 'Accept-Language': 'en-US,en;q=0.9', 'Accept-Encoding': 'identity'})
    opener = opener or urllib.request.build_opener(NoRedirect())
    try:
        response = opener.open(request, timeout=min(25, max(1, deadline-time.monotonic())))
        if response.status != 200 or response.headers.get_content_type() != 'text/html':
            response.close(); return {'error': 'provider_status_or_content_type'}
        raw = bounded(response)
        return {'raw': raw, 'acquired_at': now()}
    except urllib.error.HTTPError as exc:
        status = int(exc.code); exc.close()
        return {'error': 'provider_http_'+str(status)}
    except (ValueError, OSError, TimeoutError):
        return {'error': 'provider_transport_or_bound_failure'}


def collect(client, bucket, deadline, fetch=acquire):
    def one(name):
        item = fetch(name, deadline)
        if item.get('error'): return name, item
        raw = item.pop('raw'); digest = model.sha(raw); key = PRIVATE+digest+'.bin'
        immutable(client, bucket, key, raw, 'application/octet-stream')
        return name, {**item, 'evidence': {'sha256': digest, 'bytes': len(raw), 'key': key,
            'provider': 'aaii', 'source_url': source_url(name), 'access': 'protected_AWS_IAM_source_archive'}}
    with ThreadPoolExecutor(max_workers=2) as pool:
        return dict(pool.map(one, model.URLS))


def compile_output(inputs, read):
    if inputs.get('contract') != 'aaii-native-inputs.v1': raise ValueError('unsupported input contract')
    if set(inputs.get('sources', {})) != set(model.URLS): raise ValueError('source inventory differs')
    if not 0 <= (model.stamp(inputs['generated_at'])-model.stamp(inputs['started_at'])).total_seconds() <= 100:
        raise ValueError('collection clock exceeds bounded acquisition interval')
    sources = {}
    for name, item in inputs['sources'].items():
        if item.get('error'):
            if set(item) != {'error'} or not re.fullmatch(r'[a-z0-9_]{1,80}', item['error']):
                raise ValueError('unsafe source failure descriptor')
            sources[name] = item; continue
        ref = item['evidence']; digest = ref.get('sha256', '')
        if not model.stamp(inputs['started_at']) <= model.stamp(item['acquired_at']) <= model.stamp(inputs['generated_at']):
            raise ValueError('original acquisition is outside this collection')
        if (not re.fullmatch('[a-f0-9]{64}', digest) or ref.get('key') != PRIVATE+digest+'.bin'
                or ref.get('source_url') != source_url(name) or ref.get('provider') != 'aaii'):
            raise ValueError('protected original identity differs')
        raw = read(ref['key'])
        if model.sha(raw) != digest or type(ref.get('bytes')) is not int or len(raw) != ref['bytes']:
            raise ValueError('original bytes differ')
        sources[name] = {**item, 'raw': raw}
    output = model.compute(sources, inputs['generated_at'])
    output['collection'] = {'started_at': inputs['started_at'], 'completed_at': inputs['generated_at'],
        'source_count': len(model.URLS), 'method': 'Two labelled publisher pages reacquired; year anchored to explicit survey date.',
        'original_replay_access': 'Existing authorized AWS IAM; whole original HTML is not anonymously redistributed.'}
    return output


def checked(ref, category, read):
    digest = ref.get('sha256', '')
    if not re.fullmatch('[a-f0-9]{64}', digest) or ref.get('key') != PREFIX+category+'/'+digest+'.json':
        raise ValueError('research artifact identity differs')
    raw = read(ref['key'])
    if type(ref.get('bytes')) is not int or len(raw) != ref['bytes'] or model.sha(raw) != digest:
        raise ValueError('research artifact bytes differ')
    return json.loads(raw)


def replay(ref, read):
    key = ref.get('manifest_key', '')
    if not re.fullmatch(re.escape(PREFIX)+r'runs/[a-f0-9]{64}\.json', key): raise ValueError('run identity required')
    raw = read(key); manifest = json.loads(raw)
    if key != PREFIX+'runs/'+model.sha(raw)+'.json' or manifest.get('contract') != 'aaii-native-replay.v1':
        raise ValueError('run content identity differs')
    if set(manifest['compilers']) != {m.__name__ for m in COMPILERS}: raise ValueError('compiler inventory differs')
    for module in COMPILERS:
        code = Path(module.__file__).read_bytes(); digest = model.sha(code); compiler = manifest['compilers'][module.__name__]
        if compiler != {'key': PREFIX+'compilers/'+digest+'.py', 'sha256': digest} or read(compiler['key']) != code:
            raise ValueError('matching reviewed compiler release required')
    inputs = checked(manifest['input'], 'inputs', read)
    result = compile_output(inputs, read)
    if (result != checked(manifest['output'], 'outputs', read) or model.sha(model.encoded(result)) != ref.get('output_sha256')
            or manifest['output_sha256'] != ref['output_sha256'] or result['generated_at'] != manifest['generated_at']):
        raise ValueError('native original-source replay differs')
    return result


def retain(client, bucket, inputs, output):
    refs = {}
    for name, doc in (('input', inputs), ('output', output)):
        raw = model.encoded(doc); digest = model.sha(raw); key = PREFIX+name+'s/'+digest+'.json'
        immutable(client, bucket, key, raw); refs[name] = {'key': key, 'sha256': digest, 'bytes': len(raw)}
    compilers = {}
    for module in COMPILERS:
        raw = Path(module.__file__).read_bytes(); digest = model.sha(raw); key = PREFIX+'compilers/'+digest+'.py'
        immutable(client, bucket, key, raw, 'text/x-python'); compilers[module.__name__] = {'key': key, 'sha256': digest}
    manifest = {'contract': 'aaii-native-replay.v1', 'generated_at': output['generated_at'], **refs,
        'compilers': compilers, 'output_sha256': refs['output']['sha256'],
        'scope': 'Retained complete publisher HTML, labelled survey sections, explicit date anchor and cross-page reconciliation. No forecast or historical availability claim.'}
    raw = model.encoded(manifest); key = PREFIX+'runs/'+model.sha(raw)+'.json'; immutable(client, bucket, key, raw)
    ref = {'manifest_key': key, 'output_sha256': refs['output']['sha256']}
    if replay(ref, reader(client, bucket)) != output: raise ValueError('retained replay differs')
    return ref


def publish(client, bucket, packet):
    at = model.stamp(packet['generated_at'])
    for _ in range(4):
        try:
            obj = client.get_object(Bucket=bucket, Key=CURRENT); raw = bounded(obj['Body']); old = json.loads(raw)
            old_day = old.get('as_of') or (old.get('latest') or {}).get('week_ending')
            if model.stamp(old['generated_at']) > at: return False
            if old_day and packet.get('as_of') and old_day > packet['as_of']: return False
            if model.stamp(old['generated_at']) == at and old != packet: raise ValueError('conflicting same-clock publication')
            immutable(client, bucket, PRIVATE+model.sha(raw)+'.bin', raw, 'application/octet-stream')
            condition = {'IfMatch': obj['ETag']}
        except Exception as exc:
            if not missing(exc): raise
            condition = {'IfNoneMatch': '*'}
        try:
            client.put_object(Bucket=bucket, Key=CURRENT, Body=model.encoded(packet), ContentType='application/json',
                CacheControl='no-store', **condition)
            live = json.loads(reader(client, bucket)(CURRENT))
            if live != packet and model.stamp(live['generated_at']) <= at: raise ValueError('publication readback differs')
            return True
        except Exception as exc:
            if not conflict(exc): raise
    raise RuntimeError('publication conflict limit; immutable run retained')


def request_key(request_id):
    if not isinstance(request_id, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,100}', request_id):
        raise ValueError('canonical idempotent request id required')
    return PREFIX+'requests/'+model.sha(request_id.encode())+'.json'


def status_write(client, bucket, key, doc, **condition):
    client.put_object(Bucket=bucket, Key=key, Body=model.encoded(doc), ContentType='application/json', CacheControl='no-store', **condition)


def run(client, bucket, request_id, execution_id, remaining_seconds=180):
    started = now(); key = request_key(request_id)
    status = {'contract': 'aaii-public-request.v1', 'request_id': request_id, 'execution_id': execution_id,
        'started_at': started, 'status': 'running', 'phase': 'collect'}
    try: status_write(client, bucket, key, status, IfNoneMatch='*')
    except Exception as exc:
        if not conflict(exc): raise
        return json.loads(bounded(client.get_object(Bucket=bucket, Key=key)['Body']))
    try:
        deadline = time.monotonic()+max(0, min(80, remaining_seconds-60))
        sources = collect(client, bucket, deadline)
        inputs = {'contract': 'aaii-native-inputs.v1', 'started_at': started, 'generated_at': now(),
            'sources': sources}
        status['phase'] = 'compile'; status_write(client, bucket, key, status)
        output = compile_output(inputs, reader(client, bucket))
        status['phase'] = 'retained_replay'; status_write(client, bucket, key, status)
        ref = retain(client, bucket, inputs, output)
        status['phase'] = 'publish'; status_write(client, bucket, key, status)
        published = publish(client, bucket, {**output, 'replay': ref})
        result = {**status, 'status': 'complete', 'phase': 'complete', 'completed_at': now(), 'published': published,
            'generated_at': output['generated_at'], 'as_of': output['as_of'], 'quality': output['quality'], 'replay': ref,
            'private_account_reads': 0, 'paid_ai_calls': 0, 'notifications_sent': 0, 'portfolio_writes': 0}
        status_write(client, bucket, key, result); return result
    except Exception:
        # Diagnostics in code/tests and immutable source inventory; never serialize
        # arbitrary provider/SDK errors into a public status document or logs.
        status_write(client, bucket, key, {**status, 'status': 'failed', 'completed_at': now(),
            'error': 'native_collection_replay_or_publication_failed'})
        raise RuntimeError('native AAII publication failed; inspect reviewed request evidence') from None
