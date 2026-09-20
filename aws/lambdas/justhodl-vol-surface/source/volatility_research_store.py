"""Bounded original-source collection, protected retention, replay and CAS publication.

Public artifacts contain typed volatility measurements and source identities. Whole
licensed originals stay in the existing protected AWS archive, replayable with IAM.
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
import urllib.parse
import threading

import volatility_research_model as model

PREFIX = model.PREFIX
CURRENT = 'data/vol-surface.json'
PRIVATE = model.PRIVATE
MAX = 24*1024*1024
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


class Collector:
    """30 fixed FRED/publisher calls, bounded concurrency, rate spacing and private originals."""
    def __init__(self, client, bucket, credential, evaluation, deadline, opener=None):
        self.client,self.bucket,self.credential=client,bucket,credential
        self.evaluation,self.deadline=evaluation,deadline
        self.opener=opener or urllib.request.build_opener(NoRedirect())
        self.lock=threading.Lock();self.next_request=0.0

    def acquire(self,sid,kind):
        if not self.credential and sid not in model.PUBLISHER: return {'error':'configured_credential_unavailable'}
        url=model.source_url(sid,kind,self.evaluation)
        with self.lock:
            wait=max(0,self.next_request-time.monotonic())
            if time.monotonic()+wait>=self.deadline: return {'error':'collection_budget_exhausted'}
            time.sleep(wait);self.next_request=time.monotonic()+0.65
        request_url=url if sid in model.PUBLISHER else url+'&api_key='+urllib.parse.quote(self.credential,safe='')
        request=urllib.request.Request(request_url,headers={
            'User-Agent':'JustHodl-volatility-research/2.0','Accept':'application/json','Accept-Encoding':'identity'})
        try:
            response=self.opener.open(request,timeout=min(20,max(1,self.deadline-time.monotonic())))
            if response.status!=200:
                response.close();return {'error':'provider_status_failure'}
            raw=bounded(response)
            if len(raw)>model.MAX_BYTES or (self.credential and self.credential.encode() in raw): return {'error':'provider_body_rejected'}
            if sid in model.PUBLISHER:raw.decode('utf-8-sig')
            else:model.json_object(raw)
            collected=now();digest=model.sha(raw);key=PRIVATE+digest+'.bin'
            immutable(self.client,self.bucket,key,raw,'application/octet-stream')
            return {'acquired_at':collected,'evidence':{'sha256':digest,'bytes':len(raw),'key':key,
                'provider':'Cboe' if sid in model.PUBLISHER else 'fred','request_url':url,'access':'protected_AWS_IAM_source_archive'}}
        except urllib.error.HTTPError as exc:
            status=int(exc.code);exc.close();return {'error':'provider_http_'+str(status)}
        except (ValueError,OSError,TimeoutError): return {'error':'provider_transport_or_body_failure'}


def collect(client,bucket,credential,evaluation,deadline):
    collector=Collector(client,bucket,credential,evaluation,deadline)
    def one(sid):
        return sid,{kind:collector.acquire(sid,kind) for kind in (('observations',) if sid in model.PUBLISHER else ('definition','observations'))}
    with ThreadPoolExecutor(max_workers=4) as pool:
        return dict(pool.map(one,model.LABELS))


def compile_output(inputs,read):
    if inputs.get('contract')!='volatility-native-inputs.v1': raise ValueError('Unsupported input contract')
    if set(inputs.get('sources',{}))!=set(model.LABELS): raise ValueError('Source inventory differs')
    started=model.stamp(inputs['started_at']);completed=model.stamp(inputs['generated_at'])
    if not 0<=(completed-started).total_seconds()<=240: raise ValueError('Collection interval exceeds bound')
    if inputs.get('evaluation_date')!=started.date().isoformat() or completed.date()!=started.date():
        raise ValueError('Single current response vintage required; collection crossed midnight')
    sources={}
    for sid,items in inputs['sources'].items():
        expected_kinds={'observations'} if sid in model.PUBLISHER else {'definition','observations'}
        if set(items)!=expected_kinds:raise ValueError('Original inventory differs')
        result={}
        for kind,item in items.items():
            if item.get('error'):
                if set(item)!= {'error'} or not re.fullmatch(r'[a-z0-9_]{1,80}',item['error']): raise ValueError('Unsafe failure descriptor')
                result[kind]=item;result['error']='source_unavailable';continue
            ref=item['evidence'];digest=ref.get('sha256','')
            if not started<=model.stamp(item['acquired_at'])<=completed: raise ValueError('Original acquisition clock differs')
            if (not re.fullmatch('[a-f0-9]{64}',digest) or ref.get('key')!=PRIVATE+digest+'.bin'
                or ref.get('request_url')!=model.source_url(sid,kind,inputs['evaluation_date']) or ref.get('provider')!=('Cboe' if sid in model.PUBLISHER else 'fred')):
                raise ValueError('Protected source request identity differs')
            raw=read(ref['key'])
            if model.sha(raw)!=digest or type(ref.get('bytes')) is not int or len(raw)!=ref['bytes']: raise ValueError('Original bytes differ')
            result[kind]={**item,'raw':raw}
        sources[sid]=result
    output=model.compute(sources,inputs['generated_at'])
    output['collection']={'started_at':inputs['started_at'],'completed_at':inputs['generated_at'],
        'source_count':len(model.LABELS),'request_count':2*len(model.CATALOG)+len(model.PUBLISHER),
        'method':'Complete requested FRED current-vintage definition/observation pairs and direct publisher CSV originals.',
        'original_replay_access':'Protected AWS IAM source archive; full new histories are not redistributed anonymously.'}
    output['elapsed_s']=round((completed-started).total_seconds(),3)
    output['history_reference']={'legacy_key':'data/vol-surface-history.json','role':'legacy_unverified_snapshots',
        'used_in_native_calculations':False,'native_original_history_access':'Protected AWS IAM source archive',
        'note':'Previous hourly snapshots remain dated and unchanged; they are not independent daily observations or qualified backtests.'}
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
    if key != PREFIX+'runs/'+model.sha(raw)+'.json' or manifest.get('contract') != 'volatility-native-replay.v1':
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
    manifest = {'contract': 'volatility-native-replay.v1', 'generated_at': output['generated_at'], **refs,
        'compilers': compilers, 'output_sha256': refs['output']['sha256'],
        'scope': 'Retained complete FRED definitions/observations and publisher CSV files, strict index units, matching dates and explicit reference windows. No forecast, futures curve, tail probability or historical availability claim.'}
    raw = model.encoded(manifest); key = PREFIX+'runs/'+model.sha(raw)+'.json'; immutable(client, bucket, key, raw)
    ref = {'manifest_key': key, 'output_sha256': refs['output']['sha256']}
    if replay(ref, reader(client, bucket)) != output: raise ValueError('retained replay differs')
    return ref


def publish(client, bucket, packet):
    at = model.stamp(packet['generated_at'])
    for _ in range(4):
        try:
            obj = client.get_object(Bucket=bucket, Key=CURRENT); raw = bounded(obj['Body']); old = json.loads(raw)
            old_day = old.get('as_of') or old.get('data_date')
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


def run(client, bucket, request_id, execution_id, credential, remaining_seconds=300):
    started = now(); key = request_key(request_id)
    status = {'contract': 'volatility-public-request.v1', 'request_id': request_id, 'execution_id': execution_id,
        'started_at': started, 'status': 'running', 'phase': 'collect'}
    try: status_write(client, bucket, key, status, IfNoneMatch='*')
    except Exception as exc:
        if not conflict(exc): raise
        return json.loads(bounded(client.get_object(Bucket=bucket, Key=key)['Body']))
    try:
        deadline = time.monotonic()+max(0, min(210, remaining_seconds-80))
        sources = collect(client, bucket, credential, model.stamp(started).date().isoformat(), deadline)
        inputs = {'contract': 'volatility-native-inputs.v1', 'started_at': started, 'generated_at': now(),
            'sources': sources, 'evaluation_date': model.stamp(started).date().isoformat()}
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
        raise RuntimeError('native volatility publication failed; inspect reviewed request evidence') from None
