"""Retained coefficient replay and conditional publication; no source API."""
from collections import OrderedDict,deque
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Lock
import json,re,sys
import option_population_research as desk
import option_population_model as model
import option_flow_research as upstream
import option_flow_store as source_store

COMPILERS=(*source_store.COMPILERS,model,desk,sys.modules[__name__])
PRIVATE=upstream.PRIVATE
PREFIX=model.PREFIX
MAX=model.MAX_ARTIFACT
now=source_store.now
bounded=source_store.bounded
code=source_store.code
missing=source_store.missing
conflict=source_store.conflict


def artifact(key):
    return isinstance(key,str) and bool(re.fullmatch(re.escape(PREFIX)+
        r'(?:groups|chains|inputs|outputs|runs)/[a-f0-9]{64}\.json|'+re.escape(PREFIX)+r'compilers/[a-f0-9]{64}\.py',key))


def ref(raw,kind):
    if kind not in ('groups','chains','inputs','outputs','runs','compilers'):raise ValueError('Reviewed population artifact required')
    if not isinstance(raw,bytes) or not 0<len(raw)<=MAX:raise ValueError('Bounded exact artifact bytes required')
    digest=upstream.sha(raw)
    return {'key':PREFIX+kind+'/'+digest+('.py' if kind=='compilers' else '.json'),'sha256':digest,'bytes':len(raw)}


def reader(client,bucket,capacity=128*1024*1024):
    parent=source_store.reader(client,bucket);cache=OrderedDict();size=0;lock=Lock()
    def remember(key,raw):
        nonlocal size
        if not artifact(key) or len(raw)>MAX or key.rsplit('/',1)[-1].split('.')[0]!=upstream.sha(raw):
            raise ValueError('Verified content-addressed population bytes required')
        if len(raw)>capacity:return
        with lock:
            if key in cache:size-=len(cache.pop(key))
            while cache and size+len(raw)>capacity:
                _,old=cache.popitem(last=False);size-=len(old)
            cache[key]=raw;size+=len(raw)
    def read(key):
        if artifact(key):
            with lock:
                if key in cache:cache.move_to_end(key);return cache[key]
            raw=bounded(client.get_object(Bucket=bucket,Key=key)['Body']);remember(key,raw);return raw
        if key in desk.PREDECESSORS:return bounded(client.get_object(Bucket=bucket,Key=key)['Body'])
        return parent(key)
    read.remember=remember
    return read


def immutable(client,bucket,key,raw,read):
    if not artifact(key) or not isinstance(raw,bytes) or not 0<len(raw)<=MAX or key.rsplit('/',1)[-1].split('.')[0]!=upstream.sha(raw):
        raise ValueError('Reviewed immutable population bytes required')
    try:client.put_object(Bucket=bucket,Key=key,Body=raw,IfNoneMatch='*',
        ContentType='text/x-python' if key.endswith('.py') else 'application/json',CacheControl='public, max-age=31536000, immutable')
    except Exception as exc:
        if not conflict(exc):raise
    if bounded(client.get_object(Bucket=bucket,Key=key)['Body'])!=raw:raise ValueError('Population artifact readback differs')
    read.remember(key,raw)


def checked(identity,kind,read):
    if not isinstance(identity,dict) or not re.fullmatch(re.escape(PREFIX+kind+'/')+r'[a-f0-9]{64}\.json',identity.get('key','')):
        raise ValueError('Reviewed population reference required')
    raw=read(identity['key'])
    if identity!=ref(raw,kind):raise ValueError('Population evidence bytes differ')
    return json.loads(raw)


class Writer:
    def __init__(self,client,bucket,read):self.client,self.bucket,self.read=client,bucket,read;self.pool=ThreadPoolExecutor(max_workers=6);self.pending=deque()
    def __enter__(self):return self
    def __call__(self,key,raw):
        if len(self.pending)>=12:self.pending.popleft().result()
        self.pending.append(self.pool.submit(immutable,self.client,self.bucket,key,raw,self.read))
    def __exit__(self,exc_type,exc,tb):
        try:
            if exc_type is None:
                while self.pending:self.pending.popleft().result()
        finally:self.pool.shutdown(wait=True,cancel_futures=True)


def verified_run(identity,read):
    if not isinstance(identity,dict) or set(identity)!={'manifest_key','output_sha256'} or not re.fullmatch('[a-f0-9]{64}',identity.get('output_sha256','')):
        raise ValueError('Exact population run required')
    key=identity['manifest_key']
    if not re.fullmatch(re.escape(PREFIX+'runs/')+r'[a-f0-9]{64}\.json',key):raise ValueError('Reviewed population run path required')
    raw=read(key);run=json.loads(raw)
    if key!=ref(raw,'runs')['key'] or run.get('contract')!='option-population-replay.v1' or run.get('output_sha256')!=identity['output_sha256']:
        raise ValueError('Population run bytes differ')
    if set(run['compilers'])!={m.__name__ for m in COMPILERS}:raise ValueError('Population compiler inventory differs')
    for module in COMPILERS:
        body=Path(module.__file__).read_bytes();expected=ref(body,'compilers')
        if run['compilers'][module.__name__]!=expected or read(expected['key'])!=body:raise ValueError('Reviewed population compiler bytes differ')
    return run


def replay(identity,read):
    run=verified_run(identity,read);inputs=checked(run['input'],'inputs',read)
    def verify(key,raw):
        if read(key)!=raw:raise ValueError('Reconstructed population artifact differs')
    output=desk.build(inputs,read,verify)
    if output!=checked(run['output'],'outputs',read) or upstream.sha(upstream.encoded(output))!=identity['output_sha256']:
        raise ValueError('Population output replay differs')
    return output


def retain(client,bucket,inputs,output,read,checkpoint):
    refs={}
    for label,value in (('input',inputs),('output',output)):
        raw=upstream.encoded(value);refs[label]=ref(raw,label+'s');immutable(client,bucket,refs[label]['key'],raw,read)
    compilers={}
    for module in COMPILERS:
        raw=Path(module.__file__).read_bytes();identity=ref(raw,'compilers');compilers[module.__name__]=identity
        immutable(client,bucket,identity['key'],raw,read)
    run={'contract':'option-population-replay.v1','generated_at':output['generated_at'],**refs,
        'compilers':compilers,'output_sha256':refs['output']['sha256']}
    raw=upstream.encoded(run);key=ref(raw,'runs')['key'];immutable(client,bucket,key,raw,read)
    identity={'manifest_key':key,'output_sha256':refs['output']['sha256']};checkpoint(candidate_replay=identity)
    if replay(identity,read)!=output:raise ValueError('Original population replay differs')
    return identity


def snapshot(client,bucket,key,read):
    if key not in (*desk.PREDECESSORS,upstream.CURRENT):raise ValueError('Reviewed whole public input required')
    try:raw=read(key)
    except Exception as exc:
        if key!=desk.CURRENT or not missing(exc):raise
        return None
    if not isinstance(json.loads(raw),dict):raise ValueError('Structured whole population input required')
    return source_store.protect(client,bucket,raw)


def conditional(client,bucket,key,packet,publish=None):
    if key not in (desk.CURRENT,desk.LEGACY):raise ValueError('Reviewed population public head required')
    compiled=upstream.clock(packet['generated_at']);source=upstream.clock(packet['source_capture_completed_at'])
    if source>compiled or compiled>upstream.clock(now()):raise ValueError('Population publication clocks differ')
    for _ in range(4):
        try:
            obj=client.get_object(Bucket=bucket,Key=key);raw=bounded(obj['Body']);old=json.loads(raw)
            retiring=key==desk.LEGACY and desk.is_legacy(old)
            if not retiring:
                old_source=upstream.clock(old['source_capture_completed_at']);old_compiled=upstream.clock(old['generated_at'])
                if old_source>source or old_source==source and old_compiled>compiled:return False
                if old_source==source and old_compiled==compiled and old!=packet:raise ValueError('Conflicting same-clock population publication')
            if old==packet:return True
            source_store.protect(client,bucket,raw);condition={'IfMatch':obj['ETag']}
        except Exception as exc:
            if not missing(exc):raise
            condition={'IfNoneMatch':'*'}
        try:
            if publish is not None and key==desk.CURRENT:publish(client,bucket,key,upstream.encoded(packet),condition)
            else:client.put_object(Bucket=bucket,Key=key,Body=upstream.encoded(packet),ContentType='application/json',CacheControl='no-store',**condition)
            live=json.loads(bounded(client.get_object(Bucket=bucket,Key=key)['Body']))
            if live!=packet:
                live_order=(upstream.clock(live['source_capture_completed_at']),upstream.clock(live['generated_at']))
                if live_order<=(source,compiled):raise ValueError('Population public readback differs')
            return True
        except Exception as exc:
            if not conflict(exc):raise
    raise RuntimeError('Population publication contention; immutable evidence retained')


def run(client,bucket,request_id,execution_id,recover_run=None,publish=None,publish_current=True):
    key=source_store.request_key('population:'+request_id)
    status={'contract':'option-population-request.v1','request_id':request_id,'execution_id':execution_id,
        'started_at':now(),'status':'running','phase':'preserve','publish_current':publish_current}
    try:source_store.status_write(client,bucket,key,status,IfNoneMatch='*')
    except Exception as exc:
        if not conflict(exc):raise
        return json.loads(bounded(client.get_object(Bucket=bucket,Key=key)['Body']))
    def checkpoint(**fields):status.update(fields);source_store.status_write(client,bucket,key,status)
    try:
        read=reader(client,bucket);expected=None
        if recover_run is not None:
            old=verified_run(recover_run,read);inputs=checked(old['input'],'inputs',read);expected=checked(old['output'],'outputs',read)
            checkpoint(recovered_from=recover_run)
        else:
            source=snapshot(client,bucket,upstream.CURRENT,read)
            predecessors={key:snapshot(client,bucket,key,read) for key in desk.PREDECESSORS}
            inputs={'contract':'option-population-inputs.v1','compiled_at':now(),
                'source_publication':source,'predecessors':predecessors}
        raw=upstream.encoded(inputs);identity=ref(raw,'inputs');immutable(client,bucket,identity['key'],raw,read)
        checkpoint(phase='compile',retained_input=identity)
        with Writer(client,bucket,read) as emit:output=desk.build(inputs,read,emit)
        if expected is not None and output!=expected:raise ValueError('Recovered population output differs')
        checkpoint(phase='retained_replay');identity=retain(client,bucket,inputs,output,read,checkpoint)
        checkpoint(phase='publish');published=alias=False
        if publish_current and output['quality']['status']!='unavailable':
            packet={**output,'replay':identity};published=conditional(client,bucket,desk.CURRENT,packet,publish)
            if published:alias=conditional(client,bucket,desk.LEGACY,desk.compatibility(packet))
        checkpoint(status='complete',phase='complete',completed_at=now(),generated_at=output['generated_at'],
            source_capture_completed_at=output['source_capture_completed_at'],source_run=output['source_run'],
            published=published,compatibility_published=alias,replay=identity,quality=output['quality'],
            provider_requests=0,private_account_reads=0,paid_ai_calls=0,notifications_sent=0,signals_emitted=0,portfolio_writes=0)
        return status
    except Exception as exc:
        checkpoint(status='failed',completed_at=now(),error='population_original_replay_or_publication_failed',failure_class=type(exc).__name__)
        raise RuntimeError('Native population research failed; inspect retained request evidence') from None
