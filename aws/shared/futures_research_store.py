"""Bounded original futures capture, exact replay and conditional publication."""
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime,timedelta
from pathlib import Path
from threading import Lock
from zoneinfo import ZoneInfo
import json,re,sys,time
import futures_research_model as model
import futures_source_capture as capture
import futures_session_calendar as calendar
import option_snapshot_capture as primitives

COMPILERS=(primitives,capture,calendar,model,sys.modules[__name__])
POST_CAPTURE_RESERVE_SECONDS=45
now=capture.now


def code(exc):return str(getattr(exc,'response',{}).get('Error',{}).get('Code',''))
def missing(exc):return code(exc) in ('404','NoSuchKey')
def conflict(exc):return code(exc) in ('409','412','PreconditionFailed','ConditionalRequestConflict')
def bounded(stream):
    try:raw=stream.read(model.MAX+1)
    finally:stream.close()
    if not 0<len(raw)<=model.MAX:raise ValueError('Bounded futures evidence required')
    return raw
def artifact(key):
    return isinstance(key,str) and bool(re.fullmatch(re.escape(model.PRIVATE)+r'[a-f0-9]{64}\.bin|'
        +re.escape(model.PREFIX)+r'(?:inputs|outputs|runs|records)/[a-f0-9]{64}\.json|'
        +re.escape(model.PREFIX)+r'compilers/[a-f0-9]{64}\.py',key))


def reader(client,bucket,capacity=64*1024*1024):
    cache=OrderedDict();size=0;lock=Lock()
    def remember(key,raw):
        nonlocal size
        if (not artifact(key) or not isinstance(raw,bytes) or not 0<len(raw)<=model.MAX
                or key.rsplit('/',1)[-1].split('.')[0]!=model.sha(raw)):raise ValueError('Exact futures evidence bytes required')
        if len(raw)>capacity:return
        with lock:
            if key in cache:size-=len(cache.pop(key))
            while cache and size+len(raw)>capacity:
                _,old=cache.popitem(last=False);size-=len(old)
            cache[key]=raw;size+=len(raw)
    def read(key):
        if not artifact(key):raise ValueError('Reviewed immutable futures evidence path required')
        with lock:
            if key in cache:cache.move_to_end(key);return cache[key]
        raw=bounded(client.get_object(Bucket=bucket,Key=key)['Body']);remember(key,raw);return raw
    def cached(key):
        with lock:return cache.get(key)
    read.remember=remember;read.cached=cached;return read


def immutable(client,bucket,ref,raw,read):
    key=ref.get('key')
    if (not artifact(key) or not isinstance(raw,bytes) or not 0<len(raw)<=model.MAX
            or ref!={'key':key,'sha256':model.sha(raw),'bytes':len(raw)}
            or key.rsplit('/',1)[-1].split('.')[0]!=model.sha(raw)):raise ValueError('Exact bounded futures artifact write required')
    private=key.startswith(model.PRIVATE)
    prior=read.cached(key) if hasattr(read,'cached') else None
    if prior is not None:
        if prior!=raw:raise ValueError('Cached immutable futures bytes differ')
        return
    try:client.put_object(Bucket=bucket,Key=key,Body=raw,IfNoneMatch='*',
        ContentType='application/octet-stream' if private else 'text/x-python' if key.endswith('.py') else 'application/json',
        CacheControl='no-store' if private else 'public, max-age=31536000, immutable')
    except Exception as exc:
        if not conflict(exc):raise
    if bounded(client.get_object(Bucket=bucket,Key=key)['Body'])!=raw:raise ValueError('futures immutable readback differs')
    read.remember(key,raw)
def protect(client,bucket,raw,read):
    ref={'key':model.PRIVATE+model.sha(raw)+'.bin','sha256':model.sha(raw),'bytes':len(raw)}
    immutable(client,bucket,ref,raw,read);return ref
def checked(ref,kind,read):
    if not isinstance(ref,dict) or not artifact(ref.get('key')):raise ValueError('Exact futures artifact reference required')
    raw=read(ref['key'])
    if ref!=model.ref(raw,kind):raise ValueError('futures artifact reference differs')
    return model.strict(raw)
def snapshot(client,bucket,key,read):
    if key not in (model.LEGACY,model.CURRENT):raise ValueError('Reviewed futures predecessor required')
    try:raw=bounded(client.get_object(Bucket=bucket,Key=key)['Body'])
    except Exception as exc:
        if not missing(exc):raise
        return None
    if not isinstance(model.strict(raw),dict):raise ValueError('Whole structured futures predecessor required')
    return {'source_key':key,'original':protect(client,bucket,raw,read),'acquired_at':now()}


def request_key(request_id):
    if not isinstance(request_id,str) or not re.fullmatch('[A-Za-z0-9._:-]{1,160}',request_id):raise ValueError('Durable bounded futures request identity required')
    return model.PRIVATE+'requests/'+model.sha(request_id.encode())+'.json'
def status_write(client,bucket,key,value,**conditions):
    if not isinstance(key,str) or not re.fullmatch(re.escape(model.PRIVATE)+r'requests/[a-f0-9]{64}(?:/datasets/[a-f0-9]{64})?\.json',key):
        raise ValueError('Reviewed private futures request path required')
    client.put_object(Bucket=bucket,Key=key,Body=model.encoded(value),ContentType='application/json',CacheControl='no-store',**conditions)


def collect(client,bucket,credential,read,deadline,attempt_key):
    if not credential:raise ValueError('Existing managed futures credential required')
    if not isinstance(attempt_key,str) or not re.fullmatch(re.escape(model.PRIVATE)+r'requests/[a-f0-9]{64}\.json',attempt_key):
        raise ValueError('Reviewed durable futures attempt required')
    end=datetime.now(ZoneInfo('America/New_York')).date();start=end-timedelta(days=90)
    budget=capture.Budget()
    def one(scope):
        name=scope['product']+':'+scope['kind']+(':'+scope['ticker'] if scope['ticker'] else '')
        def checkpoint(value):status_write(client,bucket,attempt_key[:-5]+'/datasets/'+model.sha(name.encode())+'.json',value)
        result=capture.collect(scope,credential,deadline,lambda raw:protect(client,bucket,raw,read),budget,checkpoint)
        return name,result
    initial=[capture.spec(kind,product,start.isoformat(),end.isoformat())
        for product in capture.PRODUCTS for kind in ('products','contracts','schedules')]
    with ThreadPoolExecutor(max_workers=4) as pool:sources=dict(pool.map(one,initial))
    selected={}
    for product in capture.PRODUCTS:
        source=sources[product+':contracts'];rows,_,complete=model.source_rows(source,now(),read)
        selected[product]=capture.select_contracts(rows,product,end.isoformat(),complete)
    bars=[capture.spec('bars',product,start.isoformat(),end.isoformat(),item['ticker'])
        for product,selection in selected.items() for item in selection['selected']]
    with ThreadPoolExecutor(max_workers=4) as pool:sources.update(dict(pool.map(one,bars)))
    return {'sources':sources,'provider_requests':budget.requests,'source_bytes':budget.bytes}


def compile_output(inputs,read,emit):
    if (not isinstance(inputs,dict) or inputs.get('contract')!='futures-original-inputs.v1'
            or set(inputs.get('predecessors',{}))!={model.LEGACY,model.CURRENT}):raise ValueError('Complete futures input inventory required')
    for key,value in inputs['predecessors'].items():
        if value is None:
            if key==model.LEGACY:raise ValueError('Whole legacy futures predecessor required')
            continue
        if value.get('source_key')!=key or model.clock(value['acquired_at'])>model.clock(inputs['generated_at']):
            raise ValueError('futures predecessor identity or capture clock differs')
        old=model.strict(model.checked_original(value['original'],read))
        if not isinstance(old,dict):raise ValueError('Whole futures predecessor required')
        legacy=(key==model.LEGACY and old.get('engine')=='justhodl-polygon-futures-curves' and old.get('version')=='2.0.1'
            and isinstance(old.get('product_data'),dict) and isinstance(old.get('identity'),dict))
        native=old.get('contract')==('futures-original-compatibility.v1' if key==model.LEGACY else model.CONTRACT)
        if not (legacy or native):raise ValueError('Unreviewed futures predecessor schema')
    return {**model.compile_output(inputs['sources'],inputs['generated_at'],read,emit),'predecessors':inputs['predecessors']}


def materialize(client,bucket,inputs,read):
    """Independent content-addressed blocks may write concurrently; all must verify."""
    with ThreadPoolExecutor(max_workers=4) as pool:
        pending={};total=0
        def emit(kind,doc):
            nonlocal total
            raw=model.encoded(doc);ref=model.ref(raw,kind)
            if ref['key'] not in pending:
                total+=len(raw)
                if len(pending)>=2048 or total>64*1024*1024:raise ValueError('Bounded futures record materialization required')
                pending[ref['key']]=pool.submit(immutable,client,bucket,ref,raw,read)
            return ref
        output=compile_output(inputs,read,emit)
        for future in pending.values():future.result()
    return output


def verified_run(identity,read):
    if (not isinstance(identity,dict) or set(identity)!={'manifest_key','output_sha256'}
            or not isinstance(identity['manifest_key'],str) or not re.fullmatch(re.escape(model.PREFIX)+r'runs/[a-f0-9]{64}\.json',identity['manifest_key'])
            or not isinstance(identity['output_sha256'],str) or not re.fullmatch('[a-f0-9]{64}',identity['output_sha256'])):
        raise ValueError('Reviewed futures replay identity required')
    raw=read(identity['manifest_key']);run=model.strict(raw)
    if (model.ref(raw,'runs')['key']!=identity['manifest_key'] or run.get('contract')!='futures-original-replay.v1'
            or run.get('output_sha256')!=identity['output_sha256']):raise ValueError('futures run bytes differ')
    if set(run.get('compilers',{}))!={m.__name__ for m in COMPILERS}:raise ValueError('Complete futures compiler inventory required')
    for module in COMPILERS:
        raw=Path(module.__file__).read_bytes();ref=model.ref(raw,'compilers')
        if run['compilers'][module.__name__]!=ref or read(ref['key'])!=raw:raise ValueError('Reviewed futures compiler bytes differ')
    return run
def replay(identity,read):
    run=verified_run(identity,read);inputs=checked(run['input'],'inputs',read)
    def verify(kind,doc):
        raw=model.encoded(doc);ref=model.ref(raw,kind)
        if read(ref['key'])!=raw:raise ValueError('Reconstructed futures bar block differs')
        return ref
    output=compile_output(inputs,read,verify)
    if (output!=checked(run['output'],'outputs',read) or model.sha(model.encoded(output))!=identity['output_sha256']
            or output['generated_at']!=run['generated_at']):raise ValueError('Original futures replay differs')
    return output
def retain(client,bucket,inputs,output,read,checkpoint):
    refs={};compilers={}
    for label,doc in (('input',inputs),('output',output)):
        raw=model.encoded(doc);ref=model.ref(raw,label+'s');immutable(client,bucket,ref,raw,read);refs[label]=ref
    for module in COMPILERS:
        raw=Path(module.__file__).read_bytes();ref=model.ref(raw,'compilers');immutable(client,bucket,ref,raw,read);compilers[module.__name__]=ref
    run={'contract':'futures-original-replay.v1','generated_at':output['generated_at'],**refs,
        'compilers':compilers,'output_sha256':refs['output']['sha256']}
    raw=model.encoded(run);ref=model.ref(raw,'runs');immutable(client,bucket,ref,raw,read)
    identity={'manifest_key':ref['key'],'output_sha256':refs['output']['sha256']};checkpoint(candidate_replay=identity)
    if replay(identity,read)!=output:raise ValueError('Retained futures reconstruction differs')
    return identity


def compatibility(packet):
    if packet.get('contract')!=model.CONTRACT or 'replay' not in packet or any(packet.get(k) is not False for k in model.FLAGS):
        raise ValueError('Native descriptive futures publication required')
    return {'contract':'futures-original-compatibility.v1','generated_at':packet['generated_at'],
        'canonical':{'key':model.CURRENT,'replay':packet['replay']},'status':'superseded_by_original_futures_research',
        'source_capture_completed_at':packet['source_capture_completed_at'],
        'definition_date':packet['definition_date'],'dataset_capture_clocks':{p:v.isoformat() for p,v in clocks(packet).items()},
        'retained_predecessor':packet['predecessors'][model.LEGACY],
        'signals':[],'product_data':{},'identity':{},'identity_ok':False,'call':None,'score':None,'portfolio_action':'WAIT',
        'independent_investment_votes':0,**model.PERMISSIONS,
        'meaning':'Dated contract measurements replace unqualified signals. Empty legacy collections are not zero prices or a risk regime.'}
def clocks(packet):
    if packet.get('contract')==model.CONTRACT:
        return {name:model.clock(d['source_capture_completed_at']) for name,d in packet['datasets'].items()}
    if packet.get('contract')=='futures-original-compatibility.v1':return {p:model.clock(v) for p,v in packet['dataset_capture_clocks'].items()}
    raise ValueError('Reviewed native futures head contract required')
def conditional(client,bucket,key,packet,read,publish=None):
    if key not in (model.CURRENT,model.LEGACY):raise ValueError('Reviewed futures public target required')
    if (packet.get('contract')!=('futures-original-compatibility.v1' if key==model.LEGACY else model.CONTRACT)
            or any(packet.get(k) is not False for k in model.FLAGS)):raise ValueError('Descriptive futures publication contract required')
    stamp=model.clock(packet['generated_at']);new_clocks=clocks(packet)
    required={p+':'+k for p in capture.PRODUCTS for k in ('products','contracts','schedules')}
    if stamp>model.clock(now()) or not required<=set(new_clocks) or any(t>stamp for t in new_clocks.values()):
        raise ValueError('Consistent futures publication clocks required')
    for _ in range(4):
        try:
            obj=client.get_object(Bucket=bucket,Key=key);raw=bounded(obj['Body']);old=model.strict(raw)
            retiring=(key==model.LEGACY and old.get('engine')=='justhodl-polygon-futures-curves' and old.get('version')=='2.0.1'
                and isinstance(old.get('product_data'),dict) and isinstance(old.get('identity'),dict))
            if retiring:
                if model.checked_original(packet['retained_predecessor']['original'],read)!=raw:raise ValueError('futures legacy head changed after capture')
            else:
                old_clocks=clocks(old)
                if not required<=set(old_clocks):raise ValueError('Whole prior futures catalog inventory required')
                if (model.clock(old['generated_at'])>stamp or packet['definition_date']<old['definition_date']
                        or any(new_clocks[p]<old_clocks[p] for p in set(new_clocks)&set(old_clocks))):return False
                if model.clock(old['generated_at'])==stamp and old!=packet:raise ValueError('Conflicting same-clock futures publication')
            if old==packet:return True
            protect(client,bucket,raw,read);condition={'IfMatch':obj['ETag']}
        except Exception as exc:
            if not missing(exc):raise
            condition={'IfNoneMatch':'*'}
        try:
            raw=model.encoded(packet)
            if publish is not None and key==model.CURRENT:publish(client,bucket,key,raw,condition)
            else:client.put_object(Bucket=bucket,Key=key,Body=raw,ContentType='application/json',CacheControl='no-store',**condition)
            live=model.strict(bounded(client.get_object(Bucket=bucket,Key=key)['Body']))
            if live!=packet:
                later=clocks(live)
                if (model.clock(live['generated_at'])<=stamp or live['definition_date']<packet['definition_date']
                        or not required<=set(later) or any(later[p]<new_clocks[p] for p in set(later)&set(new_clocks))):raise ValueError('futures public readback differs')
            return True
        except Exception as exc:
            if not conflict(exc):raise
    raise RuntimeError('futures publication contention; immutable evidence retained')


def run(client,bucket,request_id,execution_id,credential='',remaining_seconds=90,recover_run=None,publish=None,publish_current=True):
    if not isinstance(execution_id,str) or not execution_id:raise ValueError('Durable futures execution identity required')
    key=request_key(request_id);end=time.monotonic()+max(1,min(90,remaining_seconds))
    status={'contract':'futures-original-request.v1','request_id':request_id,'execution_id':execution_id,
        'started_at':now(),'status':'running','phase':'preserve','publish_current':publish_current}
    try:status_write(client,bucket,key,status,IfNoneMatch='*')
    except Exception as exc:
        if not conflict(exc):raise
        return model.strict(bounded(client.get_object(Bucket=bucket,Key=key)['Body']))
    def checkpoint(**fields):status.update(fields);status_write(client,bucket,key,status)
    try:
        read=reader(client,bucket);expected=None
        if recover_run is not None:
            old=verified_run(recover_run,read);inputs=checked(old['input'],'inputs',read)
            expected=checked(old['output'],'outputs',read);checkpoint(recovered_from=recover_run)
        else:
            if end-time.monotonic()<=POST_CAPTURE_RESERVE_SECONDS+5:raise RuntimeError('Insufficient futures collection and replay budget')
            predecessors={name:snapshot(client,bucket,name,read) for name in (model.LEGACY,model.CURRENT)}
            if predecessors[model.LEGACY] is None:raise ValueError('Whole existing futures predecessor required')
            checkpoint(phase='collect_originals',predecessors=predecessors)
            collection=collect(client,bucket,credential,read,end-POST_CAPTURE_RESERVE_SECONDS,key)
            inputs={'contract':'futures-original-inputs.v1','generated_at':now(),'predecessors':predecessors,**collection}
        raw=model.encoded(inputs);ref=model.ref(raw,'inputs');immutable(client,bucket,ref,raw,read)
        checkpoint(phase='compile',retained_input=ref)
        output=materialize(client,bucket,inputs,read)
        if expected is not None and output!=expected:raise ValueError('Recovered futures output differs')
        identity=retain(client,bucket,inputs,output,read,checkpoint);checkpoint(phase='publish')
        published=alias=False
        usable=any(c['comparisons']['close']['1']['available'] for p in output['products'].values() for c in p['contracts'])
        if publish_current and usable:
            # Detect a concurrent legacy writer before publishing either head.
            # The alias still uses its own conditional write to close the race.
            live_legacy=bounded(client.get_object(Bucket=bucket,Key=model.LEGACY)['Body'])
            if model.strict(live_legacy).get('contract')!='futures-original-compatibility.v1':
                if model.checked_original(output['predecessors'][model.LEGACY]['original'],read)!=live_legacy:
                    raise ValueError('futures predecessor changed before publication')
            packet={**output,'replay':identity};published=conditional(client,bucket,model.CURRENT,packet,read,publish)
            if published:alias=conditional(client,bucket,model.LEGACY,compatibility(packet),read)
        checkpoint(status='complete',phase='complete',completed_at=now(),generated_at=output['generated_at'],
            replay=identity,published=published,compatibility_published=alias,quality=output['quality'],
            provider_requests_this_execution=0 if recover_run is not None else inputs['provider_requests'],
            engine_invocations=0,private_account_reads=0,paid_ai_calls=0,notifications_sent=0,portfolio_writes=0)
        return status
    except Exception as exc:
        checkpoint(status='failed',completed_at=now(),error='futures_original_replay_or_publication_failed',failure_class=type(exc).__name__)
        raise RuntimeError('Native futures research failed; inspect retained request evidence') from None
