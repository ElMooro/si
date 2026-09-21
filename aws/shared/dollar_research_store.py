"""Retain, reconstruct and conditionally publish original-source Dollar research.

Existing canonical originals only: no credentials, provider collection, private
account reads, notifications, AI or downstream invocation.
"""
from collections import OrderedDict
from datetime import datetime,timezone
from pathlib import Path
from threading import Lock
import gzip,io,json,re,sys
import dollar_research_model as model
import dollar_research_catalog as catalog
import canonical_fred_replay,report_observations,research_brief_model,evidence_store

MAX=32*1024*1024
SOURCES=('data/report-measurements.json',model.CURRENT,model.HISTORY,*catalog.CONTEXT_KEYS)
COMPILERS=(model,catalog,canonical_fred_replay,report_observations,research_brief_model,evidence_store,sys.modules[__name__])


def now():return datetime.now(timezone.utc).isoformat()
def code(exc):return str(getattr(exc,'response',{}).get('Error',{}).get('Code',''))
def missing(exc):return code(exc) in ('404','NoSuchKey')
def conflict(exc):return code(exc) in ('409','412','PreconditionFailed','ConditionalRequestConflict')
def bounded(stream):
    try:raw=stream.read(MAX+1)
    finally:stream.close()
    if not 0<len(raw)<=MAX:raise ValueError('Dollar evidence byte bound')
    return raw
def artifact(key):
    return isinstance(key,str) and bool(re.fullmatch(re.escape(model.PRIVATE)+r'[a-f0-9]{64}\.bin|'
        +re.escape(model.PREFIX)+r'(?:inputs|outputs|runs)/[a-f0-9]{64}\.json|'
        +re.escape(model.PREFIX)+r'compilers/[a-f0-9]{64}\.py',key))
def canonical(key):
    return isinstance(key,str) and bool(re.fullmatch(r'data/(?:report-research/(?:runs|compilers)/[a-f0-9]{64}\.(?:json|py)|evidence/fred/[a-f0-9]{64}/[a-f0-9]{64}\.bin\.gz)',key))
def reader(client,bucket,capacity=32*1024*1024):
    cache=OrderedDict();size=0;lock=Lock()
    def remember(key,raw):
        nonlocal size
        if not (artifact(key) or canonical(key)) or key.rsplit('/',1)[-1].split('.')[0]!=model.sha(raw):raise ValueError('Verified immutable source bytes required')
        if len(raw)>capacity:return
        with lock:
            if key in cache:size-=len(cache.pop(key))
            while cache and size+len(raw)>capacity:
                _,old=cache.popitem(last=False);size-=len(old)
            cache[key]=raw;size+=len(raw)
    def read(key):
        if key not in SOURCES and not (artifact(key) or canonical(key)):raise ValueError('Unreviewed Dollar research path')
        with lock:
            if key in cache:cache.move_to_end(key);return cache[key]
        raw=bounded(client.get_object(Bucket=bucket,Key=key)['Body'])
        if key.endswith('.gz'):raw=bounded(gzip.GzipFile(fileobj=io.BytesIO(raw)))
        if key not in SOURCES:remember(key,raw)
        return raw
    read.remember=remember
    return read
def identity(raw,kind):
    if kind not in ('originals','inputs','outputs','runs','compilers') or not isinstance(raw,bytes) or not 0<len(raw)<=MAX:raise ValueError('Bounded original or compiler artifact required')
    digest=model.sha(raw);key=model.PRIVATE+digest+'.bin' if kind=='originals' else model.PREFIX+kind+'/'+digest+('.py' if kind=='compilers' else '.json')
    return {'key':key,'sha256':digest,'bytes':len(raw)}
def immutable(client,bucket,ref,raw,read):
    key=ref.get('key')
    if (not artifact(key) or type(ref.get('bytes')) is not int or not 0<len(raw)<=MAX
        or ref['bytes']!=len(raw) or ref.get('sha256')!=model.sha(raw)
        or key.rsplit('/',1)[-1].split('.')[0]!=ref['sha256']):raise ValueError('Exact bounded immutable artifact required')
    kind='application/octet-stream' if key.startswith(model.PRIVATE) else 'text/x-python' if key.endswith('.py') else 'application/json'
    try:client.put_object(Bucket=bucket,Key=key,Body=raw,ContentType=kind,IfNoneMatch='*',
        CacheControl='no-store' if key.startswith(model.PRIVATE) else 'public, max-age=31536000, immutable')
    except Exception as exc:
        if not conflict(exc):raise
    if bounded(client.get_object(Bucket=bucket,Key=key)['Body'])!=raw:raise ValueError('Immutable Dollar readback differs')
    read.remember(key,raw)
def protect(client,bucket,raw,read):
    ref=identity(raw,'originals');immutable(client,bucket,ref,raw,read);return ref
def original(ref,read):
    if not model.valid_original(ref):raise ValueError('Reviewed retained original identity required')
    raw=read(ref['key'])
    if len(raw)!=ref['bytes'] or model.sha(raw)!=ref['sha256']:raise ValueError('Retained original bytes differ')
    return raw
def capture_inputs(client,bucket,read,checkpoint):
    captures={};total=0
    for key in SOURCES:
        try:
            raw=read(key);doc=json.loads(raw)
            if not isinstance(doc,(dict,list)):raise ValueError('Whole research object or history required')
            captures[key]={'source_key':key,'status':'retained','acquired_at':now(),'original':protect(client,bucket,raw,read)}
            total+=len(raw)
            if total>128*1024*1024:raise ValueError('Whole Dollar capture budget')
        except Exception as exc:
            if not missing(exc):raise
            captures[key]={'source_key':key,'status':'missing','acquired_at':now(),'original':None}
        checkpoint(captures=captures,captured_bytes=total)
    if any(captures[k]['status']!='retained' for k in SOURCES[:3]):raise ValueError('Canonical packet and both complete predecessors required')
    return {'contract':'dollar-original-inputs.v1','generated_at':now(),'captures':captures}
def compile_output(inputs,read):
    if inputs.get('contract')!='dollar-original-inputs.v1' or set(inputs.get('captures',{}))!=set(SOURCES):raise ValueError('Complete Dollar input inventory required')
    refs={};contexts={};packet=None
    for key,row in inputs['captures'].items():
        if row.get('source_key')!=key or model.clock(row['acquired_at'])>model.clock(inputs['generated_at']):raise ValueError('Capture identity or clock differs')
        if row.get('status')=='retained':doc=json.loads(original(row['original'],read));refs[key]=row['original']
        elif row.get('status')=='missing' and row.get('original') is None:doc=None;refs[key]=None
        else:raise ValueError('Reviewed source availability required')
        if key==SOURCES[0]:packet=doc
        if key in catalog.CONTEXT_KEYS:
            doc=doc if isinstance(doc,dict) else {}
            contexts[key]={'status':'retained_unqualified_context' if refs[key] else 'missing','original':refs[key],
                'reported_contract':doc.get('contract') if isinstance(doc.get('contract'),str) else None,
                'reported_generated_at':doc.get('generated_at') if isinstance(doc.get('generated_at'),str) else None,'independent_votes':0}
        # Whole input bytes remain protected and hash-bound. Parsed legacy
        # trees are not used for arithmetic and need not coexist in memory.
        del doc
    if not isinstance(packet,dict) or any(refs[k] is None for k in SOURCES[:3]):raise ValueError('Whole required predecessor and canonical sources required')
    originals=canonical_fred_replay.restore(packet,catalog.SERIES,read)
    return model.build(packet,originals,inputs['generated_at'],contexts,{k:refs[k] for k in (model.CURRENT,model.HISTORY)})
def checked(ref,kind,read):
    if (kind not in ('inputs','outputs') or not isinstance(ref,dict)
        or not re.fullmatch('[a-f0-9]{64}',ref.get('sha256','')) or ref.get('key')!=model.PREFIX+kind+'/'+ref['sha256']+'.json'
        or type(ref.get('bytes')) is not int):raise ValueError('Reviewed Dollar content identity required')
    raw=read(ref['key'])
    if len(raw)!=ref['bytes'] or model.sha(raw)!=ref['sha256']:raise ValueError('Dollar content bytes differ')
    return json.loads(raw)
def verified_run(ref,read):
    if (not isinstance(ref,dict) or set(ref)!={'manifest_key','output_sha256'}
        or not re.fullmatch(re.escape(model.PREFIX)+r'runs/[a-f0-9]{64}\.json',ref.get('manifest_key',''))
        or not re.fullmatch('[a-f0-9]{64}',ref.get('output_sha256',''))):raise ValueError('Exact Dollar run identity required')
    raw=read(ref['manifest_key']);run=json.loads(raw)
    if identity(raw,'runs')['key']!=ref['manifest_key'] or run.get('contract')!='dollar-original-replay.v1' or run.get('output_sha256')!=ref['output_sha256']:raise ValueError('Dollar run bytes differ')
    if set(run.get('compilers',{}))!={m.__name__ for m in COMPILERS}:raise ValueError('Whole compiler inventory required')
    for module in COMPILERS:
        raw=Path(module.__file__).read_bytes();expected=identity(raw,'compilers')
        if run['compilers'][module.__name__]!=expected or read(expected['key'])!=raw:raise ValueError('Matching reviewed Dollar compiler bytes required')
    return run
def replay(ref,read):
    run=verified_run(ref,read);inputs=checked(run['input'],'inputs',read);output=compile_output(inputs,read)
    if output!=checked(run['output'],'outputs',read) or model.sha(model.encoded(output))!=ref['output_sha256'] or output['generated_at']!=run['generated_at']:raise ValueError('Dollar original replay differs')
    return output
def retain(client,bucket,inputs,output,read,checkpoint):
    refs={};compilers={}
    for label,doc in (('input',inputs),('output',output)):
        raw=model.encoded(doc);ref=identity(raw,label+'s');immutable(client,bucket,ref,raw,read);refs[label]=ref
    for module in COMPILERS:
        raw=Path(module.__file__).read_bytes();ref=identity(raw,'compilers');immutable(client,bucket,ref,raw,read);compilers[module.__name__]=ref
    run={'contract':'dollar-original-replay.v1','generated_at':output['generated_at'],**refs,'compilers':compilers,'output_sha256':refs['output']['sha256']}
    raw=model.encoded(run);ref=identity(raw,'runs');immutable(client,bucket,ref,raw,read)
    out={'manifest_key':ref['key'],'output_sha256':refs['output']['sha256']};checkpoint(candidate_replay=out)
    if replay(out,read)!=output:raise ValueError('Retained Dollar reconstruction differs')
    return out
def conditional(client,bucket,packet,read):
    if packet.get('contract')!=model.CONTRACT or any(packet.get(k) is not False for k in model.PERMISSIONS):raise ValueError('Descriptive Dollar publication required')
    stamp=model.clock(packet['generated_at'])
    if stamp>model.clock(now()):raise ValueError('Future Dollar publication')
    previous=original(packet['retained_predecessors'][model.CURRENT],read)
    for _ in range(4):
        obj=client.get_object(Bucket=bucket,Key=model.CURRENT);raw=bounded(obj['Body']);old=json.loads(raw)
        if old==packet:return True
        if old.get('generated_at') and model.clock(old['generated_at'])>=stamp:
            if model.clock(old['generated_at'])==stamp:raise ValueError('Conflicting same-clock Dollar publication')
            return False
        if raw!=previous:return False  # Qualifies exactly the predecessor retained in this run.
        if old.get('contract')==model.CONTRACT:
            if model.clock(old['source_generated_at'])>model.clock(packet['source_generated_at']):return False
            for sid,row in old['series'].items():
                current=packet['series'].get(sid)
                if current is None:return False
                if row.get('acquired_at') and (not current.get('acquired_at') or model.clock(current['acquired_at'])<model.clock(row['acquired_at'])):return False
                old_date=(row.get('latest_observation') or {}).get('date');new_date=(current.get('latest_observation') or {}).get('date')
                if old_date and (not new_date or new_date<old_date):return False
        elif old.get('engine')!='justhodl-dollar-radar' or str(old.get('schema_version'))!='3.0':raise ValueError('Unreviewed Dollar predecessor')
        protect(client,bucket,raw,read)
        try:
            client.put_object(Bucket=bucket,Key=model.CURRENT,Body=model.encoded(packet),ContentType='application/json',CacheControl='no-store',IfMatch=obj['ETag'])
            actual=json.loads(bounded(client.get_object(Bucket=bucket,Key=model.CURRENT)['Body']))
            if actual!=packet and model.clock(actual['generated_at'])<=stamp:raise ValueError('Dollar publication readback differs')
            return True
        except Exception as exc:
            if not conflict(exc):raise
    raise RuntimeError('Dollar publication contention; recorded evidence retained')
def request_key(request_id):
    if not isinstance(request_id,str) or not re.fullmatch('[A-Za-z0-9_-]{1,120}',request_id):raise ValueError('Bounded durable Dollar request required')
    return model.PRIVATE+'requests/'+model.sha(request_id.encode())+'.json'
def status_write(client,bucket,key,doc,**condition):
    if not re.fullmatch(re.escape(model.PRIVATE)+r'requests/[a-f0-9]{64}\.json',key):raise ValueError('Private Dollar status path required')
    client.put_object(Bucket=bucket,Key=key,Body=model.encoded(doc),ContentType='application/json',CacheControl='no-store',**condition)
def run(client,bucket,request_id,execution_id,recover_run=None,publish_current=True):
    if not isinstance(execution_id,str) or not execution_id:raise ValueError('Durable execution identity required')
    key=request_key(request_id);status={'contract':'dollar-original-request.v1','request_id':request_id,'execution_id':execution_id,
        'started_at':now(),'status':'running','phase':'capture','publish_current':publish_current}
    try:status_write(client,bucket,key,status,IfNoneMatch='*')
    except Exception as exc:
        if not conflict(exc):raise
        return json.loads(bounded(client.get_object(Bucket=bucket,Key=key)['Body']))
    def checkpoint(**fields):status.update(fields);status_write(client,bucket,key,status)
    try:
        read=reader(client,bucket);expected=None
        if recover_run is None:inputs=capture_inputs(client,bucket,read,checkpoint)
        else:
            run_doc=verified_run(recover_run,read);inputs=checked(run_doc['input'],'inputs',read);expected=checked(run_doc['output'],'outputs',read)
            checkpoint(recovered_from=recover_run)
        raw=model.encoded(inputs);ref=identity(raw,'inputs');immutable(client,bucket,ref,raw,read);checkpoint(phase='compile',retained_input=ref)
        output=compile_output(inputs,read)
        if expected is not None and expected!=output:raise ValueError('Recovered Dollar calculation differs')
        checkpoint(phase='retained_replay');ref=retain(client,bucket,inputs,output,read,checkpoint)
        checkpoint(phase='publish');published=conditional(client,bucket,{**output,'replay':ref},read) if publish_current else False
        checkpoint(status='complete',phase='complete',completed_at=now(),published=published,generated_at=output['generated_at'],
            replay=ref,quality=output['quality'],provider_requests=0,engine_invocations=0,private_account_reads=0,
            paid_ai_calls=0,notifications_sent=0,portfolio_writes=0)
        return status
    except Exception as exc:
        checkpoint(status='failed',completed_at=now(),failure_class=type(exc).__name__,error='dollar_capture_replay_or_publication_failed')
        raise RuntimeError('Native Dollar research failed; inspect retained request evidence') from None
