"""Retain whole inputs, replay the composition, then conditionally publish it."""
from collections import OrderedDict
from datetime import datetime,timezone
from pathlib import Path
import re,sys
import flow_state_model as model

MAX=32*1024*1024
COMPILERS=(model,sys.modules[__name__])
def now():return datetime.now(timezone.utc).isoformat()
def code(exc):return str(getattr(exc,'response',{}).get('Error',{}).get('Code',''))
def missing(exc):return code(exc) in ('NoSuchKey','404')
def conflict(exc):return code(exc) in ('409','412','PreconditionFailed','ConditionalRequestConflict')
def bounded(stream):
    try:raw=stream.read(MAX+1)
    finally:stream.close()
    if not 0<len(raw)<=MAX:raise ValueError('Whole research byte bound')
    return raw
def artifact_key(key):
    if not isinstance(key,str):return False
    if re.fullmatch(re.escape(model.PRIVATE)+r'[a-f0-9]{64}\.bin',key):return True
    prefixes={model.PREFIX,*('data/'+v[1]+'/' for v in model.PARENTS.values())}
    return any(re.fullmatch(re.escape(p)+r'(?:inputs|outputs|compilers|runs)/[a-f0-9]{64}\.(?:json|py)',key) for p in prefixes)
def reader(client,bucket):
    cache=OrderedDict();used=0
    def read(key):
        nonlocal used
        if not artifact_key(key):raise ValueError('Reviewed immutable artifact required')
        if key in cache:cache.move_to_end(key);return cache[key]
        raw=bounded(client.get_object(Bucket=bucket,Key=key)['Body'])
        if len(raw)<=8*1024*1024:
            while cache and used+len(raw)>16*1024*1024:
                _,old=cache.popitem(last=False);used-=len(old)
            cache[key]=raw;used+=len(raw)
        return raw
    return read
def put_immutable(client,bucket,key,raw):
    if not artifact_key(key) or not(key.startswith(model.PREFIX) or key.startswith(model.PRIVATE)):raise ValueError('Composition retention prefix required')
    if not isinstance(raw,bytes) or not 0<len(raw)<=MAX:raise ValueError('Whole bounded bytes required')
    suffix=key.rsplit('/',1)[-1].split('.')[0]
    if suffix!=model.sha(raw):raise ValueError('Content-addressed retention required')
    try:client.put_object(Bucket=bucket,Key=key,Body=raw,ContentType='text/x-python' if key.endswith('.py') else 'application/octet-stream' if key.endswith('.bin') else 'application/json',
        CacheControl='no-store' if key.startswith(model.PRIVATE) else 'public, max-age=31536000, immutable',IfNoneMatch='*')
    except Exception as exc:
        if not conflict(exc):raise
    if bounded(client.get_object(Bucket=bucket,Key=key)['Body'])!=raw:raise ValueError('Retained bytes differ')
def protect(client,bucket,raw):
    ref={'key':model.PRIVATE+model.sha(raw)+'.bin','sha256':model.sha(raw),'bytes':len(raw)}
    put_immutable(client,bucket,ref['key'],raw);return ref
def identity(raw,kind):return {'key':model.PREFIX+kind+'/'+model.sha(raw)+('.py' if kind=='compilers' else '.json'),'sha256':model.sha(raw),'bytes':len(raw)}
def checked(ref,kind,read):
    raw=read(ref['key'])
    if identity(raw,kind)!=ref:raise ValueError('Recorded '+kind+' differs')
    return model.strict(raw)
def retain(client,bucket,inputs,output):
    refs={}
    for kind,doc in (('inputs',inputs),('outputs',output)):
        raw=model.encoded(doc);ref=identity(raw,kind);put_immutable(client,bucket,ref['key'],raw);refs[kind[:-1]]=ref
    compilers={}
    for module in COMPILERS:
        raw=Path(module.__file__).read_bytes();ref=identity(raw,'compilers');put_immutable(client,bucket,ref['key'],raw);compilers[module.__name__]=ref
    run={'contract':'flow-state-replay.v1','generated_at':output['generated_at'],**refs,'output_sha256':refs['output']['sha256'],'compilers':compilers}
    raw=model.encoded(run);ref=identity(raw,'runs');put_immutable(client,bucket,ref['key'],raw)
    return {'manifest_key':ref['key'],'output_sha256':refs['output']['sha256']}
def verified_run(ref,read):
    if (not isinstance(ref,dict) or set(ref)!={'manifest_key','output_sha256'} or not isinstance(ref['manifest_key'],str)
        or not re.fullmatch(re.escape(model.PREFIX)+r'runs/[a-f0-9]{64}\.json',ref['manifest_key'])):raise ValueError('Recorded composition identity required')
    raw=read(ref['manifest_key']);run=model.strict(raw)
    if identity(raw,'runs')['key']!=ref['manifest_key'] or run.get('contract')!='flow-state-replay.v1' or run.get('output_sha256')!=ref['output_sha256']:raise ValueError('Recorded run differs')
    if set(run.get('compilers',{}))!={m.__name__ for m in COMPILERS}:raise ValueError('Exact compiler set required')
    for module in COMPILERS:
        raw=Path(module.__file__).read_bytes();compiler=identity(raw,'compilers')
        if run['compilers'][module.__name__]!=compiler or read(compiler['key'])!=raw:raise ValueError('Use the matching frozen compiler checkout')
    return run
def replay(ref,read):
    run=verified_run(ref,read);inputs=checked(run['input'],'inputs',read)
    output=model.compile_output(inputs,read);expected=checked(run['output'],'outputs',read)
    if output!=expected or model.digest(output)!=run['output_sha256'] or output['generated_at']!=run['generated_at']:raise ValueError('Composition replay differs')
    return output
def request_key(request_id):
    if not isinstance(request_id,str) or not 1<=len(request_id)<=200:raise ValueError('Bounded durable request identity required')
    return model.PRIVATE+'requests/'+model.sha(request_id.encode())+'.json'
def status_write(client,bucket,key,value,**condition):
    if not re.fullmatch(re.escape(model.PRIVATE)+r'requests/[a-f0-9]{64}\.json',key):raise ValueError('Reviewed request path required')
    raw=model.encoded(value)
    client.put_object(Bucket=bucket,Key=key,Body=raw,ContentType='application/json',CacheControl='no-store',**condition)
    if bounded(client.get_object(Bucket=bucket,Key=key)['Body'])!=raw:raise ValueError('Request readback differs')
def source_capture(client,bucket,key):
    if key not in model.CAPTURE_KEYS:raise ValueError('Explicit public research capture required')
    obj=client.get_object(Bucket=bucket,Key=key);raw=bounded(obj['Body'])
    if not isinstance(model.strict(raw),dict):raise ValueError('Whole public research object required')
    return {'source_key':key,'acquired_at':now(),'original':protect(client,bucket,raw)},obj['ETag']
def not_older(packet,old):
    if model.clock(packet['generated_at'])<=model.clock(old['generated_at']):return False
    if old.get('contract')!=model.CONTRACT:return True
    for key,parent in packet['parents'].items():
        if model.clock(parent['generated_at'])<model.clock(old['parents'][key]['generated_at']):return False
    if packet['foreign_flows']['observation_date']<old['foreign_flows']['observation_date']:return False
    previous={r['category']:r['period'] for r in old['asset_class_rotation'] if r['period']}
    for row in packet['asset_class_rotation']:
        if row['period'] and row['category'] in previous and row['period']['end_date']<previous[row['category']]['end_date']:return False
    return True
def publish(client,bucket,packet,inputs,etag):
    if packet.get('contract')!=model.CONTRACT or any(packet.get(k) is not False for k in model.FLAGS):raise ValueError('Descriptive native composition required')
    old=model.strict(model.original(inputs['captures'][model.CURRENT]['original'],reader(client,bucket)))
    if not not_older(packet,old):return {'published':False,'reason':'source_or_compilation_rollback'}
    # One conditional write. A changed predecessor is never rebased or retried.
    try:client.put_object(Bucket=bucket,Key=model.CURRENT,Body=model.encoded(packet),ContentType='application/json',CacheControl='no-store',IfMatch=etag)
    except Exception as exc:
        if conflict(exc):return {'published':False,'reason':'predecessor_changed'}
        raise
    raw=bounded(client.get_object(Bucket=bucket,Key=model.CURRENT)['Body'])
    if raw!=model.encoded(packet):raise ValueError('Current publication readback differs')
    return {'published':True,'reason':'conditional_publication_verified'}
def run(client,bucket,request_id,execution_id=None):
    key=request_key(request_id);read=reader(client,bucket)
    try:prior=model.strict(bounded(client.get_object(Bucket=bucket,Key=key)['Body']))
    except Exception as exc:
        if not missing(exc):raise
        prior=None
    if prior:
        if prior.get('status')!='complete':raise ValueError('Existing incomplete request; inspect it without recapture')
        replay(prior['replay'],read);return {**prior,'adopted_completed_request':True}
    status_write(client,bucket,key,{'status':'claimed','request_id':request_id,'execution_id':execution_id,'generated_at':now()},IfNoneMatch='*')
    captures={};etag=None;total=0
    try:
        for source in model.CAPTURE_KEYS:
            captures[source],observed=source_capture(client,bucket,source)
            if source==model.CURRENT:etag=observed
            total+=captures[source]['original']['bytes']
            if total>128*1024*1024:raise ValueError('Aggregate source byte bound')
            status_write(client,bucket,key,{'status':'capturing','request_id':request_id,'execution_id':execution_id,'captures':captures})
        inputs={'contract':'flow-state-inputs.v1','generated_at':now(),'captures':captures}
        output=model.compile_output(inputs,read);ref=retain(client,bucket,inputs,output)
        if replay(ref,read)!=output:raise ValueError('Pre-publication replay differs')
        packet={**output,'replay':ref};result=publish(client,bucket,packet,inputs,etag)
        complete={'status':'complete','request_id':request_id,'execution_id':execution_id,'generated_at':inputs['generated_at'],'replay':ref,**result}
        status_write(client,bucket,key,complete);return complete
    except Exception as exc:
        status_write(client,bucket,key,{'status':'failed','request_id':request_id,'execution_id':execution_id,'captures':captures,'failure_class':type(exc).__name__})
        raise
