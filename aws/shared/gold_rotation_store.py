"""Retain whole inputs, replay the Gold research, then conditionally publish it."""
from collections import OrderedDict
from datetime import datetime,timezone,timedelta
from concurrent.futures import ThreadPoolExecutor,as_completed
from pathlib import Path
import re,sys,time,urllib.request,urllib.error
import gold_rotation_model as model

MAX=8*1024*1024
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
    prefixes={model.PREFIX}
    return any(re.fullmatch(re.escape(p)+r'(?:(?:inputs|outputs|runs)/[a-f0-9]{64}\.json|compilers/[a-f0-9]{64}\.py)',key) for p in prefixes)
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
    if not artifact_key(key) or not(key.startswith(model.PREFIX) or key.startswith(model.PRIVATE)):raise ValueError('Gold research retention prefix required')
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
    run={'contract':'gold-rotation-replay.v1','generated_at':output['generated_at'],**refs,'output_sha256':refs['output']['sha256'],'compilers':compilers}
    raw=model.encoded(run);ref=identity(raw,'runs');put_immutable(client,bucket,ref['key'],raw)
    return {'manifest_key':ref['key'],'output_sha256':refs['output']['sha256']}
def verified_run(ref,read):
    if (not isinstance(ref,dict) or set(ref)!={'manifest_key','output_sha256'} or not isinstance(ref['manifest_key'],str)
        or not re.fullmatch(re.escape(model.PREFIX)+r'runs/[a-f0-9]{64}\.json',ref['manifest_key'])):raise ValueError('Recorded Gold research identity required')
    raw=read(ref['manifest_key']);run=model.strict(raw)
    if identity(raw,'runs')['key']!=ref['manifest_key'] or run.get('contract')!='gold-rotation-replay.v1' or run.get('output_sha256')!=ref['output_sha256']:raise ValueError('Recorded run differs')
    if set(run.get('compilers',{}))!={m.__name__ for m in COMPILERS}:raise ValueError('Exact compiler set required')
    for module in COMPILERS:
        raw=Path(module.__file__).read_bytes();compiler=identity(raw,'compilers')
        if run['compilers'][module.__name__]!=compiler or read(compiler['key'])!=raw:raise ValueError('Use the matching frozen compiler checkout')
    return run
def replay(ref,read):
    run=verified_run(ref,read);inputs=checked(run['input'],'inputs',read)
    output=model.compile_output(inputs,read);expected=checked(run['output'],'outputs',read)
    if output!=expected or model.digest(output)!=run['output_sha256'] or output['generated_at']!=run['generated_at']:raise ValueError('Gold research replay differs')
    return output
def request_key(request_id):
    if not isinstance(request_id,str) or not 1<=len(request_id)<=200:raise ValueError('Bounded durable request identity required')
    return model.PRIVATE+'requests/'+model.sha(request_id.encode())+'.json'
def status_write(client,bucket,key,value,**condition):
    if not re.fullmatch(re.escape(model.PRIVATE)+r'requests/[a-f0-9]{64}\.json',key):raise ValueError('Reviewed request path required')
    raw=model.encoded(value)
    client.put_object(Bucket=bucket,Key=key,Body=raw,ContentType='application/json',CacheControl='no-store',**condition)
    if bounded(client.get_object(Bucket=bucket,Key=key)['Body'])!=raw:raise ValueError('Request readback differs')

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs):raise ValueError('Provider redirect refused')

def capture(client,bucket,symbol,kind,start,end,credential,deadline,transport=None):
    url=model.source_url(symbol,kind,start,end)
    if time.monotonic()>=deadline:raise TimeoutError('Capture deadline exceeded')
    opener=transport or urllib.request.build_opener(NoRedirect()).open
    request=urllib.request.Request(url,headers={'User-Agent':'JustHodl-gold-research/1.0','apikey':credential})
    try:
        response=opener(request,timeout=max(1,min(15,deadline-time.monotonic())))
        http=response.status;raw=bounded(response)
    except urllib.error.HTTPError as exc:http=exc.code;raw=bounded(exc)
    except (OSError,TimeoutError):return {'symbol':symbol,'kind':kind,'source_url':url,'received_at':now(),'status':'transport_unavailable','original':None}
    if credential.encode() in raw:raise ValueError('Credential echo refused')
    return {'symbol':symbol,'kind':kind,'source_url':url,'received_at':now(),'http_status':http,
        'status':'response_retained' if http==200 else 'provider_error_retained','original':protect(client,bucket,raw)}

def not_older(packet,old):
    stamp=old.get('generated_at') or old.get('as_of')
    if stamp and model.clock(packet['generated_at'])<=model.clock(stamp):return False
    if old.get('contract')!=model.CONTRACT:return True
    for symbol,row in old['instruments'].items():
        date=row.get('observation_date');new=packet['instruments'][symbol].get('observation_date')
        if date and (not new or new<date):return False
        if row.get('latest'):
            for kind in model.PRICE:
                prior=row['latest'].get(kind);latest=packet['instruments'][symbol].get('latest') or {}
                if prior is not None and latest.get(kind) is None:return False
    return True

def publish(client,bucket,packet,inputs,etag):
    if packet.get('contract')!=model.CONTRACT or any(packet.get(k) is not False for k in model.FLAGS):raise ValueError('Descriptive native observations required')
    old=model.strict(model.original(inputs['predecessor'],reader(client,bucket)))
    if not not_older(packet,old):return {'published':False,'reason':'observation_or_compilation_rollback'}
    try:client.put_object(Bucket=bucket,Key=model.CURRENT,Body=model.encoded(packet),ContentType='application/json',CacheControl='no-store',IfMatch=etag)
    except Exception as exc:
        if conflict(exc):return {'published':False,'reason':'predecessor_changed'}
        raise
    if bounded(client.get_object(Bucket=bucket,Key=model.CURRENT)['Body'])!=model.encoded(packet):raise ValueError('Current readback differs')
    return {'published':True,'reason':'conditional_publication_verified'}

def run(client,bucket,request_id,execution_id,credential,transport=None):
    if not isinstance(credential,str) or not credential:raise ValueError('Configured provider credential required')
    key=request_key(request_id);read=reader(client,bucket)
    try:prior=model.strict(bounded(client.get_object(Bucket=bucket,Key=key)['Body']))
    except Exception as exc:
        if not missing(exc):raise
        prior=None
    if prior:
        if prior.get('status')!='complete':raise ValueError('Existing incomplete request; inspect without recapture')
        replay(prior['replay'],read);return {**prior,'adopted_completed_request':True}
    status_write(client,bucket,key,{'status':'claimed','request_id':request_id,'execution_id':execution_id,'started_at':now()},IfNoneMatch='*')
    captures={};predecessor=None
    try:
        obj=client.get_object(Bucket=bucket,Key=model.CURRENT);old=bounded(obj['Body']);etag=obj['ETag'];predecessor=protect(client,bucket,old)
        today=model.clock(now()).date();start=str(today-timedelta(days=900));end=str(today-timedelta(days=1));deadline=time.monotonic()+120
        with ThreadPoolExecutor(max_workers=4) as pool:
            jobs={pool.submit(capture,client,bucket,symbol,kind,start,end,credential,deadline,transport):(symbol,kind) for symbol in model.INSTRUMENTS for kind in model.KINDS}
            for future in as_completed(jobs):
                symbol,kind=jobs[future];captures[symbol+':'+kind]=future.result()
                status_write(client,bucket,key,{'status':'capturing','request_id':request_id,'execution_id':execution_id,'predecessor':predecessor,'captures':captures})
        if sum((v['original'] or {}).get('bytes',0) for v in captures.values())>64*1024*1024:raise ValueError('Aggregate source byte bound')
        inputs={'contract':'gold-rotation-inputs.v1','generated_at':now(),'range':{'from':start,'to':end},'predecessor':predecessor,'captures':captures}
        output=model.compile_output(inputs,read);ref=retain(client,bucket,inputs,output)
        if replay(ref,read)!=output:raise ValueError('Pre-publication original replay differs')
        result=publish(client,bucket,{**output,'replay':ref},inputs,etag)
        complete={'status':'complete','request_id':request_id,'execution_id':execution_id,'generated_at':inputs['generated_at'],'replay':ref,**result}
        status_write(client,bucket,key,complete);return complete
    except Exception as exc:
        status_write(client,bucket,key,{'status':'failed','request_id':request_id,'execution_id':execution_id,'predecessor':predecessor,'captures':captures,'failure_class':type(exc).__name__})
        raise
