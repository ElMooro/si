"""Protected upstream snapshots, bounded replay and conditional public publication."""
from pathlib import Path
from datetime import datetime,timezone
import json,re,sys,time
import extremes_native_model as model
PREFIX=model.PREFIX;PRIVATE=model.PRIVATE;MAX=12*1024*1024;TOTAL=48*1024*1024
COMPILERS=(model,sys.modules[__name__],*model.COMPANIONS)

def now():return datetime.now(timezone.utc).isoformat()
def current(engine):
    if engine not in model.INPUTS:raise ValueError('Reviewed engine required')
    return 'data/'+engine+'.json'
def bounded(stream):
    try:raw=stream.read(MAX+1)
    finally:stream.close()
    if len(raw)>MAX:raise ValueError('Research byte bound exceeded')
    return raw
def error_code(exc):return str(getattr(exc,'response',{}).get('Error',{}).get('Code',''))
def missing(exc):return error_code(exc) in ('404','NoSuchKey')
def conflict(exc):return error_code(exc) in ('409','412','PreconditionFailed','ConditionalRequestConflict')
def artifact(key):return isinstance(key,str) and bool(re.fullmatch(re.escape(PRIVATE)+r'[a-f0-9]{64}\.bin|'+re.escape(PREFIX)+r'(?:inputs|outputs|runs|compilers)/[a-f0-9]{64}\.(?:json|py)',key))
def reader(client,bucket):
    def read(key):
        if not artifact(key) and key not in (current(e) for e in model.INPUTS):raise ValueError('Reviewed replay path required')
        return bounded(client.get_object(Bucket=bucket,Key=key)['Body'])
    return read
def immutable(client,bucket,key,raw,kind='application/json'):
    if not artifact(key) or not isinstance(raw,bytes) or len(raw)>MAX or key.rsplit('/',1)[-1].split('.')[0]!=model.sha(raw):raise ValueError('Bounded content identity required')
    try:client.put_object(Bucket=bucket,Key=key,Body=raw,ContentType=kind,IfNoneMatch='*',CacheControl='no-store' if key.startswith(PRIVATE) else 'public, max-age=31536000, immutable')
    except Exception as exc:
        if not conflict(exc):raise
    if reader(client,bucket)(key)!=raw:raise ValueError('Retained bytes differ')
def retain_original(client,bucket,raw):
    digest=model.sha(raw);key=PRIVATE+digest+'.bin';immutable(client,bucket,key,raw,'application/octet-stream')
    return {'key':key,'sha256':digest,'bytes':len(raw)}
def original(ref,read):
    if not isinstance(ref,dict) or not re.fullmatch('[a-f0-9]{64}',str(ref.get('sha256',''))) or ref.get('key')!=PRIVATE+ref['sha256']+'.bin':raise ValueError('Protected source identity required')
    raw=read(ref['key'])
    if type(ref.get('bytes')) is not int or len(raw)!=ref['bytes'] or model.sha(raw)!=ref['sha256']:raise ValueError('Protected source bytes differ')
    return raw
def upstream_path(name,key,category):return bool(re.fullmatch('data/'+re.escape(model.SOURCES[name][2] or 'no-source')+'/'+category+r'/[a-f0-9]{64}\.json',str(key)))
def verify_upstream(name,packet,run_raw,out_raw):
    ref=packet['replay'];run=json.loads(run_raw)
    if not model.identity(packet,name) or ref['manifest_key'].rsplit('/',1)[-1]!=model.sha(run_raw)+'.json':raise ValueError('Upstream run identity differs')
    out=run['output'];digest=ref['output_sha256']
    if not upstream_path(name,out['key'],'outputs') or out['key'].rsplit('/',1)[-1]!=digest+'.json':raise ValueError('Upstream output path differs')
    if out.get('sha256')!=digest or run.get('output_sha256')!=digest or out.get('bytes')!=len(out_raw) or model.sha(out_raw)!=digest:raise ValueError('Upstream output bytes differ')
    if json.loads(out_raw)!={k:v for k,v in packet.items() if k!='replay'} or run['generated_at']!=packet['generated_at']:raise ValueError('Upstream publication differs from retained run')

def collect(client,bucket,engine,deadline):
    entries={};total=0
    for name in model.INPUTS[engine]:
        key,contract,_=model.SOURCES[name]
        entry={'source_key':key,'acquired_at':now(),'status':'missing','upstream_identity_verified':False}
        if time.monotonic()>deadline:raise RuntimeError('Capture deadline; preserve prior publication')
        try:raw=bounded(client.get_object(Bucket=bucket,Key=key)['Body'])
        except Exception as exc:
            if not missing(exc):raise
            entries[name]=entry;continue
        total+=len(raw)
        if total>TOTAL:raise ValueError('Total capture bound exceeded')
        entry.update(status='retained',packet=retain_original(client,bucket,raw),acquired_at=now())
        try:p=json.loads(raw)
        except (ValueError,UnicodeDecodeError):entry['status']='malformed_source_json';entries[name]=entry;continue
        if contract and model.identity(p,name):
            run_key=p['replay']['manifest_key'];run_raw=bounded(client.get_object(Bucket=bucket,Key=run_key)['Body']);run=json.loads(run_raw)
            out_key=run['output']['key']
            if not upstream_path(name,out_key,'outputs'):raise ValueError('Upstream source path refused')
            out_raw=bounded(client.get_object(Bucket=bucket,Key=out_key)['Body']);verify_upstream(name,p,run_raw,out_raw)
            total+=len(run_raw)+len(out_raw)
            if total>TOTAL:raise ValueError('Total capture bound exceeded')
            entry.update(upstream_identity_verified=True,upstream_run={'source_key':run_key,**retain_original(client,bucket,run_raw)},
                upstream_output={'source_key':out_key,**retain_original(client,bucket,out_raw)})
        entries[name]=entry
    return entries

def compile_output(inputs,read):
    if inputs.get('contract')!='extremes-native-inputs.v1':raise ValueError('Native input contract required')
    start=model.clock(inputs['started_at']);at=model.clock(inputs['generated_at']);engine=inputs['engine'];entries=inputs['sources']
    if not 0<=(at-start).total_seconds()<=120 or engine not in model.INPUTS or set(entries)!=set(model.INPUTS[engine]):raise ValueError('Capture clock or inventory differs')
    packets={};total=0
    for name,entry in entries.items():
        if entry.get('source_key')!=model.SOURCES[name][0] or not start<=model.clock(entry['acquired_at'])<=at:raise ValueError('Source identity or acquisition clock differs')
        if entry.get('status')=='missing':
            if entry.get('packet') or entry.get('upstream_identity_verified') is not False:raise ValueError('Missing source contains proof')
            packets[name]=None;continue
        if entry.get('status') not in ('retained','malformed_source_json'):raise ValueError('Unknown source status')
        raw=original(entry['packet'],read);total+=len(raw)
        try:packet=json.loads(raw)
        except (ValueError,UnicodeDecodeError):
            if entry['status']!='malformed_source_json' or entry.get('upstream_identity_verified') is not False:raise
            packets[name]=None;continue
        if not isinstance(packet,dict):packet=None
        if entry.get('upstream_identity_verified') is True:
            run_raw=original(entry['upstream_run'],read);out_raw=original(entry['upstream_output'],read);total+=len(run_raw)+len(out_raw)
            verify_upstream(name,packet,run_raw,out_raw)
            if entry['upstream_run']['source_key']!=packet['replay']['manifest_key'] or entry['upstream_output']['source_key']!=json.loads(run_raw)['output']['key']:raise ValueError('Upstream retained path differs')
        elif entry.get('upstream_identity_verified') is not False:raise ValueError('Explicit upstream verification state required')
        packets[name]=packet
    if total>TOTAL:raise ValueError('Total replay bound exceeded')
    return model.compute(engine,packets,entries,inputs['generated_at'])

def checked(ref,category,read):
    digest=ref.get('sha256','')
    if not re.fullmatch('[a-f0-9]{64}',digest) or ref.get('key')!=PREFIX+category+'/'+digest+'.json':raise ValueError('Artifact identity differs')
    raw=read(ref['key'])
    if type(ref.get('bytes')) is not int or len(raw)!=ref['bytes'] or model.sha(raw)!=digest:raise ValueError('Artifact bytes differ')
    return json.loads(raw)
def replay(ref,read):
    key=ref.get('manifest_key','')
    if not re.fullmatch(re.escape(PREFIX)+r'runs/[a-f0-9]{64}\.json',key):raise ValueError('Reviewed run identity required')
    raw=read(key);m=json.loads(raw)
    if key!=PREFIX+'runs/'+model.sha(raw)+'.json' or m.get('contract')!='extremes-native-replay.v1':raise ValueError('Run bytes differ')
    if set(m['compilers'])!={c.__name__ for c in COMPILERS}:raise ValueError('Compiler inventory differs')
    for c in COMPILERS:
        code=Path(c.__file__).read_bytes();digest=model.sha(code);refc=m['compilers'][c.__name__]
        if refc!={'key':PREFIX+'compilers/'+digest+'.py','sha256':digest} or read(refc['key'])!=code:raise ValueError('Matching reviewed compiler release required')
    out=compile_output(checked(m['input'],'inputs',read),read)
    if out!=checked(m['output'],'outputs',read) or model.sha(model.encoded(out))!=ref['output_sha256'] or m['output_sha256']!=ref['output_sha256'] or out['generated_at']!=m['generated_at']:raise ValueError('Synthesis replay differs')
    return out
def retain(client,bucket,inputs,output):
    refs={};compilers={}
    for name,doc in (('input',inputs),('output',output)):
        raw=model.encoded(doc);digest=model.sha(raw);key=PREFIX+name+'s/'+digest+'.json';immutable(client,bucket,key,raw)
        refs[name]={'key':key,'sha256':digest,'bytes':len(raw)}
    for module in COMPILERS:
        raw=Path(module.__file__).read_bytes();digest=model.sha(raw);key=PREFIX+'compilers/'+digest+'.py';immutable(client,bucket,key,raw,'text/x-python');compilers[module.__name__]={'key':key,'sha256':digest}
    manifest={'contract':'extremes-native-replay.v1','generated_at':output['generated_at'],'engine':output['engine'],**refs,'compilers':compilers,'output_sha256':refs['output']['sha256'],'scope':output['replay_scope']}
    raw=model.encoded(manifest);key=PREFIX+'runs/'+model.sha(raw)+'.json';immutable(client,bucket,key,raw)
    ref={'manifest_key':key,'output_sha256':refs['output']['sha256']}
    if replay(ref,reader(client,bucket))!=output:raise ValueError('Retained replay differs')
    return ref
def publish(client,bucket,packet):
    key=current(packet['engine']);at=model.clock(packet['generated_at'])
    for _ in range(4):
        try:
            obj=client.get_object(Bucket=bucket,Key=key);raw=bounded(obj['Body']);old=json.loads(raw)
            if old.get('generated_at') and model.clock(old['generated_at'])>at:return False
            if old.get('generated_at') and model.clock(old['generated_at'])==at and old!=packet:raise ValueError('Conflicting same-clock publication')
            retain_original(client,bucket,raw);condition={'IfMatch':obj['ETag']}
        except Exception as exc:
            if not missing(exc):raise
            condition={'IfNoneMatch':'*'}
        try:
            client.put_object(Bucket=bucket,Key=key,Body=model.encoded(packet),ContentType='application/json',CacheControl='no-store',**condition)
            live=json.loads(reader(client,bucket)(key))
            if live!=packet and model.clock(live['generated_at'])<=at:raise ValueError('Publication readback differs')
            return True
        except Exception as exc:
            if not conflict(exc):raise
    raise RuntimeError('Publication conflict limit')
def request_key(engine,request_id):
    current(engine)
    if not isinstance(request_id,str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,100}',request_id):raise ValueError('Canonical request identity required')
    return PREFIX+'requests/'+model.sha((engine+':'+request_id).encode())+'.json'
def run(client,bucket,engine,request_id,execution_id,remaining_seconds=60):
    start=now();key=request_key(engine,request_id)
    def write(doc,**condition):client.put_object(Bucket=bucket,Key=key,Body=model.encoded(doc),ContentType='application/json',CacheControl='no-store',**condition)
    status={'contract':'extremes-public-request.v1','engine':engine,'request_id':request_id,'execution_id':execution_id,'started_at':start,'status':'running','phase':'capture'}
    try:write(status,IfNoneMatch='*')
    except Exception as exc:
        if not conflict(exc):raise
        return json.loads(bounded(client.get_object(Bucket=bucket,Key=key)['Body']))
    try:
        sources=collect(client,bucket,engine,time.monotonic()+max(0,min(80,remaining_seconds-25)))
        inputs={'contract':'extremes-native-inputs.v1','engine':engine,'started_at':start,'generated_at':now(),'sources':sources}
        status['phase']='compile';write(status);output=compile_output(inputs,reader(client,bucket))
        status['phase']='retained_replay';write(status);ref=retain(client,bucket,inputs,output)
        status['phase']='publish';write(status);published=publish(client,bucket,{**output,'replay':ref})
        result={**status,'status':'complete','phase':'complete','completed_at':now(),'published':published,'generated_at':output['generated_at'],'quality':output['quality'],'replay':ref,
            'provider_requests':0,'private_account_reads':0,'paid_ai_calls':0,'notifications_sent':0,'portfolio_writes':0}
        write(result);return result
    except Exception:
        write({**status,'status':'failed','completed_at':now(),'error':'native_capture_replay_or_publication_failed'})
        raise RuntimeError('Native synthesis failed; prior publication preserved') from None
