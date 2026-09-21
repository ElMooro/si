"""Canonical-original cycle replay; public inputs, protected retention and CAS.

Reads no credentials or account packets, makes no provider/LLM calls and invokes
no consumer. The existing root source archive supplies the reviewed originals.
"""
from datetime import datetime,timezone
from pathlib import Path
import gzip,io,json,re,sys
import cycle_research_model as model
import cycle_research_catalog as catalog
import canonical_fred_replay
import report_observations
import research_brief_model
import evidence_store

PREFIX=model.PREFIX;CURRENT=model.CURRENT;PRIVATE=model.PRIVATE;MAX=32*1024*1024
SOURCES=('data/report-measurements.json','data/cycle-clock.json','data/cycle-clock-history.json',*catalog.DEPENDENCIES)
COMPILERS=(model,catalog,canonical_fred_replay,report_observations,research_brief_model,evidence_store,sys.modules[__name__])


def now():return datetime.now(timezone.utc).isoformat()
def bounded(stream):
    try:raw=stream.read(MAX+1)
    finally:stream.close()
    if len(raw)>MAX:raise ValueError('Research artifact exceeds bound')
    return raw

def error_code(exc):return str(getattr(exc,'response',{}).get('Error',{}).get('Code',''))
def missing(exc):return error_code(exc) in ('404','NoSuchKey')
def conflict(exc):return error_code(exc) in ('409','412','PreconditionFailed','ConditionalRequestConflict')

def artifact(key):
    return isinstance(key,str) and bool(re.fullmatch(re.escape(PRIVATE)+r'[a-f0-9]{64}\.bin|'+re.escape(PREFIX)+r'(?:inputs|outputs|runs|compilers)/[a-f0-9]{64}\.(?:json|py)',key))

def allowed(key):
    return isinstance(key,str) and (key in SOURCES or artifact(key) or bool(re.fullmatch(
        r'data/(?:report-research/(?:runs|compilers)/[a-f0-9]{64}\.(?:json|py)|evidence/fred/[a-f0-9]{64}/[a-f0-9]{64}\.bin\.gz)',key)))


def reader(client,bucket):
    def read(key):
        if not allowed(key):raise ValueError('Unapproved research source path')
        raw=bounded(client.get_object(Bucket=bucket,Key=key)['Body'])
        if key.endswith('.gz'):raw=bounded(gzip.GzipFile(fileobj=io.BytesIO(raw)))
        return raw
    return read

def immutable(client,bucket,key,raw,kind='application/json'):
    if not artifact(key) or not isinstance(raw,bytes) or len(raw)>MAX:raise ValueError('Bounded immutable artifact required')
    if key.rsplit('/',1)[-1].split('.')[0]!=model.sha(raw):raise ValueError('Content address differs')
    try:client.put_object(Bucket=bucket,Key=key,Body=raw,ContentType=kind,IfNoneMatch='*',
        CacheControl='no-store' if key.startswith(PRIVATE) else 'public, max-age=31536000, immutable')
    except Exception as exc:
        if not conflict(exc):raise
    if reader(client,bucket)(key)!=raw:raise ValueError('Immutable readback differs')

def snapshot(client,bucket,key):
    if key not in SOURCES:raise ValueError('Unapproved source snapshot')
    raw=reader(client,bucket)(key);doc=json.loads(raw)
    if not isinstance(doc,list if key=='data/cycle-clock-history.json' else dict):raise ValueError('Declared source shape required')
    digest=model.sha(raw);target=PRIVATE+digest+'.bin'
    immutable(client,bucket,target,raw,'application/octet-stream')
    return {'source_key':key,'key':target,'sha256':digest,'bytes':len(raw)}

def source(ref,key,read):
    digest=ref.get('sha256','')
    if (ref.get('source_key')!=key or not re.fullmatch('[a-f0-9]{64}',digest)
        or ref.get('key')!=PRIVATE+digest+'.bin' or type(ref.get('bytes')) is not int):raise ValueError('Source identity differs')
    raw=read(ref['key'])
    if len(raw)!=ref['bytes'] or model.sha(raw)!=digest:raise ValueError('Source bytes differ')
    doc=json.loads(raw)
    if not isinstance(doc,list if key=='data/cycle-clock-history.json' else dict):raise ValueError('Declared source shape required')
    return doc

def compile_output(inputs,read):
    if inputs.get('contract')!='cycle-native-inputs.v1':raise ValueError('Unsupported input contract')
    packet=source(inputs['macro'],SOURCES[0],read)
    if set(inputs['legacy'])!=set(SOURCES[1:]):raise ValueError('Exact retained input inventory required')
    legacy={key:source(ref,key,read) if ref else None for key,ref in inputs['legacy'].items()}
    originals=canonical_fred_replay.restore(packet,model.SERIES,read)
    packets={key:legacy[key] for key in catalog.DEPENDENCIES}
    refs={key:inputs['legacy'][key] for key in catalog.DEPENDENCIES}
    return model.build(packet,originals,packets,refs,inputs['generated_at'])


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
    if key != PREFIX+'runs/'+model.sha(raw)+'.json' or manifest.get('contract') != 'cycle-native-replay.v1':
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
        raise ValueError('native cycle original-source replay differs')
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
    manifest = {'contract': 'cycle-native-replay.v1', 'generated_at': output['generated_at'], **refs,
        'compilers': compilers, 'output_sha256': refs['output']['sha256'],
        'scope': 'Exact canonical source definitions and observations, preserved predecessor input inventory, calendar-aligned descriptive transforms and declared root overlap; no calibrated forecast or portfolio authority.'}
    raw = model.encoded(manifest); key = PREFIX+'runs/'+model.sha(raw)+'.json'; immutable(client, bucket, key, raw)
    ref = {'manifest_key': key, 'output_sha256': refs['output']['sha256']}
    if replay(ref, reader(client, bucket)) != output: raise ValueError('retained replay differs')
    return ref


def publish(client, bucket, packet):
    at = model.clock(packet['generated_at'])
    for _ in range(4):
        try:
            obj = client.get_object(Bucket=bucket, Key=CURRENT); raw = bounded(obj['Body']); old = json.loads(raw)
            old_clock = old.get('generated_at') or old.get('as_of')
            if old_clock and model.clock(old_clock) > at: return False
            if old_clock and model.clock(old_clock) == at and old != packet: raise ValueError('conflicting same-clock publication')
            if old.get('contract') == model.CONTRACT and model.clock(old['source_generated_at']) > model.clock(packet['source_generated_at']): return False
            if old.get('contract')==model.CONTRACT:
                previous={n['key']:n for n in old['dependency_graph']['inputs']}
                for node in packet['dependency_graph']['inputs']:
                    prior=previous.get(node['key'],{})
                    if prior.get('clock_status')=='publication_clock_only' and node.get('clock_status')=='publication_clock_only':
                        if model.clock(prior['source_generated_at'])>model.clock(node['source_generated_at']):return False
            immutable(client, bucket, PRIVATE+model.sha(raw)+'.bin', raw, 'application/octet-stream')
            condition = {'IfMatch': obj['ETag']}
        except Exception as exc:
            if not missing(exc): raise
            condition = {'IfNoneMatch': '*'}
        try:
            client.put_object(Bucket=bucket, Key=CURRENT, Body=model.encoded(packet), ContentType='application/json',
                CacheControl='no-store', **condition)
            live = json.loads(reader(client, bucket)(CURRENT))
            if live != packet and model.clock(live['generated_at']) <= at: raise ValueError('publication readback differs')
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


def run(client,bucket,request_id,execution_id):
    started=now();key=request_key(request_id)
    status={'contract':'cycle-public-request.v1','request_id':request_id,'execution_id':execution_id,
        'started_at':started,'status':'running','phase':'snapshot'}
    try:status_write(client,bucket,key,status,IfNoneMatch='*')
    except Exception as exc:
        if not conflict(exc):raise
        return json.loads(bounded(client.get_object(Bucket=bucket,Key=key)['Body']))
    try:
        macro=snapshot(client,bucket,SOURCES[0])
        legacy={}
        for source_key in SOURCES[1:]:
            try:legacy[source_key]=snapshot(client,bucket,source_key)
            except Exception as exc:
                if not missing(exc):raise
                legacy[source_key]=None
        inputs={'contract':'cycle-native-inputs.v1','macro':macro,'legacy':legacy,'generated_at':now()}
        status['phase']='compile';status_write(client,bucket,key,status)
        output=compile_output(inputs,reader(client,bucket))
        status['phase']='retained_replay';status_write(client,bucket,key,status)
        ref=retain(client,bucket,inputs,output)
        status['phase']='publish';status_write(client,bucket,key,status)
        published=publish(client,bucket,{**output,'replay':ref})
        result={**status,'status':'complete','phase':'complete','completed_at':now(),'published':published,
            'generated_at':output['generated_at'],'quality':output['quality'],'replay':ref,
            'private_account_reads':0,'provider_requests':0,'paid_ai_calls':0,'notifications_sent':0,'portfolio_writes':0}
        status_write(client,bucket,key,result);return result
    except Exception:
        status_write(client,bucket,key,{**status,'status':'failed','completed_at':now(),'error':'native_replay_or_publication_failed'})
        raise RuntimeError('Native cycle publication failed; inspect reviewed request evidence') from None
