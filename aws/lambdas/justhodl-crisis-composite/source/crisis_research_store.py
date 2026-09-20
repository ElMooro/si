"""Original-source Crisis replay and conditional publication; public inputs only."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime,timezone
from pathlib import Path
import gzip,hashlib,io,json,re,sys
from urllib.parse import urlsplit,parse_qs

import crisis_research_model as model
import report_observations
import research_brief_model
import ciss_source_model
import evidence_store
from evidence_store import public_source_url

MAX=32*1024*1024
PRIVATE='audit-private/20260909-originals/crisis-composite/'
COMPILERS=(model,report_observations,research_brief_model,ciss_source_model,evidence_store,sys.modules[__name__])
MUTABLE=(model.CURRENT,'data/defcon.json','data/crisis-composite-history.json')
PUBLIC_INPUTS={'data/report-measurements.json','data/ciss-stress.json',*MUTABLE,*('data/'+k+'.json' for k in model.CONTEXT_KEYS)}


def bounded(stream):
    try:raw=stream.read(MAX+1)
    finally:stream.close()
    if len(raw)>MAX:raise ValueError('research artifact exceeds bound')
    return raw


def error_code(exc):return str(getattr(exc,'response',{}).get('Error',{}).get('Code',''))
def conflict(exc):return error_code(exc) in ('412','409','PreconditionFailed','ConditionalRequestConflict')
def missing(exc):return error_code(exc) in ('404','NoSuchKey')


def allowed(key):
    return isinstance(key,str) and (key in PUBLIC_INPUTS or bool(re.fullmatch(
        r'data/(?:evidence/(?:fred|ecb)/[a-f0-9]{64}/[a-f0-9]{64}\.bin\.gz|'
        r'(?:report-research|ciss-research|crisis-research)/(?:runs|inputs|outputs|compilers)/[a-f0-9]{64}\.(?:json|py))',key)))


def reader(client,bucket):
    def read(key):
        if not allowed(key):raise ValueError('unapproved public source path')
        raw=bounded(client.get_object(Bucket=bucket,Key=key)['Body'])
        if key.endswith('.gz'):raw=bounded(gzip.GzipFile(fileobj=io.BytesIO(raw)))
        return raw
    return read


def immutable(client,bucket,key,raw,kind='application/json',private=False):
    if not isinstance(raw,bytes) or len(raw)>MAX:raise ValueError('retained artifact exceeds bound')
    if private and not re.fullmatch(re.escape(PRIVATE)+r'[a-f0-9]{64}\.bin',key):raise ValueError('protected archive identity required')
    if not private and not allowed(key):raise ValueError('research artifact identity required')
    try:client.put_object(Bucket=bucket,Key=key,Body=raw,ContentType=kind,IfNoneMatch='*',CacheControl='no-store' if private else 'public, max-age=31536000, immutable')
    except Exception as exc:
        if not conflict(exc):raise
    if bounded(client.get_object(Bucket=bucket,Key=key)['Body'])!=raw:raise ValueError('retained artifact differs')


def pinned(module,ref,prefix,read):
    raw=Path(module.__file__).read_bytes();sha=hashlib.sha256(raw).hexdigest()
    if ref!={'key':prefix+'compilers/'+sha+'.py','sha256':sha} or read(ref['key'])!=raw:
        raise ValueError('reviewed compiler differs; matching release required')


def upstream(packet,prefix,contract,module,read):
    ref=packet.get('replay') or {};key=ref.get('manifest_key','')
    if not re.fullmatch(re.escape(prefix)+r'runs/[a-f0-9]{64}\.json',key):raise ValueError('upstream replay required')
    raw=read(key);manifest=json.loads(raw)
    if key!=prefix+'runs/'+model.digest(manifest)+'.json' or manifest.get('contract')!=contract:raise ValueError('upstream run identity differs')
    if manifest.get('output_sha256')!=ref.get('output_sha256') or model.digest({k:v for k,v in packet.items() if k!='replay'})!=ref['output_sha256']:
        raise ValueError('upstream packet differs')
    if manifest['generated_at']!=packet['generated_at']:raise ValueError('upstream clock differs')
    pinned(module,manifest['compiler'],prefix,read)
    return manifest


def evidence(ref,provider,expected,read):
    if ref.get('source_url')!=public_source_url(expected):raise ValueError('retained request differs')
    sha=ref.get('sha256','');request_sha=hashlib.sha256(ref['source_url'].encode()).hexdigest()
    if (ref.get('contract')!='source-evidence.v1' or ref.get('provider')!=provider or ref.get('captured') is not True
            or not re.fullmatch('[a-f0-9]{64}',sha) or ref.get('key')!='data/evidence/'+provider+'/'+request_sha+'/'+sha+'.bin.gz'
            or type(ref.get('bytes')) is not int or not 0<ref['bytes']<=MAX):raise ValueError('original evidence identity differs')
    model.clock(ref['first_received_at'])
    raw=read(ref['key'])
    if len(raw)!=ref['bytes'] or hashlib.sha256(raw).hexdigest()!=sha:raise ValueError('original source bytes differ')
    return raw


def fred_originals(packet,read):
    manifest=upstream(packet,'data/report-research/','report-research-replay.v1',report_observations,read)
    def one(sid):
        if sid not in packet.get('measurements',{}):return sid,None
        entry=manifest['inputs'][sid];item={'evidence':entry['evidence'],'acquired_at':entry['acquired_at']}
        if set(entry['evidence'])!={'definition','observations'}:raise ValueError('both FRED originals required')
        for part,ref in entry['evidence'].items():
            url=ref['source_url'];parsed=urlsplit(url);q=parse_qs(parsed.query)
            path='/fred/series'+('/observations' if part=='observations' else '')
            if parsed.scheme!='https' or parsed.netloc!='api.stlouisfed.org' or parsed.path!=path or q.get('series_id')!=[sid]:raise ValueError('FRED series request differs')
            # This producer clocks response acquisition before parsing/capture.
            # First evidence capture can therefore be later than acquisition;
            # neither timestamp may be after the compiled upstream packet.
            if model.clock(ref['first_received_at'])>model.clock(packet['generated_at']):raise ValueError('original receipt is future')
            item[part]=json.loads(evidence(ref,'fred',url,read))
        return sid,item
    with ThreadPoolExecutor(max_workers=4) as pool:return {sid:item for sid,item in pool.map(one,model.SERIES) if item is not None}


def ciss_original(packet,read):
    manifest=upstream(packet,ciss_source_model.PREFIX,'ciss-research-replay.v1',ciss_source_model,read)
    key=ciss_source_model.HEAD;entry=manifest['histories'][key]
    expected='https://data-api.ecb.europa.eu/service/data/'+key.replace('.','/',1)+'?format=csvdata'
    if entry['request_url']!=expected:raise ValueError('original ECB request differs')
    raw=evidence(entry['evidence'],'ecb',expected,read)
    return {'packet':packet,'entry':entry,'raw':raw}


def checked(ref,category,read):
    sha=ref.get('sha256','')
    if not re.fullmatch('[a-f0-9]{64}',sha) or ref.get('key')!=model.PREFIX+category+'/'+sha+'.json':raise ValueError('artifact identity differs')
    raw=read(ref['key'])
    if len(raw)!=ref['bytes'] or hashlib.sha256(raw).hexdigest()!=sha:raise ValueError('artifact content differs')
    return json.loads(raw)


def compile_output(inputs,read):
    if inputs.get('contract')!='crisis-inputs.v1':raise ValueError('unsupported input contract')
    originals=fred_originals(inputs['macro'],read)
    ciss=model.ciss_native(ciss_original(inputs['ciss'],read),inputs['generated_at']) if inputs.get('ciss') else None
    return model.build(inputs['macro'],originals,inputs['context'],inputs['generated_at'],ciss)


def replay(ref,read):
    key=ref.get('manifest_key','')
    if not re.fullmatch(re.escape(model.PREFIX)+r'runs/[a-f0-9]{64}\.json',key):raise ValueError('Crisis run identity required')
    raw=read(key);manifest=json.loads(raw)
    if key!=model.PREFIX+'runs/'+model.digest(manifest)+'.json' or manifest.get('contract')!='crisis-replay.v1' or manifest.get('output_sha256')!=ref.get('output_sha256'):
        raise ValueError('Crisis run binding differs')
    if set(manifest['compilers'])!={m.__name__ for m in COMPILERS}:raise ValueError('compiler set differs')
    for module in COMPILERS:pinned(module,manifest['compilers'][module.__name__],model.PREFIX,read)
    inputs=checked(manifest['input'],'inputs',read)
    output=compile_output(inputs,read)
    if output!=checked(manifest['output'],'outputs',read) or model.digest(output)!=manifest['output_sha256'] or output['generated_at']!=manifest['generated_at']:
        raise ValueError('original-source Crisis replay differs')
    return output


def retain(client,bucket,inputs,output):
    refs={}
    for kind,doc in (('input',inputs),('output',output)):
        raw=model.encoded(doc);sha=hashlib.sha256(raw).hexdigest();key=model.PREFIX+kind+'s/'+sha+'.json'
        immutable(client,bucket,key,raw);refs[kind]={'key':key,'sha256':sha,'bytes':len(raw)}
    compilers={}
    for module in COMPILERS:
        raw=Path(module.__file__).read_bytes();sha=hashlib.sha256(raw).hexdigest();key=model.PREFIX+'compilers/'+sha+'.py'
        immutable(client,bucket,key,raw,'text/x-python');compilers[module.__name__]={'key':key,'sha256':sha}
    manifest={'contract':'crisis-replay.v1','generated_at':inputs['generated_at'],**refs,'compilers':compilers,'output_sha256':model.digest(output),
        'scope':'23 FRED original pairs and canonical ECB headline original; other public packets are explicitly non-voting metadata.'}
    key=model.PREFIX+'runs/'+model.digest(manifest)+'.json';immutable(client,bucket,key,model.encoded(manifest))
    ref={'manifest_key':key,'output_sha256':manifest['output_sha256']}
    if replay(ref,reader(client,bucket))!=output:raise ValueError('retained replay differs')
    return ref


def publish(client,bucket,key,packet):
    if key not in MUTABLE:raise ValueError('unsupported mutable destination')
    stamp=model.clock(packet['generated_at'])
    for _ in range(4):
        try:
            obj=client.get_object(Bucket=bucket,Key=key);raw=bounded(obj['Body']);old=json.loads(raw)
            old_clock=old.get('generated_at') or old.get('updated_at')
            if old_clock:
                if model.clock(old_clock)>stamp:return False
                if model.clock(old_clock)==stamp and old!=packet:raise ValueError('conflicting same-clock publication')
            if old.get('contract')==packet.get('contract')==model.CONTRACT:
                if model.clock(old['source_generated_at'])>model.clock(packet['source_generated_at']):return False
                old_ciss=(old.get('ciss') or {}).get('source_generated_at');new_ciss=(packet.get('ciss') or {}).get('source_generated_at')
                if old_ciss and new_ciss and model.clock(old_ciss)>model.clock(new_ciss):return False
            immutable(client,bucket,PRIVATE+hashlib.sha256(raw).hexdigest()+'.bin',raw,'application/octet-stream',True)
            condition={'IfMatch':obj['ETag']}
        except Exception as exc:
            if not missing(exc):raise
            condition={'IfNoneMatch':'*'}
        try:
            client.put_object(Bucket=bucket,Key=key,Body=model.encoded(packet),ContentType='application/json',CacheControl='no-store',**condition)
            live=json.loads(reader(client,bucket)(key))
            if live!=packet and model.clock(live['generated_at'])<=stamp:raise ValueError('publication readback differs')
            return True
        except Exception as exc:
            if not conflict(exc):raise
    raise RuntimeError('publication conflict limit; immutable run remains retained')


def run(client,bucket,validation_only=False):
    read=reader(client,bucket);macro=json.loads(read('data/report-measurements.json'))
    try:ciss=json.loads(read('data/ciss-stress.json'))
    except Exception as exc:
        if not missing(exc):raise
        ciss=None
    context={}
    for leaf in model.CONTEXT_KEYS:
        key='data/'+leaf+'.json'
        try:
            raw=read(key);doc=json.loads(raw);sha=hashlib.sha256(raw).hexdigest()
            immutable(client,bucket,PRIVATE+sha+'.bin',raw,'application/octet-stream',True)
            status=(doc.get('quality') or {}).get('status') if isinstance(doc.get('quality'),dict) else None
            context[leaf]={'source':key,'source_generated_at':doc.get('generated_at'),'source_contract':doc.get('contract'),
                'declared_quality':status,'content_sha256':sha,'available':True,'verification':'retained_metadata_only',
                'note':'Complete response is protected for audit; original measurements are not qualified by this context link.'}
        except Exception as exc:
            context[leaf]={'source':key,'available':False,'error':type(exc).__name__,'verification':'unavailable'}
    inputs={'contract':'crisis-inputs.v1','generated_at':datetime.now(timezone.utc).isoformat(),'macro':macro,'ciss':ciss,'context':context}
    output=compile_output(inputs,read)
    if validation_only:return {'validation_only':True,'quality':output['quality'],'artifact_size_bytes':len(model.encoded(output))}
    ref=retain(client,bucket,inputs,output);packet={**output,'replay':ref}
    published=publish(client,bucket,model.CURRENT,packet)
    if published:
        publish(client,bucket,'data/defcon.json',packet)
        publish(client,bucket,'data/crisis-composite-history.json',{'contract':'crisis-history.v2','generated_at':output['generated_at'],
            'native_history_run':ref,'snapshots':[],'note':'Current native histories are retained in immutable runs; the previous score history is protected and unqualified.',**model.PERMISSIONS})
    return {'published':published,'generated_at':output['generated_at'],'quality':output['quality'],'replay':ref}
