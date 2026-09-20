"""Retain public originals, independently reconstruct, replay, and publish with CAS."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime,timezone
from pathlib import Path
import gzip,hashlib,io,json,re,sys
from urllib.parse import urlsplit,parse_qs

import plumbing_research_model as model
import plumbing_research_catalog
import report_observations
import research_brief_model
import evidence_store

MAX=32*1024*1024
PRIVATE='audit-private/20260909-originals/crisis-plumbing/'
COMPILERS=(model,plumbing_research_catalog,report_observations,research_brief_model,evidence_store,sys.modules[__name__])
PUBLIC_INPUTS={model.CURRENT,'data/report-measurements.json','data/eurodollar-plumbing.json','data/repo-market.json'}


def bounded(stream):
    try:raw=stream.read(MAX+1)
    finally:stream.close()
    if len(raw)>MAX:raise ValueError('research artifact exceeds bound')
    return raw


def code(exc):return str(getattr(exc,'response',{}).get('Error',{}).get('Code',''))
def conflict(exc):return code(exc) in ('412','409','PreconditionFailed','ConditionalRequestConflict')
def missing(exc):return code(exc) in ('404','NoSuchKey')


def allowed(key):
    return isinstance(key,str) and (key in PUBLIC_INPUTS or bool(re.fullmatch(
        r'data/(?:evidence/(?:fred|funding)/[a-f0-9]{64}/[a-f0-9]{64}\.bin\.gz|'
        r'(?:report-research|funding-research|plumbing-research)/(?:runs|inputs|outputs|compilers|histories)/[a-f0-9]{64}\.(?:json|py))',key)))


def reader(client,bucket):
    def read(key):
        if not allowed(key):raise ValueError('unapproved public source path')
        raw=bounded(client.get_object(Bucket=bucket,Key=key)['Body'])
        return bounded(gzip.GzipFile(fileobj=io.BytesIO(raw))) if key.endswith('.gz') else raw
    return read


def immutable(client,bucket,key,raw,kind='application/json',private=False):
    if not isinstance(raw,bytes) or len(raw)>MAX:raise ValueError('retained artifact exceeds bound')
    if private:
        if not re.fullmatch(re.escape(PRIVATE)+r'[a-f0-9]{64}\.bin',key):raise ValueError('protected archive identity required')
    elif not re.fullmatch(re.escape(model.PREFIX)+r'(?:runs|inputs|outputs|compilers|histories)/[a-f0-9]{64}\.(?:json|py)',key):
        raise ValueError('plumbing artifact identity required')
    try:client.put_object(Bucket=bucket,Key=key,Body=raw,ContentType=kind,IfNoneMatch='*',CacheControl='no-store' if private else 'public, max-age=31536000, immutable')
    except Exception as exc:
        if not conflict(exc):raise
    if bounded(client.get_object(Bucket=bucket,Key=key)['Body'])!=raw:raise ValueError('retained artifact differs')


def pinned(module,ref,prefix,read):
    raw=Path(module.__file__).read_bytes();sha=hashlib.sha256(raw).hexdigest()
    if ref!={'key':prefix+'compilers/'+sha+'.py','sha256':sha} or read(ref['key'])!=raw:
        raise ValueError('reviewed compiler differs; matching release required')


def upstream(packet,prefix,contract,read):
    ref=packet.get('replay') or {};key=ref.get('manifest_key','')
    if not re.fullmatch(re.escape(prefix)+r'runs/[a-f0-9]{64}\.json',key):raise ValueError('upstream replay required')
    manifest=json.loads(read(key))
    if key!=prefix+'runs/'+model.digest(manifest)+'.json' or manifest.get('contract')!=contract:raise ValueError('upstream run identity differs')
    if manifest.get('output_sha256')!=ref.get('output_sha256') or model.digest({k:v for k,v in packet.items() if k!='replay'})!=ref['output_sha256']:
        raise ValueError('upstream packet differs')
    if manifest['generated_at']!=packet['generated_at']:raise ValueError('upstream clock differs')
    return manifest


def evidence(ref,provider,expected,at,read):
    if ref.get('source_url')!=evidence_store.public_source_url(expected):raise ValueError('retained request differs')
    sha=ref.get('sha256','');request_sha=hashlib.sha256(ref['source_url'].encode()).hexdigest()
    if (ref.get('contract')!='source-evidence.v1' or ref.get('provider')!=provider or ref.get('captured') is not True
            or not re.fullmatch('[a-f0-9]{64}',sha) or ref.get('key')!='data/evidence/'+provider+'/'+request_sha+'/'+sha+'.bin.gz'
            or type(ref.get('bytes')) is not int or not 0<ref['bytes']<=MAX):raise ValueError('original evidence identity differs')
    if model.clock(ref['first_received_at'])>model.clock(at):raise ValueError('original receipt is future')
    raw=read(ref['key'])
    if len(raw)!=ref['bytes'] or hashlib.sha256(raw).hexdigest()!=sha:raise ValueError('original source bytes differ')
    return raw


def fred_originals(packet,read):
    prefix='data/report-research/'
    manifest=upstream(packet,prefix,'report-research-replay.v1',read)
    pinned(report_observations,manifest['compiler'],prefix,read)
    if packet['replay'].get('compiler_sha256')!=manifest['compiler']['sha256']:raise ValueError('macro compiler reference differs')
    def one(sid):
        if sid not in packet.get('measurements',{}):return sid,None
        entry=manifest['inputs'][sid];item={'evidence':entry['evidence'],'acquired_at':entry['acquired_at']}
        if not model.clock(entry['acquired_at'])<=model.clock(packet['generated_at']):raise ValueError('macro acquisition is future')
        if set(entry['evidence'])!={'definition','observations'}:raise ValueError('both FRED originals required')
        for part,ref in entry['evidence'].items():
            url=ref['source_url'];parsed=urlsplit(url);q=parse_qs(parsed.query)
            path='/fred/series'+('/observations' if part=='observations' else '')
            if parsed.scheme!='https' or parsed.netloc!='api.stlouisfed.org' or parsed.path!=path or q.get('series_id')!=[sid]:raise ValueError('FRED series request differs')
            if q.get('file_type')!=['json'] or q.get('api_key'):raise ValueError('safe FRED request required')
            item[part]=json.loads(evidence(ref,'fred',url,packet['generated_at'],read))
        return sid,item
    with ThreadPoolExecutor(max_workers=4) as pool:return {sid:item for sid,item in pool.map(one,model.SERIES) if item is not None}


def checked(ref,category,read,prefix=None):
    prefix=prefix or model.PREFIX;sha=ref.get('sha256','')
    if not re.fullmatch('[a-f0-9]{64}',sha) or ref.get('key')!=prefix+category+'/'+sha+'.json':raise ValueError('artifact identity differs')
    raw=read(ref['key'])
    if type(ref.get('bytes')) is not int or len(raw)!=ref['bytes'] or hashlib.sha256(raw).hexdigest()!=sha:raise ValueError('artifact content differs')
    return json.loads(raw)


def ofr_original(packet,read):
    """Bind the whole carrier/run/input, then independently parse just OFR originals.

    Other Funding calculations are neither reused nor represented as replayed.
    """
    prefix='data/funding-research/'
    manifest=upstream(packet,prefix,'funding-original-replay.v1',read)
    if checked(manifest['output'],'outputs',read,prefix)!={k:v for k,v in packet.items() if k!='replay'}:raise ValueError('OFR carrier output differs')
    inputs=checked(manifest['input'],'inputs',read,prefix)
    if inputs.get('contract')!='funding-original-inputs.v1':raise ValueError('OFR carrier inputs differ')
    entry=inputs.get('originals',{}).get('ofr_fsi')
    if not entry:return None
    if entry.get('url')!=model.OFR_URL or model.clock(entry['acquired_at'])>model.clock(packet['generated_at']):raise ValueError('OFR acquisition request/clock differs')
    raw=evidence(entry['evidence'],'funding',model.OFR_URL,packet['generated_at'],read)
    return {**entry,'raw':raw}


def compile_output(inputs,read):
    if inputs.get('contract')!='plumbing-inputs.v1':raise ValueError('unsupported input contract')
    originals=fred_originals(inputs['macro'],read);ofr=None
    if inputs.get('funding'):
        original=ofr_original(inputs['funding'],read)
        if original:ofr=model.ofr_native(inputs['funding'],original,inputs['generated_at'])
    return model.build(inputs['macro'],originals,inputs['generated_at'],ofr,inputs.get('context'))


def replay(ref,read):
    key=ref.get('manifest_key','')
    if not re.fullmatch(re.escape(model.PREFIX)+r'runs/[a-f0-9]{64}\.json',key):raise ValueError('Plumbing run identity required')
    manifest=json.loads(read(key))
    if key!=model.PREFIX+'runs/'+model.digest(manifest)+'.json' or manifest.get('contract')!='plumbing-replay.v1' or manifest.get('output_sha256')!=ref.get('output_sha256'):
        raise ValueError('Plumbing run binding differs')
    if set(manifest['compilers'])!={m.__name__ for m in COMPILERS}:raise ValueError('compiler set differs')
    for module in COMPILERS:pinned(module,manifest['compilers'][module.__name__],model.PREFIX,read)
    output,histories=compile_output(checked(manifest['input'],'inputs',read),read)
    if output!=checked(manifest['output'],'outputs',read) or model.digest(output)!=manifest['output_sha256'] or output['generated_at']!=manifest['generated_at']:
        raise ValueError('original-source Plumbing replay differs')
    for key,raw in histories.items():
        if read(key)!=raw:raise ValueError('native history shard differs')
    return output


def retain(client,bucket,inputs,output,histories):
    for key,raw in histories.items():immutable(client,bucket,key,raw)
    refs={}
    for kind,doc in (('input',inputs),('output',output)):
        raw=model.encoded(doc);sha=hashlib.sha256(raw).hexdigest();key=model.PREFIX+kind+'s/'+sha+'.json'
        immutable(client,bucket,key,raw);refs[kind]={'key':key,'sha256':sha,'bytes':len(raw)}
    compilers={}
    for module in COMPILERS:
        raw=Path(module.__file__).read_bytes();sha=hashlib.sha256(raw).hexdigest();key=model.PREFIX+'compilers/'+sha+'.py'
        immutable(client,bucket,key,raw,'text/x-python');compilers[module.__name__]={'key':key,'sha256':sha}
    manifest={'contract':'plumbing-replay.v1','generated_at':inputs['generated_at'],**refs,'compilers':compilers,'output_sha256':model.digest(output),
        'scope':'All requested FRED originals and independently parsed canonical OFR CSV; historical ranks are descriptive, not forecast qualification.'}
    key=model.PREFIX+'runs/'+model.digest(manifest)+'.json';immutable(client,bucket,key,model.encoded(manifest))
    ref={'manifest_key':key,'output_sha256':manifest['output_sha256']}
    if replay(ref,reader(client,bucket))!=output:raise ValueError('retained replay differs')
    return ref


def publish(client,bucket,packet):
    stamp=model.clock(packet['generated_at'])
    for _ in range(4):
        try:
            obj=client.get_object(Bucket=bucket,Key=model.CURRENT);raw=bounded(obj['Body']);old=json.loads(raw)
            if old.get('generated_at'):
                if model.clock(old['generated_at'])>stamp:return False
                if model.clock(old['generated_at'])==stamp and old!=packet:raise ValueError('conflicting same-clock publication')
            if old.get('contract')==packet.get('contract')==model.CONTRACT:
                for source,previous in old.get('source_clocks',{}).items():
                    current=packet.get('source_clocks',{}).get(source)
                    if previous and current and model.clock(previous)>model.clock(current):return False
            immutable(client,bucket,PRIVATE+hashlib.sha256(raw).hexdigest()+'.bin',raw,'application/octet-stream',True)
            condition={'IfMatch':obj['ETag']}
        except Exception as exc:
            if not missing(exc):raise
            condition={'IfNoneMatch':'*'}
        try:
            client.put_object(Bucket=bucket,Key=model.CURRENT,Body=model.encoded(packet),ContentType='application/json',CacheControl='no-store',**condition)
            live=json.loads(reader(client,bucket)(model.CURRENT))
            if live!=packet and model.clock(live['generated_at'])<=stamp:raise ValueError('publication readback differs')
            return True
        except Exception as exc:
            if not conflict(exc):raise
    raise RuntimeError('publication conflict limit; immutable run remains retained')


def run(client,bucket,validation_only=False):
    read=reader(client,bucket);macro=json.loads(read('data/report-measurements.json'))
    try:funding=json.loads(read('data/eurodollar-plumbing.json'))
    except Exception as exc:
        if not missing(exc):raise
        funding=None
    context={}
    try:
        raw=read('data/repo-market.json');doc=json.loads(raw);sha=hashlib.sha256(raw).hexdigest()
        if not validation_only:immutable(client,bucket,PRIVATE+sha+'.bin',raw,'application/octet-stream',True)
        context['repo_market']={'source':'data/repo-market.json','source_generated_at':doc.get('generated_at'),
            'source_contract':doc.get('contract'),'content_sha256':sha,'available':True,'verification':'metadata_only',
            'note':'Prior repo sidecar remains linked. Its tail score does not establish an early-warning forecast or voting authority.',**model.AUTHORITY}
    except Exception as exc:
        if not missing(exc):raise
        context['repo_market']={'source':'data/repo-market.json','available':False,'verification':'unavailable',**model.AUTHORITY}
    inputs={'contract':'plumbing-inputs.v1','generated_at':datetime.now(timezone.utc).isoformat(),'macro':macro,'funding':funding,'context':context}
    output,histories=compile_output(inputs,read)
    if validation_only:return {'validation_only':True,'quality':output['quality'],'artifact_size_bytes':len(model.encoded(output))}
    ref=retain(client,bucket,inputs,output,histories);packet={**output,'replay':ref}
    published=publish(client,bucket,packet)
    return {'published':published,'generated_at':output['generated_at'],'quality':output['quality'],'replay':ref,
        'history_shards':len(histories),'private_account_reads':0,'paid_ai_calls':0,'notifications_sent':0,'portfolio_writes':0,'signals_emitted':0}
