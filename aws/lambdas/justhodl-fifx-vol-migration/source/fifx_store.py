"""Complete source retention, sequential shard replay and conditional publication."""
from datetime import datetime, timezone
from pathlib import Path
import gzip, hashlib, io, json, re, sys, time
import fifx_model as model
import fifx_candidate as arithmetic
import fifx_catalog as catalog
import fifx_originals as originals
import fifx_timezones as timezones
import verify_fifx_arithmetic as independent
import fifx_acquire as acquisition
import fifx_qualification as qualification
import canonical_fred_replay as canonical
import report_observations, research_brief_model, evidence_store

MAX=64*1024*1024
ROOT=Path(__file__).resolve().parent
SOURCE='data/report-measurements.json'
BOND='data/bond-vol.json'
COMPILERS=(model,arithmetic,catalog,originals,timezones,independent,acquisition,qualification,
           canonical,report_observations,research_brief_model,evidence_store,sys.modules[__name__])
sha=lambda raw:hashlib.sha256(raw).hexdigest()
now=lambda:datetime.now(timezone.utc).isoformat()
error=lambda exc:str(getattr(exc,'response',{}).get('Error',{}).get('Code',''))
conflict=lambda exc:error(exc) in ('409','412','ConditionalRequestConflict','PreconditionFailed')


def bounded(stream,limit=MAX):
    try:raw=stream.read(limit+1)
    finally:stream.close()
    if not 0<len(raw)<=limit:raise ValueError('Complete bounded artifact required')
    return raw


def qualified_arithmetic():
    for name,digest in qualification.QUALIFIED.items():
        if sha((ROOT/name).read_bytes())!=digest:raise ValueError('Qualified FI/FX arithmetic differs: '+name)


def allowed(key):
    return isinstance(key,str) and (key in (SOURCE,BOND,model.CURRENT,model.HISTORY) or bool(re.fullmatch(
        r'data/(?:evidence/fred/[a-f0-9]{64}/[a-f0-9]{64}\.bin\.gz|'
        r'report-research/(?:runs|inputs|outputs|compilers)/[a-f0-9]{64}\.(?:json|py)|'
        r'(?:bond-vol-research|fifx-vol-research)/(?:runs|inputs|outputs|compilers|snapshots|series|quotes|views|originals|receipts)/[a-f0-9]{64}\.(?:json|py|bin))',key)))


def reader(client,bucket):
    def read(key):
        if not allowed(key):raise ValueError('Unreviewed source artifact path')
        raw=bounded(client.get_object(Bucket=bucket,Key=key)['Body'])
        return bounded(gzip.GzipFile(fileobj=io.BytesIO(raw))) if key.endswith('.gz') else raw
    return read


def retain_bytes(client,bucket,raw,category,extension='json',private=False):
    if not isinstance(raw,bytes) or not 0<len(raw)<=MAX:raise ValueError('Complete bounded retained bytes required')
    if category not in ('runs','inputs','outputs','compilers','snapshots','series','views','originals','receipts') or extension not in ('json','py','bin'):
        raise ValueError('Unreviewed artifact category')
    key=model.PRIVATE+sha(raw)+'.bin' if private else model.PREFIX+category+'/'+sha(raw)+'.'+extension
    try:
        client.put_object(Bucket=bucket,Key=key,Body=raw,IfNoneMatch='*',
            ContentType='application/octet-stream' if private or extension=='bin' else 'text/plain' if extension=='py' else 'application/json',
            CacheControl='no-store' if private else 'public, max-age=31536000, immutable')
    except Exception as exc:
        if not conflict(exc):raise
    if bounded(client.get_object(Bucket=bucket,Key=key)['Body'])!=raw:raise ValueError('Retained artifact differs')
    return {'key':key,'sha256':sha(raw),'bytes':len(raw)}


def checked(ref,category,read,extension='json',prefix=model.PREFIX):
    if (not isinstance(ref,dict) or not re.fullmatch('[a-f0-9]{64}',str(ref.get('sha256'))) or
        ref.get('key')!=prefix+category+'/'+ref['sha256']+'.'+extension or type(ref.get('bytes')) is not int or not 0<ref['bytes']<=MAX):
        raise ValueError('Exact immutable coordinates required')
    raw=read(ref['key'])
    if len(raw)!=ref['bytes'] or sha(raw)!=ref['sha256']:raise ValueError('Immutable original differs')
    return raw if extension!='json' else json.loads(raw)


def binding(packet,read):
    key=(packet.get('replay') or {}).get('manifest_key','')
    if not re.fullmatch(re.escape(model.PREFIX)+r'runs/[a-f0-9]{64}\.json',key):raise ValueError('Native run required')
    raw=read(key);run=json.loads(raw)
    if key!=model.PREFIX+'runs/'+sha(raw)+'.json' or run.get('contract')!='fifx-vol-replay.v1':raise ValueError('Native run binding differs')
    if (packet['replay'].get('view_sha256')!=run['view']['sha256'] or checked(run['view'],'views',read)!={k:v for k,v in packet.items() if k!='replay'}
        or packet['generated_at']!=run['generated_at']):raise ValueError('Current packet differs from immutable view')
    return run


def definitions(inputs,read):
    source=checked(inputs['macro'],'snapshots',read)
    restored=canonical.restore(source,catalog.FRED,read)
    return {sid:restored[sid]['definition'] if restored[sid] is not None else None for sid in catalog.FRED}


def compile_source(sid,inputs,defs,read):
    entry=inputs['sources'][sid]
    raw=checked(entry['original'],'originals',read,'bin') if entry['original'] else None
    receipt=checked(entry['receipt'],'receipts',read) if entry['receipt'] else None
    out=arithmetic.build_source(sid,raw,receipt,inputs['generated_at'],defs.get(sid))
    proof=independent.verify(out,raw,receipt,defs.get(sid))
    return out,proof


def replay(packet,read):
    qualified_arithmetic();run=binding(packet,read)
    if set(run['compilers'])!={m.__name__ for m in COMPILERS}:raise ValueError('Complete native compiler closure required')
    for module in COMPILERS:
        if checked(run['compilers'][module.__name__],'compilers',read,'py')!=Path(module.__file__).read_bytes():
            raise ValueError('Reviewed compiler bytes differ')
    inputs=checked(run['input'],'inputs',read)
    if inputs.get('contract')!='fifx-vol-inputs.v1' or set(inputs['sources'])!=set(catalog.SOURCES) or set(run['series'])!=set(catalog.SOURCES):
        raise ValueError('Every native source identity required')
    defs=definitions(inputs,read);summaries={};marks={};proofs={}
    for sid in catalog.SOURCES:
        output,proof=compile_source(sid,inputs,defs,read);raw=model.encoded(output);ref=run['series'][sid]
        if ref!={'key':model.PREFIX+'series/'+sha(raw)+'.json','sha256':sha(raw),'bytes':len(raw)}:
            raise ValueError('Whole source reconstruction differs: '+sid)
        if checked(ref,'series',read)!=output:raise ValueError('Retained complete source differs')
        summaries[sid],marks[sid]=model.compact_source(output,ref,inputs['previous_watermarks']);proofs[sid]=proof
        del output,raw
    if proofs!=checked(run['proofs'],'outputs',read):raise ValueError('Complete independent proof differs')
    expected=model.packet(inputs['generated_at'],summaries,marks,inputs['predecessors'],inputs['context'],inputs['acquisition'])
    if expected!={k:v for k,v in packet.items() if k!='replay'}:raise ValueError('Native compact projection differs')
    return proofs


def retain(client,bucket,inputs):
    qualified_arithmetic()
    put=lambda value,kind:retain_bytes(client,bucket,model.encoded(value),kind)
    read=reader(client,bucket);defs=definitions(inputs,read);refs={};summaries={};marks={};proofs={}
    for sid in catalog.SOURCES:
        output,proof=compile_source(sid,inputs,defs,read)
        refs[sid]=put(output,'series');proofs[sid]=proof
        summaries[sid],marks[sid]=model.compact_source(output,refs[sid],inputs['previous_watermarks'])
        del output
    view=model.packet(inputs['generated_at'],summaries,marks,inputs['predecessors'],inputs['context'],inputs['acquisition'])
    run={'contract':'fifx-vol-replay.v1','generated_at':inputs['generated_at'],'input':put(inputs,'inputs'),
         'view':put(view,'views'),'series':refs,'proofs':put(proofs,'outputs'),
         'compilers':{m.__name__:retain_bytes(client,bucket,Path(m.__file__).read_bytes(),'compilers','py') for m in COMPILERS}}
    ref=put(run,'runs');packet={**view,'replay':{'manifest_key':ref['key'],'view_sha256':run['view']['sha256']}}
    if replay(packet,reader(client,bucket))!=proofs:raise ValueError('Fresh retained original replay differs')
    return packet


def previous_state(client,bucket):
    raw=bounded(client.get_object(Bucket=bucket,Key=model.CURRENT)['Body'])
    current=retain_bytes(client,bucket,raw,'snapshots',private=True)
    try:packet=json.loads(raw)
    except (ValueError,UnicodeError):packet={}
    if not isinstance(packet,dict):packet={}
    if packet.get('contract')==model.CONTRACT:
        binding(packet,reader(client,bucket))
        return packet['predecessors'],model.watermarks(packet)
    history=bounded(client.get_object(Bucket=bucket,Key=model.HISTORY)['Body'])
    return {'packet':current,'history':retain_bytes(client,bucket,history,'snapshots',private=True)},model.empty_watermarks()


def existing_move(packet,read):
    prefix='data/bond-vol-research/';key=(packet.get('replay') or {}).get('manifest_key','')
    if packet.get('contract')!='bond-vol-research.v1' or not re.fullmatch(re.escape(prefix)+r'runs/[a-f0-9]{64}\.json',key):
        raise ValueError('Bound Bond Vol source required')
    raw=read(key);run=json.loads(raw)
    if key!=prefix+'runs/'+sha(raw)+'.json' or run.get('contract')!='bond-vol-replay.v1' or run['output_sha256']!=packet['replay']['output_sha256']:
        raise ValueError('Bond source run differs')
    if checked(run['view'],'views',read,prefix=prefix)!={k:v for k,v in packet.items() if k!='replay'}:
        raise ValueError('Bond source view differs')
    inputs=checked(run['input'],'inputs',read,prefix=prefix)
    if inputs['quote'] is None:return None,None
    raw=checked(inputs['quote'],'originals',read,'bin',prefix=prefix)
    receipt=checked(inputs['quote_receipt'],'receipts',read,prefix=prefix)
    originals.receipt_check('^MOVE',raw,receipt,now())
    return raw,receipt


def publish(client,bucket,packet,key=model.CURRENT):
    if key not in (model.CURRENT,model.HISTORY):raise ValueError('Native publication head required')
    if (packet.get('contract')!=model.CONTRACT or any(packet.get(k) is not False for k in catalog.AUTHORITY) or
        packet.get('decision')!={'verb':'WAIT','meaning':'abstain'} or packet.get('call') is not None or packet.get('regime') is not None or
        packet.get('signals')!=[] or packet.get('portfolio_consequences',{}).get('status')!='UNAVAILABLE' or
        packet['portfolio_consequences'].get('target_weights') is not None or set(packet.get('series',{}))!=set(catalog.SOURCES) or
        any(any(row.get(k) is not False for k in catalog.AUTHORITY) for row in packet['series'].values())):
        raise ValueError('Complete research-only packet required')
    binding(packet,reader(client,bucket));stamp=model.clock(packet['generated_at']);marks=model.watermarks(packet)
    for _ in range(4):
        obj=client.get_object(Bucket=bucket,Key=key);raw=bounded(obj['Body'])
        try:old=json.loads(raw)
        except (ValueError,UnicodeError):old={}
        if not isinstance(old,dict):old={}
        if old.get('generated_at'):
            prior=model.clock(old['generated_at'])
            if prior>stamp:return False
            if prior==stamp:
                if old!=packet:raise ValueError('Conflicting same-clock head')
                return True
        if old.get('contract')==model.CONTRACT:
            binding(old,reader(client,bucket))
            for sid,before in model.watermarks(old).items():
                after=marks[sid]
                if before['acquired_at'] and (not after['acquired_at'] or model.clock(after['acquired_at'])<model.clock(before['acquired_at'])):return False
                if before['observation_date'] and (not after['observation_date'] or after['observation_date']<before['observation_date']):return False
                row=packet['series'][sid]
                if row['current']:
                    acquired=(row.get('receipt') or {}).get('acquired_at');observed=row['current']['date']
                    if before['acquired_at'] and (not acquired or model.clock(acquired)<model.clock(before['acquired_at'])):return False
                    if before['observation_date'] and observed<before['observation_date']:return False
        retain_bytes(client,bucket,raw,'snapshots',private=True)
        try:
            client.put_object(Bucket=bucket,Key=key,Body=model.encoded(packet),ContentType='application/json',CacheControl='no-store',IfMatch=obj['ETag'])
            live=json.loads(reader(client,bucket)(key))
            if live!=packet and model.clock(live['generated_at'])<=stamp:raise ValueError('Head readback differs')
            return True
        except Exception as exc:
            if not conflict(exc):raise
    raise RuntimeError('Publication conflict ceiling; whole immutable run retained')


def run(client,bucket,request_id):
    qualified_arithmetic()
    if not isinstance(request_id,str) or not 1<=len(request_id)<=200:raise ValueError('Native request identity required')
    key=model.PRIVATE+'requests/'+sha(('native:'+request_id).encode())+'.json'
    progress={'status':'claimed','started_at':now(),'provider_requests':0,'sources':{}}
    def journal(claim=False):
        raw=model.encoded(progress)
        client.put_object(Bucket=bucket,Key=key,Body=raw,ContentType='application/json',CacheControl='no-store',**({'IfNoneMatch':'*'} if claim else {}))
        if bounded(client.get_object(Bucket=bucket,Key=key)['Body'])!=raw:raise ValueError('Native claim differs')
    def save(raw,receipt):
        return {'original':retain_bytes(client,bucket,raw,'originals','bin') if raw is not None else None,
                'receipt':retain_bytes(client,bucket,model.encoded(receipt),'receipts') if receipt is not None else None}
    journal(True)
    try:
        predecessors,previous=previous_state(client,bucket);read=reader(client,bucket)
        macro=retain_bytes(client,bucket,read(SOURCE),'snapshots')
        try:bond_raw=read(BOND)
        except Exception as exc:
            bond_raw=None;context={'source_key':BOND,'original':None,'independent_votes':0,'status':'unavailable','error_type':type(exc).__name__}
        else:context={'source_key':BOND,'original':retain_bytes(client,bucket,bond_raw,'snapshots',private=True),
                     'independent_votes':0,'status':'retained_unqualified_context','captured_at':now()}
        sources={}
        try:raw,receipt=existing_move(json.loads(bond_raw) if bond_raw else {},read)
        except Exception as exc:
            raw=receipt=None;progress['sources']['^MOVE']={'status':'bound_source_unavailable','error_type':type(exc).__name__}
        else:progress['sources']['^MOVE']={'status':'reused_bound_original' if raw is not None else 'source_unavailable'}
        sources['^MOVE']=save(raw,receipt);journal()
        source_plan=acquisition.plan(now());deadline=time.monotonic()+100
        for sid,url in source_plan.items():
            remaining=deadline-time.monotonic()
            if remaining<=1:
                progress['sources'][sid]={'status':'acquisition_budget_exhausted'};sources[sid]=save(None,None);journal();continue
            progress['sources'][sid]={'status':'attempt_recorded'};progress['provider_requests']+=1;journal()
            try:raw,receipt=acquisition.acquire(url,timeout=min(20,remaining))
            except Exception as exc:
                raw=receipt=None;progress['sources'][sid]={'status':'unavailable','error_type':type(exc).__name__}
            else:progress['sources'][sid]={'status':'retained_response','http_status':receipt['http_status']}
            sources[sid]=save(raw,receipt);journal()
        inputs={'contract':'fifx-vol-inputs.v1','generated_at':now(),'macro':macro,'sources':sources,'predecessors':predecessors,
                'previous_watermarks':previous,'context':context,'acquisition':{'provider_requests':progress['provider_requests'],
                'request_budget_seconds':100,'max_attempts_per_source':1,'sources':progress['sources']}}
        packet=retain(client,bucket,inputs)
        primary=publish(client,bucket,packet,model.CURRENT)
        history=publish(client,bucket,packet,model.HISTORY) if primary else False
        result={'published':primary,'history_directory_published':history,'generated_at':packet['generated_at'],
                'quality':packet['quality'],'replay':packet['replay'],'current_bytes':len(model.encoded(packet)),
                'provider_requests':progress['provider_requests'],'paid_api_calls':0,'private_account_reads':0,'notifications_sent':0,'portfolio_writes':0}
        progress.update(status='complete',result=result);journal();return result
    except Exception as exc:
        progress.update(status='failed',error_type=type(exc).__name__);journal();raise
