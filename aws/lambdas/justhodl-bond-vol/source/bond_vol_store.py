"""Whole source retention, exact replay and conditional native publication."""
from datetime import datetime,timezone
from pathlib import Path
import gzip,hashlib,io,json,re,sys,urllib.request
import canonical_fred_replay as canonical
import report_observations,research_brief_model,evidence_store
import bond_vol_model as model
import bond_vol_candidate as arithmetic
import bond_vol_catalog as catalog
import bond_vol_timezone as timezone_data
import verify_bond_vol_arithmetic as independent
import bond_vol_qualification as qualification

MAX=64*1024*1024
PRIVATE=arithmetic.PRIVATE
SOURCE='data/report-measurements.json'
COMPILERS=(model,arithmetic,catalog,timezone_data,independent,qualification,canonical,
    report_observations,research_brief_model,evidence_store,sys.modules[__name__])
ROOT=Path(__file__).resolve().parent
sha=lambda raw:hashlib.sha256(raw).hexdigest()
now=lambda:datetime.now(timezone.utc).isoformat()
error=lambda exc:str(getattr(exc,'response',{}).get('Error',{}).get('Code',''))
conflict=lambda exc:error(exc) in ('409','412','ConditionalRequestConflict','PreconditionFailed')

def bounded(stream,limit=MAX):
    try:raw=stream.read(limit+1)
    finally:stream.close()
    if not 0<len(raw)<=limit:raise ValueError('Whole nonempty bounded artifact required')
    return raw

def qualified_arithmetic():
    for name,digest in qualification.QUALIFIED.items():
        if sha((ROOT/name).read_bytes())!=digest:raise ValueError('Accepted Bond Vol resource differs: '+name)

def allowed(key):
    return isinstance(key,str) and (key in (SOURCE,model.CURRENT) or bool(re.fullmatch(
        r'data/(?:evidence/fred/[a-f0-9]{64}/[a-f0-9]{64}\.bin\.gz|'
        r'report-research/(?:runs|inputs|outputs|compilers)/[a-f0-9]{64}\.(?:json|py)|'
        r'bond-vol-research/(?:runs|inputs|outputs|compilers|snapshots|series|quotes|views|originals|receipts)/[a-f0-9]{64}\.(?:json|py|bin))',key)))

def reader(client,bucket):
    def read(key):
        if not allowed(key):raise ValueError('Unapproved original path')
        raw=bounded(client.get_object(Bucket=bucket,Key=key)['Body'])
        return bounded(gzip.GzipFile(fileobj=io.BytesIO(raw))) if key.endswith('.gz') else raw
    return read

def retain_bytes(client,bucket,raw,category,extension='json',private=False):
    if not isinstance(raw,bytes) or not 0<len(raw)<=MAX:raise ValueError('Whole bounded artifact required')
    if category not in ('runs','inputs','outputs','compilers','snapshots','series','quotes','views','originals','receipts') or extension not in ('json','py','bin'):
        raise ValueError('Unapproved artifact category')
    key=PRIVATE+sha(raw)+'.bin' if private else model.PREFIX+category+'/'+sha(raw)+'.'+extension
    try:
        client.put_object(Bucket=bucket,Key=key,Body=raw,IfNoneMatch='*',
            ContentType='application/octet-stream' if private or extension=='bin' else 'text/plain' if extension=='py' else 'application/json',
            CacheControl='no-store' if private else 'public, max-age=31536000, immutable')
    except Exception as exc:
        if not conflict(exc):raise
    if bounded(client.get_object(Bucket=bucket,Key=key)['Body'])!=raw:raise ValueError('Whole retained artifact readback differs')
    return {'key':key,'sha256':sha(raw),'bytes':len(raw)}

def checked(ref,category,read,extension='json'):
    if (not isinstance(ref,dict) or not re.fullmatch('[a-f0-9]{64}',str(ref.get('sha256')))
        or ref.get('key')!=model.PREFIX+category+'/'+ref['sha256']+'.'+extension
        or type(ref.get('bytes')) is not int or not 0<ref['bytes']<=MAX):raise ValueError('Exact artifact reference required')
    raw=read(ref['key'])
    if len(raw)!=ref['bytes'] or sha(raw)!=ref['sha256']:raise ValueError('Artifact bytes differ')
    return raw if extension!='json' else json.loads(raw)

def compile_output(inputs,read):
    qualified_arithmetic()
    if inputs.get('contract')!='bond-vol-inputs.v1':raise ValueError('Complete native input contract required')
    source=checked(inputs['macro'],'snapshots',read);originals=canonical.restore(source,catalog.SERIES,read)
    raw=checked(inputs['quote'],'originals',read,'bin') if inputs['quote'] else None
    receipt=checked(inputs['quote_receipt'],'receipts',read) if inputs['quote_receipt'] else None
    return model.build(source,originals,inputs['generated_at'],inputs['context'],inputs['predecessors'],inputs['previous_watermarks'],raw,receipt,inputs['quote_acquisition'])

def binding(packet,read):
    ref=packet.get('replay',{});key=ref.get('manifest_key','')
    if not re.fullmatch(re.escape(model.PREFIX)+r'runs/[a-f0-9]{64}\.json',key):raise ValueError('Exact native run required')
    raw=read(key);manifest=json.loads(raw)
    if key!=model.PREFIX+'runs/'+sha(raw)+'.json' or manifest.get('contract')!='bond-vol-replay.v1' or manifest.get('output_sha256')!=ref.get('output_sha256'):
        raise ValueError('Native replay binding differs')
    view=checked(manifest['view'],'views',read)
    if view!={k:v for k,v in packet.items() if k!='replay'} or view['generated_at']!=manifest['generated_at']:
        raise ValueError('Published view differs from retained original')
    return manifest

def replay(packet,read,verify_shards=True):
    manifest=binding(packet,read)
    if set(manifest['compilers'])!={m.__name__ for m in COMPILERS}:raise ValueError('Complete compiler closure required')
    for module in COMPILERS:
        if checked(manifest['compilers'][module.__name__],'compilers',read,'py')!=Path(module.__file__).read_bytes():
            raise ValueError('Reviewed compiler bytes differ')
    output=compile_output(checked(manifest['input'],'inputs',read),read)
    if model.digest(output)!=manifest['output_sha256'] or output!=checked(manifest['output'],'outputs',read):raise ValueError('Complete original replay differs')
    if set(manifest['series'])!=set(catalog.SERIES):raise ValueError('All series required')
    for sid,row in output['series'].items():
        raw=model.encoded(row);ref=manifest['series'][sid]
        if ref!={'key':model.PREFIX+'series/'+sha(raw)+'.json','sha256':sha(raw),'bytes':len(raw)}:raise ValueError('Whole series identity differs')
        if verify_shards and checked(ref,'series',read)!=row:raise ValueError('Complete series bytes differ')
    if checked(manifest['quote'],'quotes',read)!=output['move']:raise ValueError('Complete quote artifact differs')
    if model.compact(output,manifest['series'],manifest['quote'])!={k:v for k,v in packet.items() if k!='replay'}:raise ValueError('Compact projection differs')
    return output

def retain(client,bucket,inputs,output):
    put=lambda value,category:retain_bytes(client,bucket,model.encoded(value),category)
    series={sid:put(row,'series') for sid,row in output['series'].items()};quote=put(output['move'],'quotes')
    view=model.compact(output,series,quote)
    manifest={'contract':'bond-vol-replay.v1','generated_at':inputs['generated_at'],'input':put(inputs,'inputs'),
        'output':put(output,'outputs'),'output_sha256':model.digest(output),'series':series,'quote':quote,'view':put(view,'views'),
        'compilers':{m.__name__:retain_bytes(client,bucket,Path(m.__file__).read_bytes(),'compilers','py') for m in COMPILERS}}
    ref=put(manifest,'runs');packet={**view,'replay':{'manifest_key':ref['key'],'output_sha256':manifest['output_sha256']}}
    if replay(packet,reader(client,bucket),verify_shards=False)!=output:raise ValueError('Fresh retained reconstruction differs')
    return packet

def previous_state(client,bucket):
    raw=bounded(client.get_object(Bucket=bucket,Key=model.CURRENT)['Body']);ref=retain_bytes(client,bucket,raw,'snapshots',private=True)
    try:packet=json.loads(raw)
    except (ValueError,UnicodeDecodeError):packet={}
    if not isinstance(packet,dict):packet={}
    if packet.get('contract')==model.CONTRACT:
        binding(packet,reader(client,bucket))
        return packet['predecessors'],model.watermarks(packet)
    history=bounded(client.get_object(Bucket=bucket,Key='data/bond-vol-history.json')['Body'])
    return {'packet':ref,'history':retain_bytes(client,bucket,history,'snapshots',private=True)},model.empty_watermarks()

def context(client,bucket):
    key='data/funding-plumbing.json'
    try:raw=bounded(client.get_object(Bucket=bucket,Key=key)['Body'])
    except Exception as exc:
        if error(exc) not in ('404','NoSuchKey'):raise
        return {'status':'missing','source_key':key,'original':None,'independent_votes':0}
    return {'status':'retained_unqualified_context','source_key':key,'original':retain_bytes(client,bucket,raw,'snapshots',private=True),
        'captured_at':now(),'independent_votes':0}

def publish(client,bucket,packet):
    flags=arithmetic.AUTHORITY
    if (packet.get('contract')!=model.CONTRACT or any(packet.get(k) is not False for k in flags)
        or packet.get('call') is not None or packet.get('regime') is not None or packet.get('composite_z_score') is not None
        or packet.get('signals')!=[] or packet.get('decision')!={'verb':'WAIT','meaning':'abstain'}
        or packet.get('portfolio_consequences',{}).get('status')!='UNAVAILABLE' or packet['portfolio_consequences'].get('target_weights') is not None
        or packet.get('view',{}).get('contract')!='bond-vol-summary.v1' or set(packet.get('series',{}))!=set(catalog.SERIES)
        or any(any(row.get(k) is not False for k in flags) for row in [*packet.get('series',{}).values(),packet.get('move',{})])):
        raise ValueError('Complete research-only packet required')
    binding(packet,reader(client,bucket));stamp=model.clock(packet['generated_at']);marks=model.watermarks(packet)
    for _ in range(4):
        obj=client.get_object(Bucket=bucket,Key=model.CURRENT);raw=bounded(obj['Body'])
        try:old=json.loads(raw)
        except (ValueError,UnicodeDecodeError):old={}
        if not isinstance(old,dict):old={}
        if old.get('generated_at'):
            prior=model.clock(old['generated_at'])
            if prior>stamp:return False
            if prior==stamp:
                if old!=packet:raise ValueError('Conflicting same-clock publication')
                return True
        if old.get('contract')==model.CONTRACT:
            if model.clock(old['source_generated_at'])>model.clock(packet['source_generated_at']):return False
            for sid,before in model.watermarks(old).items():
                after=marks[sid]
                if before['acquired_at'] and (not after['acquired_at'] or model.clock(after['acquired_at'])<model.clock(before['acquired_at'])):return False
                if before['observation_date'] and (not after['observation_date'] or after['observation_date']<before['observation_date']):return False
                row=packet['move'] if sid=='MOVE' else packet['series'][sid]
                if row['current']:
                    acquired=(row.get('receipt') or {}).get('acquired_at') if sid=='MOVE' else row['acquired_at']
                    observed=row['current']['session_date' if sid=='MOVE' else 'end_date']
                    if before['acquired_at'] and (not acquired or model.clock(acquired)<model.clock(before['acquired_at'])):return False
                    if before['observation_date'] and observed<before['observation_date']:return False
        retain_bytes(client,bucket,raw,'snapshots',private=True)
        try:
            client.put_object(Bucket=bucket,Key=model.CURRENT,Body=model.encoded(packet),ContentType='application/json',CacheControl='no-store',IfMatch=obj['ETag'])
            live=json.loads(reader(client,bucket)(model.CURRENT))
            if live!=packet and model.clock(live['generated_at'])<=stamp:raise ValueError('Published view readback differs')
            return True
        except Exception as exc:
            if not conflict(exc):raise
    raise RuntimeError('Publication conflict ceiling; whole immutable run retained')

def acquire_quote():
    request=urllib.request.Request(catalog.MOVE_URL,headers={'User-Agent':'Mozilla/5.0 (JustHodl research source review)','Accept':'application/json'})
    with urllib.request.urlopen(request,timeout=35) as response:
        if response.geturl()!=catalog.MOVE_URL:raise ValueError('Unreviewed quote redirect')
        headers={k:v for k,v in response.headers.items() if k.lower() in ('date','etag','last-modified','content-type')}
        status=response.status;raw=bounded(response,8*1024*1024)
    return raw,{'source_url':catalog.MOVE_URL,'http_status':status,'headers':headers,'acquired_at':now(),'sha256':sha(raw),'bytes':len(raw)}

def run(client,bucket,request_id):
    qualified_arithmetic()
    if not isinstance(request_id,str) or not 1<=len(request_id)<=200:raise ValueError('Native request identity required')
    key=PRIVATE+'requests/'+sha(('native:'+request_id).encode())+'.json';progress={'status':'claimed','started_at':now(),'provider_request_attempts':0}
    def journal(claim=False):
        raw=model.encoded(progress);client.put_object(Bucket=bucket,Key=key,Body=raw,ContentType='application/json',CacheControl='no-store',**({'IfNoneMatch':'*'} if claim else {}))
        if bounded(client.get_object(Bucket=bucket,Key=key)['Body'])!=raw:raise ValueError('Native claim readback differs')
    journal(True)
    try:
        predecessors,previous=previous_state(client,bucket)
        source=retain_bytes(client,bucket,reader(client,bucket)(SOURCE),'snapshots')
        donor=context(client,bucket)
        progress.update(status='quote_acquisition_attempted',provider_request_attempts=1);journal()
        quote_ref=receipt_ref=None
        try:raw,receipt=acquire_quote()
        except Exception as exc:acquisition={'status':'unavailable','error_type':type(exc).__name__}
        else:
            quote_ref=retain_bytes(client,bucket,raw,'originals','bin');receipt_ref=retain_bytes(client,bucket,model.encoded(receipt),'receipts')
            acquisition={'status':'received'}
        progress.update(quote=quote_ref,quote_receipt=receipt_ref,acquisition=acquisition);journal()
        inputs={'contract':'bond-vol-inputs.v1','generated_at':now(),'macro':source,'context':donor,'predecessors':predecessors,
            'previous_watermarks':previous,'quote':quote_ref,'quote_receipt':receipt_ref,'quote_acquisition':acquisition}
        output=compile_output(inputs,reader(client,bucket));packet=retain(client,bucket,inputs,output)
        published=publish(client,bucket,packet)
        result={'published':published,'generated_at':packet['generated_at'],'quality':packet['quality'],'replay':packet['replay'],
            'current_bytes':len(model.encoded(packet)),'provider_requests':1,'paid_ai_calls':0,'private_account_reads':0,'notifications_sent':0,'portfolio_writes':0}
        progress.update(status='complete',result=result);journal();return result
    except Exception as exc:
        progress.update(status='failed',error_type=type(exc).__name__);journal();raise
