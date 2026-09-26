"""Complete original-source replay, compact views and conditional publication."""
from datetime import datetime,timezone
from pathlib import Path
import gzip,hashlib,io,json,re,sys
import canonical_fred_replay as canonical
import report_observations,research_brief_model,evidence_store
import liquidity_agent_arithmetic as arithmetic
import liquidity_agent_catalog as catalog
import liquidity_flow_arithmetic as flow
import liquidity_agent_model as model

MAX=32*1024*1024
PRIVATE=arithmetic.PRIVATE
SOURCE='data/report-measurements.json'
COMPILERS=(model,arithmetic,catalog,flow,canonical,report_observations,research_brief_model,evidence_store,sys.modules[__name__])
QUALIFIED={'liquidity_agent_arithmetic':'b1904e9004ea65c37be72480b8e4a41afda2a3f88f8be232bafbf6790ea25891',
    'liquidity_agent_catalog':'e11bbf8435c544e2275c2e8ca1f604b0f4dadbd2f9bbdf367c9ffcc625c75fb6',
    'liquidity_flow_arithmetic':'5ac6ad37f28589c9a898e3a06d4526e2e23168370e713a874dc2b7dd0c622856'}
sha=lambda raw:hashlib.sha256(raw).hexdigest()


def bounded(stream):
    try:raw=stream.read(MAX+1)
    finally:stream.close()
    if len(raw)>MAX:raise ValueError('Complete artifact exceeds publication bound')
    return raw


def error(exc):return str(getattr(exc,'response',{}).get('Error',{}).get('Code',''))
def missing(exc):return error(exc) in ('404','NoSuchKey')
def conflict(exc):return error(exc) in ('409','412','ConditionalRequestConflict','PreconditionFailed')


def qualified_arithmetic():
    for module in (arithmetic,catalog,flow):
        if sha(Path(module.__file__).read_bytes())!=QUALIFIED[module.__name__]:
            raise ValueError('Accepted original-source arithmetic differs')


def allowed(key):
    return isinstance(key,str) and (key in (SOURCE,model.CURRENT,*catalog.CONTEXT_KEYS) or bool(re.fullmatch(
        r'data/(?:evidence/fred/[a-f0-9]{64}/[a-f0-9]{64}\.bin\.gz|'
        r'(?:report-research|liquidity-agent-research)/(?:runs|inputs|outputs|compilers|snapshots|series|views)/[a-f0-9]{64}\.(?:json|py))',key)))


def reader(client,bucket):
    def read(key):
        if not allowed(key):raise ValueError('Unapproved liquidity original path')
        raw=bounded(client.get_object(Bucket=bucket,Key=key)['Body'])
        return bounded(gzip.GzipFile(fileobj=io.BytesIO(raw))) if key.endswith('.gz') else raw
    return read


def immutable(client,bucket,key,raw,private=False):
    if not isinstance(raw,bytes) or not 0<len(raw)<=MAX:raise ValueError('Whole bounded artifact required')
    pattern=re.escape(model.PREFIX)+r'(?:runs|inputs|outputs|compilers|snapshots|series|views)/'+sha(raw)+r'\.(?:json|py)'
    if (key!=PRIVATE+sha(raw)+'.bin' if private else not re.fullmatch(pattern,key)):
        raise ValueError('Exact content-addressed artifact path required')
    try:
        client.put_object(Bucket=bucket,Key=key,Body=raw,IfNoneMatch='*',
            ContentType='application/octet-stream' if private else 'text/plain' if key.endswith('.py') else 'application/json',
            CacheControl='no-store' if private else 'public, max-age=31536000, immutable')
    except Exception as exc:
        if not conflict(exc):raise
    if bounded(client.get_object(Bucket=bucket,Key=key)['Body'])!=raw:
        raise ValueError('Retained whole artifact readback differs')


def retain_bytes(client,bucket,raw,category):
    key=model.PREFIX+category+'/'+sha(raw)+'.json';immutable(client,bucket,key,raw)
    return {'key':key,'sha256':sha(raw),'bytes':len(raw)}


def private_bytes(client,bucket,raw):
    key=PRIVATE+sha(raw)+'.bin';immutable(client,bucket,key,raw,True)
    return {'key':key,'sha256':sha(raw),'bytes':len(raw)}


def checked(ref,category,read):
    if (not isinstance(ref,dict) or not re.fullmatch('[a-f0-9]{64}',str(ref.get('sha256')))
        or ref.get('key')!=model.PREFIX+category+'/'+ref['sha256']+'.json'
        or type(ref.get('bytes')) is not int or not 0<ref['bytes']<=MAX):
        raise ValueError('Exact complete artifact reference required')
    raw=read(ref['key'])
    if len(raw)!=ref['bytes'] or sha(raw)!=ref['sha256']:raise ValueError('Artifact content differs')
    return json.loads(raw)


def compile_output(inputs,read):
    qualified_arithmetic()
    if inputs.get('contract')!='liquidity-agent-inputs.v1':raise ValueError('Native input contract required')
    source=checked(inputs['macro'],'snapshots',read)
    originals=canonical.restore(source,catalog.SERIES,read)
    return model.build(source,originals,inputs['generated_at'],inputs['contexts'],inputs['predecessor'],inputs['previous_watermarks'])


def replay(ref,read,verify_series=True):
    key=ref.get('manifest_key','')
    if not re.fullmatch(re.escape(model.PREFIX)+r'runs/[a-f0-9]{64}\.json',key):
        raise ValueError('Exact native run path required')
    raw=read(key);manifest=json.loads(raw)
    if (key!=model.PREFIX+'runs/'+sha(raw)+'.json' or manifest.get('contract')!='liquidity-agent-replay.v1'
        or manifest.get('output_sha256')!=ref.get('output_sha256')):
        raise ValueError('Native replay binding differs')
    if set(manifest['compilers'])!={m.__name__ for m in COMPILERS}:raise ValueError('Complete compiler closure required')
    for module in COMPILERS:
        body=Path(module.__file__).read_bytes();expected={'key':model.PREFIX+'compilers/'+sha(body)+'.py','sha256':sha(body)}
        if manifest['compilers'][module.__name__]!=expected or read(expected['key'])!=body:
            raise ValueError('Matching complete reviewed compiler required')
    inputs=checked(manifest['input'],'inputs',read);output=compile_output(inputs,read)
    if (model.digest(output)!=manifest['output_sha256'] or output!=checked(manifest['output'],'outputs',read)
        or output['generated_at']!=manifest['generated_at']):raise ValueError('Native original reconstruction differs')
    if set(manifest['series'])!=set(catalog.SERIES):raise ValueError('Every requested series artifact required')
    for sid,row in output['series'].items():
        encoded=model.encoded(row);ref=manifest['series'][sid]
        if ref!={'key':model.PREFIX+'series/'+sha(encoded)+'.json','sha256':sha(encoded),'bytes':len(encoded)}:
            raise ValueError('Series artifact identity differs: '+sid)
        if verify_series and checked(ref,'series',read)!=row:raise ValueError('Whole series artifact differs: '+sid)
    view=model.compact(output,manifest['series'])
    if view!=checked(manifest['view'],'views',read):raise ValueError('Compact projection differs')
    return output,view


def retain(client,bucket,inputs,output):
    qualified_arithmetic()
    refs={name:retain_bytes(client,bucket,model.encoded(value),name+'s') for name,value in (('input',inputs),('output',output))}
    series={sid:retain_bytes(client,bucket,model.encoded(row),'series') for sid,row in output['series'].items()}
    view=model.compact(output,series);refs['view']=retain_bytes(client,bucket,model.encoded(view),'views')
    compilers={}
    for module in COMPILERS:
        raw=Path(module.__file__).read_bytes();key=model.PREFIX+'compilers/'+sha(raw)+'.py'
        immutable(client,bucket,key,raw);compilers[module.__name__]={'key':key,'sha256':sha(raw)}
    manifest={'contract':'liquidity-agent-replay.v1','generated_at':inputs['generated_at'],**refs,
        'compilers':compilers,'series':series,'output_sha256':model.digest(output)}
    raw=model.encoded(manifest);key=model.PREFIX+'runs/'+sha(raw)+'.json';immutable(client,bucket,key,raw)
    ref={'manifest_key':key,'output_sha256':manifest['output_sha256']}
    # All shards were read back on immutable writes; this second reconstruction
    # still checks every shard's identity and exact view from source originals.
    restored,projected=replay(ref,reader(client,bucket),verify_series=False)
    if restored!=output or projected!=view:raise ValueError('Fresh retained native replay differs')
    return ref,view


def previous_state(client,bucket):
    # Migration requires a whole predecessor, including an explicitly empty one
    # only if it was actually stored. A missing key is never a fabricated source.
    raw=bounded(client.get_object(Bucket=bucket,Key=model.CURRENT)['Body'])
    ref=private_bytes(client,bucket,raw)
    try:packet=json.loads(raw)
    except (ValueError,UnicodeDecodeError):packet={}
    if not isinstance(packet,dict):packet={}
    if packet.get('contract')==model.CONTRACT:
        return packet['predecessor'],model.watermarks(packet)
    return ref,{sid:None for sid in catalog.SERIES}


def contexts(client,bucket):
    result={}
    for key in catalog.CONTEXT_KEYS:
        try:raw=bounded(client.get_object(Bucket=bucket,Key=key)['Body'])
        except Exception as exc:
            if not missing(exc):raise
            result[key]={'status':'missing','source_key':key,'original':None,'independent_votes':0};continue
        result[key]={'status':'retained_unqualified_context','source_key':key,'original':private_bytes(client,bucket,raw),
            'captured_at':datetime.now(timezone.utc).isoformat(),'same_snapshot_as_baseline':False,'independent_votes':0}
    return result


def publish(client,bucket,packet):
    # Enforce research-only authority again at the mutable publication boundary.
    # A later caller cannot promote measurements merely by changing a field.
    flags=('calls_eligible','sizing_eligible','execution_eligible','forecast_qualified','point_in_time_backtest_qualified')
    if (packet.get('contract')!=model.CONTRACT or any(packet.get(k) is not False for k in flags)
        or packet.get('call') is not None or packet.get('decision')!={'verb':'WAIT','meaning':'abstain'}
        or packet.get('view',{}).get('contract')!='liquidity-agent-summary.v1'
        or set(packet.get('series',{}))!=set(catalog.SERIES)
        or not re.fullmatch(re.escape(model.PREFIX)+r'runs/[a-f0-9]{64}\.json',packet.get('replay',{}).get('manifest_key',''))
        or not re.fullmatch('[a-f0-9]{64}',packet.get('replay',{}).get('output_sha256',''))):
        raise ValueError('Complete replay-bound research-only publication required')
    for row in packet['series'].values():
        if any(row.get(k) is not False for k in flags):raise ValueError('Series acquired unqualified authority')
    stamp=model.clock(packet['generated_at']);watermarks=model.watermarks(packet)
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
                if before and (not watermarks[sid] or model.clock(watermarks[sid])<model.clock(before)):return False
                row=packet['series'][sid]
                if before and row['current']['value'] is not None and (not row['acquired_at'] or model.clock(row['acquired_at'])<model.clock(before)):return False
        private_bytes(client,bucket,raw)
        try:
            client.put_object(Bucket=bucket,Key=model.CURRENT,Body=model.encoded(packet),ContentType='application/json',
                CacheControl='no-store',IfMatch=obj['ETag'])
            live=json.loads(reader(client,bucket)(model.CURRENT))
            if live!=packet and model.clock(live['generated_at'])<=stamp:raise ValueError('Published view readback differs')
            return True
        except Exception as exc:
            if not conflict(exc):raise
    raise RuntimeError('Publication conflict ceiling; immutable run preserved')


def run(client,bucket):
    qualified_arithmetic()
    read=reader(client,bucket);raw=read(SOURCE);source=json.loads(raw)
    canonical.restore(source,catalog.SERIES,read)
    source_ref=retain_bytes(client,bucket,raw,'snapshots')
    predecessor,previous=previous_state(client,bucket)
    context=contexts(client,bucket)
    inputs={'contract':'liquidity-agent-inputs.v1','generated_at':datetime.now(timezone.utc).isoformat(),
        'macro':source_ref,'contexts':context,'predecessor':predecessor,'previous_watermarks':previous}
    output=compile_output(inputs,read);ref,view=retain(client,bucket,inputs,output)
    published=publish(client,bucket,{**view,'replay':ref})
    return {'published':published,'generated_at':view['generated_at'],'quality':view['quality'],'replay':ref,
        'current_bytes':len(model.encoded({**view,'replay':ref})),'complete_original_rows':view['view']['complete_original_rows'],
        'provider_requests':0,'paid_ai_calls':0,'private_account_reads':0,'notifications_sent':0,'portfolio_writes':0}
