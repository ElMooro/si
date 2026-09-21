"""Original-source sector fusion replay and conditional publication.

Only reviewed public research packets and their retained originals are read.
No provider refresh, credentials, private account, consumer or notification call.
"""
from datetime import datetime,timezone
from pathlib import Path
import gzip,io,json,re,sys
import sector_fusion_model as model
import sector_fusion_issuer as issuer
import sector_issuer_native
import sector_fusion_pins
import sector_research_store as prices
import money_volume_store as volume

MAX=64*1024*1024
PRIVATE=model.PRIVATE
ROOTS=('data/sector-rotation.json','data/money-flow-state.json','data/etf-true-flows.json')
SOURCES=ROOTS+('data/etf-flows.json','data/flow-lookthrough.json','data/rotation-chains.json','data/dark-pool.json',
    'data/13f-positions.json','data/insider-aggregate-history.json','data/liquidity-flow.json','data/smart-beta.json',
    'data/sector-flow-state.json','data/finviz-groups.json','data/political-trades.json','data/risk-regime.json',
    'data/polygon-fx-regime.json','data/dollar-radar.json','data/capital-inflows.json','data/gold-equity-rotation.json',
    'data/tic-flows.json','data/sector-capital-fusion.json','data/etf-desk.json','data/universe.json',
    'data/chart-patterns.json','data/accumulation-radar.json','flow-data.json','data/capital-flow-radar.json')
COMPILERS=tuple({m.__name__:m for m in (model,issuer,sector_issuer_native,sector_fusion_pins,*prices.COMPILERS,*volume.COMPILERS,sys.modules[__name__])}.values())
KINDS={'flow':(model.PREFIX,model.CURRENT,model.CONTRACT),'capital':(model.CAPITAL_PREFIX,model.CAPITAL_CURRENT,model.CAPITAL_CONTRACT)}


def now():return datetime.now(timezone.utc).isoformat()
def code(exc):return str(getattr(exc,'response',{}).get('Error',{}).get('Code',''))
def missing(exc):return code(exc) in ('404','NoSuchKey')
def conflict(exc):return code(exc) in ('409','412','PreconditionFailed','ConditionalRequestConflict')
def bounded(stream):
    try:raw=stream.read(MAX+1)
    finally:stream.close()
    if len(raw)>MAX:raise ValueError('Reviewed research byte bound')
    return raw
def artifact(key):
    return isinstance(key,str) and bool(re.fullmatch(re.escape(PRIVATE)+r'[a-f0-9]{64}\.bin|data/sector-(?:fusion|capital)-research/(?:inputs|outputs|runs|compilers)/[a-f0-9]{64}\.(?:json|py)',key))
def allowed(key):return isinstance(key,str) and (key in SOURCES or artifact(key) or prices.allowed(key) or volume.allowed(key) or issuer.artifact(key))
def reader(client,bucket):
    def read(key):
        if not allowed(key):raise ValueError('Unreviewed source path')
        raw=bounded(client.get_object(Bucket=bucket,Key=key)['Body'])
        return bounded(gzip.GzipFile(fileobj=io.BytesIO(raw))) if key.endswith('.gz') else raw
    return read


def immutable(client,bucket,key,raw,kind='application/json'):
    if not artifact(key) or not isinstance(raw,bytes) or len(raw)>MAX or key.rsplit('/',1)[-1].split('.')[0]!=model.sha(raw):raise ValueError('Bounded content-addressed artifact required')
    try:client.put_object(Bucket=bucket,Key=key,Body=raw,ContentType=kind,IfNoneMatch='*',
        CacheControl='no-store' if key.startswith(PRIVATE) else 'public, max-age=31536000, immutable')
    except Exception as exc:
        if not conflict(exc):raise
    if reader(client,bucket)(key)!=raw:raise ValueError('Immutable readback differs')


def snapshot(client,bucket,key):
    if key not in SOURCES:raise ValueError('Unreviewed snapshot path')
    raw=reader(client,bucket)(key);doc=json.loads(raw)
    if not isinstance(doc,(dict,list)):raise ValueError('Research packet shape required')
    digest=model.sha(raw);target=PRIVATE+digest+'.bin';immutable(client,bucket,target,raw,'application/octet-stream')
    return {'source_key':key,'key':target,'sha256':digest,'bytes':len(raw)}


def source(ref,key,read,metadata=False):
    digest=ref.get('sha256','')
    if ref.get('source_key')!=key or not re.fullmatch('[a-f0-9]{64}',digest) or ref.get('key')!=PRIVATE+digest+'.bin' or type(ref.get('bytes')) is not int:raise ValueError('Retained source identity differs')
    raw=read(ref['key'])
    if len(raw)!=ref['bytes'] or model.sha(raw)!=digest:raise ValueError('Retained source bytes differ')
    doc=json.loads(raw)
    if not isinstance(doc,(dict,list)):raise ValueError('Retained source shape differs')
    if metadata:return {k:doc.get(k) for k in ('generated_at','as_of')} if isinstance(doc,dict) else {}
    return doc


def compile_output(inputs,read):
    if inputs.get('contract')!='sector-fusion-inputs.v1' or inputs.get('kind') not in KINDS:raise ValueError('Typed fusion input required')
    refs=inputs['sources'];at=inputs['generated_at']
    if inputs['kind']=='capital':
        if set(refs)!={model.CURRENT,model.CAPITAL_CURRENT}:raise ValueError('Exact projection input inventory required')
        canonical=source(refs[model.CURRENT],model.CURRENT,read)
        if canonical.get('contract')!=model.CONTRACT or not canonical.get('replay',{}).get('manifest_key','').startswith(model.PREFIX+'runs/'):raise ValueError('Canonical matrix source required')
        if replay(canonical['replay'],read)!={k:v for k,v in canonical.items() if k!='replay'}:raise ValueError('Canonical publication differs from original replay')
        if refs[model.CAPITAL_CURRENT]:source(refs[model.CAPITAL_CURRENT],model.CAPITAL_CURRENT,read,True)
        return model.capital_projection(canonical,at,refs[model.CAPITAL_CURRENT])
    if set(refs)!=set(SOURCES):raise ValueError('Exact retained sector input inventory required')
    restored={}
    for key,module in ((ROOTS[0],prices),(ROOTS[1],volume)):
        packet=source(refs[key],key,read)
        if packet.get('contract')!=module.model.CONTRACT:raise ValueError('Native sector price or stock-volume root required')
        if module.replay(packet['replay'],read)!={k:v for k,v in packet.items() if k!='replay'}:raise ValueError('Root publication differs from original replay')
        restored[key]=packet
    issuance=issuer.restore(source(refs[ROOTS[2]],ROOTS[2],read),read)
    contexts={k:source(ref,k,read,True) if ref else None for k,ref in refs.items()}
    return model.build(restored[ROOTS[0]],restored[ROOTS[1]],issuance,at,contexts,refs)


def checked(ref,prefix,kind,read):
    digest=ref.get('sha256','')
    if not re.fullmatch('[a-f0-9]{64}',digest) or ref.get('key')!=prefix+kind+'/'+digest+'.json':raise ValueError('Fusion artifact identity differs')
    raw=read(ref['key'])
    if type(ref.get('bytes')) is not int or len(raw)!=ref['bytes'] or model.sha(raw)!=digest:raise ValueError('Fusion artifact bytes differ')
    return json.loads(raw)


def replay(ref,read):
    key=ref.get('manifest_key','');kinds=[k for k,(p,_,_) in KINDS.items() if re.fullmatch(re.escape(p)+r'runs/[a-f0-9]{64}\.json',key)]
    if len(kinds)!=1:raise ValueError('Fusion run identity required')
    kind=kinds[0];prefix,_,contract=KINDS[kind];raw=read(key);run=json.loads(raw)
    if key!=prefix+'runs/'+model.sha(raw)+'.json' or run.get('contract')!='sector-fusion-replay.v1' or run.get('kind')!=kind:raise ValueError('Fusion run identity differs')
    if set(run['compilers'])!={m.__name__ for m in COMPILERS}:raise ValueError('Fusion compiler inventory differs')
    for module in COMPILERS:
        body=Path(module.__file__).read_bytes();digest=model.sha(body);expected={'key':prefix+'compilers/'+digest+'.py','sha256':digest}
        if run['compilers'][module.__name__]!=expected or read(expected['key'])!=body:raise ValueError('Matching reviewed fusion compiler required')
    inputs=checked(run['input'],prefix,'inputs',read)
    if inputs.get('kind')!=kind:raise ValueError('Fusion run/input kind differs')
    output=compile_output(inputs,read)
    if output.get('contract')!=contract or output!=checked(run['output'],prefix,'outputs',read) or model.sha(model.encoded(output))!=ref.get('output_sha256') or run['output_sha256']!=ref['output_sha256'] or output['generated_at']!=run['generated_at']:raise ValueError('Original-source fusion replay differs')
    return output


def retain(client,bucket,inputs,output):
    kind=inputs['kind'];prefix=KINDS[kind][0];refs={}
    for name,doc in (('input',inputs),('output',output)):
        raw=model.encoded(doc);digest=model.sha(raw);key=prefix+name+'s/'+digest+'.json';immutable(client,bucket,key,raw)
        refs[name]={'key':key,'sha256':digest,'bytes':len(raw)}
    compilers={}
    for module in COMPILERS:
        raw=Path(module.__file__).read_bytes();digest=model.sha(raw);key=prefix+'compilers/'+digest+'.py';immutable(client,bucket,key,raw,'text/x-python')
        compilers[module.__name__]={'key':key,'sha256':digest}
    manifest={'contract':'sector-fusion-replay.v1','kind':kind,'generated_at':output['generated_at'],**refs,'compilers':compilers,
        'output_sha256':refs['output']['sha256'],'scope':'Exact source replay of dated price, selected issuer histories and stock-volume roots; separate units, periods and covered universes; no independent investment vote.'}
    raw=model.encoded(manifest);key=prefix+'runs/'+model.sha(raw)+'.json';immutable(client,bucket,key,raw)
    ref={'manifest_key':key,'output_sha256':refs['output']['sha256']}
    if replay(ref,reader(client,bucket))!=output:raise ValueError('Retained original replay differs')
    return ref


def publish(client,bucket,kind,packet):
    _,current,contract=KINDS[kind];at=model.clock(packet['generated_at'])
    if packet.get('contract')!=contract:raise ValueError('Publication contract differs')
    for _ in range(4):
        try:
            obj=client.get_object(Bucket=bucket,Key=current);raw=bounded(obj['Body']);old=json.loads(raw)
            old_at=old.get('generated_at')
            if old_at and model.clock(old_at)>at:return False
            if old_at and model.clock(old_at)==at and old!=packet:raise ValueError('Conflicting same-clock publication')
            if old.get('contract')==contract:
                for key,value in old['source_clocks'].items():
                    if model.clock(value['source_generated_at'])>model.clock(packet['source_clocks'][key]['source_generated_at']):return False
                if kind=='capital' and model.clock(old['canonical_generated_at'])>model.clock(packet['canonical_generated_at']):return False
            immutable(client,bucket,PRIVATE+model.sha(raw)+'.bin',raw,'application/octet-stream');condition={'IfMatch':obj['ETag']}
        except Exception as exc:
            if not missing(exc):raise
            condition={'IfNoneMatch':'*'}
        try:
            client.put_object(Bucket=bucket,Key=current,Body=model.encoded(packet),ContentType='application/json',CacheControl='no-store',**condition)
            live=json.loads(reader(client,bucket)(current))
            if live!=packet and model.clock(live['generated_at'])<=at:raise ValueError('Publication readback differs')
            return True
        except Exception as exc:
            if not conflict(exc):raise
    raise RuntimeError('Publication conflict limit; immutable run retained')


def request_key(kind,request_id):
    if not isinstance(request_id,str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,100}',request_id):raise ValueError('Canonical idempotent request identity required')
    return KINDS[kind][0]+'requests/'+model.sha(request_id.encode())+'.json'
def status_write(client,bucket,key,doc,**condition):client.put_object(Bucket=bucket,Key=key,Body=model.encoded(doc),ContentType='application/json',CacheControl='no-store',**condition)


def run(client,bucket,kind,request_id,execution_id):
    key=request_key(kind,request_id);status={'contract':'sector-fusion-request.v1','kind':kind,'request_id':request_id,
        'execution_id':execution_id,'started_at':now(),'status':'running','phase':'snapshot'}
    try:status_write(client,bucket,key,status,IfNoneMatch='*')
    except Exception as exc:
        if not conflict(exc):raise
        return json.loads(bounded(client.get_object(Bucket=bucket,Key=key)['Body']))
    try:
        refs={};sources=SOURCES if kind=='flow' else (model.CURRENT,model.CAPITAL_CURRENT)
        for source_key in sources:
            try:refs[source_key]=snapshot(client,bucket,source_key)
            except Exception as exc:
                if not missing(exc) or source_key in ROOTS or (kind=='capital' and source_key==model.CURRENT):raise
                refs[source_key]=None
        inputs={'contract':'sector-fusion-inputs.v1','kind':kind,'sources':refs,'generated_at':now()}
        status['phase']='compile';status_write(client,bucket,key,status);output=compile_output(inputs,reader(client,bucket))
        status['phase']='retained_replay';status_write(client,bucket,key,status);ref=retain(client,bucket,inputs,output)
        status['phase']='publish';status_write(client,bucket,key,status);published=publish(client,bucket,kind,{**output,'replay':ref})
        result={**status,'status':'complete','phase':'complete','completed_at':now(),'published':published,
            'generated_at':output['generated_at'],'quality':output['quality'],'replay':ref,
            'private_account_reads':0,'provider_requests':0,'paid_ai_calls':0,'notifications_sent':0,'portfolio_writes':0}
        status_write(client,bucket,key,result);return result
    except Exception:
        status_write(client,bucket,key,{**status,'status':'failed','completed_at':now(),'error':'native_replay_or_publication_failed'})
        raise RuntimeError('Native sector fusion failed; inspect reviewed request evidence') from None
