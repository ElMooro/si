"""Protected ETF desk originals, complete replay and conditional public research."""
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
import json,re,sys,time
import etf_desk_model as model
import etf_desk_catalog as catalog
import etf_profile_native as native
import etf_profile_collect as collector
import provider_flow_native as flow_native
import provider_flow_model as flow_model
import provider_flow_collect as flow_collect
import provider_flow_store as flow_store
import provider_flow_catalog as flow_catalog
import etf_holdings_native as holdings_native
import etf_holdings_model as holdings_model
import etf_holdings_collect as holdings_collect
import etf_holdings_store as holdings_store

MAX=64*1024*1024
PRIVATE=native.PRIVATE
CONTEXTS=('data/etf-desk.json','data/etf-derived.json','data/etf-global.json','data/etf-global-desk-meta.json',
    'data/etf-holdings-complete.json','data/etf-holdings-index.json',flow_model.CURRENT,holdings_model.CURRENT,holdings_model.LOOK_CURRENT)
ALIASES=CONTEXTS[:6]
MIGRATION=model.PREFIX+'migration.json'
COMPILERS=(native,model,catalog,collector,flow_native,flow_model,flow_collect,flow_store,flow_catalog,
    holdings_native,holdings_model,holdings_collect,holdings_store,sys.modules[__name__])
now=holdings_store.now
code,missing,conflict,bounded=holdings_store.code,holdings_store.missing,holdings_store.conflict,holdings_store.bounded


def own_artifact(key):
    return isinstance(key,str) and bool(re.fullmatch(re.escape(PRIVATE)+r'[a-f0-9]{64}\.bin|'
        +re.escape(model.PREFIX)+r'(?:inputs|outputs|runs|compilers|profiles)/[a-f0-9]{64}\.(?:json|py)',key))


def artifact(key):return own_artifact(key) or flow_store.artifact(key) or holdings_store.artifact(key)


def source_key(key):
    return isinstance(key,str) and (key in CONTEXTS or key==model.CURRENT or bool(re.fullmatch(r'data/etf-flow-hist/(?:_index|[A-Z][A-Z0-9.\-]{0,14})\.json',key)))


def reader(client,bucket,capacity=768*1024*1024):
    cache=OrderedDict();size=0;lock=Lock()
    def remember(key,raw):
        nonlocal size
        if not artifact(key) or not isinstance(raw,bytes) or len(raw)>MAX or key.rsplit('/',1)[-1].split('.')[0]!=model.sha(raw):
            raise ValueError('Verified immutable desk cache entry required')
        if len(raw)>capacity:return
        with lock:
            if key in cache:size-=len(cache.pop(key))
            while cache and size+len(raw)>capacity:
                _,old=cache.popitem(last=False);size-=len(old)
            cache[key]=raw;size+=len(raw)
    def read(key):
        if not (artifact(key) or source_key(key) or key==MIGRATION):raise ValueError('Unreviewed desk read')
        if artifact(key):
            with lock:
                if key in cache:cache.move_to_end(key);return cache[key]
        raw=bounded(client.get_object(Bucket=bucket,Key=key)['Body'])
        if artifact(key):remember(key,raw)
        return raw
    read.remember=remember
    return read


def immutable(client,bucket,key,raw,kind='application/json',read=None):
    if not artifact(key) or not isinstance(raw,bytes) or len(raw)>MAX or key.rsplit('/',1)[-1].split('.')[0]!=model.sha(raw):
        raise ValueError('Exact immutable desk artifact required')
    try:client.put_object(Bucket=bucket,Key=key,Body=raw,ContentType=kind,IfNoneMatch='*',
        CacheControl='no-store' if key.startswith('audit-private/') else 'public, max-age=31536000, immutable')
    except Exception as exc:
        if not conflict(exc):raise
    if bounded(client.get_object(Bucket=bucket,Key=key)['Body'])!=raw:raise ValueError('Desk immutable readback differs')
    if read is not None:read.remember(key,raw)


def protect(client,bucket,raw,read=None,prefix=PRIVATE):
    if prefix not in (PRIVATE,flow_native.PRIVATE,holdings_native.PRIVATE):raise ValueError('Reviewed provider evidence family required')
    digest=model.sha(raw);key=prefix+digest+'.bin';immutable(client,bucket,key,raw,'application/octet-stream',read)
    return {'key':key,'sha256':digest,'bytes':len(raw)}


def protected(ref,read):
    digest=ref.get('sha256','')
    if not re.fullmatch('[a-f0-9]{64}',digest) or ref.get('key')!=PRIVATE+digest+'.bin' or type(ref.get('bytes')) is not int or not 0<ref['bytes']<=MAX:
        raise ValueError('Protected desk reference required')
    raw=read(ref['key'])
    if len(raw)!=ref['bytes'] or model.sha(raw)!=digest:raise ValueError('Protected desk bytes differ')
    return raw


def snapshot(client,bucket,key,read):
    if not source_key(key):raise ValueError('Reviewed desk predecessor required')
    raw=bounded(client.get_object(Bucket=bucket,Key=key)['Body'])
    if not isinstance(json.loads(raw),(dict,list)):raise ValueError('Whole structured predecessor required')
    return {'source_key':key,**protect(client,bucket,raw,read)}


def source(ref,key,read):
    if not source_key(key) or ref.get('source_key')!=key:raise ValueError('Desk source identity differs')
    return json.loads(protected(ref,read))


def validate_contexts(contexts,read):
    required=set(CONTEXTS)|{'data/etf-flow-hist/_index.json'}
    if not required<=set(contexts) or len(contexts)>1100:raise ValueError('Whole bounded desk predecessor inventory required')
    for key,ref in contexts.items():
        if not source_key(key):raise ValueError('Unreviewed predecessor context')
        if ref is not None:source(ref,key,read)
    if not contexts[CONTEXTS[0]] or not contexts['data/etf-flow-hist/_index.json']:raise ValueError('Complete desk and history index predecessor required')


def status_write(client,bucket,key,doc,**condition):
    client.put_object(Bucket=bucket,Key=key,Body=model.encoded(doc),ContentType='application/json',CacheControl='no-store',**condition)


def preserve(client,bucket,read):
    try:marker=json.loads(read(MIGRATION))
    except Exception as exc:
        if not missing(exc):raise
        keys=set(CONTEXTS)|{'data/etf-flow-hist/_index.json'}
        for page in client.get_paginator('list_objects_v2').paginate(Bucket=bucket,Prefix='data/etf-flow-hist/'):
            for obj in page.get('Contents',[]):
                if not source_key(obj['Key']):raise ValueError('Unreviewed legacy history path')
                keys.add(obj['Key'])
            if len(keys)>1100:raise ValueError('Legacy history inventory bound')
        refs={}
        for key in sorted(keys):
            try:refs[key]=snapshot(client,bucket,key,read)
            except Exception as error:
                if not missing(error):raise
                refs[key]=None
        validate_contexts(refs,read)
        if source(refs[CONTEXTS[0]],CONTEXTS[0],read).get('contract')=='etf-desk-compatibility.v1':raise ValueError('Original desk migration marker missing')
        body={'contract':'etf-desk-preservation.v1','preserved_at':now(),'contexts':refs}
        marker=protect(client,bucket,model.encoded(body),read)
        try:status_write(client,bucket,MIGRATION,marker,IfNoneMatch='*')
        except Exception as error:
            if not conflict(error):raise
            marker=json.loads(read(MIGRATION))
    doc=json.loads(protected(marker,read))
    if doc.get('contract')!='etf-desk-preservation.v1':raise ValueError('Desk preservation contract differs')
    validate_contexts(doc['contexts'],read)
    return doc['contexts']


def warm(refs,read):
    unique={}
    for ref in refs:
        if ref:
            identity={k:ref[k] for k in ('key','sha256','bytes')}
            if ref['key'] in unique and unique[ref['key']]!=identity:raise ValueError('Conflicting immutable reference')
            unique[ref['key']]=identity
    if len(unique)>10000:raise ValueError('Canonical evidence read bound')
    def one(ref):
        raw=read(ref['key'])
        if len(raw)!=ref['bytes'] or model.sha(raw)!=ref['sha256']:raise ValueError('Canonical evidence bytes differ')
    with ThreadPoolExecutor(max_workers=8) as pool:
        for _ in pool.map(one,unique.values()):pass


def warm_sources(flow_packet,holding_packet,read):
    for kind,packet,store_,prefix in [('flow',flow_packet,flow_store,flow_model.PREFIX),('holdings',holding_packet,holdings_store,holdings_model.PREFIX)]:
        run=json.loads(read(packet['replay']['manifest_key']));inputs=store_.checked(run['input'],prefix,'inputs',read)
        refs=[*inputs['contexts'].values(),inputs.get('previous')]
        collections=inputs['collections'].values() if kind=='flow' else [c for pair in inputs['collections'].values() for c in pair.values()]
        for c in collections:
            for p in [c.get('selection'),*c.get('pages',[])]:
                if p and p.get('original'):refs.append(p['original'])
            if c.get('rejected_original'):refs.append(c['rejected_original'])
        warm(refs,read)
    warm([flow_packet['reference'],*[f['history'] for f in flow_packet['funds'].values()]],read)
    refs=[holding_packet['security_directory']]
    for f in holding_packet['funds'].values():refs.extend((f['current']['snapshot'],f['prior']['snapshot'],f['comparison']))
    warm(refs,read);parts=[]
    for ref in refs:
        doc=model.checked(ref,read);parts.extend(doc.get('parts',[])+doc.get('index_parts',[])+list(doc.get('buckets',{}).values()))
    warm(parts,read)


def compile_output(inputs,read,emit):
    validate_contexts(inputs['contexts'],read)
    if type(inputs.get('provider_requests')) is not int or not 0<=inputs['provider_requests']<=10000:raise ValueError('Bounded desk request count required')
    if type(inputs.get('original_provider_bytes')) is not int or not 0<=inputs['original_provider_bytes']<=384*1024*1024:raise ValueError('Bounded provider evidence bytes required')
    flow_packet=source(inputs['canonical_flows'],flow_model.CURRENT,read)
    holding_packet=source(inputs['canonical_holdings'],holdings_model.CURRENT,read)
    warm_sources(flow_packet,holding_packet,read)
    for packet,store_ in ((flow_packet,flow_store),(holding_packet,holdings_store)):
        if store_.replay(packet['replay'],read)!={k:v for k,v in packet.items() if k!='replay'}:raise ValueError('Canonical packet differs from original replay')
    previous=source(inputs['previous'],model.CURRENT,read) if inputs.get('previous') else None
    if previous:recorded_output(previous,read)
    families=[([c for pair in inputs['profiles'].values() for c in pair.values()],protected),
        (inputs['extra_flows'].values(),flow_store.protected),
        ([c for pair in inputs['extra_holdings'].values() for c in pair.values()],holdings_store.protected)]
    for collections,verify in families:
        for c in collections:
            refs=[p['original'] for p in c.get('pages',[])]
            if (c.get('selection') or {}).get('original'):refs.append(c['selection']['original'])
            if c.get('rejected_original'):refs.append(c['rejected_original'])
            for ref in refs:verify(ref,read)
    return model.build(inputs,read,emit,flow_packet,holding_packet,previous)


def checked(ref,kind,read):
    digest=ref.get('sha256','')
    if not re.fullmatch('[a-f0-9]{64}',digest) or ref.get('key')!=model.PREFIX+kind+'/'+digest+'.json':raise ValueError('Desk replay artifact identity differs')
    return model.checked(ref,read)


def recorded_output(packet,read):
    if packet.get('contract')!=model.CONTRACT or any(packet.get(k) is not False for k in model.PERMISSIONS):
        raise ValueError('Reviewed prior desk contract required')
    ref=packet['replay'];key=ref['manifest_key']
    if not re.fullmatch(re.escape(model.PREFIX)+r'runs/[a-f0-9]{64}\.json',key):raise ValueError('Recorded desk run required')
    raw=read(key);run=json.loads(raw)
    if (key!=model.PREFIX+'runs/'+model.sha(raw)+'.json' or run.get('contract')!='etf-desk-replay.v1'
            or run.get('generated_at')!=packet.get('generated_at') or run.get('output_sha256')!=ref.get('output_sha256')):
        raise ValueError('Recorded desk run differs')
    output=checked(run['output'],'outputs',read)
    if output!={k:v for k,v in packet.items() if k!='replay'} or model.sha(model.encoded(output))!=ref['output_sha256']:
        raise ValueError('Recorded desk publication differs')
    return packet


def verified_run(ref,read):
    if not isinstance(ref,dict) or set(ref)!={'manifest_key','output_sha256'} or not re.fullmatch('[a-f0-9]{64}',ref.get('output_sha256','')):raise ValueError('Exact desk run reference required')
    key=ref.get('manifest_key','')
    if not re.fullmatch(re.escape(model.PREFIX)+r'runs/[a-f0-9]{64}\.json',key):raise ValueError('Desk run path required')
    raw=read(key);run=json.loads(raw)
    if key!=model.PREFIX+'runs/'+model.sha(raw)+'.json' or run.get('contract')!='etf-desk-replay.v1':raise ValueError('Desk run bytes or contract differ')
    if set(run['compilers'])!={m.__name__ for m in COMPILERS}:raise ValueError('Desk compiler inventory differs')
    for module in COMPILERS:
        body=Path(module.__file__).read_bytes();digest=model.sha(body);expected={'key':model.PREFIX+'compilers/'+digest+'.py','sha256':digest}
        if run['compilers'][module.__name__]!=expected or read(expected['key'])!=body:raise ValueError('Matching reviewed desk compiler required')
    return run


def replay(ref,read):
    run=verified_run(ref,read)
    def verify(key,body):
        if read(key)!=body:raise ValueError('Desk reconstructed artifact differs')
    output=compile_output(checked(run['input'],'inputs',read),read,verify)
    if (output!=checked(run['output'],'outputs',read) or output.get('contract')!=model.CONTRACT
            or model.sha(model.encoded(output))!=ref.get('output_sha256') or run['output_sha256']!=ref['output_sha256']
            or output['generated_at']!=run['generated_at']):raise ValueError('Original desk replay differs')
    return output



def recovery_inputs(ref,read):
    run=verified_run(ref,read);inputs=checked(run['input'],'inputs',read);output=checked(run['output'],'outputs',read)
    if (inputs.get('contract')!='etf-desk-inputs.v1' or output.get('contract')!=model.CONTRACT
            or inputs['generated_at']!=output['generated_at'] or output['generated_at']!=run['generated_at']
            or model.sha(model.encoded(output))!=ref['output_sha256'] or run['output_sha256']!=ref['output_sha256']
            or model.clock(output['generated_at'])>model.clock(now()) or any(output.get(k) is not False for k in model.PERMISSIONS)):
        raise ValueError('Recovery desk output or clock contract differs')
    due=[f['flows'].get('source_valid_until') for f in output['funds'].values()]
    due+=[f['profiles']['current'].get('source_valid_until') for f in output['funds'].values()]
    if not any(v and model.clock(now())<model.clock(v) for v in due):raise ValueError('Recovery has no current source checks; retained clocks cannot be refreshed')
    return inputs,output


class ArtifactWriter(holdings_store.ArtifactWriter):
    def write(self,key,body):immutable(self.client,self.bucket,key,body,read=self.read)


def retain(client,bucket,inputs,output,read,checkpoint=None):
    refs={}
    if len(model.encoded(output))>model.MAX_PUBLIC_ARTIFACT:raise ValueError('Desk public root byte bound')
    for name,doc in (('input',inputs),('output',output)):
        raw=model.encoded(doc);digest=model.sha(raw);key=model.PREFIX+name+'s/'+digest+'.json'
        immutable(client,bucket,key,raw,read=read);refs[name]={'key':key,'sha256':digest,'bytes':len(raw)}
    compilers={}
    for module in COMPILERS:
        raw=Path(module.__file__).read_bytes();digest=model.sha(raw);key=model.PREFIX+'compilers/'+digest+'.py'
        immutable(client,bucket,key,raw,'text/x-python',read);compilers[module.__name__]={'key':key,'sha256':digest}
    run={'contract':'etf-desk-replay.v1','generated_at':output['generated_at'],**refs,'compilers':compilers,'output_sha256':refs['output']['sha256']}
    raw=model.encoded(run);key=model.PREFIX+'runs/'+model.sha(raw)+'.json';immutable(client,bucket,key,raw,read=read)
    ref={'manifest_key':key,'output_sha256':refs['output']['sha256']}
    if checkpoint is not None:checkpoint(ref)
    if replay(ref,read)!=output:raise ValueError('Retained original desk replay differs')
    return ref


def regresses(old,new):
    for ticker,previous in old.get('funds',{}).items():
        candidate=new.get('funds',{}).get(ticker)
        if candidate is None:return True
        for family in ('profiles','holdings'):
            for role in ('current','prior'):
                a=previous[family][role].get('processed_date');b=candidate[family][role].get('processed_date')
                if a and b and b<a:return True
        a=previous['flows'].get('latest_effective_date');b=candidate['flows'].get('latest_effective_date')
        if a and b and b<a:return True
    return False


def conditional(client,bucket,key,packet):
    if key not in (model.CURRENT,*ALIASES):raise ValueError('Reviewed public desk target required')
    at=model.clock(packet['generated_at'])
    for _ in range(4):
        try:
            obj=client.get_object(Bucket=bucket,Key=key);raw=bounded(obj['Body']);old=json.loads(raw)
            if old.get('generated_at') and model.clock(old['generated_at'])>at:return False
            if old.get('generated_at') and model.clock(old['generated_at'])==at and old!=packet:raise ValueError('Conflicting same-clock desk publication')
            if old.get('contract')==model.CONTRACT and regresses(old,packet):return False
            protect(client,bucket,raw);condition={'IfMatch':obj['ETag']}
        except Exception as exc:
            if not missing(exc):raise
            condition={'IfNoneMatch':'*'}
        try:
            status_write(client,bucket,key,packet,**condition)
            live=json.loads(bounded(client.get_object(Bucket=bucket,Key=key)['Body']))
            if live!=packet and model.clock(live['generated_at'])<=at:raise ValueError('Desk public readback differs')
            return True
        except Exception as exc:
            if not conflict(exc):raise
    raise RuntimeError('Desk publication conflicts; immutable evidence retained')


def compatibility(packet,target):
    if target not in ALIASES or packet.get('contract')!=model.CONTRACT or 'replay' not in packet:raise ValueError('Retained native desk required')
    return {'contract':'etf-desk-compatibility.v1','generated_at':packet['generated_at'],'source_key':target,
        'canonical':{'key':model.CURRENT,'replay':packet['replay']},'status':'superseded_by_native_research',
        'fund_inventory':list(packet['funds']),'by_etf':{},'by_ticker':{},'by_stock':{},'risk':{},'summary':{},
        'retained_predecessor':packet['legacy_contexts'].get(target),'independent_investment_votes':0,
        'scope':'Dated profiles, complete holdings and reported flows are in the canonical desk. Whole old histories remain separately preserved. No implied stock purchases, risk score, normalized weight or guessed expense ratio is supplied.',**model.permissions()}


def request_key(request_id):
    if not isinstance(request_id,str) or not re.fullmatch(r'[A-Za-z0-9._:-]{1,160}',request_id):raise ValueError('Bounded desk request identity required')
    return model.PREFIX+'requests/'+model.sha(request_id.encode())+'.json'


def collect(client,bucket,credential,read,deadline):
    profiles=collector.Collector(credential,lambda raw:protect(client,bucket,raw,read),deadline)
    pairs,p_requests,p_bytes=profiles.collect()
    extras=sorted(set(catalog.DESK)-set(flow_catalog.ETF_UNIVERSE))
    hold=holdings_collect.Collector(credential,lambda raw:protect(client,bucket,raw,read,holdings_native.PRIVATE),deadline,profiles.query_date)
    with ThreadPoolExecutor(max_workers=6) as pool:holding_pairs=dict(zip(extras,pool.map(hold.fund,extras)))
    flows=flow_collect.Collector(credential,lambda raw:protect(client,bucket,raw,read,flow_native.PRIVATE),deadline);flows.query_date=profiles.query_date
    with ThreadPoolExecutor(max_workers=6) as pool:flow_rows=dict(zip(extras,pool.map(flows.fund,extras)))
    return {'query_date':profiles.query_date,'profiles':pairs,'extra_flows':flow_rows,'extra_holdings':holding_pairs,
        'provider_requests':p_requests+hold.requests+flows.requests,'original_provider_bytes':p_bytes+hold.source_bytes+flows.source_bytes}


def run(client,bucket,request_id,execution_id,credential='',remaining_seconds=900,recover_run=None):
    end=time.monotonic()+max(1,min(remaining_seconds,900));key=request_key(request_id)
    status={'contract':'etf-desk-request.v1','request_id':request_id,'execution_id':execution_id,'started_at':now(),'status':'running','phase':'preserve'}
    try:status_write(client,bucket,key,status,IfNoneMatch='*')
    except Exception as exc:
        if not conflict(exc):raise
        return json.loads(bounded(client.get_object(Bucket=bucket,Key=key)['Body']))
    try:
        read=reader(client,bucket);expected_output=None
        if recover_run is not None:
            inputs,expected_output=recovery_inputs(recover_run,read);status['recovered_from']=recover_run
        else:
            contexts=preserve(client,bucket,read)
            canonical_flows=snapshot(client,bucket,flow_model.CURRENT,read);canonical_holdings=snapshot(client,bucket,holdings_model.CURRENT,read)
            try:previous=snapshot(client,bucket,model.CURRENT,read)
            except Exception as exc:
                if not missing(exc):raise
                previous=None
            status['phase']='collect_originals';status_write(client,bucket,key,status)
            collections=collect(client,bucket,credential,read,end-360)
            inputs={'contract':'etf-desk-inputs.v1','generated_at':now(),'contexts':contexts,
                'canonical_flows':canonical_flows,'canonical_holdings':canonical_holdings,'previous':previous,**collections}
        raw=model.encoded(inputs);digest=model.sha(raw);input_key=model.PREFIX+'inputs/'+digest+'.json';immutable(client,bucket,input_key,raw,read=read)
        status.update(phase='compile',phase_started_at=now(),retained_input={'key':input_key,'sha256':digest,'bytes':len(raw)},provider_requests=inputs['provider_requests'])
        status_write(client,bucket,key,status)
        with ArtifactWriter(client,bucket,read) as emit:output=compile_output(inputs,read,emit)
        if expected_output is not None and output!=expected_output:raise ValueError('Recovered original desk calculation differs')
        status.update(phase='retained_replay',phase_started_at=now());status_write(client,bucket,key,status)
        def checkpoint(ref):status['candidate_replay']=ref;status_write(client,bucket,key,status)
        ref=retain(client,bucket,inputs,output,read,checkpoint)
        status['phase']='publish';status_write(client,bucket,key,status);published=False;aliases={}
        if output['quality']['status']!='unavailable':
            packet={**output,'replay':ref};published=conditional(client,bucket,model.CURRENT,packet)
            if published:
                for target in ALIASES:aliases[target]=conditional(client,bucket,target,compatibility(packet,target))
        result={**status,'status':'complete','phase':'complete','completed_at':now(),'published':published,
            'generated_at':output['generated_at'],'quality':output['quality'],'replay':ref,'compatibility_publications':aliases,
            'provider_requests':inputs['provider_requests'],'provider_requests_this_execution':0 if recover_run is not None else inputs['provider_requests'],'private_account_reads':0,'paid_ai_calls':0,'signals_emitted':0,'notifications_sent':0,'portfolio_writes':0}
        status_write(client,bucket,key,result);return result
    except Exception:
        status_write(client,bucket,key,{**status,'status':'failed','completed_at':now(),'error':'original_desk_replay_or_publication_failed'})
        raise RuntimeError('Native desk research failed; inspect retained request evidence') from None
