"""Retain originals, shard complete histories, replay and conditionally publish."""
from concurrent.futures import ThreadPoolExecutor,as_completed
from datetime import datetime,timezone
import gzip,hashlib,io,json,re,time,urllib.error,urllib.request
from pathlib import Path

import canonical_macro_sources
import evidence_store
import funding_original as native
import funding_research as model
import funding_research_catalog
import report_observations
import research_brief_model

MAX_BYTES=32*1024*1024
PRIVATE='audit-private/20260909-originals/funding-research/'
COMPILERS=(model,native,funding_research_catalog,canonical_macro_sources,evidence_store,report_observations,research_brief_model)
CONTEXTS=('data/ofr-stfm.json','data/hkma.json','data/crypto-liquidity.json','data/usd-funding.json',
          'data/polygon-fx-regime.json','data/bis-crossborder.json')


def now():return datetime.now(timezone.utc).isoformat()
def code(exc):return str(getattr(exc,'response',{}).get('Error',{}).get('Code',''))
def missing(exc):return code(exc) in ('404','NoSuchKey')
def conflict(exc):return code(exc) in ('412','409','PreconditionFailed','ConditionalRequestConflict')


def bounded(body,limit=MAX_BYTES):
    try:raw=body.read(limit+1)
    finally:body.close()
    if len(raw)>limit:raise ValueError('funding artifact bound exceeded')
    return raw


def raw_reader(client,bucket):
    def read(key):
        if not isinstance(key,str) or not re.fullmatch(r'data/[A-Za-z0-9_./-]+',key) or '..' in key:raise ValueError('public evidence path required')
        raw=bounded(client.get_object(Bucket=bucket,Key=key)['Body'])
        return bounded(gzip.GzipFile(fileobj=io.BytesIO(raw))) if key.endswith('.gz') else raw
    return read


def immutable(client,bucket,key,raw,kind='application/json'):
    if not(key.startswith(model.PREFIX) or key.startswith(PRIVATE)):raise ValueError('funding retention prefix differs')
    try:client.put_object(Bucket=bucket,Key=key,Body=raw,ContentType=kind,IfNoneMatch='*',
        CacheControl='no-store' if key.startswith(PRIVATE) else 'public, max-age=31536000, immutable')
    except Exception as exc:
        if not conflict(exc):raise
    if bounded(client.get_object(Bucket=bucket,Key=key)['Body'])!=raw:raise ValueError('retained bytes differ')


def project_legacy(packet):
    out=[]
    for layer,data in (packet.get('layers') or {}).items():
        if not re.fullmatch(r'[a-z_]{1,40}',layer) or not isinstance(data,dict):raise ValueError('legacy layer differs')
        rows=data.get('metrics') or []
        if not isinstance(rows,list) or len(rows)>150:raise ValueError('legacy metric bound')
        for row in rows:
            identifier=row.get('id');unit=row.get('unit');asof=row.get('asof')
            if not isinstance(identifier,str) or not re.fullmatch(r'[A-Za-z0-9_ -]{1,100}',identifier):raise ValueError('legacy identity differs')
            if unit is not None and (not isinstance(unit,str) or len(unit)>40):raise ValueError('legacy unit bound')
            if asof is not None and (not isinstance(asof,str) or not re.fullmatch(r'[0-9TWZ:+/ .-]{1,50}',asof)):asof=None
            out.append({'layer':layer,'id':identifier,'reported_decimal':native.amount(row.get('value')),'declared_unit':unit,
                        'declared_asof':asof,'status':'retained_legacy_claim_unqualified'})
    return out


def preserve(client,bucket,read):
    key=model.PREFIX+'migration.json'
    try:marker=json.loads(read(key))
    except Exception as exc:
        if not missing(exc):raise
        raw=read(model.CURRENT);packet=json.loads(raw)
        if packet.get('contract')==model.CONTRACT:raise ValueError('legacy retention marker missing')
        sha=hashlib.sha256(raw).hexdigest();immutable(client,bucket,PRIVATE+sha+'.bin',raw,'application/octet-stream')
        inventory=project_legacy(packet);body=model.encoded(inventory);target=model.PREFIX+'inventories/'+model.digest(inventory)+'.json'
        immutable(client,bucket,target,body)
        marker={'contract':'funding-legacy-preservation.v1','source':model.CURRENT,'sha256':sha,'bytes':len(raw),'protected_backup':True,
                'legacy_generated_at':packet.get('generated_at'),'inventory':{'key':target,'sha256':model.digest(inventory),'bytes':len(body)}}
        immutable(client,bucket,key,model.encoded(marker))
    if marker.get('contract')!='funding-legacy-preservation.v1':raise ValueError('legacy marker differs')
    ref=marker['inventory'];body=read(ref['key'])
    if ref['key']!=model.PREFIX+'inventories/'+ref['sha256']+'.json' or len(body)!=ref['bytes'] or hashlib.sha256(body).hexdigest()!=ref['sha256']:
        raise ValueError('legacy inventory differs')
    return marker,json.loads(body)


def contexts(client,bucket,read):
    """Keep auxiliary desk snapshots for audit, without laundering them as originals."""
    out={}
    for key in CONTEXTS:
        try:raw=read(key)
        except Exception as exc:
            if not missing(exc):raise
            out[key]={'status':'unavailable','source':key};continue
        packet=json.loads(raw);sha=hashlib.sha256(raw).hexdigest()
        immutable(client,bucket,PRIVATE+sha+'.bin',raw,'application/octet-stream')
        stamp=packet.get('generated_at') or packet.get('generated')
        try:native.clock(stamp)
        except (ValueError,TypeError):stamp=None
        out[key]={'source':key,'source_generated_at':stamp,'sha256':sha,'bytes':len(raw),'protected_backup':True,
                  'status':'retained_engine_context_unqualified','original_provider_verified':False,
                  'note':'Engine snapshot retained privately; no implicit unit, score, independent vote or original-source qualification.'}
    return out


def acquire(client,bucket,name,fmp_key=None):
    public_url=native.URLS[name];url=public_url
    if name in native.FX:
        if not fmp_key:raise ValueError('configured FX data credential unavailable')
        from urllib.parse import urlencode
        url+='&'+urlencode({'apikey':fmp_key})
    request=urllib.request.Request(url,headers={'User-Agent':'JustHodl funding source research','Accept':'*/*'})
    for attempt in range(2):
        try:
            with urllib.request.build_opener().open(request,timeout=20) as response:raw=response.read(8*1024*1024+1)
            break
        except urllib.error.HTTPError as exc:
            if attempt or exc.code not in (429,500,502,503,504):raise
            time.sleep(2)
    if not raw or len(raw)>8*1024*1024:raise ValueError('funding original response size bound')
    if fmp_key and len(fmp_key)>=12 and fmp_key.encode() in raw:raise ValueError('credential echoed in response; capture refused')
    stamp=now();receipt=evidence_store.capture(client,bucket,'funding',public_url,raw,native.clock(stamp))
    return {'url':public_url,'acquired_at':stamp,'evidence':receipt}


def publish(client,bucket,packet):
    for _ in range(5):
        try:
            obj=client.get_object(Bucket=bucket,Key=model.CURRENT);old=json.loads(bounded(obj['Body']))
            if old.get('contract')==model.CONTRACT:
                if native.clock(old['generated_at'])>native.clock(packet['generated_at']):return False
                if old['generated_at']==packet['generated_at'] and old!=packet:raise ValueError('same-clock conflicting funding output')
                for key,stamp in packet['source_clocks'].items():
                    previous=old.get('source_clocks',{}).get(key)
                    if previous and native.clock(previous)>native.clock(stamp):return False
            condition={'IfMatch':obj['ETag']}
        except Exception as exc:
            if not missing(exc):raise
            condition={'IfNoneMatch':'*'}
        try:
            client.put_object(Bucket=bucket,Key=model.CURRENT,Body=model.encoded(packet),ContentType='application/json',CacheControl='no-store',**condition)
            return True
        except Exception as exc:
            if not conflict(exc):raise
    raise RuntimeError('funding publication contention bound')


def replay(manifest,read):
    if manifest.get('contract')!='funding-original-replay.v1' or set(manifest.get('compilers',{}))!={m.__name__ for m in COMPILERS}:
        raise ValueError('funding replay contract differs')
    for module in COMPILERS:
        raw=Path(module.__file__).read_bytes();sha=hashlib.sha256(raw).hexdigest();ref=manifest['compilers'][module.__name__]
        if ref!={'key':model.PREFIX+'compilers/'+sha+'.py','sha256':sha} or read(ref['key'])!=raw:raise ValueError('reviewed compiler differs; use matching checkout')
    def load(name):
        ref=manifest[name];body=read(ref['key'])
        if ref['key']!=model.PREFIX+name+'s/'+ref['sha256']+'.json' or len(body)!=ref['bytes'] or hashlib.sha256(body).hexdigest()!=ref['sha256']:
            raise ValueError('retained '+name+' differs')
        return json.loads(body)
    output,histories=model.build(load('input'),read,manifest['generated_at'])
    if output!=load('output') or model.digest(output)!=manifest['output_sha256']:raise ValueError('funding original replay differs')
    for key,body in histories.items():
        if read(key)!=body:raise ValueError('funding history shard differs')
    return output


def run(client,bucket,fmp_key=None):
    read=raw_reader(client,bucket);legacy,inventory=preserve(client,bucket,read)
    source=json.loads(read('data/report-measurements.json'));originals={};errors={}
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures={pool.submit(acquire,client,bucket,name,fmp_key):name for name in native.URLS}
        for future in as_completed(futures):
            name=futures[future]
            try:originals[name]=future.result()
            except Exception as exc:errors[name]='HTTP_'+str(exc.code) if isinstance(exc,urllib.error.HTTPError) else type(exc).__name__
    try:fails=json.loads(read('data/settlement-fails.json'))
    except Exception as exc:
        if not missing(exc):raise
        fails=None
    retained_contexts=contexts(client,bucket,read);stamp=now()
    inputs={'contract':'funding-original-inputs.v1','source':source,'originals':originals,'acquisition_errors':errors,
            'legacy_ref':legacy,'legacy_inventory':inventory,'contexts':retained_contexts,'fails':fails}
    output,histories=model.build(inputs,read,stamp)
    for key,body in histories.items():immutable(client,bucket,key,body)
    refs={}
    for name,value in (('input',inputs),('output',output)):
        body=model.encoded(value);sha=model.digest(value);key=model.PREFIX+name+'s/'+sha+'.json'
        immutable(client,bucket,key,body);refs[name]={'key':key,'sha256':sha,'bytes':len(body)}
    compilers={}
    for module in COMPILERS:
        body=Path(module.__file__).read_bytes();sha=hashlib.sha256(body).hexdigest();key=model.PREFIX+'compilers/'+sha+'.py'
        immutable(client,bucket,key,body,'text/x-python');compilers[module.__name__]={'key':key,'sha256':sha}
    manifest={'contract':'funding-original-replay.v1','generated_at':stamp,'compilers':compilers,**refs,'output_sha256':refs['output']['sha256']}
    key=model.PREFIX+'runs/'+model.digest(manifest)+'.json';immutable(client,bucket,key,model.encoded(manifest))
    if replay(manifest,read)!=output:raise ValueError('pre-publication funding replay differs')
    ref={'manifest_key':key,'output_sha256':manifest['output_sha256']}
    published=publish(client,bucket,{**output,'replay':ref})
    return {'published':published,'generated_at':stamp,'replay':ref,'quality':output['quality'],
            'original_measurements':len(output['measurements']),'history_shards':len(histories),
            'signals_emitted':0,'paid_ai_calls':0,'notifications_sent':0,'private_account_reads':0,'portfolio_writes':0}
