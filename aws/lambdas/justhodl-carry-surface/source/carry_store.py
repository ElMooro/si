"""Retain complete native responses and replay before conditional publication."""
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import gzip, hashlib, io, json, re, threading, time, urllib.error, urllib.request
from pathlib import Path
from urllib.parse import urlencode
import evidence_store, report_observations
import carry_catalog, carry_equity
import carry_original as native
import carry_research as model

PRIVATE='audit-private/20260909-originals/carry-research/'
COMPILERS=(carry_catalog,carry_equity,native,model,evidence_store,report_observations)
MAX_BYTES=64*1024*1024
MIN_EQUITIES=85
MIN_RATES=30
_lock=threading.Lock()
_last={'fred':0.0,'fmp':0.0}

def now():return datetime.now(timezone.utc).isoformat()
def code(exc):return str(getattr(exc,'response',{}).get('Error',{}).get('Code',''))
def missing(exc):return code(exc) in ('404','NoSuchKey')
def conflict(exc):return code(exc) in ('412','409','PreconditionFailed','ConditionalRequestConflict')
def bounded(body,limit=MAX_BYTES):
    try:raw=body.read(limit+1)
    finally:body.close()
    if len(raw)>limit:raise ValueError('carry artifact bound exceeded')
    return raw

def raw_reader(client,bucket):
    cache={};size=0
    def read(key):
        nonlocal size
        if not isinstance(key,str) or not re.fullmatch(r'data/[A-Za-z0-9_./-]+',key) or '..' in key:raise ValueError('public evidence path required')
        if key in cache:return cache[key]
        raw=bounded(client.get_object(Bucket=bucket,Key=key)['Body'])
        raw=bounded(gzip.GzipFile(fileobj=io.BytesIO(raw))) if key.endswith('.gz') else raw
        if re.search(r'/[a-f0-9]{64}\.(?:bin\.gz|json|py)$',key) and size+len(raw)<=128*1024*1024:
            cache[key]=raw;size+=len(raw)
        return raw
    return read

def immutable(client,bucket,key,raw,kind='application/json'):
    if not(key.startswith(model.PREFIX) or key.startswith(PRIVATE)):raise ValueError('carry retention prefix differs')
    try:client.put_object(Bucket=bucket,Key=key,Body=raw,ContentType=kind,IfNoneMatch='*',
        CacheControl='no-store' if key.startswith(PRIVATE) else 'public, max-age=31536000, immutable')
    except Exception as exc:
        if not conflict(exc):raise
    if bounded(client.get_object(Bucket=bucket,Key=key)['Body'])!=raw:raise ValueError('retained bytes differ')

def preserve(client,bucket,read):
    key=model.PREFIX+'migration.json'
    try:marker=json.loads(read(key))
    except Exception as exc:
        if not missing(exc):raise
        raw=read(model.CURRENT);packet=json.loads(raw)
        if packet.get('contract')==model.CONTRACT:raise ValueError('legacy retention marker missing')
        sha=hashlib.sha256(raw).hexdigest();immutable(client,bucket,PRIVATE+sha+'.bin',raw,'application/octet-stream')
        marker={'contract':'carry-legacy-preservation.v1','source':model.CURRENT,'sha256':sha,'bytes':len(raw),
          'protected_backup':True,'legacy_generated_at':packet.get('generated_at'),
          'status':'Complete preceding engine packet retained privately; no measurements promoted without original evidence.'}
        immutable(client,bucket,key,model.encoded(marker))
    if marker.get('contract')!='carry-legacy-preservation.v1' or not re.fullmatch('[a-f0-9]{64}',marker.get('sha256','')):raise ValueError('legacy marker differs')
    return marker

def acquire(client,bucket,url,key=None,key_name='api_key',deadline=None):
    if deadline is not None and time.monotonic()>deadline:raise TimeoutError('source acquisition budget exceeded')
    actual=url+('&'+urlencode({key_name:key}) if key else '')
    request=urllib.request.Request(actual,headers={'User-Agent':'JustHodl original carry research','Accept':'application/json,text/csv'})
    provider='fred' if url.startswith('https://api.stlouisfed.org/') else 'fmp' if url.startswith('https://financialmodelingprep.com/') else None
    for attempt in range(3):
        if deadline is not None and time.monotonic()>deadline:raise TimeoutError('source acquisition budget exceeded')
        if provider:
            with _lock:
                time.sleep(max(0,(.8 if provider=='fred' else .16)-(time.monotonic()-_last[provider])));_last[provider]=time.monotonic()
        try:
            with urllib.request.build_opener().open(request,timeout=15) as response:raw=response.read(8*1024*1024+1)
            break
        except urllib.error.HTTPError as exc:
            if attempt==2 or exc.code not in (429,500,502,503,504):raise
            time.sleep(2*(attempt+1))
    if not raw or len(raw)>8*1024*1024:raise ValueError('carry source response bound')
    if key and len(key)>=12 and key.encode() in raw:raise ValueError('credential echoed in response')
    stamp=now();receipt=evidence_store.capture(client,bucket,'carry',url,raw,native.clock(stamp))
    return {'url':url,'acquired_at':stamp,'evidence':receipt}

def collect(client,bucket,fred_key,fmp_key,read,deadline):
    results={};errors={};definitions={};fred={};equities={};stamp=now()
    def requests(items):
        with ThreadPoolExecutor(max_workers=8) as pool:
            pending={pool.submit(acquire,client,bucket,url,key,key_name,deadline):name for name,(url,key,key_name) in items.items()}
            for future in as_completed(pending):
                name=pending[future]
                try:results[name]=future.result()
                except Exception as exc:errors[':'.join(name)]='HTTP_'+str(exc.code) if isinstance(exc,urllib.error.HTTPError) else type(exc).__name__
    requests({(sid,'definition'):(native.definition_url(sid),fred_key,'api_key') for sid in carry_catalog.FRED})
    observations={}
    for sid in carry_catalog.FRED:
        key=(sid,'definition')
        if key not in results:continue
        try:
            meta=native.definition(sid,native.original(results[key],read,native.definition_url(sid),now()))
            urls=native.observation_urls(sid,meta,stamp);definitions[sid]=urls
            observations.update({(sid,str(i)):(url,fred_key,'api_key') for i,url in enumerate(urls)})
        except Exception as exc:errors[sid+':definition']='validation_'+type(exc).__name__
    observations[('ECB','series')]=(carry_catalog.ECB_EURIBOR_URL,None,'')
    observations.update({(symbol,path):(native.fmp_url(symbol,path),fmp_key,'apikey') for symbol in carry_catalog.EQUITIES for path in carry_catalog.FMP_PATHS})
    requests(observations)
    for sid,urls in definitions.items():
        if urls and all((sid,str(i)) in results for i in range(len(urls))):
            fred[sid]={'definition':results[(sid,'definition')],'observations':[results[(sid,str(i))] for i in range(len(urls))]}
    for symbol in carry_catalog.EQUITIES:
        if all((symbol,path) in results for path in carry_catalog.FMP_PATHS):equities[symbol]={path:results[(symbol,path)] for path in carry_catalog.FMP_PATHS}
    # Even partial families retain every successful source response for inspection.
    partial={':'.join(name):ref for name,ref in results.items()}
    return fred,equities,results.get(('ECB','series')),errors,partial


def publish(client,bucket,packet):
    for _ in range(5):
        try:
            obj=client.get_object(Bucket=bucket,Key=model.CURRENT);old=json.loads(bounded(obj['Body']))
            if old.get('contract')==model.CONTRACT:
                if native.clock(old['generated_at'])>native.clock(packet['generated_at']):return False
                if old['generated_at']==packet['generated_at'] and old!=packet:raise ValueError('same-clock conflicting carry output')
                for name,stamp in packet['source_clocks'].items():
                    previous=old.get('source_clocks',{}).get(name)
                    if previous and native.clock(previous)>native.clock(stamp):return False
            condition={'IfMatch':obj['ETag']}
        except Exception as exc:
            if not missing(exc):raise
            condition={'IfNoneMatch':'*'}
        try:
            client.put_object(Bucket=bucket,Key=model.CURRENT,Body=model.encoded(packet),ContentType='application/json',CacheControl='no-store',**condition);return True
        except Exception as exc:
            if not conflict(exc):raise
    raise RuntimeError('carry publication contention bound')

def replay(manifest,read):
    if manifest.get('contract')!='carry-original-replay.v1' or set(manifest.get('compilers',{}))!={m.__name__ for m in COMPILERS}:raise ValueError('carry replay contract differs')
    for module in COMPILERS:
        raw=Path(module.__file__).read_bytes();sha=hashlib.sha256(raw).hexdigest();ref=manifest['compilers'][module.__name__]
        if ref!={'key':model.PREFIX+'compilers/'+sha+'.py','sha256':sha} or read(ref['key'])!=raw:raise ValueError('reviewed compiler differs; use matching checkout')
    def load(name):
        ref=manifest[name];body=read(ref['key'])
        if ref['key']!=model.PREFIX+name+'s/'+ref['sha256']+'.json' or len(body)!=ref['bytes'] or hashlib.sha256(body).hexdigest()!=ref['sha256']:raise ValueError('retained '+name+' differs')
        return native.strict_json(body)
    output,histories=model.build(load('input'),read,manifest['generated_at'])
    # Ordinary JSON retains numeric output types; source parsing alone uses exact strings.
    reference=json.loads(read(manifest['output']['key']))
    if output!=reference or model.digest(output)!=manifest['output_sha256']:raise ValueError('carry original replay differs')
    load('output')
    for key,body in histories.items():
        if read(key)!=body:raise ValueError('carry history shard differs')
    return output

def run(client,bucket,fred_key,fmp_key,context=None):
    if not fred_key or not fmp_key:raise ValueError('configured market-data credential unavailable')
    remaining=context.get_remaining_time_in_millis()/1000 if context and hasattr(context,'get_remaining_time_in_millis') else 600
    deadline=time.monotonic()+max(10,min(380,remaining-180))
    read=raw_reader(client,bucket);legacy=preserve(client,bucket,read)
    fred,equities,ecb,errors,retained=collect(client,bucket,fred_key,fmp_key,read,deadline);stamp=now()
    inputs={'contract':'carry-original-inputs.v1','fred':fred,'equities':equities,'ecb':ecb,'acquisition_errors':errors,'retained_sources':retained,'legacy':legacy}
    output,histories=model.build(inputs,read,stamp)
    print('[carry-research] '+json.dumps({'verified_equities':len(output['equities']),'verified_rates':len(output['measurements']),
      'error_count':output['errors'],'first_error_keys':sorted(output['source_status_codes'])[:12]},sort_keys=True))
    if output['quality']['status']=='unavailable':raise ValueError('no verified carry sources')
    if len(output['equities'])<MIN_EQUITIES or len(output['measurements'])<MIN_RATES:raise ValueError('carry source coverage below publication floor; retain last verified snapshot')
    for key,body in histories.items():immutable(client,bucket,key,body)
    refs={}
    for name,value in (('input',inputs),('output',output)):
        body=model.encoded(value);sha=model.digest(value);key=model.PREFIX+name+'s/'+sha+'.json'
        immutable(client,bucket,key,body);refs[name]={'key':key,'sha256':sha,'bytes':len(body)}
    compilers={}
    for module in COMPILERS:
        body=Path(module.__file__).read_bytes();sha=hashlib.sha256(body).hexdigest();key=model.PREFIX+'compilers/'+sha+'.py'
        immutable(client,bucket,key,body,'text/x-python');compilers[module.__name__]={'key':key,'sha256':sha}
    manifest={'contract':'carry-original-replay.v1','generated_at':stamp,'compilers':compilers,**refs,'output_sha256':refs['output']['sha256']}
    key=model.PREFIX+'runs/'+model.digest(manifest)+'.json';immutable(client,bucket,key,model.encoded(manifest))
    if replay(manifest,read)!=output:raise ValueError('pre-publication replay differs')
    ref={'manifest_key':key,'output_sha256':manifest['output_sha256']};published=publish(client,bucket,{**output,'replay':ref})
    return {'published':published,'generated_at':stamp,'replay':ref,'quality':output['quality'],'source_errors':output['source_status_codes'],
      'history_shards':len(histories),'signals_emitted':0,'paid_ai_calls':0,'notifications_sent':0,'private_account_reads':0,'portfolio_writes':0}
