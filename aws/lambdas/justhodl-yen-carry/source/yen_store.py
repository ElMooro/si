"""Retain complete native responses and replay before conditional publication."""
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import gzip, hashlib, io, json, re, threading, time, urllib.error, urllib.request
from pathlib import Path
from urllib.parse import urlencode
import evidence_store, report_observations
import yen_original as native
import yen_research as model

PRIVATE='audit-private/20260909-originals/yen-research/'
COMPILERS=(native,model,evidence_store,report_observations)
MAX_BYTES=32*1024*1024
_lock=threading.Lock()
_last=0.0

def now():return datetime.now(timezone.utc).isoformat()
def code(exc):return str(getattr(exc,'response',{}).get('Error',{}).get('Code',''))
def missing(exc):return code(exc) in ('404','NoSuchKey')
def conflict(exc):return code(exc) in ('412','409','PreconditionFailed','ConditionalRequestConflict')
def bounded(body,limit=MAX_BYTES):
    try:raw=body.read(limit+1)
    finally:body.close()
    if len(raw)>limit:raise ValueError('yen artifact bound exceeded')
    return raw

def raw_reader(client,bucket):
    def read(key):
        if not isinstance(key,str) or not re.fullmatch(r'data/[A-Za-z0-9_./-]+',key) or '..' in key:raise ValueError('public evidence path required')
        raw=bounded(client.get_object(Bucket=bucket,Key=key)['Body'])
        return bounded(gzip.GzipFile(fileobj=io.BytesIO(raw))) if key.endswith('.gz') else raw
    return read

def immutable(client,bucket,key,raw,kind='application/json'):
    if not(key.startswith(model.PREFIX) or key.startswith(PRIVATE)):raise ValueError('yen retention prefix differs')
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
        marker={'contract':'yen-legacy-preservation.v1','source':model.CURRENT,'sha256':sha,'bytes':len(raw),
          'protected_backup':True,'legacy_generated_at':packet.get('generated_at'),
          'status':'Complete preceding engine packet retained privately; no measurements promoted without original evidence.'}
        immutable(client,bucket,key,model.encoded(marker))
    if marker.get('contract')!='yen-legacy-preservation.v1' or not re.fullmatch('[a-f0-9]{64}',marker.get('sha256','')):raise ValueError('legacy marker differs')
    return marker

def acquire(client,bucket,url,key=None):
    global _last
    actual=url+('&'+urlencode({'api_key':key}) if key else '')
    request=urllib.request.Request(actual,headers={'User-Agent':'JustHodl original yen research','Accept':'application/json'})
    for attempt in range(3):
        if url.startswith('https://api.stlouisfed.org/'):
            with _lock:
                time.sleep(max(0,.8-(time.monotonic()-_last)));_last=time.monotonic()
        try:
            with urllib.request.build_opener().open(request,timeout=20) as response:raw=response.read(8*1024*1024+1)
            break
        except urllib.error.HTTPError as exc:
            if attempt==2 or exc.code not in (429,500,502,503,504):raise
            time.sleep(2*(attempt+1))
    if not raw or len(raw)>8*1024*1024:raise ValueError('yen source response bound')
    if key and len(key)>=12 and key.encode() in raw:raise ValueError('credential echoed in response')
    stamp=now();receipt=evidence_store.capture(client,bucket,'yen',url,raw,native.clock(stamp))
    return {'url':url,'acquired_at':stamp,'evidence':receipt}

def collect(client,bucket,fred_key):
    stamp=now();requests={};fred={};errors={}
    for sid in native.SERIES:
        definition,observations=native.fred_urls(sid,stamp)
        requests[(sid,'definition')]=(definition,fred_key)
        requests.update({(sid,str(i)):(url,fred_key) for i,url in enumerate(observations)})
    requests.update({('CFTC',name):(url,None) for name,url in native.CFTC_URLS.items()})
    results={}
    with ThreadPoolExecutor(max_workers=4) as pool:
        pending={pool.submit(acquire,client,bucket,url,key):name for name,(url,key) in requests.items()}
        for future in as_completed(pending):
            name=pending[future]
            try:results[name]=future.result()
            except Exception as exc:errors[':'.join(name)]='HTTP_'+str(exc.code) if isinstance(exc,urllib.error.HTTPError) else type(exc).__name__
    for sid in native.SERIES:
        wanted=[k for k in requests if k[0]==sid]
        if all(k in results for k in wanted):fred[sid]={'definition':results[(sid,'definition')],
          'observations':[results[(sid,str(i))] for i in range(len(wanted)-1)]}
    cftc={k:results[('CFTC',k)] for k in native.CFTC_URLS} if all(('CFTC',k) in results for k in native.CFTC_URLS) else None
    return fred,cftc,errors

def publish(client,bucket,packet):
    for _ in range(5):
        try:
            obj=client.get_object(Bucket=bucket,Key=model.CURRENT);old=json.loads(bounded(obj['Body']))
            if old.get('contract')==model.CONTRACT:
                if native.clock(old['generated_at'])>native.clock(packet['generated_at']):return False
                if old['generated_at']==packet['generated_at'] and old!=packet:raise ValueError('same-clock conflicting yen output')
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
    raise RuntimeError('yen publication contention bound')

def replay(manifest,read):
    if manifest.get('contract')!='yen-original-replay.v1' or set(manifest.get('compilers',{}))!={m.__name__ for m in COMPILERS}:raise ValueError('yen replay contract differs')
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
    if output!=reference or model.digest(output)!=manifest['output_sha256']:raise ValueError('yen original replay differs')
    load('output')
    for key,body in histories.items():
        if read(key)!=body:raise ValueError('yen history shard differs')
    return output

def run(client,bucket,fred_key):
    if not fred_key:raise ValueError('configured FRED credential unavailable')
    read=raw_reader(client,bucket);legacy=preserve(client,bucket,read)
    fred,cftc,errors=collect(client,bucket,fred_key);stamp=now()
    inputs={'contract':'yen-original-inputs.v1','fred':fred,'cftc':cftc,'acquisition_errors':errors,'legacy':legacy}
    output,histories=model.build(inputs,read,stamp)
    if output['quality']['status']=='unavailable':raise ValueError('no verified yen sources')
    for key,body in histories.items():immutable(client,bucket,key,body)
    refs={}
    for name,value in (('input',inputs),('output',output)):
        body=model.encoded(value);sha=model.digest(value);key=model.PREFIX+name+'s/'+sha+'.json'
        immutable(client,bucket,key,body);refs[name]={'key':key,'sha256':sha,'bytes':len(body)}
    compilers={}
    for module in COMPILERS:
        body=Path(module.__file__).read_bytes();sha=hashlib.sha256(body).hexdigest();key=model.PREFIX+'compilers/'+sha+'.py'
        immutable(client,bucket,key,body,'text/x-python');compilers[module.__name__]={'key':key,'sha256':sha}
    manifest={'contract':'yen-original-replay.v1','generated_at':stamp,'compilers':compilers,**refs,'output_sha256':refs['output']['sha256']}
    key=model.PREFIX+'runs/'+model.digest(manifest)+'.json';immutable(client,bucket,key,model.encoded(manifest))
    if replay(manifest,read)!=output:raise ValueError('pre-publication replay differs')
    ref={'manifest_key':key,'output_sha256':manifest['output_sha256']};published=publish(client,bucket,{**output,'replay':ref})
    return {'published':published,'generated_at':stamp,'replay':ref,'quality':output['quality'],'source_errors':output['errors'],
      'history_shards':len(histories),'signals_emitted':0,'paid_ai_calls':0,'notifications_sent':0,'private_account_reads':0,'portfolio_writes':0}
