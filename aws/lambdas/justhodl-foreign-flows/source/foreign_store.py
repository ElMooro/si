"""Retain complete native responses and replay before conditional publication."""
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import gzip, hashlib, io, json, re, threading, time, urllib.error, urllib.request
from pathlib import Path
from urllib.parse import urlencode
import evidence_store
import foreign_original as native
import foreign_research as model
import foreign_supplement as supplement

PRIVATE='audit-private/20260909-originals/foreign-research/'
COMPILERS=(native,supplement,model,evidence_store)
MAX_BYTES=32*1024*1024
_lock=threading.Lock()
_last=0.0

def now():return datetime.now(timezone.utc).isoformat()
def code(exc):
    response=getattr(exc,'response',None)
    error=response.get('Error') if isinstance(response,dict) else None
    return str(error.get('Code','')) if isinstance(error,dict) else ''
def missing(exc):return code(exc) in ('404','NoSuchKey')
def conflict(exc):return code(exc) in ('412','409','PreconditionFailed','ConditionalRequestConflict')
def bounded(body,limit=MAX_BYTES):
    try:raw=body.read(limit+1)
    finally:body.close()
    if len(raw)>limit:raise ValueError('foreign artifact bound exceeded')
    return raw

def raw_reader(client,bucket):
    def read(key):
        if not isinstance(key,str) or not re.fullmatch(r'data/[A-Za-z0-9_./-]+',key) or '..' in key:raise ValueError('public evidence path required')
        raw=bounded(client.get_object(Bucket=bucket,Key=key)['Body'])
        return bounded(gzip.GzipFile(fileobj=io.BytesIO(raw))) if key.endswith('.gz') else raw
    return read

def immutable(client,bucket,key,raw,kind='application/json'):
    if not(key.startswith(model.PREFIX) or key.startswith(PRIVATE)):raise ValueError('foreign retention prefix differs')
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
        marker={'contract':'foreign-legacy-preservation.v1','source':model.CURRENT,'sha256':sha,'bytes':len(raw),
          'protected_backup':True,'legacy_generated_at':packet.get('generated_at'),
          'status':'Complete preceding engine packet retained privately; no measurements promoted without original evidence.'}
        immutable(client,bucket,key,model.encoded(marker))
    if marker.get('contract')!='foreign-legacy-preservation.v1' or not re.fullmatch('[a-f0-9]{64}',marker.get('sha256','')):raise ValueError('legacy marker differs')
    return marker

def acquire(client,bucket,url,key=None,deadline=None):
    global _last
    if deadline is not None and time.monotonic()>=deadline:raise TimeoutError('foreign-flow acquisition budget')
    actual=url+('&'+urlencode({'api_key':key}) if key else '')
    request=urllib.request.Request(actual,headers={'User-Agent':'JustHodl original foreign-flow research','Accept':'application/json,application/zip'})
    limit=native.MAX_ARCHIVE if url==native.CSLT_URL else 4*1024*1024
    for attempt in range(3):
        if deadline is not None and time.monotonic()>=deadline:raise TimeoutError('foreign-flow acquisition budget')
        if key:
            with _lock:
                time.sleep(max(0,1-(time.monotonic()-_last)));_last=time.monotonic()
        try:
            with urllib.request.build_opener().open(request,timeout=25) as response:raw=response.read(limit+1)
            break
        except urllib.error.HTTPError as exc:
            if attempt==2 or exc.code not in (429,500,502,503,504):raise
            time.sleep(2*(attempt+1))
    if not raw or len(raw)>limit:raise ValueError('foreign-flow response bound')
    if key and len(key)>=12 and key.encode() in raw:raise ValueError('credential echoed')
    stamp=now();receipt=evidence_store.capture(client,bucket,'tic',url,raw,native.clock(stamp))
    return {'url':url,'acquired_at':stamp,'evidence':receipt}


def collect(client,bucket,fred_key,read,deadline):
    stamp=now();vintage=native.clock(stamp).date().isoformat();results={};errors={}
    def get(name,url,key=None):
        try:results[name]=acquire(client,bucket,url,key,deadline)
        except Exception as exc:errors[name]='HTTP_'+str(exc.code) if isinstance(exc,urllib.error.HTTPError) else type(exc).__name__
    get('bulk',native.CSLT_URL)
    if 'bulk' not in results:return results,errors,vintage
    get('table5',native.TABLE_URL)
    get('mspd',native.MSPD_URL)
    for name,url in native.AUCTION_URLS.items():get(name,url)
    if fred_key:
        for name,url in native.calendar_urls(stamp).items():get(name,url,fred_key)
    else:errors['release_calendar']='configured_credential_unavailable'
    return results,errors,vintage


def publish(client,bucket,packet):
    for _ in range(5):
        try:
            obj=client.get_object(Bucket=bucket,Key=model.CURRENT);old=json.loads(bounded(obj['Body']))
            if old.get('contract')==model.CONTRACT:
                if native.clock(old['generated_at'])>native.clock(packet['generated_at']):return False
                if old['generated_at']==packet['generated_at'] and old!=packet:raise ValueError('same-clock conflicting foreign output')
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
    raise RuntimeError('foreign publication contention bound')

def replay(manifest,read):
    if manifest.get('contract')!='foreign-original-replay.v1' or set(manifest.get('compilers',{}))!={m.__name__ for m in COMPILERS}:raise ValueError('foreign replay contract differs')
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
    if output!=reference or model.digest(output)!=manifest['output_sha256']:raise ValueError('foreign original replay differs')
    load('output')
    for key,body in histories.items():
        if read(key)!=body:raise ValueError('foreign history shard differs')
    return output

def run(client,bucket,fred_key,context=None):
    remaining=context.get_remaining_time_in_millis()/1000 if context and hasattr(context,'get_remaining_time_in_millis') else 600
    deadline=time.monotonic()+max(10,min(240,remaining-180))
    read=raw_reader(client,bucket);legacy=preserve(client,bucket,read)
    originals,errors,vintage=collect(client,bucket,fred_key,read,deadline);stamp=now()
    inputs={'contract':'foreign-original-inputs.v1','originals':originals,'acquisition_errors':errors,'requested_fred_vintage':vintage,'legacy':legacy}
    body=model.encoded(inputs);sha=model.digest(inputs);key=model.PREFIX+'inputs/'+sha+'.json'
    immutable(client,bucket,key,body);refs={'input':{'key':key,'sha256':sha,'bytes':len(body)}}
    compilers={}
    for module in COMPILERS:
        body=Path(module.__file__).read_bytes();sha=hashlib.sha256(body).hexdigest();key=model.PREFIX+'compilers/'+sha+'.py'
        immutable(client,bucket,key,body,'text/x-python');compilers[module.__name__]={'key':key,'sha256':sha}
    try:output,histories=model.build(inputs,read,stamp)
    except Exception as exc:
        # Failed acquisitions remain navigable and reproducible; never replace
        # the last verified publication with a partial or empty candidate.
        failed={'contract':'foreign-failed-attempt.v1','generated_at':stamp,'input':refs['input'],'compilers':compilers,
            'source_status_codes':errors,'failure_class':type(exc).__name__,'published':False}
        key=model.PREFIX+'attempts/'+model.digest(failed)+'.json';immutable(client,bucket,key,model.encoded(failed))
        print('[foreign-research] '+json.dumps({'failed_attempt':key,'source_status_codes':errors,'failure_class':type(exc).__name__}))
        raise
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures=[pool.submit(immutable,client,bucket,key,body) for key,body in histories.items()]
        for future in as_completed(futures):future.result()
    body=model.encoded(output);sha=model.digest(output);key=model.PREFIX+'outputs/'+sha+'.json'
    immutable(client,bucket,key,body);refs['output']={'key':key,'sha256':sha,'bytes':len(body)}
    manifest={'contract':'foreign-original-replay.v1','generated_at':stamp,'compilers':compilers,**refs,'output_sha256':sha}
    key=model.PREFIX+'runs/'+model.digest(manifest)+'.json';immutable(client,bucket,key,model.encoded(manifest))
    if replay(manifest,read)!=output:raise ValueError('pre-publication replay differs')
    ref={'manifest_key':key,'output_sha256':sha};published=publish(client,bucket,{**output,'replay':ref})
    return {'published':published,'generated_at':stamp,'replay':ref,'quality':output['quality'],'source_status_codes':output['source_status_codes'],
        'history_shards':len(histories),'signals_emitted':0,'paid_ai_calls':0,'notifications_sent':0,'private_account_reads':0,'portfolio_writes':0}
