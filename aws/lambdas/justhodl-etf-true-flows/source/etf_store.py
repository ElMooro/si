"""Retain and replay complete issuer evidence before conditional publication."""
from concurrent.futures import ThreadPoolExecutor,as_completed
from datetime import datetime,timezone
from pathlib import Path
from urllib.parse import urlencode
import gzip,hashlib,io,json,re,threading,time,urllib.request,urllib.error
import etf_native as native,etf_research as model,etf_universe,evidence_store

PRIVATE='audit-private/20260909-originals/etf-research/'
COMPILERS=(native,model,etf_universe,evidence_store)
MAX_BYTES=64*1024*1024
_lock=threading.Lock();_last=0.0

def now():return datetime.now(timezone.utc).isoformat()
def code(exc):return str((getattr(exc,'response',{}) or {}).get('Error',{}).get('Code',''))
def missing(exc):return code(exc) in ('404','NoSuchKey')
def conflict(exc):return code(exc) in ('409','412','ConditionalRequestConflict','PreconditionFailed')
def bounded(body,limit=MAX_BYTES):
    try:raw=body.read(limit+1)
    finally:body.close()
    if len(raw)>limit:raise ValueError('ETF artifact size bound')
    return raw

def raw_reader(client,bucket):
    def read(key):
        if not isinstance(key,str) or not re.fullmatch(r'data/[A-Za-z0-9_./-]+',key) or '..' in key:raise ValueError('public evidence path required')
        raw=bounded(client.get_object(Bucket=bucket,Key=key)['Body'])
        return bounded(gzip.GzipFile(fileobj=io.BytesIO(raw))) if key.endswith('.gz') else raw
    return read

def immutable(client,bucket,key,raw,kind='application/json'):
    if not(key.startswith(model.PREFIX) or key.startswith(PRIVATE)):raise ValueError('ETF retention prefix differs')
    try:client.put_object(Bucket=bucket,Key=key,Body=raw,ContentType=kind,IfNoneMatch='*',
        CacheControl='no-store' if key.startswith(PRIVATE) else 'public, max-age=31536000, immutable')
    except Exception as exc:
        if not conflict(exc):raise
    if bounded(client.get_object(Bucket=bucket,Key=key)['Body'])!=raw:raise ValueError('retained ETF bytes differ')

def preserve(client,bucket,read):
    key=model.PREFIX+'migration.json'
    try:marker=json.loads(read(key))
    except Exception as exc:
        if not missing(exc):raise
        objects=[]
        for source in (model.CURRENT,'data/etf-shares-history.json','data/etf-shares-snapshots/latest.json'):
            try:raw=read(source)
            except Exception as error:
                if source!=model.CURRENT and missing(error):objects.append({'source':source,'status':'absent'});continue
                raise
            packet=json.loads(raw)
            if source==model.CURRENT and packet.get('contract')==model.CONTRACT:raise ValueError('legacy retention marker missing')
            sha=hashlib.sha256(raw).hexdigest();immutable(client,bucket,PRIVATE+sha+'.bin',raw,'application/octet-stream')
            objects.append({'source':source,'sha256':sha,'bytes':len(raw),'protected_backup':True})
        marker={'contract':'etf-legacy-preservation.v1','objects':objects,'status':'Complete preceding packet and legacy snapshot histories preserved. Legacy source values are not promoted to verified issuance evidence.'}
        immutable(client,bucket,key,model.encoded(marker))
    if marker.get('contract')!='etf-legacy-preservation.v1' or not any(v.get('source')==model.CURRENT and v.get('protected_backup') is True for v in marker.get('objects',[])):raise ValueError('ETF legacy marker differs')
    return marker

def acquire(client,bucket,url,key=None,deadline=None):
    global _last
    actual=url+('&'+urlencode({'apikey':key}) if key else '')
    request=urllib.request.Request(actual,headers={'User-Agent':'JustHodl original ETF research (ops@justhodl.ai)'})
    for attempt in range(3):
        if deadline is not None and time.monotonic()>=deadline:raise TimeoutError('ETF source acquisition budget')
        if key:
            with _lock:
                time.sleep(max(0,.5-(time.monotonic()-_last)));_last=time.monotonic()
        try:
            with urllib.request.build_opener().open(request,timeout=30) as response:raw=response.read(MAX_BYTES+1)
            break
        except urllib.error.HTTPError as exc:
            if attempt==2 or exc.code not in (429,500,502,503,504):raise
            time.sleep(2*(attempt+1))
    if not 0<len(raw)<=MAX_BYTES:raise ValueError('ETF source response bound')
    if key and len(key)>=12 and key.encode() in raw:raise ValueError('credential echoed')
    stamp=now();ref={'url':url,'acquired_at':stamp,'evidence':evidence_store.capture(client,bucket,'etf_original',url,raw,native.clock(stamp))}
    return ref

def collect(client,bucket,key,read,deadline):
    originals={};errors={};universe=sorted({t for members in etf_universe.ETFS.values() for t in members})
    for label,url in (('ishares_catalog',native.ISHARES_URL),('ssga_catalog',native.SSGA_URL),('proshares_splits',native.SPLIT_URL)):
        try:originals[label]=acquire(client,bucket,url,deadline=deadline)
        except Exception as exc:errors[label]='HTTP_'+str(exc.code) if isinstance(exc,urllib.error.HTTPError) else type(exc).__name__
    jobs=[]
    for label,parser in (('ishares_catalog',native.ishares_catalog),('ssga_catalog',native.ssga_catalog)):
        try:
            catalog=parser(read(originals[label]['evidence']['key']),universe)
            for ticker,identity in catalog.items():
                name=('ishares_' if label=='ishares_catalog' else 'ssga_')+ticker
                url=native.DOWNLOAD.format(pid=identity['portfolio_id']) if label=='ishares_catalog' else identity['history_url']
                jobs.append((name,url,None))
        except Exception as exc:errors[label+'_identity']=type(exc).__name__
    jobs.extend(('proshares_'+ticker,native.PRO_URL.format(ticker=ticker),None) for ticker in etf_universe.PROSHARES)
    if key:
        for ticker in universe:
            for label,endpoint in (('info','etf/info'),('shares','shares-float'),('quote','quote')):
                jobs.append(('vendor_'+ticker+'_'+label,'https://financialmodelingprep.com/stable/'+endpoint+'?symbol='+ticker,key))
    else:errors['vendor_context']='configured_vendor_credential_unavailable'
    def fetch(job):
        label,url,secret=job
        return label,acquire(client,bucket,url,secret,deadline)
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures={pool.submit(fetch,job):job[0] for job in jobs}
        for future in as_completed(futures):
            label=futures[future]
            try:_,ref=future.result();originals[label]=ref
            except Exception as exc:errors[label]='HTTP_'+str(exc.code) if isinstance(exc,urllib.error.HTTPError) else type(exc).__name__
    return originals,errors

def publish(client,bucket,packet):
    for _ in range(5):
        try:
            obj=client.get_object(Bucket=bucket,Key=model.CURRENT);old=json.loads(bounded(obj['Body']))
            if old.get('contract')==model.CONTRACT:
                if native.clock(old['generated_at'])>native.clock(packet['generated_at']) or old['reference_calendar']['latest_date']>packet['reference_calendar']['latest_date']:return False
                if old['generated_at']==packet['generated_at'] and old!=packet:raise ValueError('same-clock conflicting ETF output')
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
    raise RuntimeError('ETF publication contention bound')

def replay(manifest,read):
    if manifest.get('contract')!='etf-original-replay.v1' or set(manifest.get('compilers',{}))!={m.__name__ for m in COMPILERS}:raise ValueError('ETF replay contract differs')
    for module in COMPILERS:
        raw=Path(module.__file__).read_bytes();sha=hashlib.sha256(raw).hexdigest();ref=manifest['compilers'][module.__name__]
        if ref!={'key':model.PREFIX+'compilers/'+sha+'.py','sha256':sha} or read(ref['key'])!=raw:raise ValueError('reviewed compiler differs; use matching checkout')
    def load(name):
        ref=manifest[name];body=read(ref['key'])
        if ref['key']!=model.PREFIX+name+'s/'+ref['sha256']+'.json' or len(body)!=ref['bytes'] or hashlib.sha256(body).hexdigest()!=ref['sha256']:raise ValueError('retained '+name+' differs')
        return json.loads(body)
    output,histories=model.build(load('input'),read,manifest['generated_at'])
    if output!=load('output') or model.digest(output)!=manifest['output_sha256']:raise ValueError('ETF original replay differs')
    for key,body in histories.items():
        if read(key)!=body:raise ValueError('ETF source history differs')
    return output

def run(client,bucket,key,context=None):
    remaining=context.get_remaining_time_in_millis()/1000 if context and hasattr(context,'get_remaining_time_in_millis') else 900
    deadline=time.monotonic()+max(10,min(570,remaining-260));read=raw_reader(client,bucket);legacy=preserve(client,bucket,read)
    originals,errors=collect(client,bucket,key,read,deadline);stamp=now()
    inputs={'contract':'etf-original-inputs.v1','originals':originals,'acquisition_errors':errors,'legacy':legacy}
    body=model.encoded(inputs);sha=model.digest(inputs);key=model.PREFIX+'inputs/'+sha+'.json';immutable(client,bucket,key,body)
    refs={'input':{'key':key,'sha256':sha,'bytes':len(body)}};compilers={}
    for module in COMPILERS:
        body=Path(module.__file__).read_bytes();sha=hashlib.sha256(body).hexdigest();key=model.PREFIX+'compilers/'+sha+'.py'
        immutable(client,bucket,key,body,'text/x-python');compilers[module.__name__]={'key':key,'sha256':sha}
    try:output,histories=model.build(inputs,read,stamp)
    except Exception as exc:
        failed={'contract':'etf-failed-attempt.v1','generated_at':stamp,'input':refs['input'],'compilers':compilers,'source_status_codes':errors,'failure_class':type(exc).__name__,'published':False}
        key=model.PREFIX+'attempts/'+model.digest(failed)+'.json';immutable(client,bucket,key,model.encoded(failed))
        print('[etf-research] '+json.dumps({'failed_attempt':key,'failure_class':type(exc).__name__,'source_status_codes':errors}));raise
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures=[pool.submit(immutable,client,bucket,key,body) for key,body in histories.items()]
        for future in as_completed(futures):future.result()
    count=len(histories);del histories
    body=model.encoded(output);sha=model.digest(output);key=model.PREFIX+'outputs/'+sha+'.json';immutable(client,bucket,key,body);refs['output']={'key':key,'sha256':sha,'bytes':len(body)}
    manifest={'contract':'etf-original-replay.v1','generated_at':stamp,'compilers':compilers,**refs,'output_sha256':sha};key=model.PREFIX+'runs/'+model.digest(manifest)+'.json';immutable(client,bucket,key,model.encoded(manifest))
    if replay(manifest,read)!=output:raise ValueError('ETF pre-publication replay differs')
    ref={'manifest_key':key,'output_sha256':sha};published=publish(client,bucket,{**output,'replay':ref})
    return {'published':published,'generated_at':stamp,'replay':ref,'quality':output['quality'],'source_status_codes':output['source_status_codes'],
        'history_shards':count,'signals_emitted':0,'paid_ai_calls':0,'notifications_sent':0,'private_account_reads':0,'portfolio_writes':0}
