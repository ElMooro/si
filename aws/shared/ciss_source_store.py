"""Retain ECB CSVs, replay the compiled packet, publish with bounded CAS."""
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import gzip
import hashlib
import io
import json
from pathlib import Path
import time
import urllib.error
import urllib.request

import ciss_source_model as model
from ciss_source_model import encoded, digest, clock, PREFIX
from evidence_store import capture, public_source_url

MAX_BYTES=64*1024*1024
CURRENT='data/ciss-stress.json'
BASE='https://data-api.ecb.europa.eu/service/data/'


def code(exc):return str(getattr(exc,'response',{}).get('Error',{}).get('Code',''))


def read(client,bucket,key):
    obj=client.get_object(Bucket=bucket,Key=key)
    raw=obj['Body'].read(MAX_BYTES+1)
    if len(raw)>MAX_BYTES:raise ValueError('object exceeds bound')
    return json.loads(raw),raw,obj['ETag']


def missing(exc):return code(exc) in ('404','NoSuchKey')


def conflict(exc):return code(exc) in ('409','ConditionalRequestConflict','412','PreconditionFailed')


def immutable(client,bucket,key,raw,content_type='application/json'):
    try:client.put_object(Bucket=bucket,Key=key,Body=raw,ContentType=content_type,IfNoneMatch='*')
    except Exception as exc:
        if not conflict(exc):raise
    retained=client.get_object(Bucket=bucket,Key=key)['Body'].read(len(raw)+1)
    if retained!=raw:raise ValueError('immutable readback differs')


def original(client,bucket,receipt):
    if receipt.get('contract')!='source-evidence.v1' or receipt.get('provider')!='ecb' or receipt.get('captured') is not True:
        raise ValueError('ECB evidence required')
    request_sha=hashlib.sha256(receipt['source_url'].encode()).hexdigest()
    if receipt['key']!='data/evidence/ecb/'+request_sha+'/'+receipt['sha256']+'.bin.gz':raise ValueError('evidence key differs')
    obj=client.get_object(Bucket=bucket,Key=receipt['key'])
    zipped=obj['Body'].read(MAX_BYTES+1)
    if len(zipped)>MAX_BYTES:raise ValueError('compressed source exceeds bound')
    with gzip.GzipFile(fileobj=io.BytesIO(zipped)) as stream:raw=stream.read(MAX_BYTES+1)
    if len(raw)>MAX_BYTES or len(raw)!=receipt['bytes'] or hashlib.sha256(raw).hexdigest()!=receipt['sha256']:
        raise ValueError('ECB original bytes differ')
    return raw


def request(url,deadline):
    for attempt in range(3):
        remaining=deadline-time.monotonic()
        if remaining<3:raise TimeoutError('ECB collection budget exhausted')
        try:
            req=urllib.request.Request(url,headers={'User-Agent':'JustHodl-SourceEvidence/1.0','Accept':'text/csv'})
            with urllib.request.build_opener().open(req,timeout=min(40,remaining)) as response:
                raw=response.read(MAX_BYTES+1)
                if not raw or len(raw)>MAX_BYTES:raise ValueError('ECB response size invalid')
                return raw,datetime.now(timezone.utc)
        except urllib.error.HTTPError as exc:
            if exc.code not in (429,500,502,503,504) or attempt==2:raise
        except (TimeoutError,urllib.error.URLError):
            if attempt==2:raise
        delay=2*(attempt+1)
        if time.monotonic()+delay>=deadline:raise TimeoutError('ECB collection budget exhausted')
        time.sleep(delay)


def acquire(client,bucket,url,deadline):
    cache_key=PREFIX+'cache/'+hashlib.sha256(url.encode()).hexdigest()+'.json'
    try:cached,_,etag=read(client,bucket,cache_key)
    except Exception as exc:
        if not missing(exc):raise
        cached,etag=None,None
    now=datetime.now(timezone.utc)
    if cached and cached.get('request_url')==url and cached['evidence']['source_url']==public_source_url(url) and 0<=(now-clock(cached['acquired_at'])).total_seconds()<=3600:
        return {**cached,'raw':original(client,bucket,cached['evidence'])}
    raw,received=request(url,deadline)
    # Reject an HTML error page before committing a source-cache record.
    model.csv_series(raw)
    receipt=capture(client,bucket,'ecb',url,raw,received)
    if original(client,bucket,receipt)!=raw:raise ValueError('source readback differs')
    descriptor={'evidence':receipt,'acquired_at':received.isoformat(),'request_url':url}
    # Cache updates cannot regress a newer acquisition during concurrent runs.
    try:
        client.put_object(Bucket=bucket,Key=cache_key,Body=encoded(descriptor),ContentType='application/json',
            **({'IfMatch':etag} if etag else {'IfNoneMatch':'*'}))
    except Exception as exc:
        if not conflict(exc):raise
    return {**descriptor,'raw':raw}


def safe_error(exc):
    return 'HTTP_'+str(exc.code) if isinstance(exc,urllib.error.HTTPError) else type(exc).__name__


def collect(client,bucket,budget_seconds):
    started=datetime.now(timezone.utc).isoformat();deadline=time.monotonic()+budget_seconds
    discoveries,histories,errors={},{},{}
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures={pool.submit(acquire,client,bucket,BASE+flow+'?format=csvdata&lastNObservations=1',deadline):flow for flow in ('CISS','CLIFS')}
        for future in as_completed(futures):
            flow=futures[future]
            try:discoveries[flow]=future.result()
            except Exception as exc:errors[flow]=safe_error(exc)
    universe=set()
    for item in discoveries.values():universe.update(model.csv_series(item['raw']))
    def priority(key):
        return (0 if key==model.HEAD else 1 if key.startswith('CISS.D.U2') else 2 if key.startswith(('CISS.D.DE','CISS.D.ES','CLIFS.M.AT')) else 3,key)
    with ThreadPoolExecutor(max_workers=3) as pool:
        futures={pool.submit(acquire,client,bucket,BASE+key.replace('.', '/',1)+'?format=csvdata',deadline):key for key in sorted(universe,key=priority)}
        for future in as_completed(futures):
            key=futures[future]
            try:histories[key]=future.result()
            except Exception as exc:errors[key]=safe_error(exc)
    return started,discoveries,histories,errors


def frozen_compiler(client,bucket):
    body=Path(model.__file__).read_bytes();sha=hashlib.sha256(body).hexdigest()
    key=PREFIX+'compilers/'+sha+'.py';immutable(client,bucket,key,body,'text/plain')
    return {'key':key,'sha256':sha}


def hydrate(client,bucket,descriptors):
    out={}
    for key,item in descriptors.items():
        expected=BASE+key.replace('.', '/',1)+'?format=csvdata' if '.' in key else BASE+key+'?format=csvdata&lastNObservations=1'
        if item.get('request_url')!=expected or item['evidence']['source_url']!=public_source_url(expected):raise ValueError('ECB request identity differs')
        out[key]={**item,'raw':original(client,bucket,item['evidence'])}
    return out


def run(client,bucket,budget_seconds=480):
    started,discoveries,histories,errors=collect(client,bucket,budget_seconds)
    generated=datetime.now(timezone.utc).isoformat()
    output=model.build(discoveries,histories,generated,errors)
    output['collection_started_at']=started
    compiler=frozen_compiler(client,bucket)
    references=lambda entries:{key:{k:v for k,v in row.items() if k!='raw'} for key,row in entries.items()}
    manifest={'contract':'ciss-research-replay.v1','generated_at':generated,'collection_started_at':started,
        'discoveries':references(discoveries),'histories':references(histories),'errors':errors,
        'compiler':compiler,'output_sha256':digest(output)}
    manifest_key=PREFIX+'runs/'+digest(manifest)+'.json'
    immutable(client,bucket,manifest_key,encoded(manifest))
    # Re-read every stored original, not just the normalized input snapshot.
    reproduced=model.build(hydrate(client,bucket,manifest['discoveries']),hydrate(client,bucket,manifest['histories']),generated,errors)
    reproduced['collection_started_at']=started
    if digest(reproduced)!=manifest['output_sha256']:raise ValueError('CISS replay differs before publication')
    output['replay']={'manifest_key':manifest_key,'output_sha256':manifest['output_sha256'],'compiler':compiler}
    for _ in range(4):
        try:previous,old,etag=read(client,bucket,CURRENT)
        except Exception as exc:
            if not missing(exc):raise
            previous,old,etag=None,None,None
        if previous:
            previous_start=previous.get('collection_started_at',previous.get('generated_at'))
            if previous_start and clock(previous_start)>clock(started):return {'published':False,'reason':'newer collection already current'}
            if previous.get('contract')!=model.CONTRACT:
                immutable(client,bucket,PREFIX+'legacy-unvalidated/'+hashlib.sha256(old).hexdigest()+'.json',old)
        try:
            client.put_object(Bucket=bucket,Key=CURRENT,Body=encoded(output),ContentType='application/json',CacheControl='no-cache',
                **({'IfMatch':etag} if etag else {'IfNoneMatch':'*'}))
            return {'published':True,'generated_at':generated,'quality':output['quality'],'coverage':output['coverage'],'replay':output['replay']}
        except Exception as exc:
            if not conflict(exc):raise
    raise RuntimeError('CISS publication retry bound exhausted')


def run_commentary(client,bucket):
    source,_,_=read(client,bucket,CURRENT)
    source_ref=source.get('replay') or {}
    source_manifest,_,_=read(client,bucket,source_ref['manifest_key'])
    if source_ref['manifest_key']!=PREFIX+'runs/'+digest(source_manifest)+'.json':raise ValueError('source manifest identity differs')
    compiler=frozen_compiler(client,bucket)
    if source_manifest['compiler']!=compiler or source_ref['compiler']!=compiler:
        raise ValueError('source compiler differs from reviewed runtime')
    if source_ref['output_sha256']!=source_manifest['output_sha256'] or digest({k:v for k,v in source.items() if k!='replay'})!=source_manifest['output_sha256']:
        raise ValueError('source packet differs from retained run')
    stamp=datetime.now(timezone.utc).isoformat()
    out=model.commentary(source,stamp)
    input_key=PREFIX+'commentary/inputs/'+digest(source)+'.json'
    immutable(client,bucket,input_key,encoded(source))
    manifest={'contract':'ciss-commentary-replay.v1','generated_at':stamp,'compiler':compiler,
        'input':{'key':input_key,'sha256':digest(source)},'source_replay':source_ref,'output_sha256':digest(out)}
    key=PREFIX+'commentary/runs/'+digest(manifest)+'.json'
    immutable(client,bucket,key,encoded(manifest))
    retained,_,_=read(client,bucket,input_key)
    if digest(model.commentary(retained,stamp))!=manifest['output_sha256']:raise ValueError('commentary replay differs')
    out['replay']={'manifest_key':key,'output_sha256':manifest['output_sha256'],'compiler':compiler}
    current='data/ciss-ai.json'
    for _ in range(4):
        try:previous,old,etag=read(client,bucket,current)
        except Exception as exc:
            if not missing(exc):raise
            previous,old,etag=None,None,None
        if previous:
            if clock(previous['generated_at'])>clock(stamp):return {'published':False,'reason':'newer commentary already current'}
            if previous.get('source_generated_at') and clock(previous['source_generated_at'])>clock(source['generated_at']):
                return {'published':False,'reason':'newer source already current'}
            if previous.get('contract')!=out['contract']:
                immutable(client,bucket,PREFIX+'legacy-unvalidated/'+hashlib.sha256(old).hexdigest()+'.json',old)
        try:
            client.put_object(Bucket=bucket,Key=current,Body=encoded(out),ContentType='application/json',CacheControl='no-cache',
                **({'IfMatch':etag} if etag else {'IfNoneMatch':'*'}))
            return {'published':True,'generated_at':stamp,'quality':out['quality'],'replay':out['replay']}
        except Exception as exc:
            if not conflict(exc):raise
    raise RuntimeError('CISS commentary publication retry bound exhausted')
