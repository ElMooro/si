"""Capture ECB originals, bind canonical FRED responses and publish replayed CB research."""
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import gzip
import hashlib
import io
import json
from pathlib import Path
import re
import time
import urllib.error
import urllib.request
import canonical_macro_sources
import cb_native
import cb_research as model
import evidence_store
import pd_fails_context
import report_observations
import research_brief_model

CURRENT='data/cb-injection.json'
PREFIX='data/cb-research/'
MAX_BYTES=32*1024*1024
COMPILERS=(model,cb_native,canonical_macro_sources,pd_fails_context,report_observations,research_brief_model)


def code(exc):return str(getattr(exc,'response',{}).get('Error',{}).get('Code',''))
def missing(exc):return code(exc) in ('404','NoSuchKey')
def conflict(exc):return code(exc) in ('412','PreconditionFailed','409','ConditionalRequestConflict')
def now():return datetime.now(timezone.utc).isoformat()


def bounded(body):
    try:raw=body.read(MAX_BYTES+1)
    finally:body.close()
    if len(raw)>MAX_BYTES:raise ValueError('public evidence exceeds bound')
    return raw


def raw_reader(client,bucket):
    def read(key):
        if not isinstance(key,str) or not re.fullmatch(r'data/[A-Za-z0-9_./-]+',key) or '..' in key:
            raise ValueError('public evidence path required')
        raw=bounded(client.get_object(Bucket=bucket,Key=key)['Body'])
        if key.endswith('.gz'):raw=bounded(gzip.GzipFile(fileobj=io.BytesIO(raw)))
        return raw
    return read


def immutable(client,bucket,key,raw,kind='application/json'):
    try:client.put_object(Bucket=bucket,Key=key,Body=raw,ContentType=kind,IfNoneMatch='*')
    except Exception as exc:
        if not conflict(exc):raise
    if raw_reader(client,bucket)(key)!=raw:raise ValueError('immutable research differs')


def publish(client,bucket,key,packet):
    for _ in range(4):
        try:
            obj=client.get_object(Bucket=bucket,Key=key);etag=obj['ETag'];raw=bounded(obj['Body'])
            try:old=json.loads(raw)
            except (ValueError,UnicodeDecodeError):old={}
            if not isinstance(old,dict):old={}
        except Exception as exc:
            if not missing(exc):raise
            old={};etag=None
        if old.get('contract')==model.CONTRACT:
            if model.clock(old['generated_at'])>=model.clock(packet['generated_at']):return False
            if model.clock(old['source_generated_at'])>model.clock(packet['source_generated_at']):return False
        try:
            client.put_object(Bucket=bucket,Key=key,Body=model.encoded(packet),ContentType='application/json',CacheControl='no-store',
                **({'IfMatch':etag} if etag else {'IfNoneMatch':'*'}))
            return True
        except Exception as exc:
            if not conflict(exc):raise
    raise RuntimeError('publication conflict retry limit')


def preserve(client,bucket,read):
    refs={}
    for key in (CURRENT,):
        try:raw=read(key)
        except Exception as exc:
            if missing(exc):continue
            raise
        try:old=json.loads(raw)
        except (ValueError,UnicodeDecodeError):old=None
        if isinstance(old,dict) and old.get('contract')==model.CONTRACT:
            refs.update(old.get('legacy_context') or {});continue
        sha=hashlib.sha256(raw).hexdigest();dest=PREFIX+'legacy-unvalidated/'+sha+('.json' if old is not None else '.bin')
        immutable(client,bucket,dest,raw,'application/json' if old is not None else 'application/octet-stream')
        refs[key]={'key':dest,'sha256':sha,'bytes':len(raw),'status':'UNQUALIFIED_LEGACY'}
    return refs


def acquire_ecb(client,bucket,name,read):
    url='https://data-api.ecb.europa.eu/service/data/'+cb_native.ECB[name]
    cache_key=PREFIX+'ecb-cache/'+name+'.json';cached=None;etag=None
    try:
        obj=client.get_object(Bucket=bucket,Key=cache_key);etag=obj['ETag'];cached=json.loads(bounded(obj['Body']))
    except Exception as exc:
        if not missing(exc):raise
    def retained(descriptor):
        body=read(descriptor['evidence']['key'])
        cb_native.ecb_input(name,body,descriptor['evidence'],descriptor['acquired_at'])
        return {**descriptor,'raw':body}
    if cached:
        age=(model.clock(now())-model.clock(cached['acquired_at'])).total_seconds()
        if 0<=age<=3600:return retained(cached),None
    try:
        # A new opener avoids legacy urlopen shims that manufacture provider-shaped JSON.
        opener=urllib.request.build_opener()
        for attempt in range(3):
            try:
                request=urllib.request.Request(url,headers={'User-Agent':'JustHodl source research','Accept':'text/csv'})
                with opener.open(request,timeout=25) as response:raw=response.read(4*1024*1024+1)
                if len(raw)>4*1024*1024:raise ValueError('ECB response exceeds explicit capture bound')
                break
            except urllib.error.HTTPError as exc:
                if exc.code not in (429,500,502,503,504) or attempt==2:raise
                time.sleep(2*(attempt+1))
        stamp=now();receipt=evidence_store.capture(client,bucket,'ecb',url,raw)
        descriptor={'acquired_at':stamp,'evidence':receipt}
        result=retained(descriptor)
        try:client.put_object(Bucket=bucket,Key=cache_key,Body=model.encoded(descriptor),ContentType='application/json',
            **({'IfMatch':etag} if etag else {'IfNoneMatch':'*'}))
        except Exception as exc:
            if not conflict(exc):raise
        return result,None
    except Exception as exc:
        label='HTTP_'+str(exc.code) if isinstance(exc,urllib.error.HTTPError) else type(exc).__name__
        if cached:return retained(cached),label
        raise


def original_inputs(inputs,read):
    fred=canonical_macro_sources.originals(inputs['source'],read,cb_native.POLICY)
    ecb={name:{**descriptor,'raw':read(descriptor['evidence']['key'])} for name,descriptor in inputs['ecb'].items()}
    return fred,ecb


def run(client,bucket):
    read=raw_reader(client,bucket);source=json.loads(read('data/report-measurements.json'))
    fred=canonical_macro_sources.originals(source,read,cb_native.POLICY)
    legacy=preserve(client,bucket,read);errors={};ecb={}
    with ThreadPoolExecutor(max_workers=3) as pool:
        futures={pool.submit(acquire_ecb,client,bucket,name,read):name for name in cb_native.ECB}
        for future in as_completed(futures):
            name=futures[future]
            try:
                ecb[name],error=future.result()
                if error:errors[name]=error
            except Exception as exc:errors[name]='HTTP_'+str(exc.code) if isinstance(exc,urllib.error.HTTPError) else type(exc).__name__
    try:fails=json.loads(read('data/settlement-fails.json'))
    except Exception as exc:
        if not missing(exc):raise
        fails={};errors['settlement_fails']='source_missing'
    stamp=now();out=model.build(source,fred,ecb,fails,stamp,legacy,errors)
    inputs={'source':source,'ecb':{k:{x:v for x,v in row.items() if x!='raw'} for k,row in ecb.items()},
        'fails_context':fails,'legacy_context':legacy,'acquisition_errors':errors}
    raw=model.encoded(inputs);input_key=PREFIX+'inputs/'+model.digest(inputs)+'.json';immutable(client,bucket,input_key,raw)
    compilers={}
    for module in COMPILERS:
        body=Path(module.__file__).read_bytes();sha=hashlib.sha256(body).hexdigest();key=PREFIX+'compilers/'+sha+'.py'
        immutable(client,bucket,key,body,'text/plain');compilers[module.__name__]={'key':key,'sha256':sha}
    output_raw=model.encoded(out);output_sha=model.digest(out);output_key=PREFIX+'outputs/'+output_sha+'.json'
    immutable(client,bucket,output_key,output_raw)
    manifest={'contract':'cb-replay.v1','generated_at':stamp,'compilers':compilers,
        'input':{'key':input_key,'sha256':model.digest(inputs),'bytes':len(raw)},'output_sha256':output_sha,
        'output':{'key':output_key,'sha256':output_sha,'bytes':len(output_raw)}}
    key=PREFIX+'runs/'+model.digest(manifest)+'.json';immutable(client,bucket,key,model.encoded(manifest))
    retained=json.loads(read(input_key));r_fred,r_ecb=original_inputs(retained,read)
    rebuilt=model.build(retained['source'],r_fred,r_ecb,retained['fails_context'],stamp,retained['legacy_context'],retained['acquisition_errors'])
    if rebuilt!=out:raise ValueError('original source replay differs')
    out['replay']={'manifest_key':key,'output_sha256':output_sha,'compilers':compilers}
    published=publish(client,bucket,CURRENT,out)
    if published:
        archive={**out,'date':stamp[:10],'impulse':None,'impulse_label':'NOT_ATTRIBUTED','unwind_risk':None,'carry_conditions':'NOT_CALIBRATED'}
        for family in ('snapshots','measurements'):
            publish(client,bucket,'data/cb-injection/'+family+'/'+stamp[:10]+'.json',archive)
    return {'published':published,'generated_at':stamp,'replay':out['replay'],'quality':out['quality'],
            'paid_ai_calls':0,'notifications_sent':0,'private_account_reads':0,'portfolio_writes':0}
