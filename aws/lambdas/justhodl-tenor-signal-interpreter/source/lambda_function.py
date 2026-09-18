"""Treasury tenor research: retained official inputs and deterministic replay.

Legacy signal keys remain aliases of descriptive measurements. No policy-path,
QE, offshore-dollar, allocation or alert authority is emitted. No paid AI.
"""
import hashlib
import json
import os
from pathlib import Path
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone, timedelta
import boto3
from managed_secret import managed_secret
from evidence_store import capture, read_verified
from tenor_research_model import compile_research, CONTRACT
import tenor_research_model
import treasury_instruments

S3=boto3.client('s3',region_name='us-east-1')
BUCKET=os.environ.get('S3_BUCKET','justhodl-dashboard-live')
OUTPUT_KEY='data/auction-tenor-signals.json'
FISCAL_BASE='https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/od/auctions_query'
MAX_BYTES=8*1024*1024


def canonical(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()


def request(url, provider):
    req=urllib.request.Request(url,headers={'User-Agent':'JustHodl-Tenor-Research/2.0','Accept':'application/json'})
    with urllib.request.urlopen(req,timeout=25) as response:raw=response.read(MAX_BYTES+1)
    if len(raw)>MAX_BYTES:raise ValueError('source exceeds declared byte bound')
    doc=json.loads(raw)
    evidence=capture(S3,BUCKET,provider,url,raw)
    return doc,evidence


def fiscal_pages(as_of):
    start=(datetime.fromisoformat(as_of)-timedelta(days=180)).date().isoformat()
    pages=[];refs=[];expected=None;expected_count=None
    for page in range(1,11):
        params={'filter':f'auction_date:gte:{start},auction_date:lte:{as_of}',
                'sort':'-auction_date,cusip','format':'json','page[size]':1000,'page[number]':page}
        doc,ref=request(FISCAL_BASE+'?'+urllib.parse.urlencode(params,safe=':,[]'),'treasury_fiscaldata')
        meta=doc.get('meta') or {}
        total=int(meta.get('total-pages',0));count=int(meta.get('total-count',-1))
        if not 1<=total<=10 or count<0 or not isinstance(doc.get('data'),list):raise ValueError('incomplete FiscalData pagination contract')
        if expected is not None and (total!=expected or count!=expected_count):raise ValueError('FiscalData pagination changed during capture')
        expected=total;expected_count=count;pages.append(doc);refs.append(ref)
        if page==total:
            if sum(len(p['data']) for p in pages)!=count:raise ValueError('FiscalData row count mismatch')
            return pages,refs
    raise ValueError('FiscalData page limit reached')


def fred(as_of):
    key=managed_secret(('FRED_API_KEY','FRED_KEY'),('/justhodl/fred/api-key',))
    if not key:raise ValueError('configured FRED credential unavailable')
    params={'series_id':'DFF','api_key':key,'file_type':'json','sort_order':'asc',
            'observation_start':(datetime.fromisoformat(as_of)-timedelta(days=187)).date().isoformat(),
            'observation_end':as_of,'limit':1000}
    doc,ref=request('https://api.stlouisfed.org/fred/series/observations?'+urllib.parse.urlencode(params),'fred')
    if int(doc.get('count',-1))!=len(doc.get('observations',[])):raise ValueError('incomplete FRED observation response')
    return doc,ref


def put_once(key, raw, content_type='application/json'):
    try:S3.put_object(Bucket=BUCKET,Key=key,Body=raw,ContentType=content_type,IfNoneMatch='*')
    except Exception as exc:
        code=str(getattr(exc,'response',{}).get('Error',{}).get('Code',''))
        if code not in ('409','412','ConditionalRequestConflict','PreconditionFailed'):raise
        if S3.get_object(Bucket=BUCKET,Key=key)['Body'].read()!=raw:raise ValueError('immutable run collision') from exc


def compiler_receipts():
    result={}
    for module in (tenor_research_model,treasury_instruments):
        raw=Path(module.__file__).read_bytes();sha=hashlib.sha256(raw).hexdigest()
        key='data/tenor-research/compilers/'+sha+'.py'
        put_once(key,raw,'text/plain')
        result[module.__name__]={'sha256':sha,'key':key,'bytes':len(raw)}
    return result


def publish_current(out):
    """An older concurrent capture cannot overwrite a newer published snapshot."""
    for _ in range(3):
        condition={'IfNoneMatch':'*'}
        try:
            prior=S3.get_object(Bucket=BUCKET,Key=OUTPUT_KEY)
            old=json.loads(prior['Body'].read())
            try:older=datetime.fromisoformat(old.get('generated_at','').replace('Z','+00:00'))
            except (TypeError,ValueError):older=None
            if older and older.tzinfo and older>datetime.fromisoformat(out['generated_at']):return False
            condition={'IfMatch':prior['ETag']}
        except Exception as exc:
            if str(getattr(exc,'response',{}).get('Error',{}).get('Code','')) not in ('404','NoSuchKey'):raise
        try:
            S3.put_object(Bucket=BUCKET,Key=OUTPUT_KEY,Body=canonical(out),ContentType='application/json',
                          CacheControl='public, max-age=60, s-maxage=60',**condition)
            return True
        except Exception as exc:
            if str(getattr(exc,'response',{}).get('Error',{}).get('Code','')) not in ('409','412','ConditionalRequestConflict','PreconditionFailed'):raise
    raise ValueError('current publication remained contested')


def lambda_handler(event=None,context=None):
    started=time.monotonic();now=datetime.now(timezone.utc);as_of=now.date().isoformat()
    errors=[];pages=[];refs=[];dff=None;dff_ref=None
    try:pages,refs=fiscal_pages(as_of)
    except Exception:errors.append('FISCAL_SOURCE_CAPTURE_INCOMPLETE')
    try:dff,dff_ref=fred(as_of)
    except Exception:errors.append('DFF_SOURCE_CAPTURE_INCOMPLETE')
    # The public run binds original response receipts, exact pure compiler bytes,
    # explicit evaluation date and deterministic output. It contains no secrets.
    model=compile_research(pages,dff,as_of,source_complete=not errors)
    model['quality']['source_errors']=errors
    compilers=compiler_receipts()
    payload={'contract':'treasury-tenor-run.v1','as_of':as_of,'source_complete':not errors,
             'source_errors':errors,'fiscal_pages':refs,'fred':dff_ref,'compilers':compilers,
             'output':model,'output_sha256':hashlib.sha256(canonical(model)).hexdigest()}
    raw=canonical(payload);sha=hashlib.sha256(raw).hexdigest();run_key='data/tenor-research/runs/'+sha+'.json'
    put_once(run_key,raw)
    # Read originals back and recompile before current publication. A failure is
    # an invocation failure, never a claim that an unreplayed packet is fresh.
    replay=compile_research([json.loads(read_verified(S3,BUCKET,r)) for r in refs],
                            json.loads(read_verified(S3,BUCKET,dff_ref)) if dff_ref else None,
                            as_of,source_complete=not errors)
    replay['quality']['source_errors']=errors
    if canonical(replay)!=canonical(model):raise ValueError('retained source replay differs')
    out={**model,'engine':'justhodl-tenor-signal-interpreter','generated_at':now.isoformat(),
         'elapsed_sec':round(time.monotonic()-started,2),
         'reproducibility':{'contract':'treasury-tenor-run.v1','key':run_key,'sha256':sha,
                            'output_sha256':payload['output_sha256'],'original_response_replayed':bool(refs) and not errors,
                            'retained_output_replayed':True,
                            'scope':'Current retrieved source vintage; not historical as-known-at release reconstruction'},
         'source_evidence':{'fiscal_pages':refs,'fred':dff_ref},'paid_api_calls':0}
    published=publish_current(out)
    return {'statusCode':200,'schema_version':CONTRACT,'quality':out['quality'],
            'run':run_key,'output_sha256':payload['output_sha256'],'paid_api_calls':0,'published':published}
