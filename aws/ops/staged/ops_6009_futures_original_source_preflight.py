"""Retain futures originals and dependent packages before any native migration.

Existing market-data subscription, seven existing products, <=168 bounded
requests. No engine invocation, account data, alerts, trades or schedule writes.
"""
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import json, re, subprocess, sys, time
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/staged'),str(ROOT/'aws/ops/checks'),str(ROOT/'aws/shared')]
from ops_report import report
from ops_5975_etf_constituent_source_preflight import runtime
from ops_5998_option_population_retained_acceptance import denied_with_retry
from release_package_evidence import check_packages
import futures_source_capture as capture
import option_snapshot_capture as primitives
from massive import get_massive_key
BUCKET='justhodl-dashboard-live';FUNCTION='justhodl-polygon-futures-curves'
PRIVATE='audit-private/20260909-originals/futures-research/'
PACKETS=('data/polygon-futures-curves.json',)
CONSUMERS=tuple('justhodl-'+name for name in ('crisis-composite','prepump-alerts-router','prediction-snapshotter','polygon-signals-composite'))
REQUEST='chatgpt-futures-original-source-6009'
STATUS=PRIVATE+'requests/'+capture.sha(REQUEST.encode())+'.json'
MAX=16*1024*1024


def encoded(value):return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
def code(exc):return str(getattr(exc,'response',{}).get('Error',{}).get('Code',''))
def get(s3,key):return capture.bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body'],MAX)
def retain(s3,raw):
    assert isinstance(raw,bytes) and 0<len(raw)<=MAX
    ref={'key':PRIVATE+capture.sha(raw)+'.bin','sha256':capture.sha(raw),'bytes':len(raw)}
    try:s3.put_object(Bucket=BUCKET,Key=ref['key'],Body=raw,ContentType='application/octet-stream',CacheControl='no-store',IfNoneMatch='*')
    except Exception as exc:
        if code(exc) not in ('PreconditionFailed','ConditionalRequestConflict','409','412'):raise
    assert get(s3,ref['key'])==raw
    return ref
def checked(s3,ref):
    assert isinstance(ref,dict) and isinstance(ref.get('sha256'),str) and re.fullmatch('[a-f0-9]{64}',ref['sha256'])
    assert type(ref.get('bytes')) is int and 0<ref['bytes']<=MAX and ref['key']==PRIVATE+ref['sha256']+'.bin'
    raw=get(s3,ref['key']);assert len(raw)==ref['bytes'] and capture.sha(raw)==ref['sha256'];return raw
def write_status(s3,key,doc,**conditions):
    raw=encoded(doc);s3.put_object(Bucket=BUCKET,Key=key,Body=raw,ContentType='application/json',CacheControl='no-store',**conditions)
    assert get(s3,key)==raw
def snapshot(s3,key):
    assert key in PACKETS,'Explicit public research only'
    obj=s3.get_object(Bucket=BUCKET,Key=key);raw=capture.bounded(obj['Body'],MAX);doc=json.loads(raw)
    return {'source_key':key,'original':retain(s3,raw),'acquired_at':capture.now(),
        'source_generated_at':doc.get('generated_at'),'last_modified':obj['LastModified'].isoformat(),
        'etag':obj['ETag'],'version_id':obj.get('VersionId')}


def rows(s3, source):
    result=[]
    for page in source['pages']:
        if not page.get('original'):continue
        raw=checked(s3,page['original'])
        if page['http_status']==200:
            try:result.extend(capture.envelope(raw)['results'])
            except (ValueError,UnicodeDecodeError):pass
    return result


def summaries(s3,sources):
    return {name:{'scope':source['scope'],'stop':source['stop'],'pagination_complete':source['pagination_complete'],
        'http_statuses':[page['http_status'] for page in source['pages']],
        'pages':len(source['pages']),**capture.summarize(rows(s3,source),source['scope']['kind'])}
        for name,source in sources.items()}


def main():
    s3=boto3.client('s3',region_name='us-east-1');lam=boto3.client('lambda',region_name='us-east-1')
    events=boto3.client('events',region_name='us-east-1');scheduler=boto3.client('scheduler',region_name='us-east-1')
    with report('ops_6009_futures_original_source_preflight') as r:
        subprocess.run([sys.executable,str(ROOT/'tests/test_futures_source_capture.py')],cwd=ROOT,check=True)
        r.kv(request_status_key=STATUS)
        try:previous=json.loads(get(s3,STATUS))
        except Exception as exc:
            if code(exc) not in ('404','NoSuchKey'):raise
            previous=None
        if previous is not None:
            assert previous.get('contract')=='futures-original-source-audit-request.v1' and previous.get('status')=='complete', 'Inspect retained partial request; do not recollect blindly'
            ref=previous['manifest'];doc=json.loads(checked(s3,ref));r.kv(adopted_completed_request=True,retained_manifest=ref)
            for module in (capture,primitives):assert checked(s3,doc['compilers'][module.__name__])==Path(module.__file__).read_bytes()
            requests=0
        else:
            prior_runtime=runtime(lam,s3,events,scheduler,FUNCTION)
            packages=check_packages(lam,ROOT,CONSUMERS)
            failures=[{'function':p['function'],'mismatches':[f['member'] for f in p['files'] if not f['match']],
                'configuration':p.get('configuration_mismatches')} for p in packages if not p['pass']]
            assert not failures,failures
            predecessors={key:snapshot(s3,key) for key in PACKETS}
            compilers={module.__name__:retain(s3,Path(module.__file__).read_bytes()) for module in (capture,primitives)}
            end=datetime.now(ZoneInfo('America/New_York')).date();start=end-timedelta(days=90)
            claim={'contract':'futures-original-source-audit-request.v1','request_id':REQUEST,'status':'claimed','started_at':capture.now(),
                'from':start.isoformat(),'to':end.isoformat(),'predecessors':predecessors,'compilers':compilers,
                'runtime':prior_runtime,'consumer_packages':packages}
            write_status(s3,STATUS,claim,IfNoneMatch='*')
            secret=get_massive_key();assert secret,'Existing managed provider configuration is unavailable'
            budget=capture.Budget(requests=168);deadline=time.monotonic()+120
            def one(scope):
                name=scope['product']+':'+scope['kind']+(':'+scope['ticker'] if scope['ticker'] else '')
                key=PRIVATE+'requests/'+capture.sha((REQUEST+':'+name).encode())+'.json'
                result=capture.collect(scope,secret,deadline,lambda raw:retain(s3,raw),budget,
                    lambda doc:write_status(s3,key,doc))
                return name,result
            initial=[capture.spec(kind,product,start.isoformat(),end.isoformat())
                for product in capture.PRODUCTS for kind in ('products','contracts','schedules')]
            with ThreadPoolExecutor(max_workers=4) as pool:sources=dict(pool.map(one,initial))
            selections={}
            for product in capture.PRODUCTS:
                source=sources[product+':contracts']
                selections[product]=capture.select_contracts(rows(s3,source),product,end.isoformat(),source['pagination_complete'])
            bars=[capture.spec('bars',product,start.isoformat(),end.isoformat(),item['ticker'])
                for product,selection in selections.items() for item in selection['selected']]
            with ThreadPoolExecutor(max_workers=4) as pool:sources.update(dict(pool.map(one,bars)))
            counts=summaries(s3,sources)
            doc={'contract':'futures-original-source-audit.v1','generated_at':capture.now(),'request_id':REQUEST,
                'from':start.isoformat(),'to':end.isoformat(),'sources':sources,'summaries':counts,'selections':selections,
                'predecessors':predecessors,'runtime':prior_runtime,'consumer_packages':packages,'compilers':compilers,
                'provider_requests':budget.requests,'provider_response_bytes':budget.bytes,
                'source_bar_finality_verified':False,'no_returns_regimes_or_portfolio_decisions_computed':True}
            ref=retain(s3,encoded(doc));r.kv(retained_manifest=ref)
            claim.update(status='complete',manifest=ref,completed_at=capture.now());write_status(s3,STATUS,claim)
            requests=budget.requests
        assert summaries(s3,doc['sources'])==doc['summaries']
        assert runtime(lam,s3,events,scheduler,FUNCTION)==doc['runtime']
        # Other schedules may publish while this read-only audit runs. Preserve
        # the original snapshot and report the difference; never reset a head.
        heads_match={key:get(s3,key)==checked(s3,row['original']) for key,row in doc['predecessors'].items()}
        protected={STATUS,ref['key'],*(x['key'] for x in doc['compilers'].values()),
            *(x['original']['key'] for x in doc['predecessors'].values()),
            *(p['original']['key'] for x in doc['sources'].values() for p in x['pages'] if p.get('original')),
            *(PRIVATE+'requests/'+capture.sha((REQUEST+':'+name).encode())+'.json' for name in doc['sources'])}
        def deny(key):assert denied_with_retry('https://justhodl.ai/'+key) and denied_with_retry('https://'+BUCKET+'.s3.amazonaws.com/'+key)
        with ThreadPoolExecutor(max_workers=4) as pool:
            for _ in pool.map(deny,sorted(protected)):pass
        r.kv(retained_manifest=ref,producer_runtime=doc['runtime'],consumer_packages_checked=len(doc['consumer_packages']),
            source_summaries=doc['summaries'],contract_selections=doc['selections'],provider_requests_this_attempt=requests,original_provider_requests=doc['provider_requests'],
            public_heads_still_match_retained_snapshot=heads_match,
            provider_response_bytes=doc['provider_response_bytes'],protected_artifacts_checked=len(protected),
            originals_anonymously_denied=True,engine_invocations=0,private_account_reads=0,paid_ai_calls=0,
            notifications_sent=0,portfolio_writes=0,public_head_writes=0,schedules_changed=0)


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
