"""Capture six explicit FINRA partitions, with real pagination and drift checks.

This is a retained source candidate, not a native producer or a latest-period
claim. Each response gets a durable journal. No current public key is written.
"""
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor,as_completed
from datetime import date
import json,sys,time,subprocess,urllib.request
import boto3
ROOT=Path(__file__).resolve().parents[3];sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/ops'),str(ROOT/'aws/ops/staged')]
from ops_report import report
import offexchange_measurements as model
import ops_6038_offexchange_journalled_source_inventory as source
from ops_6039_offexchange_retained_measurement_review import REF
from ops_5975_etf_constituent_source_preflight import runtime
from ops_5998_option_population_retained_acceptance import denied_with_retry
prior=source.prior;BUCKET=source.BUCKET;PRIVATE=source.PRIVATE;REQUEST='chatgpt-offexchange-complete-partitions-6040'
STATUS=PRIVATE+'requests/'+prior.sha(REQUEST.encode())+'.json';MAX_PAGES=40;MAX_PARTITION_BYTES=48*1024*1024

def partitions():
    rows=[]
    for tier,week in (('T1','2026-08-31'),('T2','2026-08-17')):
        for code in ('ATS_W_SMBL','OTC_W_SMBL'):rows.append({'dataset':'weeklySummary','code':code,'period':week,'tier':tier})
    for month in ('2026-06-01','2026-07-01'):rows.append({'dataset':'monthlySummary','code':'OTC_M_SMBL_FIRM','period':month,'tier':'NMS'})
    return rows

def specification(part,offset):
    if part not in partitions() or type(offset) is not int or not 0<=offset<=500000:raise ValueError('Reviewed exact partition required')
    weekly=part['dataset']=='weeklySummary';field='weekStartDate' if weekly else 'monthStartDate'
    filters=[{'fieldName':k,'compareType':'EQUAL','fieldValue':v} for k,v in (('summaryTypeCode',part['code']),(field,part['period']),('tierIdentifier',part['tier']))]
    return {'url':'https://api.finra.org/data/group/otcMarket/name/'+part['dataset'],'kind':'probe',
        'body':{'limit':2000,'offset':offset,'compareFilters':filters,'sortFields':['issueSymbolIdentifier'] if weekly else ['issueSymbolIdentifier','firmCRDNumber']}}

def key_for(label):return PRIVATE+'requests/'+prior.sha((REQUEST+':'+label).encode())+'.json'

def capture(s3,label,spec,deadline,opener=None):
    key=key_for(label);source.write_status(s3,key,{'request_id':REQUEST,'source_id':label,'status':'claimed','specification':spec},True)
    try:result=source.fetch(s3,spec,opener or urllib.request.build_opener(prior.NoRedirect()),deadline)
    except Exception as exc:
        source.write_status(s3,key,{'request_id':REQUEST,'source_id':label,'status':'failed','error_type':type(exc).__name__});raise
    source.write_status(s3,key,{'request_id':REQUEST,'source_id':label,'status':'complete','capture':result})
    return {**result,'journal_key':key}

def rows_and_count(s3,capture,spec):
    if capture['http_status']==204 and capture['status']=='empty_http_response':
        ref=capture['original'];assert ref['bytes']==0 and ref['sha256']==prior.sha(b'')
        if capture['headers'].get('record-total')!='0' or spec['body']['offset']!=0:raise ValueError('Empty response does not reconcile')
        return [],{'rows':0,'reported_total':0,'offset':0,'next_offset':0,'reported_end_reached':True,'snapshot_atomic':False}
    if capture['http_status']!=200 or capture['status']!='response_retained':raise ValueError('Complete successful response required')
    raw=prior.original(s3,capture['original']);return model.strict(raw),model.page(raw,capture['headers'],spec['body']['offset'],spec['body']['limit'])

def grain(row,part):
    field='weekStartDate' if part['dataset']=='weeklySummary' else 'monthStartDate'
    if not isinstance(row,dict) or (row.get('summaryTypeCode'),row.get(field),row.get('summaryStartDate'),row.get('tierIdentifier'))!=(part['code'],part['period'],part['period'],part['tier']):raise ValueError('Returned partition differs')
    name=model.symbol(row.get('issueSymbolIdentifier'))
    if part['dataset']=='weeklySummary':
        if row.get('MPID') is not None or row.get('firmCRDNumber') is not None:raise ValueError('Symbol summary contains firm dimension')
        return (name,)
    return (name,model.number(model.scalar(row.get('firmCRDNumber'),integer=True)))

def collect(s3,part,deadline,fetcher=capture):
    label='/'.join(part.values());state_key=key_for('partition:'+label);pages=[];offset=0;expected=None;seen=set();bytes_read=0
    source.write_status(s3,state_key,{'request_id':REQUEST,'status':'claimed','partition':part},True)
    try:
        for index in range(MAX_PAGES):
            if time.monotonic()>=deadline:raise TimeoutError('Partition capture deadline')
            spec=specification(part,offset);result=fetcher(s3,label+':page:'+str(index),spec,deadline)
            pages.append(result);bytes_read+=(result.get('original') or {}).get('bytes',0)
            source.write_status(s3,state_key,{'request_id':REQUEST,'status':'capturing','partition':part,'pages':pages})
            if bytes_read>MAX_PARTITION_BYTES:raise ValueError('Whole partition byte bound exceeded')
            rows,count=rows_and_count(s3,result,spec)
            if expected is None:expected=count['reported_total']
            if expected!=count['reported_total']:raise ValueError('Reported total changed during pagination')
            for row in rows:
                key=grain(row,part)
                if key in seen:raise ValueError('Duplicate grain across source pages')
                seen.add(key)
            offset=count['next_offset']
            if count['reported_end_reached']:break
        else:raise ValueError('Pagination bound reached before reported end')
        if len(seen)!=expected:raise ValueError('Unique records do not reconcile with reported total')
        # Detect a changing first page. This is not proof of a transactionally atomic API snapshot.
        recheck=fetcher(s3,label+':first-page-recheck',specification(part,0),deadline)
        pages.append(recheck);_,count=rows_and_count(s3,recheck,specification(part,0))
        if count['reported_total']!=expected or recheck['original']!=pages[0]['original']:raise ValueError('First page changed during collection')
        result={'partition':part,'pages':pages,'rows':len(seen),'reported_total':expected,'records_reconciled':True,'snapshot_atomic':False,
            'first_page_recheck_matched':True,'source_status':'no_rows_in_reviewed_query' if expected==0 else 'records_captured',
            'provider_requests':len(pages),'provider_bytes':sum(v['original']['bytes'] for v in pages),'journal_key':state_key}
        source.write_status(s3,state_key,{'request_id':REQUEST,'status':'complete','result':result});return result
    except Exception as exc:
        source.write_status(s3,state_key,{'request_id':REQUEST,'status':'failed','partition':part,'pages':pages,'error_type':type(exc).__name__,'reason':str(exc)});raise

def main():
    s3=boto3.client('s3',region_name='us-east-1');lam=boto3.client('lambda',region_name='us-east-1')
    events=boto3.client('events',region_name='us-east-1');scheduler=boto3.client('scheduler',region_name='us-east-1')
    with report('ops_6040_offexchange_complete_partitions') as r:
        subprocess.run([sys.executable,str(ROOT/'tests/test_offexchange_partition_capture.py')],cwd=ROOT,check=True)
        try:existing=prior.strict(prior.get(s3,STATUS))
        except Exception as exc:
            if not prior.missing(exc):raise
            existing=None
        if existing:
            assert existing['status']=='complete','Never repeat incomplete request'
            ref=existing['manifest'];manifest=prior.strict(prior.original(s3,ref))
        else:
            inventory=prior.strict(prior.original(s3,REF));actual=runtime(lam,s3,events,scheduler,prior.FUNCTION)
            source.write_status(s3,STATUS,{'request_id':REQUEST,'status':'claimed','inventory':REF,'runtime':actual},True)
            results={};errors={};started=time.monotonic();deadline=started+200
            with ThreadPoolExecutor(max_workers=3) as pool:
                jobs={pool.submit(collect,s3,part,deadline):'/'.join(part.values()) for part in partitions()}
                for future in as_completed(jobs):
                    name=jobs[future]
                    try:results[name]=future.result()
                    except Exception as exc:errors[name]={'type':type(exc).__name__,'reason':str(exc)}
                    source.write_status(s3,STATUS,{'request_id':REQUEST,'status':'capturing','results':results,'errors':errors})
            r.kv(capture_failures=errors)
            assert not errors,errors
            assert runtime(lam,s3,events,scheduler,prior.FUNCTION)==actual
            manifest={'contract':'offexchange-complete-partitions.v1','request_id':REQUEST,'generated_at':prior.now(),'inventory':REF,'runtime':actual,
                'partitions':results,'elapsed_seconds':round(time.monotonic()-started,3),'daily':inventory['captures']['daily:CNMS'],
                'metadata':{k:v for k,v in inventory['captures'].items() if k.startswith('metadata:')},
                'market_coverage_complete':False,'scope':'Only six declared period/tier/category partitions. No latest-period, exchange-market, ownership or signal qualification.'}
            ref=prior.retain(s3,prior.encoded(manifest));source.write_status(s3,STATUS,{'request_id':REQUEST,'status':'complete','manifest':ref})
        protected={STATUS,ref['key']}
        for part in manifest['partitions'].values():
            protected.add(part['journal_key'])
            for page in part['pages']:protected.update((page['original']['key'],page['journal_key']))
        def deny(key):assert denied_with_retry('https://justhodl.ai/'+key) and denied_with_retry('https://'+BUCKET+'.s3.amazonaws.com/'+key)
        with ThreadPoolExecutor(max_workers=4) as pool:
            for _ in pool.map(deny,sorted(protected)):pass
        r.kv(manifest=ref,partitions={k:{a:b for a,b in v.items() if a!='pages'} for k,v in manifest['partitions'].items()},
            elapsed_seconds=manifest['elapsed_seconds'],protected_artifacts_checked=len(protected),
            provider_requests_this_run=0 if existing else sum(v['provider_requests'] for v in manifest['partitions'].values()),
            engine_invocations=0,public_head_writes=0,private_account_reads=0,paid_ai_calls=0,notifications_sent=0,portfolio_writes=0,schedules_changed=0)
if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
