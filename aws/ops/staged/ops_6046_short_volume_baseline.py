"""Retain short-volume predecessors and runtime identity without invoking either engine.

The existing rolling history is derived evidence, never an original-source series.
No provider, account, credential, notification, or public-head operations are made.
"""
from pathlib import Path
from collections import Counter
from datetime import date
from concurrent.futures import ThreadPoolExecutor
import hashlib,json,re,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/staged')]
from ops_report import report
from ops_5975_etf_constituent_source_preflight import runtime
from ops_5998_option_population_retained_acceptance import denied_with_retry
import ops_6036_offexchange_whole_source_preflight as retained
BUCKET=retained.BUCKET
PRIVATE='audit-private/20260909-originals/short-volume-research/'
REQUEST='chatgpt-short-volume-baseline-6046'
STATUS=PRIVATE+'requests/'+hashlib.sha256(REQUEST.encode()).hexdigest()+'.json'
PACKETS=('data/finra-short.json','data/short-pressure.json')
FUNCTIONS=('justhodl-finra-short','justhodl-short-pressure')
PREVIOUS={'key':retained.PRIVATE+'a87413d775af2e211170e707bc3869fbb143d89d5bf5fc3ffc5cf8c381dc75b6.bin','sha256':'a87413d775af2e211170e707bc3869fbb143d89d5bf5fc3ffc5cf8c381dc75b6','bytes':26448}
MAX=128*1024*1024

def read(s3,key):
    if key not in PACKETS and not re.fullmatch(re.escape(PRIVATE)+r'(?:[a-f0-9]{64}\.bin|requests/[a-f0-9]{64}\.json)',key):
        raise ValueError('Reviewed public-market predecessor or protected evidence required')
    return retained.bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body'],MAX)

def protect(s3,raw):
    if not isinstance(raw,bytes) or not 0<len(raw)<=MAX:raise ValueError('Complete bounded predecessor required')
    digest=retained.sha(raw);key=PRIVATE+digest+'.bin'
    try:s3.put_object(Bucket=BUCKET,Key=key,Body=raw,ContentType='application/octet-stream',CacheControl='no-store',IfNoneMatch='*')
    except Exception as exc:
        if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) not in ('409','412','ConditionalRequestConflict','PreconditionFailed'):raise
    assert read(s3,key)==raw
    return {'key':key,'sha256':digest,'bytes':len(raw)}

def checked(s3,ref):
    if ref['key']!=PRIVATE+ref['sha256']+'.bin':raise ValueError('Evidence identity differs')
    raw=read(s3,ref['key'])
    if len(raw)!=ref['bytes'] or retained.sha(raw)!=ref['sha256']:raise ValueError('Evidence bytes differ')
    return raw

def journal(s3,value,claim=False):
    raw=retained.encoded(value)
    s3.put_object(Bucket=BUCKET,Key=STATUS,Body=raw,ContentType='application/json',CacheControl='no-store',**({'IfNoneMatch':'*'} if claim else {}))
    assert read(s3,STATUS)==raw

def history_inventory(document):
    """Describe stored grain and clocks; no inference that rolling history is valid."""
    histories=document.get('tickers')
    if not isinstance(histories,dict):raise ValueError('Legacy ticker history shape differs')
    dates=Counter();lengths=Counter();keys=Counter();errors=Counter();rows=0
    for symbol,series in histories.items():
        if not isinstance(symbol,str) or not isinstance(series,list):raise ValueError('Legacy ticker series shape differs')
        lengths[len(series)]+=1;seen=set();valid_dates=[]
        for row in series:
            rows+=1
            if not isinstance(row,dict):errors['non_object_rows']+=1;continue
            keys.update(row.keys())
            stamp=row.get('date')
            try:
                if not isinstance(stamp,str) or date.fromisoformat(stamp).isoformat()!=stamp:raise ValueError()
            except (TypeError,ValueError):errors['invalid_dates']+=1;continue
            dates[stamp]+=1;valid_dates.append(stamp)
            if stamp in seen:errors['duplicate_symbol_dates']+=1
            seen.add(stamp)
            if not isinstance(row.get('ts'),(int,float)) or isinstance(row.get('ts'),bool):errors['missing_acquisition_timestamp']+=1
        if valid_dates!=sorted(valid_dates):errors['series_not_date_ordered']+=1
    return {'symbols':len(histories),'rows':rows,'date_counts':dict(sorted(dates.items())),
        'length_distribution':dict(sorted(lengths.items())),'field_counts':dict(sorted(keys.items())),
        'detected_errors':dict(errors),'last_data_date':document.get('last_data_date'),
        'last_updated':document.get('last_updated'),'provider_originals_verified':False,
        'point_in_time_membership_verified':False,'security_identity_continuity_verified':False,
        'review_status':'retained_derived_history_only_not_qualified'}

def packet_inventory(document):
    if not isinstance(document,dict):raise ValueError('Legacy publication shape differs')
    result={'top_level_fields':sorted(document),'generated_at':document.get('generated_at'),
        'data_date':document.get('data_date'),'version':document.get('version'),'contract':document.get('contract'),
        'research_qualified':False}
    for key in ('tickers','rows','names','squeeze_candidates','top_svr','top_zscore','sectors','results'):
        value=document.get(key)
        if isinstance(value,(dict,list)):result[key+'_count']=len(value)
    return result

def main():
    s3=boto3.client('s3',region_name='us-east-1');lam=boto3.client('lambda',region_name='us-east-1')
    events=boto3.client('events',region_name='us-east-1');scheduler=boto3.client('scheduler',region_name='us-east-1')
    with report('ops_6046_short_volume_baseline') as r:
        try:prior=retained.strict(read(s3,STATUS))
        except Exception as exc:
            if not retained.missing(exc):raise
            prior=None
        if prior:
            assert prior['request_id']==REQUEST and prior['status']=='complete','Inspect incomplete baseline; do not rerun it'
            ref=prior['manifest'];manifest=retained.strict(checked(s3,ref))
        else:
            journal(s3,{'request_id':REQUEST,'status':'claimed','started_at':retained.now()},True)
            progress={}
            try:
                actual={fn:runtime(lam,s3,events,scheduler,fn) for fn in FUNCTIONS}
                old=retained.strict(retained.original(s3,PREVIOUS));history_ref=old['parents']['data/finra-short-history.json']['original']
                history=history_inventory(retained.strict(retained.original(s3,history_ref)))
                progress={'runtime':actual,'retained_history':{'original':history_ref,'inventory':history},'packets':{}}
                journal(s3,{'request_id':REQUEST,'status':'capturing','progress':progress})
                for key in PACKETS:
                    started=retained.now();raw=read(s3,key);acquired=retained.now()
                    progress['packets'][key]={'original':protect(s3,raw),'requested_at':started,'received_at':acquired,
                        'inventory':packet_inventory(retained.strict(raw))}
                    journal(s3,{'request_id':REQUEST,'status':'capturing','progress':progress})
                assert {fn:runtime(lam,s3,events,scheduler,fn) for fn in FUNCTIONS}==actual,'Runtime changed during baseline'
                manifest={'contract':'short-volume-baseline.v1','generated_at':retained.now(),'request_id':REQUEST,**progress,
                    'adopted_manifest':PREVIOUS,'provider_requests':0,'engine_invocations':0,'public_head_writes':0,
                    'private_account_reads':0,'paid_ai_calls':0,'notifications_sent':0,'portfolio_writes':0,'schedules_changed':0}
                ref=protect(s3,retained.encoded(manifest));journal(s3,{'request_id':REQUEST,'status':'complete','manifest':ref})
            except Exception as exc:
                journal(s3,{'request_id':REQUEST,'status':'failed','error_type':type(exc).__name__,'progress':progress});raise
        protected={STATUS,ref['key'],PREVIOUS['key'],manifest['retained_history']['original']['key']}
        protected.update(v['original']['key'] for v in manifest['packets'].values())
        def deny(key):assert denied_with_retry('https://justhodl.ai/'+key) and denied_with_retry('https://'+BUCKET+'.s3.amazonaws.com/'+key)
        with ThreadPoolExecutor(max_workers=3) as pool:
            for _ in pool.map(deny,sorted(protected)):pass
        r.kv(manifest=ref,runtime=manifest['runtime'],history=manifest['retained_history'],packets=manifest['packets'],
            protected_artifacts_checked=len(protected),provider_requests=0,engine_invocations=0,public_head_writes=0,
            private_account_reads=0,paid_ai_calls=0,notifications_sent=0,portfolio_writes=0,schedules_changed=0)

if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
