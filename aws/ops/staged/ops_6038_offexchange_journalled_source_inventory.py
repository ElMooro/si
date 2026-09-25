"""New explicit FINRA inventory following retained 6037 diagnosis.

Adopts only the six journalled parents and three journalled metadata responses.
Five new probes get per-response journals. Empty HTTP bodies are evidence, never
zero market activity. Unjournalled 6036 blobs are not assigned invented clocks.
"""
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor,as_completed
from datetime import datetime,timezone
import hashlib,json,re,subprocess,sys,time,urllib.request,urllib.error
import boto3
ROOT=Path(__file__).resolve().parents[3];sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/staged')]
from ops_report import report
import ops_6036_offexchange_whole_source_preflight as prior
from ops_5975_etf_constituent_source_preflight import runtime
from ops_5998_option_population_retained_acceptance import denied_with_retry
BUCKET=prior.BUCKET;PRIVATE=prior.PRIVATE;REQUEST='chatgpt-offexchange-journalled-inventory-6038'
STATUS=PRIVATE+'requests/'+prior.sha(REQUEST.encode())+'.json'
HEADERS=(*prior.HEADERS,'content-length','response-payload-max-size','response-payload_max_size')
MAX=8*1024*1024

def read_response(stream,limit=MAX):
    try:raw=stream.read(limit+1)
    finally:stream.close()
    return raw if len(raw)<=limit else None

def retain_response(s3,raw):
    if not isinstance(raw,bytes) or len(raw)>MAX:raise ValueError('Complete bounded response required')
    key=PRIVATE+prior.sha(raw)+'.bin'
    try:s3.put_object(Bucket=BUCKET,Key=key,Body=raw,ContentType='application/octet-stream',CacheControl='no-store',IfNoneMatch='*')
    except Exception as exc:
        if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) not in ('409','412','ConditionalRequestConflict','PreconditionFailed'):raise
    if read_response(s3.get_object(Bucket=BUCKET,Key=key)['Body'])!=raw:raise ValueError('Original readback differs')
    return {'key':key,'sha256':prior.sha(raw),'bytes':len(raw)}

def write_status(s3,key,doc,claim=False):
    assert re.fullmatch(re.escape(PRIVATE)+r'requests/[a-f0-9]{64}\.json',key)
    raw=prior.encoded(doc);s3.put_object(Bucket=BUCKET,Key=key,Body=raw,ContentType='application/json',CacheControl='no-store',**({'IfNoneMatch':'*'} if claim else {}))
    if prior.get(s3,key)!=raw:raise ValueError('Journal readback differs')

def fetch(s3,spec,opener,deadline):
    allowed=re.fullmatch(r'https://api\.finra\.org/data/group/otcMarket/name/(?:weeklySummary|monthlySummary)',spec['url']) or re.fullmatch(r'https://cdn\.finra\.org/equity/regsho/daily/CNMSshvol\d{8}\.txt',spec['url'])
    if not allowed:raise ValueError('Reviewed FINRA destination required')
    started=prior.now()
    if time.monotonic()>=deadline:return {**spec,'requested_at':started,'received_at':prior.now(),'status':'deadline_before_request','original':None,'coverage_complete':False}
    request=urllib.request.Request(spec['url'],data=prior.encoded(spec['body']) if spec['body'] is not None else None,
        headers={'User-Agent':'JustHodl-offexchange-source-audit/1.1','Accept':'text/plain' if spec['kind']=='daily_file' else 'application/json','Content-Type':'application/json'})
    try:response=opener.open(request,timeout=max(1,min(40,deadline-time.monotonic())))
    except urllib.error.HTTPError as exc:response=exc
    except (OSError,TimeoutError):return {**spec,'requested_at':started,'received_at':prior.now(),'status':'transport_unavailable','original':None,'coverage_complete':False}
    code=response.status;headers={k.lower():v for k,v in response.headers.items() if k.lower() in HEADERS};raw=read_response(response)
    received=prior.now();base={**spec,'requested_at':started,'received_at':received,'http_status':code,'headers':headers,'coverage_complete':False}
    if raw is None:return {**base,'status':'response_over_bound','original':None,'byte_bound':MAX,'inventory':{'review_status':'whole_response_not_retained'}}
    ref=retain_response(s3,raw)
    if not raw:return {**base,'status':'empty_http_response','original':ref,'inventory':{'review_status':'empty_body_not_market_zero'}}
    try:inventory=prior.describe(raw,spec['kind'],headers)
    except (ValueError,TypeError,UnicodeError):inventory={'review_status':'invalid_source_shape','coverage_complete':False}
    return {**base,'status':'response_retained' if code==200 else 'provider_error_retained','original':ref,'inventory':inventory}

def task(s3,name,spec,deadline,opener=None):
    key=PRIVATE+'requests/'+prior.sha((REQUEST+':'+name).encode())+'.json'
    write_status(s3,key,{'request_id':REQUEST,'source_id':name,'status':'claimed','specification':spec},True)
    try:result=fetch(s3,spec,opener or urllib.request.build_opener(prior.NoRedirect()),deadline)
    except Exception as exc:
        write_status(s3,key,{'request_id':REQUEST,'source_id':name,'status':'failed','error_type':type(exc).__name__,'specification':spec})
        raise
    write_status(s3,key,{'request_id':REQUEST,'source_id':name,'status':'complete','capture':result})
    return result,key

def main():
    s3=boto3.client('s3',region_name='us-east-1');lam=boto3.client('lambda',region_name='us-east-1')
    events=boto3.client('events',region_name='us-east-1');scheduler=boto3.client('scheduler',region_name='us-east-1')
    with report('ops_6038_offexchange_journalled_source_inventory') as r:
        subprocess.run([sys.executable,str(ROOT/'tests/test_offexchange_journalled_capture.py')],cwd=ROOT,check=True)
        try:existing=prior.strict(prior.get(s3,STATUS))
        except Exception as exc:
            if not prior.missing(exc):raise
            existing=None
        if existing:
            assert existing['status']=='complete','Never repeat incomplete request'
            ref=existing['manifest'];manifest=prior.strict(prior.original(s3,ref))
        else:
            previous=prior.strict(prior.get(s3,prior.STATUS));assert previous['request_id']==prior.REQUEST and previous['status']=='capturing'
            parents=previous['parents'];assert set(parents)==set(prior.PACKETS)
            captures=previous['captures'];assert set(captures)=={'metadata:'+v for v in ('weeklySummary','monthlySummary','blocksSummary')}
            for row in [*parents.values(),*captures.values()]:prior.original(s3,row['original'])
            actual=runtime(lam,s3,events,scheduler,prior.FUNCTION)
            write_status(s3,STATUS,{'request_id':REQUEST,'status':'claimed','adopted_request':prior.REQUEST,'parents':parents,'captures':captures},True)
            requests={k:v for k,v in prior.plan(datetime.now(timezone.utc).date(),parents['data/finra-short.json']['data_date']).items() if not k.startswith('metadata:')}
            deadline=time.monotonic()+160;tasks={};errors={}
            with ThreadPoolExecutor(max_workers=3) as pool:
                jobs={pool.submit(task,s3,name,spec,deadline):name for name,spec in requests.items()}
                for future in as_completed(jobs):
                    name=jobs[future]
                    try:captures[name],tasks[name]=future.result()
                    except Exception as exc:errors[name]=type(exc).__name__
                    write_status(s3,STATUS,{'request_id':REQUEST,'status':'capturing','parents':parents,'captures':captures,'task_journals':tasks,'errors':errors})
            assert not errors,errors
            assert len(captures)==8 and runtime(lam,s3,events,scheduler,prior.FUNCTION)==actual
            manifest={'contract':'offexchange-source-preflight.v2','generated_at':prior.now(),'request_id':REQUEST,'adopted_request':prior.REQUEST,
                'diagnosis':'ops_6037_offexchange_response_diagnosis','runtime':actual,'parents':parents,'captures':captures,'task_journals':tasks,
                'provider_bytes':sum(v['original']['bytes'] for v in captures.values() if v.get('original')),
                'coverage_complete':False,'forecast_qualified':False,'scope':'Metadata, incomplete weekly/monthly first-page probes, and a CNMS response; not a market inventory or directional signal.'}
            ref=prior.retain(s3,prior.encoded(manifest));write_status(s3,STATUS,{'request_id':REQUEST,'status':'complete','manifest':ref})
        protected={STATUS,ref['key'],*manifest['task_journals'].values(),*(v['original']['key'] for v in manifest['parents'].values() if v.get('original')),*(v['original']['key'] for v in manifest['captures'].values() if v.get('original'))}
        def deny(key):assert denied_with_retry('https://justhodl.ai/'+key) and denied_with_retry('https://'+BUCKET+'.s3.amazonaws.com/'+key)
        with ThreadPoolExecutor(max_workers=4) as pool:
            for _ in pool.map(deny,sorted(protected)):pass
        r.kv(manifest=ref,runtime=manifest['runtime'],captures=manifest['captures'],parents=manifest['parents'],
            protected_artifacts_checked=len(protected),coverage_complete=False,provider_requests_this_run=0 if existing else 5,
            engine_invocations=0,public_head_writes=0,private_account_reads=0,paid_ai_calls=0,notifications_sent=0,portfolio_writes=0,schedules_changed=0)
if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
