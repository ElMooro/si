"""Verify five packages, publish one qualified accounting snapshot, replay it.

No provider requests, consumer invocations, account reads, orders or paid AI.
The failed prior request is preserved; its diagnosed insufficient runtime budget
is corrected in configuration before this new, separately journaled execution.
Accepted/ambiguous executions are never blindly retried. Whole predecessors
and existing Lambda bindings are preserved; only private readiness and this
public-source producer's canonical packet may advance.
"""
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import json, re, subprocess, sys, time, urllib.request
import boto3
from botocore.config import Config
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/shared','aws/ops','aws/ops/staged','aws/ops/checks','scripts')]
from ops_report import report
from acceptance_invoke import invoke_when_available
from market_runtime_evidence import runtime
from ops_5998_option_population_retained_acceptance import denied_with_retry
from ops_5966_sector_tilt_runtime_diagnosis import parse_runtime
from shared_dependents import dependents
import ops_6071_financial_statement_source_inventory as baseline_ops
import ops_6072_financial_statement_original_probe as baseline
import ops_6076_statement_identity_candidate as candidate
import statement_research_source as source
import statement_research_v2 as model
import statement_research_store_v2 as store
import statement_research_arithmetic_v2 as arithmetic
import statement_producer as producer
import statement_context as boundary
import statement_source_refresh as refresh

BUCKET='justhodl-dashboard-live'
FUNCTION='justhodl-forensic-screen'
PROOF='data/statement-research-verification.json'
CANDIDATE={'manifest_key':'data/statement-research/runs/f9723ff867ef9b698919e0a10ead12e2b5daa1e75c1fb8087f143c8c4adfffa4.json',
    'output_sha256':'ef0bc671db4c109f0bcd8c3ca5081cc72336f8525d6aea79f4c2afb96a14f72d'}
CONSUMER_COMMIT='41efe217e24cd3572ea5f58622de6ea7c41d12a4'
SHARED=('aws/shared/statement_context.py','aws/shared/statement_producer.py')
ASSETS=('forensic.html','statement-research.html','jh-statement-research.js','jh-statement-page.js',
    'jh-statement-context.js','jh-statement-research.css','jh-option-research.css','why.html','accumulation.html',
    'insider.html','master-rank.html','valuations.html','jh-chart-risk-v3.js','jh-chart-risk.js','jh-fund-chips.js')


def public(key,method='GET',byte_range=None):
    headers={'User-Agent':'JustHodl-research-acceptance/1.0','Cache-Control':'no-cache'}
    if byte_range:headers['Range']=byte_range
    response=urllib.request.urlopen(urllib.request.Request('https://justhodl.ai/'+key,method=method,headers=headers),timeout=40)
    status,headers=response.status,{k.lower():v for k,v in response.headers.items()}
    return store.bounded(response),headers,status


def packet(s3,key):return source.strict(producer.raw(s3,BUCKET,key))


def prepare_ready(s3,read):
    accepted=packet(s3,candidate.STATUS)
    assert accepted['status']=='complete' and accepted['replay']==CANDIDATE
    assert accepted['source_commit']=='2ee800f9db6f076746e007785ec0902cb870a928'
    run=store.verified_run(CANDIDATE,read)
    result=store.checked(run['output'],'outputs',read)
    inputs=store.checked(run['input'],'inputs',read)
    assert inputs['source_manifest']==candidate.SOURCE and inputs['identity_capture']==accepted['identity_capture']
    ready={'contract':'financial-statement-qualified-ready.v2','status':'qualified','request_id':'qualified-candidate-6076',
        'qualified_at':producer.now(),'source_manifest_sha256':candidate.SOURCE['sha256'],
        'generated_at':result['generated_at'],'replay':CANDIDATE,'qualification':accepted['qualification'],
        'provider_requests':0,'producer_invocations':0,'consumer_invocations':0,'private_account_reads':0}
    previous=None
    try:
        response=s3.get_object(Bucket=BUCKET,Key=producer.READY);raw=store.bounded(response['Body'])
        previous=producer.protect(s3,BUCKET,raw);etag=response['ETag'];prior=source.strict(raw)
        if prior.get('replay')==CANDIDATE:
            assert prior.get('status')=='qualified' and prior.get('qualification')==accepted['qualification']
            return {'advanced':False,'previous':previous}
        assert source.clock(prior['generated_at'])<source.clock(result['generated_at']), 'Never replace a newer qualified ready snapshot'
    except Exception as exc:
        if not producer.missing(exc):raise
        etag=None
    assert refresh.advance_ready(s3,ready,etag), 'Concurrent ready publication; inspect before invoking'
    return {'advanced':True,'previous':previous}


def invoke(lam,s3,commit):
    failed_request='chatgpt-statement-native-'+CONSUMER_COMMIT[:12]+'-1'
    failed=packet(s3,producer.request_key(failed_request))
    assert failed['status']=='failed' and failed['error_type']=='ValueError' and 'predecessor' not in failed
    assert failed['execution_id']=='fbbfdfae-37e8-4ef5-80a3-5c8db2c189c3'
    diagnostic=(ROOT/'aws/ops/reports/latest/ops_6078_statement_runtime_diagnosis.md').read_text(encoding='utf-8')
    assert '**Status:** success' in diagnostic and 'Insufficient source replay budget' in diagnostic
    assert source.sha(producer.raw(s3,BUCKET,producer.CURRENT))=='425b51e30afe0fc182f1160089ac0a2c5aa5d2d3f527c60857a85eda2ffd04d9'
    request='chatgpt-statement-native-'+commit[:12]+'-1'
    status_key,dispatch_key=producer.request_key(request),producer.request_key(request+'-dispatch')
    claim={'contract':'statement-native-dispatch.v1','request_id':request,'function':FUNCTION,'status':'claimed','started_at':producer.now()}
    sent=False
    try:producer.journal(s3,BUCKET,dispatch_key,claim,True)
    except Exception as exc:
        if not producer.conflict(exc):raise
        prior=packet(s3,dispatch_key)
        assert prior['request_id']==request and prior['function']==FUNCTION and prior['contract']==claim['contract']
    else:
        response,rejected=invoke_when_available(lam,{'FunctionName':FUNCTION,'InvocationType':'Event',
            'Payload':source.encoded({'request_id':request})},wait_seconds=45)
        assert response['StatusCode']==202;sent=True
        producer.journal(s3,BUCKET,dispatch_key,{**claim,'status':'accepted_async','throttle_rejections_before_acceptance':rejected})
    status,deadline=None,time.monotonic()+900
    while time.monotonic()<deadline:
        try:status=packet(s3,status_key)
        except Exception as exc:
            if not producer.missing(exc):raise
        if status and status.get('status') in ('complete','failed'):break
        time.sleep(3)
    assert status and status['status']=='complete','Inspect retained execution; never blindly reinvoke'
    assert status['result']['published'] is True and status['result']['replay']==CANDIDATE
    return {'request_id':request,'status_key':status_key,'dispatch_key':dispatch_key,'status':status,'invoke_sent':sent}


def profile(logs,status):
    execution=status['execution_id'];assert re.fullmatch('[a-f0-9-]{36}',execution)
    start=source.clock(status['started_at']);deadline=time.monotonic()+45
    while True:
        rows=logs.filter_log_events(logGroupName='/aws/lambda/'+FUNCTION,filterPattern='"'+execution+'"',
            startTime=int(start.timestamp()*1000)-60000,limit=50).get('events',[])
        values=[parse_runtime(row.get('message',''),execution) for row in rows];values=[v for v in values if v]
        if values:break
        assert time.monotonic()<deadline,'Read completed execution profile; do not reinvoke'
        time.sleep(3)
    assert all(v.get('status') not in ('error','timeout') and v.get('memory_mb')==1024
        and v.get('max_memory_mb',1024)<1024 and v.get('duration_ms',840000)<780000 for v in values)
    return {'execution_id':execution,'managed_reports':values}


def main():
    s3=boto3.client('s3',region_name='us-east-1',config=Config(max_pool_connections=16,retries={'max_attempts':2}))
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(connect_timeout=10,read_timeout=60,retries={'max_attempts':0}))
    events,scheduler=boto3.client('events',region_name='us-east-1'),boto3.client('scheduler',region_name='us-east-1')
    with report('ops_6079_statement_native_acceptance') as r:
        subprocess.run([sys.executable,str(ROOT/'aws/lambdas/justhodl-forensic-screen/tests/run_tests.py')],cwd=ROOT,check=True)
        commit=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/justhodl-forensic-screen/source',
            'aws/lambdas/justhodl-forensic-screen/config.json'],cwd=ROOT,text=True).strip()
        read=store.reader(s3,BUCKET);old=source.strict(source.original(baseline.BASELINE,read))
        actual={name:runtime(lam,s3,events,scheduler,name) for name in dependents(ROOT,SHARED)}
        assert set(actual)=={FUNCTION,'justhodl-fundamental-census','justhodl-fundamental-graphs','justhodl-opportunity-engine','justhodl-short-book'}
        assert all(value['receipt']=={'status':'matched','commit':commit if fn==FUNCTION else CONSUMER_COMMIT} for fn,value in actual.items())
        assert actual[FUNCTION]['memory_mb']==1024 and actual[FUNCTION]['timeout']==840
        for fn in baseline_ops.FUNCTIONS:
            for field in ('timeout','memory_mb','runtime','handler','architectures','role','ephemeral_storage_mb'):
                expected=({'timeout':840,'memory_mb':1024}.get(field,old['runtime'][fn][field]) if fn==FUNCTION else old['runtime'][fn][field])
                assert actual[fn][field]==expected,(fn,field)
            actual_bindings=baseline_ops.bindings(scheduler,events,lam.get_function_configuration(FunctionName=fn)['FunctionArn'])
            for field in ('schedules','classic_default_bus_rules'):assert actual_bindings[field]==old['bindings'][fn][field]
        build=source.strict(public('build-manifest.json')[0]);pages_commit=build['commit_sha']
        subprocess.run(['git','merge-base','--is-ancestor',CONSUMER_COMMIT,pages_commit],cwd=ROOT,check=True)
        subprocess.run(['git','diff','--quiet','HEAD',pages_commit,'--',*ASSETS],cwd=ROOT,check=True)
        for name in ASSETS:
            raw=public(name)[0]
            clean,n=re.subn(rb'<script\b[^>]*\bsrc="https://static\.cloudflareinsights\.com/beacon\.min\.js/v[a-f0-9]+"[^>]*></script>\n?',b'',raw)
            assert n<=1 and source.sha(clean)==build['files_sha256'][name],name
            if name in ('forensic.html','statement-research.html'):
                assert b'/jh-statement-research.js' in raw and b'/jh-statement-page.js' in raw
        r.kv(commit=commit,runtime_packages=actual,pages_commit=pages_commit)
        ready=prepare_ready(s3,read);primary=invoke(lam,s3,commit);r.kv(ready=ready,primary_request=primary)
        execution=profile(boto3.client('logs',region_name='us-east-1'),primary['status'])
        raw=public(producer.CURRENT)[0];current=source.strict(raw)
        assert raw==producer.raw(s3,BUCKET,producer.CURRENT) and current['replay']==CANDIDATE
        assert boundary.context(current)['native_reference_available']
        # Source clocks remain source clocks; publication must not relabel older
        # captured observations with the later deployment/invocation time.
        compiled=store.replay(CANDIDATE,read)
        assert compiled['packet']=={k:v for k,v in current.items() if k!='replay'}
        run=store.verified_run(CANDIDATE,read);inputs=store.checked(run['input'],'inputs',read)
        qualification=arithmetic.verify(inputs['source_manifest'],inputs['identity_capture'],compiled,read)
        assert qualification==packet(s3,candidate.STATUS)['qualification']
        keys={CANDIDATE['manifest_key'],run['input']['key'],run['output']['key'],current['identity_index']['original']['key'],
            *(v['key'] for v in run['compilers'].values()),*(v['record']['key'] for v in current['issuers'])}
        def check(key):assert public(key)[0]==store.reader(s3,BUCKET)(key),key
        with ThreadPoolExecutor(max_workers=4) as pool:
            for _ in pool.map(check,sorted(keys)):pass
        assert s3.head_object(Bucket=BUCKET,Key=producer.CURRENT)['CacheControl']=='no-store'
        for method in ('GET','HEAD'):assert 'no-store' in public(producer.CURRENT,method)[1].get('cache-control','')
        ranged,headers,code=public(producer.CURRENT,byte_range='bytes=0-99')
        assert code==206 and ranged==raw[:100] and 'no-store' in headers.get('cache-control','')
        protected={producer.READY,primary['status_key'],primary['dispatch_key'],primary['status']['predecessor']['key'],
            inputs['source_manifest']['key'],inputs['identity_capture']['key']}
        if ready['previous']:protected.add(ready['previous']['key'])
        for key in sorted(protected):
            assert denied_with_retry('https://justhodl.ai/'+key) and denied_with_retry('https://'+BUCKET+'.s3.amazonaws.com/'+key)
        assert runtime(lam,s3,events,scheduler,FUNCTION)==actual[FUNCTION]
        proof={'contract':'statement-native-acceptance.v1','generated_at':producer.now(),'commit':commit,
            'runtime_packages':actual,'execution_profile':execution,'qualified_candidate':CANDIDATE,'primary_request':primary,
            'publication':{'key':producer.CURRENT,'sha256':source.sha(raw),'bytes':len(raw),'replay':CANDIDATE,'generated_at':current['generated_at']},
            'qualification':qualification,'source_original_replay_matches':True,'complete_predecessors_retained':True,
            'public_artifacts_checked':len(keys),'protected_artifacts_checked':len(protected),'pages_commit':pages_commit,
            'assets':{name:build['files_sha256'][name] for name in ASSETS},'producer_invocations_this_acceptance':int(primary['invoke_sent']),
            'provider_requests':0,'consumer_invocations':0,'private_account_reads':0,'paid_ai_calls':0,
            'notifications_sent':0,'portfolio_writes':0,'existing_lambda_schedules_changed':0,'runtime_capacity_repaired':{'before':{'memory_mb':512,'timeout':300},'after':{'memory_mb':1024,'timeout':840}},
            'new_source_collector_workflow':'statement-source-refresh.yml'}
        try:previous=producer.raw(s3,BUCKET,PROOF)
        except Exception as exc:
            if not producer.missing(exc):raise
        else:proof['previous_verification']=producer.protect(s3,BUCKET,previous)
        body=source.encoded(proof)
        s3.put_object(Bucket=BUCKET,Key=PROOF,Body=body,ContentType='application/json',CacheControl='no-store')
        assert public(PROOF)[0]==body
        r.kv(proof_key=PROOF,publication=proof['publication'],qualification=qualification,execution_profile=execution,
            public_artifacts_checked=len(keys),protected_artifacts_checked=len(protected),runtime_packages_checked=len(actual),
            producer_invocations_this_acceptance=int(primary['invoke_sent']),provider_requests=0,consumer_invocations=0,
            private_account_reads=0,paid_ai_calls=0,notifications_sent=0,portfolio_writes=0,existing_lambda_schedules_changed=0)


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
