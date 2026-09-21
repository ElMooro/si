"""Verify actual native packages, retained populations and public pages.

Only the new no-notification GEX producer may be invoked after package proof.
No provider collection, private account reads or decision-consumer invocation.
"""
from pathlib import Path
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import json,re,subprocess,sys,time,urllib.request
import boto3
from botocore.config import Config
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/staged'),str(ROOT/'aws/ops/checks'),str(ROOT/'aws/shared')]
from ops_report import report
from acceptance_invoke import invoke_when_available
from release_package_evidence import check_packages,shared_imports
from ops_5975_etf_constituent_source_preflight import runtime
from ops_5966_sector_tilt_runtime_diagnosis import parse_runtime
from ops_5996_option_population_qualification import independent
from ops_5998_option_population_retained_acceptance import denied_with_retry
from ops_5999_population_native_candidate import verify_memberships
import option_population_store as store
desk=store.desk;model=store.model;upstream=store.upstream;source_store=store.source_store
BUCKET='justhodl-dashboard-live';FUNCTION='justhodl-dealer-gex'
QUALIFIED={'key':'audit-private/20260909-originals/options-research/9eca8d567a5aa7eda822d3d261c47715f7c7e079eabe20161250b3c539799cf8.bin',
    'sha256':'9eca8d567a5aa7eda822d3d261c47715f7c7e079eabe20161250b3c539799cf8','bytes':4950}
RECOVERY={'manifest_key':'data/option-population-research/runs/ed0d7bdae77e9f7709e5afaa42ec08856dc2517b80e55fb83af1d422f77076f3.json',
    'output_sha256':'8cd44cd5ffd0aba1d38d535abb0e9ef2c9655bc197ea7ff2502fc7f5fc182716'}
CONSUMERS=tuple('justhodl-'+n for n in ('ai-chat','best-setups','crypto-intel','institutional-footprint','invest','jhsignal-bridge',
    'massive-signals','morning-intelligence','options-confluence','page-ai-commentary','prepump-alerts-router','regime-composite'))
ASSETS=('gex/index.html','0dte/index.html','options.html','option-chain-research.html','jh-option-research.js','jh-option-research-page.js',
    'jh-option-populations.js','jh-option-populations-page.js','jh-option-populations-hub.js','jh-option-populations.css','jh-option-research.css','jh-data-feeds.js')
PROOF='data/option-population-research-verification.json'


def public(key):
    with urllib.request.urlopen(urllib.request.Request('https://justhodl.ai/'+key,headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=40) as response:return store.bounded(response)


def current(s3):
    try:raw=store.bounded(s3.get_object(Bucket=BUCKET,Key=desk.CURRENT)['Body'])
    except Exception as exc:
        if store.missing(exc):return None
        raise
    packet=json.loads(raw);assert packet['contract']==desk.CONTRACT
    return packet


def completed_request(s3,packet):
    prefix=upstream.PRIVATE+'requests/';items=[]
    for page in s3.get_paginator('list_objects_v2').paginate(Bucket=BUCKET,Prefix=prefix):
        items.extend(page.get('Contents',[]));assert len(items)<=2000,'Bounded population request discovery'
    for item in sorted(items,key=lambda x:x['LastModified'],reverse=True)[:100]:
        doc=json.loads(store.bounded(s3.get_object(Bucket=BUCKET,Key=item['Key'])['Body']))
        if doc.get('contract')=='option-population-request.v1' and doc.get('status')=='complete' and doc.get('replay')==packet['replay'] and doc.get('published') is True:
            return {'status_key':item['Key'],'status':doc,'invoke_sent':False,'adopted_completed_native_request':True}
    raise AssertionError('Inspect native publication request evidence; do not reinvoke')


def invoke(lam,s3,commit):
    existing=current(s3)
    if existing is not None:return completed_request(s3,existing)
    request='chatgpt-'+FUNCTION+'-'+commit[:12]+'-1'
    key=source_store.request_key('population:'+request);dispatch_key=source_store.request_key('population:'+request+'-dispatch')
    claim={'contract':'option-population-native-dispatch.v1','request_id':request,'started_at':store.now(),'status':'claimed','recovery':RECOVERY};sent=False
    try:source_store.status_write(s3,BUCKET,dispatch_key,claim,IfNoneMatch='*')
    except Exception as exc:
        if not store.conflict(exc):raise
        previous=json.loads(store.bounded(s3.get_object(Bucket=BUCKET,Key=dispatch_key)['Body']))
        assert previous['contract']==claim['contract'] and previous['request_id']==request and previous['recovery']==RECOVERY
    else:
        response,rejected=invoke_when_available(lam,{'FunctionName':FUNCTION,'InvocationType':'Event','Payload':upstream.encoded({'request_id':request,'recover_run':RECOVERY})})
        assert response['StatusCode']==202;sent=True;claim.update(status='accepted_async',throttle_rejections_before_acceptance=rejected)
        source_store.status_write(s3,BUCKET,dispatch_key,claim)
    status=None;deadline=time.monotonic()+660
    while time.monotonic()<deadline:
        try:status=json.loads(store.bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body']))
        except Exception as exc:
            if not store.missing(exc):raise
        if status and status.get('status') in ('complete','failed'):break
        time.sleep(3)
    assert status and status['status']=='complete','Inspect retained request; never blindly reinvoke'
    assert status['provider_requests']==0 and status['recovered_from']==RECOVERY and status['replay']==RECOVERY
    result={'request_id':request,'status_key':key,'dispatch_key':dispatch_key,'status':status,'invoke_sent':sent}
    packet=current(s3);assert packet is not None
    if packet['replay']!=status['replay']:
        assert upstream.clock(packet['source_capture_completed_at'])>=upstream.clock(status['source_capture_completed_at'])
        adopted=completed_request(s3,packet);adopted.update(invoke_sent=sent,earlier_completed_request=result);return adopted
    assert status['published'] is True and status['compatibility_published'] is True
    return result


def execution_profile(logs,status):
    execution=status['execution_id'];assert re.fullmatch('[a-f0-9-]{36}',execution)
    deadline=time.monotonic()+45;start=upstream.clock(status['started_at'])
    while True:
        rows=logs.filter_log_events(logGroupName='/aws/lambda/'+FUNCTION,filterPattern='"'+execution+'"',startTime=int(start.timestamp()*1000)-5000,limit=50).get('events',[])
        values=[parse_runtime(row.get('message',''),execution) for row in rows];values=[v for v in values if v]
        if values:break
        assert time.monotonic()<deadline,'Read completed execution report; do not reinvoke';time.sleep(3)
    assert all(v.get('status') not in ('error','timeout') and v.get('memory_mb')==2048 and v.get('max_memory_mb',2048)<2048 and v.get('duration_ms',600000)<600000 for v in values)
    return {'execution_id':execution,'managed_reports':values,'completed_request':True}


def source_commit(function):
    assert function in CONSUMERS,'Reviewed consumer required'
    source='aws/lambdas/'+function+'/source'
    files=[ROOT/p for p in subprocess.check_output(['git','ls-files',source],cwd=ROOT,text=True).splitlines()]
    paths=[source,'aws/lambdas/'+function+'/config.json',*[p.relative_to(ROOT).as_posix() for p in shared_imports(ROOT,files)]]
    commit=subprocess.check_output(['git','log','-1','--format=%H','--',*paths],cwd=ROOT,text=True).strip()
    assert re.fullmatch('[a-f0-9]{40}',commit),'Exact consumer source/configuration commit required'
    return commit


def consumer_receipts(packages):
    commits={}
    for row in packages:
        expected=source_commit(row['function']);receipt=json.loads(public('data/ops/releases/'+row['function']+'.json'))
        assert receipt['commit']==expected and receipt['code_sha256']==row['code_sha256'],row['function']
        commits[row['function']]=expected
    return commits


def main(report_name='ops_6000_population_native_acceptance'):
    s3=boto3.client('s3',region_name='us-east-1',config=Config(max_pool_connections=24))
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=60,connect_timeout=10,retries={'max_attempts':0}))
    events=boto3.client('events',region_name='us-east-1');scheduler=boto3.client('scheduler',region_name='us-east-1')
    with report(report_name) as r:
        subprocess.run([sys.executable,str(ROOT/'aws/lambdas'/FUNCTION/'tests/run_tests.py')],cwd=ROOT,check=True)
        paths=['aws/lambdas/'+FUNCTION+'/source','aws/lambdas/'+FUNCTION+'/config.json',*['aws/shared/'+m.__name__+'.py' for m in store.COMPILERS]]
        commit=subprocess.check_output(['git','log','-1','--format=%H','--',*paths],cwd=ROOT,text=True).strip();assert re.fullmatch('[a-f0-9]{40}',commit)
        actual=runtime(lam,s3,events,scheduler,FUNCTION);assert actual['receipt']=={'status':'matched','commit':commit}
        assert actual['memory_mb']==2048 and actual['timeout']==600
        read=store.reader(s3,BUCKET);qualified=json.loads(upstream.protected(QUALIFIED,read))
        assert qualified['privacy_verified'] is True and qualified['candidate_replay']==RECOVERY
        assert qualified['runtime_budget_pass'] and qualified['memory_budget_pass']
        assert actual['schedules']==qualified['predecessor_runtime']['schedules']
        for field in ('function_name','runtime','handler','architectures','role','ephemeral_storage_mb'):
            assert actual[field]==qualified['predecessor_runtime'][field]
        assert upstream.sha((ROOT/'tests/fixtures/dealer-gex-v1.3.0.py.txt').read_bytes())=='f1e66c10c0f0029bb11db3bbeb3d3f7402cc6cd8a11ad06c3a34912c2e98b321'
        # Every consumer is inspected, never invoked. Qualified aliases are checked too.
        packages=check_packages(lam,ROOT,CONSUMERS)
        failures=[{k:v for k,v in row.items() if k!='files'}|{'source_mismatches':[f for f in row['files'] if not f['match']]} for row in packages if not row['pass']]
        assert not failures,failures
        consumer_commits=consumer_receipts(packages)
        assert json.loads(public('data/ops/releases/'+FUNCTION+'.json'))['commit']==commit
        build=json.loads(public('build-manifest.json'));pages_commit=build['commit_sha']
        subprocess.run(['git','merge-base','--is-ancestor',commit,pages_commit],cwd=ROOT,check=True)
        subprocess.run(['git','diff','--quiet','HEAD',pages_commit,'--',*ASSETS],cwd=ROOT,check=True)
        for name in ASSETS:
            raw=public(name);clean,n=re.subn(rb'<script\b[^>]*\bsrc="https://static\.cloudflareinsights\.com/beacon\.min\.js/v[a-f0-9]+"[^>]*></script>\n?',b'',raw)
            assert n<=1 and upstream.sha(clean)==build['files_sha256'][name],name
            if name in ('gex/index.html','0dte/index.html'):assert b'/jh-option-populations-page.js' in raw
        history_before=read(desk.HISTORY);history_ref=source_store.protect(s3,BUCKET,history_before)
        r.kv(commit=commit,runtime=actual,consumer_packages=packages,consumer_release_commits=consumer_commits,pages_commit=pages_commit,whole_current_history=history_ref,qualified_replay=RECOVERY)
        request=invoke(lam,s3,commit);r.kv(completed_request=request)
        profile=execution_profile(boto3.client('logs',region_name='us-east-1'),request['status'])
        raw=public(desk.CURRENT);packet=json.loads(raw);assert packet==current(s3) and packet['replay']==request['status']['replay']
        output=store.replay(packet['replay'],read);assert output=={k:v for k,v in packet.items() if k!='replay'}
        run=store.verified_run(packet['replay'],read);inputs=store.checked(run['input'],'inputs',read)
        source=model.verified_packet(upstream.protected(inputs['source_publication'],read),read)
        source_run=source_store.verified_run(source['replay'],read);source_inputs=upstream.checked(source_run['input'],read,'inputs')
        protected={QUALIFIED['key'],history_ref['key'],request['status_key'],inputs['source_publication']['key']}
        attempt=request
        while attempt:
            protected.update(attempt[k] for k in ('status_key','dispatch_key') if attempt.get(k))
            attempt=attempt.get('earlier_completed_request')
        protected.update(ref['key'] for ref in inputs['predecessors'].values() if ref is not None)
        counts=Counter();checks={}
        for symbol in model.UNDERLYINGS:
            item=output['underlyings'][symbol];derived=model.restore(item['population'],read)
            summary=upstream.checked(source['chains'][symbol]['chain'],read,'chains')
            result=independent(derived,summary,source_inputs['chains'][symbol],read)
            result['verified_membership_rows']=verify_memberships(item,source,read)
            checks[symbol]=result;counts.update({k:v for k,v in result.items() if type(v) is int})
            protected.update(p['original']['key'] for p in source_inputs['chains'][symbol]['pages'] if p.get('original'))
        assert all(packet[k] is False for k in model.PERMISSIONS) and packet['call'] is None and packet['independent_investment_votes']==0
        alias=desk.compatibility(packet);assert json.loads(public(desk.LEGACY))==alias and json.loads(read(desk.LEGACY))==alias
        assert read(desk.HISTORY)==history_before and upstream.protected(history_ref,read)==history_before
        assert len(json.loads(history_before)['history'])>=720
        for key in (desk.CURRENT,desk.LEGACY):
            assert s3.head_object(Bucket=BUCKET,Key=key)['CacheControl']=='no-store'
            with urllib.request.urlopen(urllib.request.Request('https://justhodl.ai/'+key,method='HEAD',headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=30) as response:assert 'no-store' in response.headers.get('Cache-Control',''),key
        def deny(key):assert denied_with_retry('https://justhodl.ai/'+key) and denied_with_retry('https://'+BUCKET+'.s3.amazonaws.com/'+key)
        with ThreadPoolExecutor(max_workers=4) as pool:
            for _ in pool.map(deny,sorted(protected)):pass
        assert runtime(lam,s3,events,scheduler,FUNCTION)==actual
        proof={'contract':'option-population-native-acceptance.v1','generated_at':store.now(),'commit':commit,
            'runtime_package':actual,'consumer_packages':packages,'consumer_release_commits':consumer_commits,'request':request,'execution_profile':profile,
            'publication':{'key':desk.CURRENT,'sha256':upstream.sha(raw),'bytes':len(raw),'replay':packet['replay'],'generated_at':packet['generated_at'],'source_capture_completed_at':packet['source_capture_completed_at']},
            'qualified_candidate':RECOVERY,'original_replay_matches':True,'independent_source_checks':checks,'counts':dict(counts),
            'source_run':source['replay'],'whole_predecessor_history':history_ref,'whole_predecessor_history_rows':len(json.loads(history_before)['history']),
            'legacy_history_unchanged':True,'compatibility_alias_verified':True,'protected_artifacts_checked':len(protected),'originals_anonymously_denied':True,
            'pages_commit':pages_commit,'assets':{name:build['files_sha256'][name] for name in ASSETS},
            'producer_invocations_this_acceptance':int(request['invoke_sent']),'provider_requests':0,'consumer_invocations':0,
            'private_account_reads':0,'paid_ai_calls':0,'notifications_sent':0,'signals_emitted':0,'portfolio_writes':0,'schedules_changed':0}
        body=upstream.encoded(proof);s3.put_object(Bucket=BUCKET,Key=PROOF,Body=body,ContentType='application/json',CacheControl='no-store');assert public(PROOF)==body
        r.kv(proof_key=PROOF,publication=proof['publication'],counts=dict(counts),execution_profile=profile,protected_artifacts_checked=len(protected),
            whole_predecessor_history_rows=proof['whole_predecessor_history_rows'],consumer_packages_checked=len(packages),
            producer_invocations_this_acceptance=proof['producer_invocations_this_acceptance'],provider_requests=0,consumer_invocations=0,
            private_account_reads=0,paid_ai_calls=0,notifications_sent=0,signals_emitted=0,portfolio_writes=0,schedules_changed=0)


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
