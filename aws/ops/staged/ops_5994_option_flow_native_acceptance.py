"""Accept native captured option research using qualified originals only.

Runner IAM; one durable producer invocation with zero source recollection.
No private-account, decision, notification, paid AI or portfolio consumers.
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
from ops_5975_etf_constituent_source_preflight import denied,runtime
from ops_5966_sector_tilt_runtime_diagnosis import parse_runtime
from ops_5993_option_flow_recovered_qualification import verify_records,original_keys
import option_flow_store as store
import option_flow_research as model
BUCKET='justhodl-dashboard-live'
FUNCTION='justhodl-polygon-options-flow'
PREDECESSOR='0357d3e0ecac4c078ad33c8ac12ee7a4e28b6d2b'
RECOVERY={'manifest_key': 'data/option-flow-research/runs/4eb96b912e0d745388120c0af1d0769038c6f74889cb11f6c9afc4f6d8087674.json', 'output_sha256': '3c0eafc805c5416c7452ab2fff2814de6ffc8b6d83b1947187e79397e01a6abf'}
QUALIFICATION={'key': 'audit-private/20260909-originals/options-research/cc34937a95c88803a65fb3fb35ce11863bc404b094cdaa180a84f374082cff32.bin', 'sha256': 'cc34937a95c88803a65fb3fb35ce11863bc404b094cdaa180a84f374082cff32', 'bytes': 20015}
PROOF='data/option-flow-research-verification.json'
ASSETS=('option-chain-research.html','options.html','jh-option-research.js','jh-option-research-page.js','jh-option-research-hub.js','jh-option-research.css','jh-data-feeds.js')


def public(key):
    with urllib.request.urlopen(urllib.request.Request('https://justhodl.ai/'+key,
            headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=45) as response:
        return store.bounded(response)


def runtime_commit():
    paths=['aws/lambdas/'+FUNCTION+'/source','aws/lambdas/'+FUNCTION+'/config.json']
    paths += ['aws/shared/'+m.__name__+'.py' for m in store.COMPILERS]
    commit=subprocess.check_output(['git','log','-1','--format=%H','--',*paths],cwd=ROOT,text=True).strip()
    assert re.fullmatch('[a-f0-9]{40}',commit)
    return commit


def invoke(lam,s3,commit):
    request='chatgpt-'+FUNCTION+'-'+commit[:12]+'-1'
    key=store.request_key(request);dispatch_key=store.request_key(request+'-dispatch');sent=False
    claim={'contract':'option-flow-native-dispatch.v1','request_id':request,'started_at':store.now(),'status':'claimed','recovery':RECOVERY}
    try:store.status_write(s3,BUCKET,dispatch_key,claim,IfNoneMatch='*')
    except Exception as exc:
        if not store.conflict(exc):raise
        old=json.loads(store.bounded(s3.get_object(Bucket=BUCKET,Key=dispatch_key)['Body']))
        assert old['request_id']==request and old['contract']==claim['contract'] and old['recovery']==RECOVERY
    else:
        response,rejected=invoke_when_available(lam,{'FunctionName':FUNCTION,'InvocationType':'Event',
            'Payload':model.encoded({'request_id':request,'recover_run':RECOVERY})})
        assert response['StatusCode']==202;sent=True
        claim.update(status='accepted_async',throttle_rejections_before_acceptance=rejected)
        store.status_write(s3,BUCKET,dispatch_key,claim)
    status=None;deadline=time.monotonic()+960
    while time.monotonic()<deadline:
        try:status=json.loads(store.bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body']))
        except Exception as exc:
            if not store.missing(exc):raise
        if status and status.get('status') in ('complete','failed'):break
        time.sleep(3)
    assert status and status.get('status')=='complete' and status.get('published') is True,'Inspect retained request; never blindly reinvoke'
    assert status['provider_requests_this_execution']==0 and status['provider_requests']==869
    assert status['recovered_from']==RECOVERY and status['replay']['output_sha256']==RECOVERY['output_sha256']
    assert status['generated_at']=='2026-09-21T13:13:04.790664+00:00'
    assert status['compatibility_published'] is True
    return {'request_id':request,'status_key':key,'dispatch_key':dispatch_key,'invoke_sent':sent,'status':status}


def execution_profile(logs,status):
    execution=status['execution_id'];assert re.fullmatch('[a-f0-9-]{36}',execution)
    deadline=time.monotonic()+45;start=model.clock(status['started_at'])
    while True:
        rows=logs.filter_log_events(logGroupName='/aws/lambda/'+FUNCTION,filterPattern='"'+execution+'"',
            startTime=int(start.timestamp()*1000)-5000,limit=50).get('events',[])
        values=[parse_runtime(row.get('message',''),execution) for row in rows];values=[v for v in values if v]
        if values:break
        assert time.monotonic()<deadline,'Read completed execution profile; do not reinvoke'
        time.sleep(3)
    assert all(v.get('status') not in ('error','timeout') and v.get('memory_mb')==4096
        and v.get('max_memory_mb',4096)<4096 and v.get('duration_ms',900000)<900000 for v in values)
    return {'execution_id':execution,'managed_reports':values,'completed_request':True}


def main():
    s3=boto3.client('s3',region_name='us-east-1',config=Config(max_pool_connections=24))
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=60,connect_timeout=10,retries={'max_attempts':0}))
    events=boto3.client('events',region_name='us-east-1');scheduler=boto3.client('scheduler',region_name='us-east-1')
    with report('ops_5994_option_flow_native_acceptance') as r:
        subprocess.run([sys.executable,str(ROOT/'aws/lambdas'/FUNCTION/'tests/run_tests.py')],cwd=ROOT,check=True)
        commit=runtime_commit();actual=runtime(lam,s3,events,scheduler,FUNCTION)
        assert actual['receipt']=={'status':'matched','commit':commit} and actual['timeout']==900 and actual['memory_mb']==4096
        assert json.loads(public('data/ops/releases/'+FUNCTION+'.json'))['commit']==commit
        read=store.reader(s3,BUCKET);qualification=json.loads(model.protected(QUALIFICATION,read))
        assert qualification['candidate_replay']==RECOVERY and qualification['runtime_budget_pass'] and qualification['memory_budget_pass']
        assert actual['schedules']==qualification['predecessor_runtime']['schedules']
        for field in ('function_name','runtime','handler','architectures','role','ephemeral_storage_mb'):
            assert actual[field]==qualification['predecessor_runtime'][field]
        assert (ROOT/'aws/lambdas'/FUNCTION/'source/legacy_polygon_options_flow.py').read_bytes()==subprocess.check_output(
            ['git','show',PREDECESSOR+':aws/lambdas/'+FUNCTION+'/source/lambda_function.py'],cwd=ROOT)
        assert (ROOT/'docs/legacy/options-hub-pre-native-flow-20260921.txt').read_bytes()==subprocess.check_output(
            ['git','show',PREDECESSOR+':options.html'],cwd=ROOT)
        build=json.loads(public('build-manifest.json'));pages_commit=build['commit_sha']
        subprocess.run(['git','merge-base','--is-ancestor',commit,pages_commit],cwd=ROOT,check=True)
        subprocess.run(['git','diff','--quiet','HEAD',pages_commit,'--',*ASSETS],cwd=ROOT,check=True)
        for name in ASSETS:
            raw=public(name);clean,count=re.subn(rb'<script\b[^>]*\bsrc="https://static\.cloudflareinsights\.com/beacon\.min\.js/v[a-f0-9]+"[^>]*></script>\n?',b'',raw)
            assert count<=1 and model.sha(clean)==build['files_sha256'][name],'Live asset differs: '+name
            if name.endswith('.html'):assert b'/jh-option-research.js' in raw
        r.kv(commit=commit,runtime_package=actual,pages_commit=pages_commit,qualified_output_sha256=RECOVERY['output_sha256'])
        inputs,candidate=store.recovery_inputs(RECOVERY,read)
        request=invoke(lam,s3,commit)
        profile=execution_profile(boto3.client('logs',region_name='us-east-1'),request['status'])
        raw=public(model.CURRENT);packet=json.loads(raw)
        assert read(model.CURRENT)==raw and packet['replay']==request['status']['replay']
        assert {k:v for k,v in packet.items() if k!='replay'}==candidate
        assert store.replay(packet['replay'],read)==candidate
        assert len(packet['chains'])==len(qualification['coverage']) and all(packet[k] is False for k in model.PERMISSIONS)
        assert packet['call'] is None and packet['portfolio_action']=='WAIT' and packet['independent_investment_votes']==0
        counts=Counter()
        for symbol,entry in packet['chains'].items():
            summary=model.checked(entry['chain'],read,'chains')
            counts.update(verify_records(inputs['chains'][symbol],summary,read))
        assert dict(counts)==qualification['independent_checks'] and packet['quality']==qualification['quality']
        expected_alias=store.compatibility(packet)
        assert json.loads(public(model.LEGACY))==expected_alias and json.loads(read(model.LEGACY))==expected_alias
        for key in (model.CURRENT,model.LEGACY):
            assert s3.head_object(Bucket=BUCKET,Key=key)['CacheControl']=='no-store'
            with urllib.request.urlopen(urllib.request.Request('https://justhodl.ai/'+key,method='HEAD',headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=30) as response:
                assert 'no-store' in response.headers.get('Cache-Control',''),'Mutable edge response cached: '+key
        protected=original_keys(inputs)|{QUALIFICATION['key'],request['status_key'],request['dispatch_key']}
        def check(key):assert denied('https://justhodl.ai/'+key) and denied('https://'+BUCKET+'.s3.amazonaws.com/'+key)
        with ThreadPoolExecutor(max_workers=8) as pool:
            for _ in pool.map(check,sorted(protected)):pass
        assert runtime(lam,s3,events,scheduler,FUNCTION)==actual
        proof={'contract':'option-flow-native-acceptance.v1','generated_at':store.now(),'commit':commit,
            'runtime_package':actual,'request':request,'execution_profile':profile,
            'publication':{'key':model.CURRENT,'sha256':model.sha(raw),'bytes':len(raw),'replay':packet['replay'],'generated_at':packet['generated_at']},
            'qualified_candidate':RECOVERY,'original_replay_matches':True,'independent_source_checks':dict(counts),
            'selected_underlyings':len(packet['chains']),'whole_predecessors_retained':True,
            'compatibility_alias':{'key':model.LEGACY,'contract':expected_alias['contract'],'matches':True},
            'unchanged_schedules':actual['schedules'],'protected_artifacts_checked':len(protected),'originals_and_requests_anonymously_denied':True,
            'pages_commit':pages_commit,'assets':{name:build['files_sha256'][name] for name in ASSETS},
            'producer_invocations_this_acceptance':int(request['invoke_sent']),'provider_requests_this_acceptance':0,
            'private_account_reads':0,'paid_ai_calls':0,'notifications_sent':0,'signals_emitted':0,'portfolio_writes':0,'unreviewed_consumer_invocations':0}
        proof_raw=model.encoded(proof)
        s3.put_object(Bucket=BUCKET,Key=PROOF,Body=proof_raw,ContentType='application/json',CacheControl='no-store')
        assert public(PROOF)==proof_raw
        r.kv(proof_key=PROOF,publication=proof['publication'],independent_source_checks=dict(counts),
            execution_profile=profile,protected_artifacts_checked=len(protected),selected_underlyings=len(packet['chains']),
            compatibility_alias_verified=True,provider_requests_this_acceptance=0,
            producer_invocations_this_acceptance=int(request['invoke_sent']),private_account_reads=0,paid_ai_calls=0,
            notifications_sent=0,signals_emitted=0,portfolio_writes=0)


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
