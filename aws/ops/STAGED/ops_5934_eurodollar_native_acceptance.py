"""Accept only the native public eurodollar producer and its exact deployed consumers.

One idempotent public producer request, protected original-source replay and live
page verification. No consumer invocation, private account read, paid AI,
notification, portfolio write or permission expansion.
"""
from pathlib import Path
from datetime import datetime,timezone
import base64,hashlib,io,json,re,subprocess,sys,time,urllib.request,urllib.error,zipfile
import boto3
from botocore.config import Config
ROOT=Path(__file__).resolve().parents[3]
SOURCE=ROOT/'aws/lambdas/justhodl-eurodollar-stress/source'
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/checks'),str(SOURCE),str(ROOT/'aws/shared'),str(ROOT/'scripts')]
from ops_report import report
from release_package_evidence import shared_imports
from eurodollar_research_store import bounded
import eurodollar_research_store as store
import eurodollar_research_model as model
from eurodollar_research import context as research_context
from replay_eurodollar_research import verify as replay_verify
BUCKET='justhodl-dashboard-live';FN='justhodl-eurodollar-stress'
COMMIT='fcadc6d7f5d06e2199fa3546701721c2b8994da3'
FUNCTIONS=('justhodl-eurodollar-stress', 'justhodl-ai-brief', 'justhodl-alert-router', 'justhodl-allocator', 'justhodl-auction-interpreter', 'justhodl-boj-detail', 'justhodl-calibration-fleet', 'justhodl-capitulation', 'justhodl-chart-data', 'justhodl-dollar-radar', 'justhodl-ecb-detail', 'justhodl-kb-matcher', 'justhodl-market-interpreter', 'justhodl-master-allocator', 'justhodl-page-ai-commentary', 'justhodl-regime-conditional-router', 'justhodl-repo-lending', 'justhodl-reversal-radar', 'justhodl-snb-detail', 'justhodl-stress-scenarios', 'justhodl-wave-signal-logger', 'openbb-websocket-broadcast')
EXPECTED={fn:COMMIT for fn in FUNCTIONS}
ASSETS=('eurodollar.html','macro-rooms.html','plumbing.html','jh-eurodollar-research.js')
REQUEST_ID='chatgpt-eurodollar-native-'+COMMIT[:12]+'-1'


def archive_bytes(stream):
    try:raw=stream.read(64*1024*1024+1)
    finally:stream.close()
    assert len(raw)<=64*1024*1024,'AWS package exceeds reviewed inspection bound'
    return raw


def public(key):
    with urllib.request.urlopen(urllib.request.Request('https://justhodl.ai/'+key,headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=45) as response:
        return bounded(response)


def denied(url):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,method='HEAD',headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=25):return False
    except urllib.error.HTTPError as exc:return exc.code in (401,403,404)


def runtime(lam,fn):
    source=ROOT/'aws/lambdas'/fn/'source';config_path=source.parent/'config.json'
    configuration=json.loads(config_path.read_bytes()) if config_path.exists() else {}
    request={'FunctionName':fn}
    if configuration.get('release_validation') or fn in ('justhodl-engine-fusion','justhodl-khalid-risk'):request['Qualifier']='live'
    deployed=lam.get_function(**request);cfg=deployed['Configuration'];receipt=json.loads(public('data/ops/releases/'+fn+'.json'))
    expected_commit=EXPECTED[fn]
    assert receipt['commit']==expected_commit and receipt['code_sha256']==cfg['CodeSha256'],'Exact receipt required: '+fn
    assert cfg['State']=='Active' and cfg['LastUpdateStatus']=='Successful'
    archive=archive_bytes(urllib.request.urlopen(deployed['Code']['Location'],timeout=45))
    assert base64.b64encode(hashlib.sha256(archive).digest()).decode()==cfg['CodeSha256'],'AWS archive hash differs: '+fn
    files=[ROOT/p for p in subprocess.check_output(['git','ls-files',str(source.relative_to(ROOT))],cwd=ROOT,text=True).splitlines()]
    expected={p.relative_to(source).as_posix():p for p in files}
    expected.update({p.name:p for p in shared_imports(ROOT,files) if not (source/p.name).exists()})
    with zipfile.ZipFile(io.BytesIO(archive)) as z:
        for name,path in expected.items():assert z.read(name)==path.read_bytes(),'Packaged source differs: '+fn+'/'+name
    alias=None
    if request.get('Qualifier'):
        a=lam.get_alias(FunctionName=fn,Name='live')
        assert a['FunctionVersion']==cfg['Version'] and not (a.get('RoutingConfig') or {}).get('AdditionalVersionWeights')
        alias={'name':'live','version':cfg['Version'],'weighted_secondary_versions':0}
    if fn==FN:assert cfg['MemorySize']==512 and cfg['Timeout']==120
    return {'commit':expected_commit,'code_sha256':cfg['CodeSha256'],'packaged_files_checked':len(expected),'all_packaged_sources_match':True,'alias':alias}


def main():
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=45,connect_timeout=10,retries={'max_attempts':0},tcp_keepalive=True))
    s3=boto3.client('s3',region_name='us-east-1');events=boto3.client('events',region_name='us-east-1')
    with report('ops_5934_eurodollar_native_acceptance') as r:
        subprocess.run([sys.executable,str(SOURCE.parent/'tests/run_tests.py')],cwd=ROOT,check=True)
        subprocess.run([sys.executable,str(ROOT/'tests/eurodollar_consumer_test_support.py')],cwd=ROOT,check=True)
        subprocess.run([sys.executable,str(ROOT/'aws/shared/tests/test_canonical_fred_replay.py')],cwd=ROOT,check=True)
        sys.path.insert(0,str(SOURCE.parent/'tests'))
        from native_eurodollar_tests import store_fixture,Storage,STAMP
        objects,i=store_fixture();memory=Storage(objects);memory.objects[store.SOURCES[0]]=model.encoded(i['macro'])
        check_inputs={'contract':'eurodollar-native-inputs.v1','generated_at':STAMP,'macro':store.snapshot(memory,'b',store.SOURCES[0]),'fx':None}
        portable=model.sha(model.encoded(store.compile_output(check_inputs,store.reader(memory,'b'))))
        assert portable=='f7287d076e92bea6581c12f2552eee952c91947cd5af9eeff2d44e8047770c76'
        runtimes={fn:runtime(lam,fn) for fn in FUNCTIONS}
        r.kv(runtimes=runtimes,acceptance_consumer_invocations=0,private_account_reads=0,
            provider_requests=0,paid_ai_calls=0,notifications_sent=0,portfolio_writes=0)
        legacy=subprocess.check_output(['git','show','955d4fe8d:aws/lambdas/justhodl-eurodollar-stress/source/lambda_function.py'],cwd=ROOT)
        assert len(legacy)==13048 and (SOURCE/'legacy_eurodollar_stress.py').read_bytes()==legacy
        preflight_digest='2c67da94ef16e8f688430573847d7996052bd4c9921e585e47baf7414e8f7a3d'
        read=store.reader(s3,BUCKET);preflight_raw=read(model.PRIVATE+preflight_digest+'.bin')
        assert len(preflight_raw)==6225 and model.sha(preflight_raw)==preflight_digest
        preflight=json.loads(preflight_raw)
        for key in (store.CURRENT,*store.SOURCES):
            item=preflight['packets'][key];raw=read(item['key'])
            assert len(raw)==item['bytes'] and model.sha(raw)==item['sha256']
        prior_inputs={'contract':'eurodollar-native-inputs.v1','generated_at':preflight['generated_at'],
            'macro':{**preflight['packets'][store.SOURCES[0]],'source_key':store.SOURCES[0]},
            'fx':{**preflight['packets'][store.SOURCES[1]],'source_key':store.SOURCES[1]}}
        prior=store.compile_output(prior_inputs,read)
        assert prior['quality']['within_age_ceiling']==9
        assert prior['measurements']['BAMLH0A0HYM2']['value_bps']==270
        assert prior['measurements']['DTWEXBGS']['observation_date']=='2026-09-11'
        assert prior['repo_comparison']['current_comparison_available']
        r.kv(preflight_original_reconstruction={'quality':prior['quality'],'source_series':9,
            'output_sha256':model.sha(model.encoded(prior)),'manifest_sha256':preflight_digest})
        build=json.loads(public('build-manifest.json'));pages_commit=build['commit_sha']
        subprocess.run(['git','merge-base','--is-ancestor',COMMIT,pages_commit],cwd=ROOT,check=True)
        subprocess.run(['git','diff','--quiet',COMMIT,pages_commit,'--',*ASSETS],cwd=ROOT,check=True)
        for name in ASSETS:
            served=public(name);clean,count=re.subn(rb'<script\b[^>]*\bsrc="https://static\.cloudflareinsights\.com/beacon\.min\.js/v[a-f0-9]+"[^>]*></script>\n?',b'',served)
            assert count<=1 and model.sha(clean)==build['files_sha256'][name],'Built asset differs: '+name
            if name.endswith('.html'):assert b'jh-eurodollar-research.js?v=20260920-native1' in served
        cfg=lam.get_function_configuration(FunctionName=FN);names=[]
        for page in events.get_paginator('list_rule_names_by_target').paginate(TargetArn=cfg['FunctionArn']):names.extend(page['RuleNames'])
        assert names==['justhodl-eurodollar-stress-2h'],'Existing schedule inventory changed'
        rule=events.describe_rule(Name=names[0]);targets=events.list_targets_by_rule(Rule=names[0])['Targets']
        selected=[t for t in targets if t.get('Arn')==cfg['FunctionArn']]
        assert rule['State']=='ENABLED' and rule['ScheduleExpression']=='cron(16 21 * * ? *)' and len(selected)==1
        assert not json.loads(selected[0].get('Input') or '{}').get('request_id') and not selected[0].get('InputTransformer')
        # A transport uncertainty uses the same request; it never causes a second invocation.
        status_key=store.request_key(REQUEST_ID);invoked=False
        try:status=json.loads(bounded(s3.get_object(Bucket=BUCKET,Key=status_key)['Body']))
        except Exception as exc:
            if not store.missing(exc):raise
            response=lam.invoke(FunctionName=FN,InvocationType='Event',Payload=model.encoded({'request_id':REQUEST_ID}))
            assert response['StatusCode']==202;invoked=True;status=None
        deadline=time.monotonic()+180
        while time.monotonic()<deadline:
            try:status=json.loads(bounded(s3.get_object(Bucket=BUCKET,Key=status_key)['Body']))
            except Exception as exc:
                if not store.missing(exc):raise
            if status and status.get('status') in ('complete','failed'):break
            time.sleep(4)
        r.kv(request_id=REQUEST_ID,status_key=status_key,public_producer_invoke_sent=invoked,request_status=status)
        assert status and status.get('status')=='complete' and status.get('published') is True
        raw=public(store.CURRENT);packet=json.loads(raw)
        assert packet['generated_at']==status['generated_at'] and packet['replay']==status['replay']
        assert packet['quality']['within_age_ceiling']==9 and packet['generated_at']>preflight['generated_at']
        assert all(packet[k] is False for k in model.PERMISSIONS) and packet['call'] is None
        assert json.loads(read(store.CURRENT))==packet
        replay=replay_verify(packet,read);context=research_context(packet)
        assert context['available'] and len(context['measurements'])==9 and context['repo_comparison'] is not None
        run=json.loads(read(packet['replay']['manifest_key']));inputs=store.checked(run['input'],'inputs',read)
        for item in (inputs['macro'],inputs['fx']):
            if item:assert denied('https://justhodl.ai/'+item['key']) and denied('https://'+BUCKET+'.s3.amazonaws.com/'+item['key'])
        proof={'contract':'eurodollar-native-acceptance.v1','verified_at':datetime.now(timezone.utc).isoformat(),
            'runtime_commit':COMMIT,'runtimes':runtimes,'pages_commit':pages_commit,'portable_reference_digest':portable,
            'assets':{name:build['files_sha256'][name] for name in ASSETS},'public_sha256':model.sha(raw),
            'generated_at':packet['generated_at'],'source_generated_at':packet['source_generated_at'],
            'replay':packet['replay'],'source_replay':replay,'canonical_source_run':packet['dependency_graph']['source_run'],
            'measurements':{sid:{k:m[k] for k in ('value','unit','observation_date','history_coverage')} for sid,m in packet['measurements'].items()},
            'preflight_original_reconstruction_sha256':model.sha(model.encoded(prior)),
            'schedule':{'name':names[0],'state':rule['State'],'expression':rule['ScheduleExpression'],'native_targets':1},
            'request_id':REQUEST_ID,'execution_id':status['execution_id'],'acceptance_public_producer_invocations':int(invoked),
            'acceptance_consumer_invocations':0,'private_account_reads':0,'provider_requests':0,'paid_ai_calls':0,'notifications_sent':0,'portfolio_writes':0,
            'remaining':'Current-vintage bounded source windows; no release-calendar, historical first-availability or forecast qualification. FX context remains unverified. Reused inputs do not become independent votes.'}
        key='data/eurodollar-research-verification.json'
        s3.put_object(Bucket=BUCKET,Key=key,Body=model.encoded(proof),ContentType='application/json',CacheControl='no-store')
        assert json.loads(public(key))==proof
        r.kv(accepted=True,proof_key=key,generated_at=packet['generated_at'],reviewed_series=9,source_replay=replay)

if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
