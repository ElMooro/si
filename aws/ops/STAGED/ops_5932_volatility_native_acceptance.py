"""Accept only the native public volatility producer and its exact deployed consumers.

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
SOURCE=ROOT/'aws/lambdas/justhodl-vol-surface/source'
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/checks'),str(SOURCE),str(ROOT/'aws/shared'),str(ROOT/'scripts')]
from ops_report import report
from release_package_evidence import shared_imports
from volatility_research_store import bounded
import volatility_research_store as store
import volatility_research_model as model
from volatility_research import context as research_context
from replay_volatility_research import verify as replay_verify
BUCKET='justhodl-dashboard-live';FN='justhodl-vol-surface'
COMMIT='43c1944a3ffaaf371b238252155be56af18268a8'
FUNCTIONS=tuple('justhodl-'+name for name in ('vol-surface','capitulation','hedge-planner','hedge-pnl','portfolio-analytics','kb-matcher','tail-hedge'))
EXPECTED={fn:COMMIT for fn in FUNCTIONS}
ASSETS=('volatility.html','jh-volatility-research.js','intelligence/index.html')
REQUEST_ID='chatgpt-volatility-native-'+COMMIT[:12]+'-1'


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
    if fn==FN:assert cfg['MemorySize']==256 and cfg['Timeout']==300
    return {'commit':expected_commit,'code_sha256':cfg['CodeSha256'],'packaged_files_checked':len(expected),'all_packaged_sources_match':True,'alias':alias}


def main():
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=45,connect_timeout=10,retries={'max_attempts':0},tcp_keepalive=True))
    s3=boto3.client('s3',region_name='us-east-1')
    events=boto3.client('events',region_name='us-east-1')
    with report('ops_5932_volatility_native_acceptance') as r:
        subprocess.run([sys.executable,str(SOURCE.parent/'tests/run_tests.py')],cwd=ROOT,check=True)
        subprocess.run([sys.executable,str(ROOT/'tests/volatility_consumer_test_support.py')],cwd=ROOT,check=True)
        sys.path.insert(0,str(SOURCE.parent/'tests'))
        from volatility_fixtures import fixture
        from storage_volatility_tests import Memory
        inputs,bodies=fixture();memory=Memory();memory.objects.update(bodies)
        output=store.compile_output(inputs,store.reader(memory,'test'))
        portable=model.sha(model.encoded(output))
        assert portable=='1d5a40760994f9221632b1e6a723b6ca51ec2ad36a2a1a9d5359bb589d453c3b'
        ref=store.retain(memory,'test',inputs,output)
        assert replay_verify({**output,'replay':ref},store.reader(memory,'test'))['replayed']
        runtimes={fn:runtime(lam,fn) for fn in FUNCTIONS}
        r.kv(runtimes=runtimes,portable_reference_digest=portable,acceptance_consumer_invocations=0,
            private_account_reads=0,paid_ai_calls=0,notifications_sent=0,portfolio_writes=0)
        legacy=subprocess.check_output(['git','show','c21783154'+':aws/lambdas/justhodl-vol-surface/source/lambda_function.py'],cwd=ROOT)
        assert len(legacy)==18913 and (SOURCE/'legacy_vol_surface.py').read_bytes()==legacy
        for digest,size in (('b7c356804a7bf9dc66df540418e6c755afc6916b1689824ede6a6bdbbf856a0e',3423),
                            ('b1cfe75e7b4872ac48bba30a4e278161bb77538c136c04aac3890422c2bc0a9b',39740)):
            key=model.PRIVATE+digest+'.bin';raw=store.reader(s3,BUCKET)(key)
            assert len(raw)==size and model.sha(raw)==digest
            assert denied('https://justhodl.ai/'+key)
        preflight_digest='59c2b8c744ee03b7221d609ce59fc430b60734c8c28362719f863d69e46e8fb9'
        preflight_raw=store.reader(s3,BUCKET)(model.PRIVATE+preflight_digest+'.bin')
        assert model.sha(preflight_raw)==preflight_digest and len(preflight_raw)==35148
        preflight=json.loads(preflight_raw)
        started=min(v['acquired_at'] for v in preflight['sources'].values())
        prior_inputs={'contract':'volatility-native-inputs.v1','started_at':started,'generated_at':preflight['generated_at'],
            'evaluation_date':preflight['evaluation_date'],'sources':{}}
        for label,definition in model.CATALOG.items():
            prior_inputs['sources'][label]={}
            for kind in ('definition','observations'):
                item=preflight['sources'][kind+':'+definition['fred']]
                prior_inputs['sources'][label][kind]={'acquired_at':item['acquired_at'],
                    'evidence':{**{k:item[k] for k in ('key','sha256','bytes','provider','request_url')},
                        'access':'protected_AWS_IAM_source_archive'}}
        for label,item in preflight['publisher'].items():
            prior_inputs['sources'][label]={'observations':{'acquired_at':item['acquired_at'],
                'evidence':{**{k:item[k] for k in ('key','sha256','bytes','provider')},
                    'request_url':item['source_url'],'access':'protected_AWS_IAM_source_archive'}}}
        prior=store.compile_output(prior_inputs,store.reader(s3,BUCKET))
        r.kv(preflight_original_reconstruction={'quality':prior['quality'],'series':len(prior['measurements']),
            'output_sha256':model.sha(model.encoded(prior)),'manifest_sha256':preflight_digest})
        assert prior['quality']['within_age_ceiling']==16,'Preflight original parsing failed; no producer invoked'
        for label,value in (('VIX_30D',15.44),('VIX_3M',18.55),('VVIX',87.38),('SKEW',148.10)):
            assert prior['measurements'][label]['value']==value
        assert prior['tenor_comparison']['current_comparison_available']
        assert all(p['current_comparison_available'] for p in prior['cross_asset_comparisons'].values())
        build=json.loads(public('build-manifest.json'));pages_commit=build['commit_sha']
        subprocess.run(['git','merge-base','--is-ancestor',COMMIT,pages_commit],cwd=ROOT,check=True)
        subprocess.run(['git','diff','--quiet',COMMIT,pages_commit,'--',*ASSETS],cwd=ROOT,check=True)
        for name in ASSETS:
            served=public(name);clean,count=re.subn(rb'<script\b[^>]*\bsrc="https://static\.cloudflareinsights\.com/beacon\.min\.js/v[a-f0-9]+"[^>]*></script>\n?',b'',served)
            assert count<=1 and model.sha(clean)==build['files_sha256'][name],'Built asset differs: '+name
            if name=='volatility.html':assert b'jh-volatility-research.js?v=20260920-native1' in served and b'Volatility indices and position sensitivity' in served
        schedule=[]
        for name,state in (('justhodl-vol-surface-daily','ENABLED'),('justhodl-vol-surface-sched','DISABLED')):
            rule=events.describe_rule(Name=name)
            targets=events.list_targets_by_rule(Rule=name)['Targets']
            selected=[t for t in targets if t.get('Arn','').split(':function:')[-1].split(':')[0]==FN]
            assert rule['State']==state and rule['ScheduleExpression']=='cron(30 13 * * ? *)' and len(selected)==1
            for target in selected:
                payload=json.loads(target.get('Input') or '{}')
                assert not payload.get('request_id') and not target.get('InputTransformer')
                assert target['Arn'].endswith(':function:'+FN),'Schedule must target the reviewed producer'
            schedule.append({'name':name,'state':rule['State'],'expression':rule['ScheduleExpression'],'native_targets':1})
        # One request identity: if a transport result is uncertain, inspect its
        # status instead of invoking again. No raw request body is an AWS secret.
        status_key=store.request_key(REQUEST_ID);invoked=False
        try:status=json.loads(bounded(s3.get_object(Bucket=BUCKET,Key=status_key)['Body']))
        except Exception as exc:
            if not store.missing(exc):raise
            response=lam.invoke(FunctionName=FN,InvocationType='Event',Payload=model.encoded({'request_id':REQUEST_ID}))
            assert response['StatusCode']==202
            invoked=True;status=None
        deadline=time.monotonic()+360
        while time.monotonic()<deadline:
            try:status=json.loads(bounded(s3.get_object(Bucket=BUCKET,Key=status_key)['Body']))
            except Exception as exc:
                if not store.missing(exc):raise
            if status and status.get('status') in ('complete','failed'):break
            time.sleep(5)
        r.kv(request_id=REQUEST_ID,status_key=status_key,public_producer_invoke_sent=invoked,request_status=status)
        assert status and status.get('status')=='complete' and status.get('published') is True,'Native request incomplete or failed; inspect status, do not reinvoke'
        raw=public(store.CURRENT);packet=json.loads(raw)
        assert packet['generated_at']==status['generated_at'] and packet['replay']==status['replay']
        assert packet['quality']['status']=='fresh' and packet['quality']['within_age_ceiling']==16
        assert len(packet['measurements'])==16 and len(packet['source_evidence'])==30
        assert packet['generated_at']>preflight['generated_at']
        assert packet['calls_eligible'] is False and packet['forecast_qualified'] is False and packet['sizing_eligible'] is False and packet['execution_eligible'] is False
        assert packet['call'] is None and packet['portfolio_action']=='WAIT'
        assert json.loads(store.reader(s3,BUCKET)(store.CURRENT))==packet
        replay=replay_verify(packet,store.reader(s3,BUCKET))
        context=research_context(packet);assert context['available'] is True and len(context['measurements'])==16
        assert context['tenor_comparison'] is not None
        originals={}
        for item in packet['source_evidence']:
            original=store.reader(s3,BUCKET)(item['key'])
            assert len(original)==item['bytes'] and model.sha(original)==item['sha256']
            name=item['kind']+':'+item['series_id']
            originals[name]={'sha256':item['sha256'],'bytes':item['bytes'],'acquired_at':item['acquired_at']}
        for item in (packet['source_evidence'][0],next(x for x in packet['source_evidence'] if x['kind']=='observations' and x['series_id']=='VIXCLS')):
            assert denied('https://justhodl.ai/'+item['key']) and denied('https://'+BUCKET+'.s3.amazonaws.com/'+item['key'])
        old_history=bounded(s3.get_object(Bucket=BUCKET,Key='data/vol-surface-history.json')['Body'])
        assert model.sha(old_history)=='b1cfe75e7b4872ac48bba30a4e278161bb77538c136c04aac3890422c2bc0a9b'
        proof={'contract':'volatility-native-acceptance.v1','verified_at':datetime.now(timezone.utc).isoformat(),
            'runtime_commit':COMMIT,'runtimes':runtimes,'pages_commit':pages_commit,
            'assets':{name:build['files_sha256'][name] for name in ASSETS},'portable_reference_digest':portable,
            'public_sha256':model.sha(raw),'generated_at':packet['generated_at'],'as_of':packet['as_of'],
            'replay':packet['replay'],'source_replay':replay,'originals':originals,'measurements':{sid:{k:m[k] for k in ('value','unit','observation_date','history_coverage')} for sid,m in packet['measurements'].items()},
            'preflight_original_reconstruction_sha256':model.sha(model.encoded(prior)),'schedules':schedule,'request_id':REQUEST_ID,'execution_id':status['execution_id'],
            'acceptance_public_producer_invocations':int(invoked),'acceptance_consumer_invocations':0,
            'private_account_reads':0,'paid_ai_calls':0,'notifications_sent':0,'portfolio_writes':0,
            'remaining':'Current-vintage source history only; first availability and release-calendar compliance are unverified. Licensed full originals remain protected. No qualified forecast, sizing or execution.'}
        key='data/volatility-research-verification.json'
        s3.put_object(Bucket=BUCKET,Key=key,Body=model.encoded(proof),ContentType='application/json',CacheControl='no-store')
        assert json.loads(public(key))==proof
        r.kv(accepted=True,proof_key=key,generated_at=packet['generated_at'],as_of=packet['as_of'],reviewed_series=len(packet['measurements']),source_replay=replay)


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
