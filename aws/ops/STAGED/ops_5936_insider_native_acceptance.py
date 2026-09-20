"""Accept only the native public insider producer and its exact deployed consumers.

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
SOURCE=ROOT/'aws/lambdas/justhodl-insider-aggregate/source'
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/checks'),str(SOURCE),str(ROOT/'aws/shared'),str(ROOT/'scripts')]
from ops_report import report
from release_package_evidence import shared_imports
from insider_research_store import bounded
import insider_research_store as store
import insider_research_model as model
from insider_research import context as research_context
from replay_insider_research import verify as replay_verify
BUCKET='justhodl-dashboard-live';FN='justhodl-insider-aggregate'
COMMIT='94fa55d12b8dfb85fc40711d0781a8a6eb31fe68'
FUNCTIONS=('justhodl-insider-aggregate','justhodl-best-ideas','justhodl-capitulation','justhodl-market-extremes','justhodl-spinoff-desk','justhodl-insider-trades')
EXPECTED={fn:COMMIT for fn in FUNCTIONS}
ASSETS=('insider-research.html','jh-insider-research.js','baggers.html','insiders.html','sector-flow.html')
REQUEST_ID='chatgpt-insider-native-'+COMMIT[:12]+'-1'


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
    if fn==FN:assert cfg['MemorySize']==512 and cfg['Timeout']==300
    return {'commit':expected_commit,'code_sha256':cfg['CodeSha256'],'packaged_files_checked':len(expected),'all_packaged_sources_match':True,'alias':alias}


def main():
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=45,connect_timeout=10,retries={'max_attempts':0},tcp_keepalive=True))
    s3=boto3.client('s3',region_name='us-east-1');events=boto3.client('events',region_name='us-east-1')
    with report('ops_5936_insider_native_acceptance') as r:
        subprocess.run([sys.executable,str(SOURCE.parent/'tests/run_tests.py')],cwd=ROOT,check=True)
        subprocess.run([sys.executable,str(ROOT/'tests/insider_consumer_test_support.py')],cwd=ROOT,check=True)
        sys.path.insert(0,str(SOURCE.parent/'tests'))
        from test_native_insider import packet as fixture_packet
        memory,i,p=fixture_packet();portable=p['replay']['output_sha256']
        assert portable=='eba35a54c95553218c7454782a3d1aa98f6e706ea6e27c113a0036b6a4dcc1ed'
        runtimes={fn:runtime(lam,fn) for fn in FUNCTIONS}
        r.kv(runtimes=runtimes,acceptance_consumer_invocations=0,private_account_reads=0,paid_ai_calls=0,notifications_sent=0,portfolio_writes=0)
        legacy=subprocess.check_output(['git','show','c53711534:aws/lambdas/justhodl-insider-aggregate/source/lambda_function.py'],cwd=ROOT)
        assert len(legacy)==12165 and (SOURCE/'legacy_insider_aggregate.py').read_bytes()==legacy
        read=store.reader(s3,BUCKET);digest='03c3bd3bc4410817e9202de09ff91523b8fda6bd2dffd2dbe7095a8c850dc0be'
        raw=read(model.PRIVATE+digest+'.bin');assert len(raw)==8383 and model.sha(raw)==digest
        preflight=json.loads(raw)
        for key in ('data/insider-aggregate.json','data/insider-aggregate-history.json','data/insider-trades.json'):
            ref=preflight['packets'][key];whole=read(ref['key']);assert len(whole)==ref['bytes'] and model.sha(whole)==ref['sha256']
        pages=[{'page':index,'acquired_at':x['acquired_at'],'evidence':{**x['original'],'provider':'Financial Modeling Prep','request_url':x['request_url'],'access':'protected_AWS_IAM_source_archive'}} for index,x in enumerate(preflight['provider_pages'])]
        audit_inputs={'contract':'insider-native-inputs.v1','started_at':pages[0]['acquired_at'],'generated_at':preflight['generated_at'],
            'collection':{'pages':pages,'max_pages':3,'stop_reason':'page_limit','source_bytes':sum(x['evidence']['bytes'] for x in pages)}}
        before=store.compile_output(audit_inputs,read)
        assert before['coverage']['rows_received']==3000 and before['quality']['status']=='partial'
        assert before['coverage']['excluded_reason_counts']['future_or_after_filing_transaction']>0
        assert before['headline_ratio_30d_dollar'] is None and before['regime'] is None
        r.kv(preflight_reconstruction={'output_sha256':model.sha(model.encoded(before)),'coverage':before['coverage'],'windows':before['windows']})
        history_key='data/insider-aggregate-history.json'
        history_before=bounded(s3.get_object(Bucket=BUCKET,Key=history_key)['Body'])
        assert model.sha(history_before)==preflight['packets'][history_key]['sha256']
        build=json.loads(public('build-manifest.json'));pages_commit=build['commit_sha']
        subprocess.run(['git','merge-base','--is-ancestor',COMMIT,pages_commit],cwd=ROOT,check=True)
        subprocess.run(['git','diff','--quiet',COMMIT,pages_commit,'--',*ASSETS],cwd=ROOT,check=True)
        for name in ASSETS:
            served=public(name);clean,count=re.subn(rb'<script\b[^>]*\bsrc="https://static\.cloudflareinsights\.com/beacon\.min\.js/v[a-f0-9]+"[^>]*></script>\n?',b'',served)
            assert count<=1 and model.sha(clean)==build['files_sha256'][name],'Built asset differs: '+name
            if name=='insider-research.html':assert b'jh-insider-research.js?v=20260920-native1' in served
            elif name.endswith('.html'):assert b'/insider-research.html' in served
        cfg=lam.get_function_configuration(FunctionName=FN);names=[]
        for page in events.get_paginator('list_rule_names_by_target').paginate(TargetArn=cfg['FunctionArn']):names.extend(page['RuleNames'])
        assert names==['insider-aggregate-daily'],'Existing schedule inventory changed'
        rule=events.describe_rule(Name=names[0]);targets=events.list_targets_by_rule(Rule=names[0])['Targets']
        selected=[t for t in targets if t.get('Arn')==cfg['FunctionArn']]
        assert rule['State']=='ENABLED' and rule['ScheduleExpression']=='cron(30 22 ? * MON-FRI *)' and len(selected)==1
        assert not json.loads(selected[0].get('Input') or '{}').get('request_id') and not selected[0].get('InputTransformer')
        status_key=store.request_key(REQUEST_ID);invoked=False
        try:status=json.loads(bounded(s3.get_object(Bucket=BUCKET,Key=status_key)['Body']))
        except Exception as exc:
            if not store.missing(exc):raise
            response=lam.invoke(FunctionName=FN,InvocationType='Event',Payload=model.encoded({'request_id':REQUEST_ID}))
            assert response['StatusCode']==202;invoked=True;status=None
        deadline=time.monotonic()+360
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
        assert packet['generated_at']>preflight['generated_at'] and packet['quality']['status']=='partial'
        assert all(packet[k] is False for k in model.PERMISSIONS) and packet['call'] is None
        assert packet['coverage']['population_complete'] is False and packet['headline_ratio_30d_dollar'] is None
        assert json.loads(read(store.CURRENT))==packet
        replay=replay_verify(packet,read);context=research_context(packet);assert context['available']
        assert bounded(s3.get_object(Bucket=BUCKET,Key=history_key)['Body'])==history_before
        run=json.loads(read(packet['replay']['manifest_key']));inputs=store.checked(run['input'],'inputs',read)
        sources=inputs['collection']['pages']
        for item in (sources[0]['evidence'],sources[-1]['evidence']):
            assert denied('https://justhodl.ai/'+item['key']) and denied('https://'+BUCKET+'.s3.amazonaws.com/'+item['key'])
        proof={'contract':'insider-native-acceptance.v1','verified_at':datetime.now(timezone.utc).isoformat(),'runtime_commit':COMMIT,
            'runtimes':runtimes,'pages_commit':pages_commit,'portable_reference_digest':portable,
            'assets':{name:build['files_sha256'][name] for name in ASSETS},'public_sha256':model.sha(raw),
            'generated_at':packet['generated_at'],'as_of':packet['as_of'],'replay':packet['replay'],'source_replay':replay,
            'coverage':packet['coverage'],'windows':packet['windows'],'filing_windows':packet['filing_windows'],
            'preflight_reconstruction_sha256':model.sha(model.encoded(before)),
            'legacy_history_sha256':model.sha(history_before),'legacy_history_unchanged':True,'whole_originals_anonymously_denied':True,
            'schedule':{'name':names[0],'state':rule['State'],'expression':rule['ScheduleExpression'],'native_targets':1},
            'request_id':REQUEST_ID,'execution_id':status['execution_id'],'acceptance_public_producer_invocations':int(invoked),
            'acceptance_consumer_invocations':0,'private_account_reads':0,'paid_ai_calls':0,'notifications_sent':0,'portfolio_writes':0,
            'remaining':'Bounded vendor row representations only. No SEC population, amendment/transaction-line reconciliation, plan, currency, point-in-time or investment qualification.'}
        key='data/insider-research-verification.json';s3.put_object(Bucket=BUCKET,Key=key,Body=model.encoded(proof),ContentType='application/json',CacheControl='no-store')
        assert json.loads(public(key))==proof
        r.kv(accepted=True,proof_key=key,generated_at=packet['generated_at'],source_replay=replay,legacy_history_unchanged=True)

if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
