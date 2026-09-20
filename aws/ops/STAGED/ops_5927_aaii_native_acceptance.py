"""Accept only the native public AAII producer and its exact deployed consumers.

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
SOURCE=ROOT/'aws/lambdas/justhodl-aaii-sentiment/source'
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/checks'),str(SOURCE),str(ROOT/'aws/shared'),str(ROOT/'scripts')]
from ops_report import report
from release_package_evidence import shared_imports
from aaii_research_store import bounded
import aaii_research_store as store
import aaii_research_model as model
from aaii_research import context as research_context
from replay_aaii_research import verify as replay_verify
BUCKET='justhodl-dashboard-live';FN='justhodl-aaii-sentiment'
COMMIT='1891532537687eb435349d2022c230ca1852b4e1'
FUNCTIONS=tuple('justhodl-'+name for name in ('aaii-sentiment','ai-chat','asymmetric-scorer','crisis-knowledge-base','cycle-clock','market-extremes','morning-intelligence','put-call-extreme','signal-board'))
EXPECTED={fn:COMMIT for fn in FUNCTIONS}
ASSETS=('aaii.html','jh-aaii-research.js')
REQUEST_ID='chatgpt-aaii-native-'+COMMIT[:12]+'-1'


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
    if fn==FN:assert cfg['MemorySize']==512 and cfg['Timeout']==180
    return {'commit':expected_commit,'code_sha256':cfg['CodeSha256'],'packaged_files_checked':len(expected),'all_packaged_sources_match':True,'alias':alias}


def main():
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=45,connect_timeout=10,retries={'max_attempts':0},tcp_keepalive=True))
    s3=boto3.client('s3',region_name='us-east-1')
    events=boto3.client('events',region_name='us-east-1')
    with report('ops_5927_aaii_native_acceptance') as r:
        subprocess.run([sys.executable,str(SOURCE.parent/'tests/run_tests.py')],cwd=ROOT,check=True)
        subprocess.run([sys.executable,str(ROOT/'tests/aaii_consumer_test_support.py')],cwd=ROOT,check=True)
        portable=(SOURCE.parent/'tests/fixtures/expected-output.sha256').read_text().strip()
        assert portable=='1c080257a0d22cbec0f0dd91659ada08d8b41642a1b03c0ae7ab3d7d7091633b'
        runtimes={fn:runtime(lam,fn) for fn in FUNCTIONS}
        r.kv(runtimes=runtimes,portable_reference_digest=portable,acceptance_consumer_invocations=0,
            private_account_reads=0,paid_ai_calls=0,notifications_sent=0,portfolio_writes=0)
        legacy=subprocess.check_output(['git','show','ce27259326'+':aws/lambdas/justhodl-aaii-sentiment/source/lambda_function.py'],cwd=ROOT)
        assert len(legacy)==14330 and (SOURCE/'legacy_aaii_sentiment.py').read_bytes()==legacy
        old_digest='0edaf2a3a302ae9fc397a8c7fb8e0e743ef6bef9f7db63d5952c8dfc59a6ca8b'
        old_key='audit-private/20260909-originals/market-cycle/'+old_digest+'.bin'
        old_raw=bounded(s3.get_object(Bucket=BUCKET,Key=old_key)['Body'])
        assert len(old_raw)==3527 and model.sha(old_raw)==old_digest
        assert denied('https://justhodl.ai/'+old_key)
        build=json.loads(public('build-manifest.json'));pages_commit=build['commit_sha']
        subprocess.run(['git','merge-base','--is-ancestor',COMMIT,pages_commit],cwd=ROOT,check=True)
        subprocess.run(['git','diff','--quiet',COMMIT,pages_commit,'--',*ASSETS],cwd=ROOT,check=True)
        for name in ASSETS:
            served=public(name);clean,count=re.subn(rb'<script\b[^>]*\bsrc="https://static\.cloudflareinsights\.com/beacon\.min\.js/v[a-f0-9]+"[^>]*></script>\n?',b'',served)
            assert count<=1 and model.sha(clean)==build['files_sha256'][name],'Built asset differs: '+name
            if name=='aaii.html':assert b'jh-aaii-research.js?v=20260920-native1' in served and b'AAII investor sentiment' in served
        schedule=[]
        for name in ('justhodl-aaii-sentiment-daily','justhodl-aaii-weekly'):
            rule=events.describe_rule(Name=name)
            targets=events.list_targets_by_rule(Rule=name)['Targets']
            selected=[t for t in targets if t.get('Arn','').split(':function:')[-1].split(':')[0]==FN]
            assert rule['State']=='ENABLED' and len(selected)==1
            for target in selected:
                payload=json.loads(target.get('Input') or '{}')
                assert not payload.get('request_id') and not target.get('InputTransformer')
            schedule.append({'name':name,'state':rule['State'],'expression':rule.get('ScheduleExpression'),'native_targets':len(selected)})
        # One request identity: if a transport result is uncertain, inspect its
        # status instead of invoking again. No raw request body is an AWS secret.
        status_key=store.request_key(REQUEST_ID);invoked=False
        try:status=json.loads(bounded(s3.get_object(Bucket=BUCKET,Key=status_key)['Body']))
        except Exception as exc:
            if not store.missing(exc):raise
            response=lam.invoke(FunctionName=FN,InvocationType='Event',Payload=model.encoded({'request_id':REQUEST_ID}))
            assert response['StatusCode']==202
            invoked=True;status=None
        deadline=time.monotonic()+300
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
        assert packet['quality']['status']=='fresh' and packet['observation'] and len(packet['history'])>=4
        assert packet['calls_eligible'] is False and packet['forecast_qualified'] is False and packet['sizing_eligible'] is False and packet['execution_eligible'] is False
        assert packet['decision']=={'verb':'WAIT','abstain':True,'reason':'descriptive_survey_without_qualified_forecast'}
        replay=replay_verify(packet,store.reader(s3,BUCKET))
        context=research_context(packet);assert context['available'] is True
        originals={}
        for name,item in packet['source_evidence'].items():
            ref=item['evidence'];original=bounded(s3.get_object(Bucket=BUCKET,Key=ref['key'])['Body'])
            assert len(original)==ref['bytes'] and model.sha(original)==ref['sha256']
            assert denied('https://justhodl.ai/'+ref['key']) and denied('https://'+BUCKET+'.s3.amazonaws.com/'+ref['key'])
            originals[name]={'sha256':ref['sha256'],'bytes':ref['bytes'],'acquired_at':item['acquired_at'],'anonymous_denied':True}
        proof={'contract':'aaii-native-acceptance.v1','verified_at':datetime.now(timezone.utc).isoformat(),
            'runtime_commit':COMMIT,'runtimes':runtimes,'pages_commit':pages_commit,
            'assets':{name:build['files_sha256'][name] for name in ASSETS},'portable_reference_digest':portable,
            'public_sha256':model.sha(raw),'generated_at':packet['generated_at'],'as_of':packet['as_of'],
            'replay':packet['replay'],'source_replay':replay,'originals':originals,'observation':packet['observation'],
            'history_scope':packet['history_scope'],'schedules':schedule,'request_id':REQUEST_ID,'execution_id':status['execution_id'],
            'acceptance_public_producer_invocations':int(invoked),'acceptance_consumer_invocations':0,
            'private_account_reads':0,'paid_ai_calls':0,'notifications_sent':0,'portfolio_writes':0,
            'remaining':'Public recent survey window only; respondent count, full 1987 original history and historical availability unknown. No qualified forecast, sizing or execution.'}
        key='data/aaii-research-verification.json'
        s3.put_object(Bucket=BUCKET,Key=key,Body=model.encoded(proof),ContentType='application/json',CacheControl='no-store')
        assert json.loads(public(key))==proof
        r.kv(accepted=True,proof_key=key,generated_at=packet['generated_at'],as_of=packet['as_of'],history_rows=len(packet['history']),source_replay=replay)


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
