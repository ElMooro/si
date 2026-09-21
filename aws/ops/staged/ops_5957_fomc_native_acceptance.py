"""Verify exact FOMC packages, original replay and public page; invoke one reviewed public research producer.

No account consumers, paid AI, notifications, portfolio writes or account changes; leaves the existing public schedule unchanged.
"""
from pathlib import Path
from datetime import datetime,timezone
import base64,hashlib,io,json,re,subprocess,sys,time,urllib.request,urllib.error,zipfile
import boto3
from botocore.config import Config
ROOT=Path(__file__).resolve().parents[3];SOURCE=ROOT/'aws/lambdas/justhodl-fomc-reaction/source'
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/checks'),str(ROOT/'aws/shared'),str(ROOT/'scripts'),str(SOURCE)]
from ops_report import report
from release_package_evidence import shared_imports
import fomc_research_store as store
import fomc_research_model as model
from replay_fomc_research import verify as replay_verify
BUCKET='justhodl-dashboard-live'
COMMIT='76ec316c654317a5559b013bb4240bd503e5e595'
PAGE_COMMIT=COMMIT
FUNCTIONS=('justhodl-fomc-reaction','justhodl-cycle-clock')
ASSETS=('fomc.html','jh-fomc-research.js','fedwatch.html')
AUDIT=('af257adb2f65d835a72baceb289340f8356e65ddb6ac9419021ff9df1e64af27',11996)
import canonical_fred_replay
from acceptance_invoke import invoke_when_available

def public(key):
    with urllib.request.urlopen(urllib.request.Request('https://justhodl.ai/'+key,headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=45) as r:return store.bounded(r)

def denied(url):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,method='HEAD',headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=25):return False
    except urllib.error.HTTPError as exc:return exc.code in (401,403,404)

def runtime(lam,fn):
    source=ROOT/'aws/lambdas'/fn/'source';cfgpath=source.parent/'config.json'
    configuration=json.loads(cfgpath.read_bytes()) if cfgpath.exists() else {};request={'FunctionName':fn}
    if configuration.get('release_validation'):request['Qualifier']='live'
    deployed=lam.get_function(**request);cfg=deployed['Configuration'];receipt=json.loads(public('data/ops/releases/'+fn+'.json'))
    assert receipt['commit']==COMMIT and receipt['code_sha256']==cfg['CodeSha256'],'Exact receipt required: '+fn
    assert cfg['State']=='Active' and cfg['LastUpdateStatus']=='Successful'
    raw=store.bounded(urllib.request.urlopen(deployed['Code']['Location'],timeout=45))
    assert base64.b64encode(hashlib.sha256(raw).digest()).decode()==cfg['CodeSha256']
    paths=[ROOT/p for p in subprocess.check_output(['git','ls-files',str(source.relative_to(ROOT))],cwd=ROOT,text=True).splitlines()]
    expected={p.relative_to(source).as_posix():p for p in paths}
    expected.update({p.name:p for p in shared_imports(ROOT,paths) if not (source/p.name).exists()})
    with zipfile.ZipFile(io.BytesIO(raw)) as z:
        for name,p in expected.items():assert z.read(name)==p.read_bytes(),'Packaged bytes differ: '+fn+'/'+name
    alias=None
    if request.get('Qualifier'):
        a=lam.get_alias(FunctionName=fn,Name='live');assert a['FunctionVersion']==cfg['Version'] and not (a.get('RoutingConfig') or {}).get('AdditionalVersionWeights')
        alias={'name':'live','version':cfg['Version'],'weighted_secondary_versions':0}
    if fn=='justhodl-fomc-reaction':assert cfg['MemorySize']==512 and cfg['Timeout']==300
    return {'commit':COMMIT,'code_sha256':cfg['CodeSha256'],'packaged_files_checked':len(expected),'all_packaged_sources_match':True,'alias':alias}

def protected(ref,read):
    assert ref['key']==model.PRIVATE+ref['sha256']+'.bin'
    raw=read(ref['key']);assert len(raw)==ref['bytes'] and model.sha(raw)==ref['sha256']
    return raw

def invoke_public(lam,s3):
    request='chatgpt-fomc-native-'+COMMIT[:12]+'-1';key=store.request_key(request);sent=False;status=None
    try:status=json.loads(store.bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body']))
    except Exception as exc:
        if not store.missing(exc):raise
        response=lam.invoke(FunctionName='justhodl-fomc-reaction',InvocationType='Event',Payload=model.encoded({'request_id':request}));assert response['StatusCode']==202;sent=True
    deadline=time.monotonic()+300
    while time.monotonic()<deadline:
        try:status=json.loads(store.bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body']))
        except Exception as exc:
            if not store.missing(exc):raise
        if status and status.get('status') in ('complete','failed'):break
        time.sleep(3)
    assert status and status.get('status')=='complete' and status.get('published') is True,'Native public producer did not publish'
    return {'request_id':request,'status_key':key,'invoke_sent':sent,'status':status}

def schedule(lam,events):
    fn='justhodl-fomc-reaction';cfg=lam.get_function_configuration(FunctionName=fn);names=[]
    for page in events.get_paginator('list_rule_names_by_target').paginate(TargetArn=cfg['FunctionArn']):names.extend(page['RuleNames'])
    assert names==['fomc-reaction-daily'];rule=events.describe_rule(Name=names[0]);targets=events.list_targets_by_rule(Rule=names[0])['Targets']
    assert rule['State']=='ENABLED' and rule['ScheduleExpression']=='cron(35 21 * * ? *)' and len(targets)==1
    assert targets[0]['Arn']==cfg['FunctionArn'] and not targets[0].get('InputTransformer')
    assert not json.loads(targets[0].get('Input') or '{}').get('request_id')
    return {'name':names[0],'state':rule['State'],'expression':rule['ScheduleExpression'],'native_targets':1,'changed':False}

def main():
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=310,connect_timeout=10,retries={'max_attempts':0},tcp_keepalive=True))
    s3=boto3.client('s3',region_name='us-east-1');events=boto3.client('events',region_name='us-east-1')
    with report('ops_5957_fomc_native_acceptance') as r:
        subprocess.run([sys.executable,str(SOURCE.parent/'tests/run_tests.py')],cwd=ROOT,check=True)
        sys.path.insert(0,str(SOURCE.parent/'tests'));from fomc_fixture import fixture
        client,inputs,_,_=fixture();portable=model.sha(model.encoded(store.compile_output(inputs,store.reader(client,'b'))))
        assert portable=='5dd97ec325ef023242170b6b1707a7637720a4fd0cff939eb3d714d281496e5d'
        runtimes={fn:runtime(lam,fn) for fn in FUNCTIONS};cadence=schedule(lam,events)
        r.kv(runtimes=runtimes,schedule=cadence,acceptance_consumer_invocations=0,private_account_reads=0,paid_ai_calls=0,notifications_sent=0,portfolio_writes=0)
        read=store.reader(s3,BUCKET);digest,size=AUDIT;key=model.PRIVATE+digest+'.bin';raw=read(key)
        assert len(raw)==size and model.sha(raw)==digest
        audit=json.loads(raw);protected_keys={key}
        for ref in (*audit['packets'].values(),*audit['canonical_originals'].values(),audit['calendar_original'],*audit['logs'].values()):protected(ref,read);protected_keys.add(ref['key'])
        old_macro=json.loads(protected(audit['packets']['data/report-measurements.json'],read));old_fw=json.loads(protected(audit['packets']['data/fedwatch.json'],read))
        restored=canonical_fred_replay.restore(old_macro,model.SERIES,lambda key:protected(audit['canonical_originals'][key],read))
        assert sum(v is not None for v in restored.values())==3 and restored['NASDAQCOM'] is None
        old_calendar=protected(audit['calendar_original'],read);assert store.calendar_source(old_fw,read)==old_calendar
        rebuilt=model.build(old_macro,restored,old_fw,old_calendar,audit['generated_at'])
        assert rebuilt['quality']['available_histories']==3 and len(rebuilt['events'])==46
        assert rebuilt['surprise']['label'] is None and rebuilt['self_grading']['directional_accuracy_pct'] is None
        assert rebuilt['events'][-1]['event_date']=='2026-09-16'
        legacy=subprocess.check_output(['git','show','caad38757:aws/lambdas/justhodl-fomc-reaction/source/lambda_function.py'],cwd=ROOT)
        assert (SOURCE/'legacy_fomc_reaction.py').read_bytes()==legacy and len(legacy)==19413
        build=json.loads(public('build-manifest.json'));pages_commit=build['commit_sha']
        subprocess.run(['git','merge-base','--is-ancestor',PAGE_COMMIT,pages_commit],cwd=ROOT,check=True)
        subprocess.run(['git','diff','--quiet',COMMIT,pages_commit,'--',*ASSETS],cwd=ROOT,check=True)
        for name in ASSETS:
            served=public(name);clean,count=re.subn(rb'<script\b[^>]*\bsrc="https://static\.cloudflareinsights\.com/beacon\.min\.js/v[a-f0-9]+"[^>]*></script>\n?',b'',served)
            assert count<=1 and model.sha(clean)==build['files_sha256'][name],'Built asset differs: '+name
            if name=='fomc.html':assert b'jh-fomc-research.js?v=20260920-native1' in served and b'fomc-calibration.json' not in served
        r.kv(preflight_originals_replayed=3,preflight_historical_events=len(rebuilt['events']),preflight_legacy_logs_preserved=len(audit['logs']),pages_commit=pages_commit)
        request=invoke_public(lam,s3);r.kv(public_producer_request=request)
        raw=public(store.CURRENT);packet=json.loads(raw);status=request['status']
        assert packet['contract']==model.CONTRACT and packet['generated_at']==status['generated_at'] and packet['replay']==status['replay']
        assert packet['generated_at']>audit['metadata']['data/fomc-reaction.json']['generated_at'] and json.loads(read(store.CURRENT))==packet
        assert packet['call'] is None and packet['portfolio_action']=='WAIT' and all(packet[k] is False for k in model.PERMISSIONS)
        assert packet['quality']['available_histories']>=3 and len(packet['measurements'])==4
        assert packet['surprise']['label'] is None and packet['reaction_map']=={} and packet['self_grading']['directional_accuracy_pct'] is None
        assert all(s['forecast_probability'] is None and s['independent_sample_size'] is None for s in packet['summaries'])
        for event in packet['events']:
            for asset in event['assets'].values():
                if asset['event_date_change']['available']:assert asset['event_date_change']['event_observation']['date']==event['event_date']
                for horizon,out in asset['forward_changes'].items():
                    assert out['horizon_reported_numeric_observations']==int(horizon)
                    if out['available']:assert out['start']['date']==event['event_date'] and out['end']['date']>event['event_date']
        replay=replay_verify(packet,read);run=json.loads(read(packet['replay']['manifest_key']));inputs=store.checked(run['input'],'inputs',read)
        for ref in (inputs['macro'],*inputs['legacy'].values()):
            if ref:protected(ref,read);protected_keys.add(ref['key'])
        for key in sorted(protected_keys):assert denied('https://justhodl.ai/'+key) and denied('https://'+BUCKET+'.s3.amazonaws.com/'+key),'Protected source publicly readable'
        for fn in FUNCTIONS:
            cfg=lam.get_function_configuration(FunctionName=fn);receipt=json.loads(public('data/ops/releases/'+fn+'.json'))
            assert receipt['commit']==COMMIT and receipt['code_sha256']==cfg['CodeSha256']==runtimes[fn]['code_sha256']
        proof={'contract':'fomc-native-acceptance.v1','verified_at':datetime.now(timezone.utc).isoformat(),'runtime_commit':COMMIT,
            'runtimes':runtimes,'pages_commit':pages_commit,'assets':{name:build['files_sha256'][name] for name in ASSETS},'portable_reference_digest':portable,
            'publication':{'generated_at':packet['generated_at'],'source_generated_at':packet['source_generated_at'],'replay':packet['replay'],'quality':packet['quality']},
            'public_sha256':model.sha(raw),'request':request,'source_replay':replay,'schedule':cadence,
            'preflight_originals_replayed':3,'preflight_historical_events':len(rebuilt['events']),'legacy_logs_preserved':len(audit['logs']),
            'whole_originals_anonymously_denied':True,'protected_artifacts_checked':len(protected_keys),
            'acceptance_public_producer_invocations':int(request['invoke_sent']),'acceptance_consumer_invocations':0,
            'private_account_reads':0,'provider_requests':0,'paid_ai_calls':0,'notifications_sent':0,'portfolio_writes':0,
            'remaining':'Descriptive scheduled-event associations; Nasdaq Composite source absent at preflight, complete asset/unscheduled-event coverage, historical first-availability, identified intraday policy shocks, prospective forecasting calibration and portfolio recommendations remain unqualified.'}
        key='data/fomc-research-verification.json';s3.put_object(Bucket=BUCKET,Key=key,Body=model.encoded(proof),ContentType='application/json',CacheControl='no-store')
        assert json.loads(public(key))==proof
        r.kv(accepted=True,proof_key=key,source_replay=replay,protected_artifacts_checked=len(protected_keys))

if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
