"""Verify exact Cycle Clock packages, original replay and public page; invoke one reviewed public research producer.

No account consumers, paid AI, notifications, portfolio writes or account changes; leaves the existing public schedule unchanged.
"""
from pathlib import Path
from datetime import datetime,timezone
import base64,hashlib,io,json,re,subprocess,sys,time,urllib.request,urllib.error,zipfile
import boto3
from botocore.config import Config
ROOT=Path(__file__).resolve().parents[3];SOURCE=ROOT/'aws/lambdas/justhodl-cycle-clock/source'
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/checks'),str(ROOT/'aws/shared'),str(ROOT/'scripts'),str(SOURCE)]
from ops_report import report
from release_package_evidence import shared_imports
import cycle_research_store as store
import cycle_research_model as model
from replay_cycle_research import verify as replay_verify
BUCKET='justhodl-dashboard-live'
COMMIT='54d68c06e230cf31626e459da5369009af61a578'
PAGE_COMMIT=COMMIT
FUNCTIONS=('justhodl-cycle-clock','justhodl-industry-rotation','justhodl-quantum-desk','justhodl-strategist','justhodl-wl-fusion','justhodl-khalid-risk','justhodl-engine-fusion')
ASSETS=('cycle-clock.html','jh-cycle-research.js','classic-dashboard.html')
AUDIT=('f88efc667119bee9a9d7507060c667ed5d2f95b1192f3276f3bf7bd706665882',136338)
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
    if fn=='justhodl-cycle-clock':assert cfg['MemorySize']==512 and cfg['Timeout']==300
    return {'commit':COMMIT,'code_sha256':cfg['CodeSha256'],'packaged_files_checked':len(expected),'all_packaged_sources_match':True,'alias':alias}

def protected(ref,read):
    assert ref['key']==model.PRIVATE+ref['sha256']+'.bin'
    raw=read(ref['key']);assert len(raw)==ref['bytes'] and model.sha(raw)==ref['sha256']
    return raw

def invoke_public(lam,s3):
    request='chatgpt-cycle-native-'+COMMIT[:12]+'-1';key=store.request_key(request);sent=False;status=None
    try:status=json.loads(store.bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body']))
    except Exception as exc:
        if not store.missing(exc):raise
        response=lam.invoke(FunctionName='justhodl-cycle-clock',InvocationType='Event',Payload=model.encoded({'request_id':request}));assert response['StatusCode']==202;sent=True
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
    fn='justhodl-cycle-clock';cfg=lam.get_function_configuration(FunctionName=fn);names=[]
    for page in events.get_paginator('list_rule_names_by_target').paginate(TargetArn=cfg['FunctionArn']):names.extend(page['RuleNames'])
    assert names==['justhodl-cycle-clock-daily'];rule=events.describe_rule(Name=names[0]);targets=events.list_targets_by_rule(Rule=names[0])['Targets']
    assert rule['State']=='ENABLED' and rule['ScheduleExpression']=='cron(30 23 * * ? *)' and len(targets)==1
    assert targets[0]['Arn']==cfg['FunctionArn'] and not targets[0].get('InputTransformer')
    assert not json.loads(targets[0].get('Input') or '{}').get('request_id')
    return {'name':names[0],'state':rule['State'],'expression':rule['ScheduleExpression'],'native_targets':1,'changed':False}

def main():
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=310,connect_timeout=10,retries={'max_attempts':0},tcp_keepalive=True))
    s3=boto3.client('s3',region_name='us-east-1');events=boto3.client('events',region_name='us-east-1')
    with report('ops_5959_cycle_native_acceptance') as r:
        subprocess.run([sys.executable,str(SOURCE.parent/'tests/run_tests.py')],cwd=ROOT,check=True)
        sys.path.insert(0,str(SOURCE.parent/'tests'));from cycle_fixture import fixture
        client,inputs,_,_=fixture();portable=model.sha(model.encoded(store.compile_output(inputs,store.reader(client,'b'))))
        assert portable=='1f742e4a966a1b3fef1e1a3e68df066af267d4ba4bfde4aaa5620613f8225266'
        runtimes={fn:runtime(lam,fn) for fn in FUNCTIONS};cadence=schedule(lam,events)
        r.kv(runtimes=runtimes,schedule=cadence,acceptance_consumer_invocations=0,private_account_reads=0,paid_ai_calls=0,notifications_sent=0,portfolio_writes=0)
        read=store.reader(s3,BUCKET);digest,size=AUDIT;key=model.PRIVATE+digest+'.bin';raw=read(key)
        assert len(raw)==size and model.sha(raw)==digest
        audit=json.loads(raw);protected_keys={key}
        for ref in (*audit['packets'].values(),*audit['canonical_originals'].values()):protected(ref,read);protected_keys.add(ref['key'])
        old_macro=json.loads(protected(audit['packets']['data/report-measurements.json'],read))
        restored=canonical_fred_replay.restore(old_macro,model.SERIES,lambda key:protected(audit['canonical_originals'][key],read))
        assert sum(v is not None for v in restored.values())==13 and restored['SAHMCURRENT'] is None
        assert set(audit['declared_dependencies'])==set(model.DEPENDENCIES) and len(model.DEPENDENCIES)==76
        old_inputs={key:json.loads(protected(audit['packets'][key],read)) for key in model.DEPENDENCIES}
        refs={key:{**audit['packets'][key],'source_key':key} for key in model.DEPENDENCIES}
        rebuilt=model.build(old_macro,restored,old_inputs,refs,audit['generated_at'])
        assert rebuilt['quality']['available_histories']==13 and rebuilt['dependency_graph']['retained_inputs']==76
        assert rebuilt['cycle']['recession_prob_pct'] is None and rebuilt['synthesis']['score'] is None
        assert rebuilt['unemployment_rule']['official_realtime_series']['exact_value']=='-0.07'
        legacy=subprocess.check_output(['git','show','0feda4c65:aws/lambdas/justhodl-cycle-clock/source/lambda_function.py'],cwd=ROOT)
        assert (SOURCE/'legacy_cycle_clock.py').read_bytes()==legacy and len(legacy)==90235
        old_history=json.loads(protected(audit['packets']['data/cycle-clock-history.json'],read));assert len(old_history)==86
        history_before=read('data/cycle-clock-history.json')
        build=json.loads(public('build-manifest.json'));pages_commit=build['commit_sha']
        subprocess.run(['git','merge-base','--is-ancestor',PAGE_COMMIT,pages_commit],cwd=ROOT,check=True)
        subprocess.run(['git','diff','--quiet',COMMIT,pages_commit,'--',*ASSETS],cwd=ROOT,check=True)
        for name in ASSETS:
            served=public(name);clean,count=re.subn(rb'<script\b[^>]*\bsrc="https://static\.cloudflareinsights\.com/beacon\.min\.js/v[a-f0-9]+"[^>]*></script>\n?',b'',served)
            assert count<=1 and model.sha(clean)==build['files_sha256'][name],'Built asset differs: '+name
            if name=='cycle-clock.html':assert b'jh-cycle-research.js?v=20260921-native1' in served and b'cycle-clock-history.json' not in served
            if name=='classic-dashboard.html':assert b'Open verified cycle research' in served and b'gj("data/cycle-clock.json")' not in served
        r.kv(preflight_originals_replayed=13,preflight_engine_inputs=76,preflight_legacy_history_rows=len(old_history),pages_commit=pages_commit)
        request=invoke_public(lam,s3);r.kv(public_producer_request=request)
        raw=public(store.CURRENT);packet=json.loads(raw);status=request['status']
        assert packet['contract']==model.CONTRACT and packet['generated_at']==status['generated_at'] and packet['replay']==status['replay']
        assert packet['generated_at']>audit['metadata']['data/cycle-clock.json']['generated_at'] and json.loads(read(store.CURRENT))==packet
        assert packet['call'] is None and packet['portfolio_action']=='WAIT' and all(packet[k] is False for k in model.PERMISSIONS)
        assert packet['quality']['available_histories']>=13 and len(packet['measurements'])==14
        assert packet['cycle']['phase'] is None and packet['cycle']['recession_prob_pct'] is None and packet['synthesis']['score'] is None
        assert packet['dependency_graph']['retained_inputs']==76 and packet['dependency_graph']['independent_investment_votes']==0
        assert packet['track_record']['hit_rate'] is None and packet['track_record']['qualified_samples']==0
        assert read('data/cycle-clock-history.json')==history_before,'Native producer changed legacy posture history'
        for row in packet['net_liquidity_proxy']['series']:
            if row['available']:
                assert row['inputs']['WTREGEN']['observation']['date']==row['reference_date']
                assert row['inputs']['RRPONTSYD']['observation']['date']<=row['reference_date']
        replay=replay_verify(packet,read);run=json.loads(read(packet['replay']['manifest_key']));inputs=store.checked(run['input'],'inputs',read)
        for ref in (inputs['macro'],*inputs['legacy'].values()):
            if ref:protected(ref,read);protected_keys.add(ref['key'])
        for key in sorted(protected_keys):assert denied('https://justhodl.ai/'+key) and denied('https://'+BUCKET+'.s3.amazonaws.com/'+key),'Protected source publicly readable'
        for fn in FUNCTIONS:
            cfg=lam.get_function_configuration(FunctionName=fn);receipt=json.loads(public('data/ops/releases/'+fn+'.json'))
            assert receipt['commit']==COMMIT and receipt['code_sha256']==cfg['CodeSha256']==runtimes[fn]['code_sha256']
        proof={'contract':'cycle-native-acceptance.v1','verified_at':datetime.now(timezone.utc).isoformat(),'runtime_commit':COMMIT,
            'runtimes':runtimes,'pages_commit':pages_commit,'assets':{name:build['files_sha256'][name] for name in ASSETS},'portable_reference_digest':portable,
            'publication':{'generated_at':packet['generated_at'],'source_generated_at':packet['source_generated_at'],'replay':packet['replay'],'quality':packet['quality']},
            'public_sha256':model.sha(raw),'request':request,'source_replay':replay,'schedule':cadence,
            'preflight_originals_replayed':13,'preflight_engine_inputs':76,'legacy_history_rows_preserved':len(old_history),'legacy_history_unchanged_by_native':True,
            'whole_originals_anonymously_denied':True,'protected_artifacts_checked':len(protected_keys),
            'acceptance_public_producer_invocations':int(request['invoke_sent']),'acceptance_consumer_invocations':0,
            'private_account_reads':0,'provider_requests':0,'paid_ai_calls':0,'notifications_sent':0,'portfolio_writes':0,
            'remaining':'Current-vintage descriptive coordinates and balance-sheet proxy. SAHMCURRENT absent at preflight. Complete upstream transformation replay, historical first availability, independent predictive information, prospective forecasts, asset leadership and portfolio recommendations remain unqualified.'}
        key='data/cycle-research-verification.json';s3.put_object(Bucket=BUCKET,Key=key,Body=model.encoded(proof),ContentType='application/json',CacheControl='no-store')
        assert json.loads(public(key))==proof
        r.kv(accepted=True,proof_key=key,source_replay=replay,protected_artifacts_checked=len(protected_keys))

if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
