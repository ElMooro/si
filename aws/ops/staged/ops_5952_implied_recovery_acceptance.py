"""Verify exact Implied packages, original replay and public page; recover source evidence after a lost response, then invoke only native Implied.

No account consumers, paid AI, notifications, portfolio writes or schedule changes.
"""
from pathlib import Path
from datetime import datetime,timezone
import base64,hashlib,io,json,re,subprocess,sys,time,urllib.request,urllib.error,zipfile
import boto3
from botocore.config import Config
ROOT=Path(__file__).resolve().parents[3];SOURCE=ROOT/'aws/lambdas/justhodl-implied-prob/source'
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/checks'),str(ROOT/'aws/shared'),str(ROOT/'scripts'),str(SOURCE)]
from ops_report import report
from release_package_evidence import shared_imports
import implied_research_store as store
import implied_research_model as model
from replay_implied_research import verify as replay_verify
BUCKET='justhodl-dashboard-live'
COMMIT='4747fba874db7508c85ff9841c50fcd5b9cc83b9'
PAGE_COMMIT=COMMIT
FUNCTIONS=('justhodl-daily-report-v3','justhodl-implied-prob')
ASSETS=('implied-prob.html','jh-implied-research.js')
AUDIT=('f18a7dc3d509ebb7085794b816ac27bd5f9e0246d39a79e068c32ff2af0fed33',20818)
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
    if fn=='justhodl-implied-prob':assert cfg['MemorySize']==512 and cfg['Timeout']==240
    return {'commit':COMMIT,'code_sha256':cfg['CodeSha256'],'packaged_files_checked':len(expected),'all_packaged_sources_match':True,'alias':alias}

def schedule(lam,events):
    cfg=lam.get_function_configuration(FunctionName='justhodl-implied-prob');names=[]
    for page in events.get_paginator('list_rule_names_by_target').paginate(TargetArn=cfg['FunctionArn']):names.extend(page['RuleNames'])
    assert names==['justhodl-implied-prob-cadence'];rule=events.describe_rule(Name=names[0]);targets=events.list_targets_by_rule(Rule=names[0])['Targets']
    selected=[t for t in targets if t.get('Arn')==cfg['FunctionArn']]
    assert rule['State']=='ENABLED' and rule['ScheduleExpression']=='cron(25 22 ? * MON-FRI *)' and len(selected)==1
    assert not json.loads(selected[0].get('Input') or '{}').get('request_id') and not selected[0].get('InputTransformer')
    return {'name':names[0],'state':rule['State'],'expression':rule['ScheduleExpression'],'native_targets':1}

def protected(ref,read):
    assert ref['key']==model.PRIVATE+ref['sha256']+'.bin'
    raw=read(ref['key']);assert len(raw)==ref['bytes'] and model.sha(raw)==ref['sha256']
    return raw

def canonical_refresh(lam,s3):
    # Recover source evidence after the synchronous connection was closed. Never
    # send the ambiguous collector invocation again or invent a Lambda response.
    request='chatgpt-implied-canonical-'+COMMIT[:12]+'-1';key=store.request_key(request)
    prior=json.loads(store.bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body']))
    assert prior.get('contract')=='implied-canonical-acceptance-request.v1' and prior.get('request_id')==request
    assert prior.get('status') in ('claimed','source_publication_verified')
    read=store.reader(s3,BUCKET);deadline=time.monotonic()+900
    while True:
        packet=json.loads(read('data/report-measurements.json'))
        if model.clock(packet['generated_at'])>model.clock(prior['started_at']):
            manifest=canonical_fred_replay.pinned_report(packet,read)
            if set(model.SERIES)<=set(manifest['catalog']):break
        assert time.monotonic()<deadline,'New reviewed canonical source publication not available; no duplicate collector sent'
        time.sleep(10)
    observed={**prior,'status':'source_publication_verified','verified_at':datetime.now(timezone.utc).isoformat(),
        'publication':{k:packet[k] for k in ('generated_at','quality','replay')},
        'evidence_basis':'Exact canonical current/run/compiler bytes after the original claim. This is observed source completion, not a received Lambda invocation response.',
        'original_request_outcome':'connection_closed_without_response','collector_dispatch_attempts_before_recovery':1,
        'collector_dispatches_during_recovery':0,'invoke_sent':False}
    store.status_write(s3,BUCKET,key,observed)
    return observed

def invoke_public(lam,s3):
    request='chatgpt-implied-native-'+COMMIT[:12]+'-1';key=store.request_key(request);sent=False;status=None
    try:status=json.loads(store.bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body']))
    except Exception as exc:
        if not store.missing(exc):raise
        response=lam.invoke(FunctionName='justhodl-implied-prob',InvocationType='Event',Payload=model.encoded({'request_id':request}));assert response['StatusCode']==202;sent=True
    deadline=time.monotonic()+300
    while time.monotonic()<deadline:
        try:status=json.loads(store.bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body']))
        except Exception as exc:
            if not store.missing(exc):raise
        if status and status.get('status') in ('complete','failed'):break
        time.sleep(3)
    assert status and status.get('status')=='complete' and status.get('published') is True,'Native public producer did not publish'
    return {'request_id':request,'status_key':key,'invoke_sent':sent,'status':status}

def main():
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=910,connect_timeout=10,retries={'max_attempts':0},tcp_keepalive=True))
    s3=boto3.client('s3',region_name='us-east-1');events=boto3.client('events',region_name='us-east-1')
    with report('ops_5952_implied_recovery_acceptance') as r:
        subprocess.run([sys.executable,str(SOURCE.parent/'tests/run_tests.py')],cwd=ROOT,check=True)
        sys.path.insert(0,str(SOURCE.parent/'tests'));from implied_fixture import fixture
        client,inputs,_,_=fixture();portable=model.sha(model.encoded(store.compile_output(inputs,store.reader(client,'b'))))
        assert portable=='5de642a22663214c0330b3062f28f8bebc438e2ec77a09ce1822fde8a80f4cc2'
        runtimes={fn:runtime(lam,fn) for fn in FUNCTIONS};cadence=schedule(lam,events)
        r.kv(runtimes=runtimes,schedule=cadence,acceptance_consumer_invocations=0,private_account_reads=0,paid_ai_calls=0,notifications_sent=0,portfolio_writes=0)
        read=store.reader(s3,BUCKET);digest,size=AUDIT;key=model.PRIVATE+digest+'.bin';raw=read(key)
        assert len(raw)==size and model.sha(raw)==digest
        audit=json.loads(raw);protected_keys={key}
        for ref in (*audit['packets'].values(),*audit['canonical_originals'].values()):protected(ref,read);protected_keys.add(ref['key'])
        old_macro=json.loads(protected(audit['packets']['data/report-measurements.json'],read))
        old_read=lambda key:protected(audit['canonical_originals'][key],read)
        old_restored=canonical_fred_replay.restore(old_macro,model.SERIES,old_read)
        assert sum(v is not None for v in old_restored.values())==12
        legacy=subprocess.check_output(['git','show','610064bb7:aws/lambdas/justhodl-implied-prob/source/lambda_function.py'],cwd=ROOT)
        assert (SOURCE/'legacy_implied_prob.py').read_bytes()==legacy and len(legacy)==23950
        build=json.loads(public('build-manifest.json'));pages_commit=build['commit_sha']
        subprocess.run(['git','merge-base','--is-ancestor',PAGE_COMMIT,pages_commit],cwd=ROOT,check=True)
        subprocess.run(['git','diff','--quiet',COMMIT,pages_commit,'--',*ASSETS],cwd=ROOT,check=True)
        for name in ASSETS:
            served=public(name);clean,count=re.subn(rb'<script\b[^>]*\bsrc="https://static\.cloudflareinsights\.com/beacon\.min\.js/v[a-f0-9]+"[^>]*></script>\n?',b'',served)
            assert count<=1 and model.sha(clean)==build['files_sha256'][name],'Built asset differs: '+name
            if name=='implied-prob.html':assert b'jh-implied-research.js?v=20260920-native1' in served
        canonical=canonical_refresh(lam,s3);r.kv(canonical_collection=canonical)
        macro=json.loads(read('data/report-measurements.json'))
        assert macro['generated_at']==canonical['publication']['generated_at'] and macro['replay']==canonical['publication']['replay']
        assert macro['generated_at']>old_macro['generated_at']
        cm=canonical_fred_replay.pinned_report(macro,read);assert set(model.SERIES)<=set(cm['catalog'])
        restored=canonical_fred_replay.restore(macro,model.SERIES,read)
        coverage={sid:{'originals_replayed':v is not None,'observation_date':macro['measurements'].get(sid,{}).get('date'),
            'source_error':cm.get('errors',{}).get(sid)} for sid,v in restored.items()}
        assert sum(v is not None for v in restored.values())>=12,'Existing declared source coverage regressed'
        request=invoke_public(lam,s3);r.kv(public_producer_request=request,declared_source_coverage=coverage)
        raw=public(store.CURRENT);packet=json.loads(raw);status=request['status']
        assert packet['contract']==model.CONTRACT and packet['generated_at']==status['generated_at'] and packet['replay']==status['replay']
        assert packet['source_generated_at']==macro['generated_at'] and json.loads(read(store.CURRENT))==packet
        assert packet['call'] is None and packet['portfolio_action']=='WAIT' and all(packet[k] is False for k in model.PERMISSIONS)
        assert packet['fed']['meeting_probabilities'] is None and packet['fed']['near_term_stance'] is None
        assert packet['recession']['ny_fed_12m_prob_pct'] is None and packet['recession']['composite_score_0_100'] is None
        for d in packet['descriptive_comparisons'].values():
            if d.get('inputs'):assert {v['date'] for v in d['inputs'].values()}=={d['observation_date']}
        assert packet['descriptive_comparisons']['recession_estimate_12m_change']['unit']=='percentage_points'
        assert packet['volatility_identities']['VIXCLS']['underlying']=='SPX'
        assert packet['volatility_identities']['VXVCLS']['index_horizon_calendar_days']==90
        replay=replay_verify(packet,read)
        run=json.loads(read(packet['replay']['manifest_key']));inputs=store.checked(run['input'],'inputs',read)
        for ref in (inputs['macro'],*inputs['legacy'].values()):
            if ref:protected(ref,read);protected_keys.add(ref['key'])
        for key in sorted(protected_keys):assert denied('https://justhodl.ai/'+key) and denied('https://'+BUCKET+'.s3.amazonaws.com/'+key),'Protected source publicly readable'
        # Recheck receipts after source collection and publication.
        for fn in FUNCTIONS:
            cfg=lam.get_function_configuration(FunctionName=fn);receipt=json.loads(public('data/ops/releases/'+fn+'.json'))
            assert receipt['commit']==COMMIT and receipt['code_sha256']==cfg['CodeSha256']==runtimes[fn]['code_sha256']
        proof={'contract':'implied-native-acceptance.v1','verified_at':datetime.now(timezone.utc).isoformat(),'runtime_commit':COMMIT,
            'runtimes':runtimes,'pages_commit':pages_commit,'assets':{name:build['files_sha256'][name] for name in ASSETS},'portable_reference_digest':portable,
            'publication':{'generated_at':packet['generated_at'],'source_generated_at':packet['source_generated_at'],'replay':packet['replay'],'quality':packet['quality']},
            'public_sha256':model.sha(raw),'request':request,'canonical_collection':canonical,'source_replay':replay,'declared_source_coverage':coverage,'schedule':cadence,
            'preflight_originals_replayed':12,'whole_originals_anonymously_denied':True,'protected_artifacts_checked':len(protected_keys),
            'acceptance_public_producer_invocations':int(request['invoke_sent']),'prior_canonical_dispatch_attempts':1,'canonical_dispatches_during_recovery':0,'acceptance_consumer_invocations':0,
            'private_account_reads':0,'paid_ai_calls':0,'notifications_sent':0,'portfolio_writes':0,
            'remaining':'Dated descriptive research. Reviewed futures meeting models, physical return calibration, earnings-event pricing and portfolio recommendations remain unqualified.'}
        key='data/implied-research-verification.json';s3.put_object(Bucket=BUCKET,Key=key,Body=model.encoded(proof),ContentType='application/json',CacheControl='no-store')
        assert json.loads(public(key))==proof
        r.kv(accepted=True,proof_key=key,source_replay=replay,protected_artifacts_checked=len(protected_keys))

if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
