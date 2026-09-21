"""Verify exact Weekly Activity packages, original replay and public page; invoke one reviewed public research producer.

No account consumers, paid AI, notifications, portfolio writes or account changes; leaves the existing public schedule unchanged.
"""
from pathlib import Path
from datetime import datetime,timezone
import base64,hashlib,io,json,re,subprocess,sys,time,urllib.request,urllib.error,zipfile
import boto3
from botocore.config import Config
ROOT=Path(__file__).resolve().parents[3];SOURCE=ROOT/'aws/lambdas/justhodl-activity-nowcast/source'
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/checks'),str(ROOT/'aws/shared'),str(ROOT/'scripts'),str(SOURCE)]
from ops_report import report
from release_package_evidence import shared_imports
import activity_research_store as store
import activity_research_model as model
from replay_activity_research import verify as replay_verify
BUCKET='justhodl-dashboard-live'
COMMIT='9ded714981d97caaf194c94e6d55b6efebe80ecd'
PAGE_COMMIT=COMMIT
FUNCTIONS=('justhodl-activity-nowcast','justhodl-daily-report-v3')
ASSETS=('activity-nowcast.html','jh-activity-research.js')
AUDIT=('269a422be906ceb70fc4692aaad62cad99f5be2899a3636b5ca319cae7af5a64',46734)
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
    if fn=='justhodl-activity-nowcast':assert cfg['MemorySize']==256 and cfg['Timeout']==90
    return {'commit':COMMIT,'code_sha256':cfg['CodeSha256'],'packaged_files_checked':len(expected),'all_packaged_sources_match':True,'alias':alias}

def protected(ref,read):
    assert ref['key']==model.PRIVATE+ref['sha256']+'.bin'
    raw=read(ref['key']);assert len(raw)==ref['bytes'] and model.sha(raw)==ref['sha256']
    return raw

def invoke_public(lam,s3):
    request='chatgpt-activity-native-'+COMMIT[:12]+'-1';key=store.request_key(request);sent=False;status=None
    try:status=json.loads(store.bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body']))
    except Exception as exc:
        if not store.missing(exc):raise
        response=lam.invoke(FunctionName='justhodl-activity-nowcast',InvocationType='Event',Payload=model.encoded({'request_id':request}));assert response['StatusCode']==202;sent=True
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
    fn='justhodl-activity-nowcast';cfg=lam.get_function_configuration(FunctionName=fn);names=[]
    for page in events.get_paginator('list_rule_names_by_target').paginate(TargetArn=cfg['FunctionArn']):names.extend(page['RuleNames'])
    assert names==['activity-nowcast-daily'];rule=events.describe_rule(Name=names[0]);targets=events.list_targets_by_rule(Rule=names[0])['Targets']
    assert rule['State']=='ENABLED' and rule['ScheduleExpression']=='cron(30 12 * * ? *)' and len(targets)==1
    assert targets[0]['Arn']==cfg['FunctionArn'] and not targets[0].get('InputTransformer')
    assert not json.loads(targets[0].get('Input') or '{}').get('request_id')
    return {'name':names[0],'state':rule['State'],'expression':rule['ScheduleExpression'],'native_targets':1,'changed':False}



def canonical_refresh(lam,s3):
    request='chatgpt-activity-canonical-'+COMMIT[:12]+'-1';key=store.request_key(request)
    read=store.reader(s3,BUCKET);started=datetime.now(timezone.utc).isoformat();sent=False
    required=set(model.SERIES)|{'RRSFS'}
    def ready(after=None):
        packet=json.loads(read('data/report-measurements.json'))
        manifest=canonical_fred_replay.pinned_report(packet,read)
        if not required<=set(manifest['catalog']):return None
        if after and model.clock(packet['generated_at'])<=model.clock(after):return None
        if (datetime.now(timezone.utc)-model.clock(packet['generated_at'])).total_seconds()>26*3600:return None
        return packet
    packet=ready()
    if packet:return {'invoke_sent':False,'basis':'Existing independently pinned canonical publication with the expanded catalog','publication':{k:packet[k] for k in ('generated_at','quality','replay')}}
    claim={'contract':'activity-canonical-acceptance-request.v1','request_id':request,'started_at':started,'status':'claimed'}
    try:store.status_write(s3,BUCKET,key,claim,IfNoneMatch='*')
    except Exception as exc:
        if not store.conflict(exc):raise
        claim=json.loads(store.bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body']))
        assert claim['contract']=='activity-canonical-acceptance-request.v1' and claim['request_id']==request
    else:
        response,rejected=invoke_when_available(lam,{'FunctionName':'justhodl-daily-report-v3','InvocationType':'Event','Payload':model.encoded({'action':'research_measurements'})})
        assert response['StatusCode']==202;sent=True
        claim.update(status='accepted_async',lambda_http_status=202,throttle_rejections_before_acceptance=rejected)
        store.status_write(s3,BUCKET,key,claim)
    deadline=time.monotonic()+900
    while True:
        packet=ready(claim['started_at'])
        if packet:break
        assert time.monotonic()<deadline,'Expanded canonical publication absent; do not repeat the claimed collector call'
        time.sleep(10)
    observed={**claim,'status':'source_publication_verified','verified_at':datetime.now(timezone.utc).isoformat(),
        'publication':{k:packet[k] for k in ('generated_at','quality','replay')},
        'basis':'Observed pinned source publication after the durable claim; asynchronous acceptance is not a fabricated synchronous Lambda result.'}
    store.status_write(s3,BUCKET,key,observed)
    return {**observed,'invoke_sent':sent}

def main():
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=310,connect_timeout=10,retries={'max_attempts':0},tcp_keepalive=True))
    s3=boto3.client('s3',region_name='us-east-1');events=boto3.client('events',region_name='us-east-1')
    with report('ops_5963_activity_native_acceptance') as r:
        subprocess.run([sys.executable,str(ROOT/'tests/test_activity_acceptance.py')],cwd=ROOT,check=True)
        subprocess.run([sys.executable,str(SOURCE.parent/'tests/run_tests.py')],cwd=ROOT,check=True)
        sys.path.insert(0,str(SOURCE.parent/'tests'));from activity_fixture import fixture
        client,inputs,_,_=fixture();portable=model.sha(model.encoded(store.compile_output(inputs,store.reader(client,'b'))))
        assert portable=='db4f6bb23bba928dc8d5167c88fdec9e336c60247d8aca10f7c20b69498438ce'
        runtimes={fn:runtime(lam,fn) for fn in FUNCTIONS};cadence=schedule(lam,events)
        r.kv(runtimes=runtimes,schedule=cadence,acceptance_consumer_invocations=0,private_account_reads=0,paid_ai_calls=0,notifications_sent=0,portfolio_writes=0)
        read=store.reader(s3,BUCKET);digest,size=AUDIT;key=model.PRIVATE+digest+'.bin';raw=read(key)
        assert len(raw)==size and model.sha(raw)==digest
        audit=json.loads(raw);protected_keys={key}
        for ref in (*audit['packets'].values(),*audit['canonical_originals'].values(),*audit['legacy_snapshots'].values()):protected(ref,read);protected_keys.add(ref['key'])
        old_macro=json.loads(protected(audit['packets']['data/report-measurements.json'],read))
        restored=canonical_fred_replay.restore(old_macro,model.CORE,lambda key:protected(audit['canonical_originals'][key],read))
        assert sum(v is not None for v in restored.values())==5 and restored['WEI'] is None
        assert len(audit['legacy_snapshots'])==123 and audit['legacy_snapshot_bytes']==13490
        for key,ref in audit['legacy_snapshots'].items():assert store.bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body'])==protected(ref,read)
        original=subprocess.check_output(['git','show','d615d88e6:aws/lambdas/justhodl-activity-nowcast/source/lambda_function.py'],cwd=ROOT)
        assert (SOURCE/'legacy_activity_nowcast.py').read_bytes()==original and len(original)==12066
        old_packet=json.loads(protected(audit['packets']['data/activity-nowcast.json'],read))
        assert old_packet['activity_index']==72 and old_packet['divergence']['gap']==72.2
        build=json.loads(public('build-manifest.json'));pages_commit=build['commit_sha']
        subprocess.run(['git','merge-base','--is-ancestor',PAGE_COMMIT,pages_commit],cwd=ROOT,check=True)
        subprocess.run(['git','diff','--quiet',COMMIT,pages_commit,'--',*ASSETS],cwd=ROOT,check=True)
        for name in ASSETS:
            served=public(name);clean,count=re.subn(rb'<script\b[^>]*\bsrc="https://static\.cloudflareinsights\.com/beacon\.min\.js/v[a-f0-9]+"[^>]*></script>\n?',b'',served)
            assert count<=1 and model.sha(clean)==build['files_sha256'][name],'Built asset differs: '+name
            if name=='activity-nowcast.html':assert b'jh-activity-research.js?v=20260921-native1' in served
        r.kv(preflight_originals_replayed=5,pages_commit=pages_commit,whole_predecessor_bytes=len(original),legacy_snapshots_preserved=123)
        canonical=canonical_refresh(lam,s3);r.kv(canonical_source_publication=canonical)
        request=invoke_public(lam,s3);r.kv(public_producer_request=request)
        raw=public(store.CURRENT);packet=json.loads(raw);status=request['status']
        assert packet['contract']==model.CONTRACT and packet['generated_at']==status['generated_at'] and packet['replay']==status['replay']
        assert packet['generated_at']>audit['metadata']['data/activity-nowcast.json']['generated_at'] and json.loads(read(store.CURRENT))==packet
        assert packet['call'] is None and packet['portfolio_action']=='WAIT' and all(packet[k] is False for k in model.PERMISSIONS)
        assert packet['quality']['available_histories']==8 and len(packet['measurements'])==8
        assert packet['quality']['within_age_ceiling']==8
        assert packet['regime'] is None and packet['activity_index'] is None and packet['activity_z'] is None
        assert packet['quality']['independent_investment_votes']==0 and packet['dependency_graph']['independent_votes']==0
        weeks=packet['weekly_context'];assert len(weeks['trail'])==156 and weeks['current'] and weeks['current']['available_components']==6
        assert packet['measurements']['WEI']['value'] is not None and packet['measurements']['GDPNOW']['frequency']=='Q'
        assert packet['regional_context']['T10Y3M']['cleveland_recession_probability'] is None
        assert packet['divergence']['available'] is False and packet['divergence']['gap'] is None
        for c in weeks['current']['components'].values():
            assert c['observation']['date']<=c['reference_week_ending']
            if c['prior_standardization']:assert c['prior_standardization']['window_end_exclusive']==c['observation']['date']
        replay=replay_verify(packet,read);run=json.loads(read(packet['replay']['manifest_key']));inputs=store.checked(run['input'],'inputs',read)
        for ref in (inputs['macro'],*inputs['legacy'].values()):
            if ref:protected(ref,read);protected_keys.add(ref['key'])
        for key,ref in audit['legacy_snapshots'].items():assert store.bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body'])==protected(ref,read),'Legacy snapshot changed'
        for key in sorted(protected_keys):assert denied('https://justhodl.ai/'+key) and denied('https://'+BUCKET+'.s3.amazonaws.com/'+key),'Protected source publicly readable'
        for fn in FUNCTIONS:
            cfg=lam.get_function_configuration(FunctionName=fn);receipt=json.loads(public('data/ops/releases/'+fn+'.json'))
            assert receipt['commit']==COMMIT and receipt['code_sha256']==cfg['CodeSha256']==runtimes[fn]['code_sha256']
        proof={'contract':'activity-native-acceptance.v1','verified_at':datetime.now(timezone.utc).isoformat(),'runtime_commit':COMMIT,
            'runtimes':runtimes,'pages_commit':pages_commit,'assets':{name:build['files_sha256'][name] for name in ASSETS},'portable_reference_digest':portable,
            'publication':{'generated_at':packet['generated_at'],'source_generated_at':packet['source_generated_at'],'replay':packet['replay'],'quality':packet['quality']},
            'public_sha256':model.sha(raw),'request':request,'canonical_source':canonical,'source_replay':replay,'schedule':cadence,
            'preflight_originals_replayed':5,'whole_predecessor_bytes_preserved':len(original),'whole_originals_anonymously_denied':True,
            'legacy_snapshots_unchanged':123,'legacy_snapshot_bytes_preserved':13490,'protected_artifacts_checked':len(protected_keys),
            'acceptance_public_producer_invocations':int(request['invoke_sent'])+int(canonical['invoke_sent']),'acceptance_consumer_invocations':0,
            'private_account_reads':0,'native_provider_requests':0,'canonical_provider_scope':'Existing reviewed FRED collection only, expanded by WEI, GDPNOW and RRSFS; catalog and budget bound apply.',
            'paid_ai_calls':0,'notifications_sent':0,'portfolio_writes':0,
            'remaining':'Current-vintage descriptive activity and source-model context. Historical first availability, complete root lineage, forecast-vintage path, independent leading edge, GDP forecasts and portfolio recommendations remain unqualified.'}
        key='data/activity-research-verification.json';s3.put_object(Bucket=BUCKET,Key=key,Body=model.encoded(proof),ContentType='application/json',CacheControl='no-store')
        assert json.loads(public(key))==proof
        r.kv(accepted=True,proof_key=key,source_replay=replay,protected_artifacts_checked=len(protected_keys))

if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
