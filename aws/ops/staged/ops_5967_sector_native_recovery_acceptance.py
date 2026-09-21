"""Verify exact sector packages, original replay and public pages; invoke reviewed public research producers.

No account consumers, paid AI, notifications, portfolio writes or account changes; retains reviewed cadence configurations and removes duplicate public producer schedules.
"""
from pathlib import Path
from datetime import datetime,timezone
import base64,hashlib,io,json,re,subprocess,sys,time,urllib.request,urllib.error,zipfile
import boto3
from botocore.config import Config
ROOT=Path(__file__).resolve().parents[3];SOURCE=ROOT/'aws/lambdas/justhodl-sector-rotation/source'
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/checks'),str(ROOT/'aws/shared'),str(ROOT/'scripts'),str(SOURCE)]
from ops_report import report
from release_package_evidence import shared_imports
import sector_research_store as store
import sector_research_model as model
from replay_sector_research import verify as replay_verify
BUCKET='justhodl-dashboard-live'
COMMIT='a2a8577e523fba2a4a88d8ea7c8d550ee7242e76'
BASE_COMMIT='cc78b3e809e7d9c7c3375db420dc26b2d078d2b8'
PAGE_COMMIT='6e39f6020b7dad0672351fcf4b7536c51aa1e705'
FUNCTIONS=('justhodl-ai-brief', 'justhodl-ai-chat', 'justhodl-alert-router', 'justhodl-allocator', 'justhodl-best-setups', 'justhodl-daily-report-v3', 'justhodl-deal-scanner', 'justhodl-invest', 'justhodl-macro-confluence', 'justhodl-master-ranker', 'justhodl-morning-brief-tg', 'justhodl-morning-intelligence', 'justhodl-sector-capital-fusion', 'justhodl-sector-flow-state', 'justhodl-sector-rotation', 'justhodl-sector-tilt', 'justhodl-wave-signal-logger')
ASSETS=('rotation/index.html','sectors.html','sector-tilt.html','jh-sector-research.js','jh-sector-research.css','alpha-scoreboard.html','sector-flow.html')
AUDIT=('0f2626f156592935f76faa19e4e9e2413394c745b7a74c3d3ce4060b2311b5a4',62188)
import sector_market_replay
import sector_tilt_store as tilt
from replay_sector_tilt_research import verify as tilt_verify
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
    assert receipt['commit']==expected_commit(fn) and receipt['code_sha256']==cfg['CodeSha256'],'Exact receipt required: '+fn
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
    if fn=='justhodl-sector-rotation':assert cfg['MemorySize']==1024 and cfg['Timeout']==300
    if fn=='justhodl-sector-tilt':assert cfg['MemorySize']==1024 and cfg['Timeout']==300
    return {'commit':expected_commit(fn),'code_sha256':cfg['CodeSha256'],'packaged_files_checked':len(expected),'all_packaged_sources_match':True,'alias':alias}

def protected(ref,read):
    assert ref['key']==model.PRIVATE+ref['sha256']+'.bin'
    raw=read(ref['key']);assert len(raw)==ref['bytes'] and model.sha(raw)==ref['sha256']
    return raw

def retain_config(s3,doc):
    raw=json.dumps(doc,sort_keys=True,separators=(',',':'),default=str).encode();digest=model.sha(raw);key=model.PRIVATE+digest+'.bin'
    store.immutable(s3,BUCKET,key,raw,'application/octet-stream')
    return {'key':key,'sha256':digest,'bytes':len(raw)}

def schedules(lam,s3,events,scheduler):
    fn='justhodl-sector-rotation';cfg=lam.get_function_configuration(FunctionName=fn);names=[]
    for page in events.get_paginator('list_rule_names_by_target').paginate(TargetArn=cfg['FunctionArn']):names.extend(page['RuleNames'])
    expected={'justhodl-sector-rotation-6h','justhodl-sector-rotation-hourly','justhodl-sector-rotation-sched'}
    assert set(names)==expected,'Unreviewed Rotation cadence'
    out=[];refs=[]
    for name in sorted(names):
        rule=events.describe_rule(Name=name);targets=events.list_targets_by_rule(Rule=name)['Targets']
        assert len(targets)==1 and targets[0]['Arn']==cfg['FunctionArn'] and not targets[0].get('InputTransformer')
        assert json.loads(targets[0].get('Input') or '{}')=={},'Unexpected scheduled source action'
        refs.append(retain_config(s3,{'kind':'EventBridge rule','rule':rule,'targets':targets}))
        if name!='justhodl-sector-rotation-hourly' and rule['State']=='ENABLED':events.disable_rule(Name=name)
        after=events.describe_rule(Name=name);assert events.list_targets_by_rule(Rule=name)['Targets']==targets
        assert after['State']==('ENABLED' if name.endswith('-hourly') else 'DISABLED')
        if name.endswith('-hourly'):assert after['ScheduleExpression']=='cron(38 * * * ? *)'
        out.append({'kind':'EventBridge rule','name':name,'before_state':rule['State'],'state':after['State'],'expression':after['ScheduleExpression']})
    fn='justhodl-sector-tilt';cfg=lam.get_function_configuration(FunctionName=fn);names=[]
    for page in events.get_paginator('list_rule_names_by_target').paginate(TargetArn=cfg['FunctionArn']):names.extend(page['RuleNames'])
    assert not names,'Unreviewed Tilt classic schedule'
    found=[]
    for fn in ('justhodl-sector-rotation','justhodl-sector-tilt'):
        arn=lam.get_function_configuration(FunctionName=fn)['FunctionArn']
        for page in scheduler.get_paginator('list_schedules').paginate(NamePrefix=fn):
            for row in page.get('Schedules',[]):
                if row.get('Target',{}).get('Arn')!=arn:continue
                current=scheduler.get_schedule(Name=row['Name'],GroupName=row['GroupName']);found.append(current)
    assert len(found)==1,'Exact one direct Tilt schedule required'
    current=found[0];assert current['Name']=='justhodl-sector-tilt-research-hourly' and current['State']=='ENABLED'
    assert current['ScheduleExpression']=='cron(46 * * * ? *)' and current['ScheduleExpressionTimezone']=='UTC'
    assert current['Target']['Arn']==cfg['FunctionArn'] and current['Target']['RoleArn']=='arn:aws:iam::857687956942:role/justhodl-scheduler-role'
    assert json.loads(current['Target'].get('Input') or '{}')=={} and current['Target']['RetryPolicy']=={'MaximumRetryAttempts':0,'MaximumEventAgeInSeconds':900}
    refs.append(retain_config(s3,{'kind':'EventBridge Scheduler','schedule':current}))
    out.append({'kind':'EventBridge Scheduler','name':current['Name'],'state':current['State'],'expression':current['ScheduleExpression'],'timezone':'UTC'})
    return {'schedules':out,'retained_configurations':refs,'enabled_public_producer_schedules':2}

def canonical_refresh(lam,s3):
    request='chatgpt-sector-canonical-'+COMMIT[:12]+'-1';key=store.request_key(request)
    read=store.reader(s3,BUCKET);started=datetime.now(timezone.utc).isoformat();sent=False
    def ready(after=None):
        packet=json.loads(read('data/report.json'));now=datetime.now(timezone.utc)
        if not set(model.SYMBOLS)<=set(packet.get('stocks',{})):return None
        if after and model.clock(packet['generated_at'])<=model.clock(after):return None
        if not 0<=(now-model.clock(packet['generated_at'])).total_seconds()<26*3600:return None
        sources=sector_market_replay.restore(packet,model.SYMBOLS,read)
        if any(v is None or not 0<=(now-model.clock(v['acquired_at'])).total_seconds()<26*3600 for v in sources.values()):return None
        return packet
    packet=ready()
    if packet:return {'invoke_sent':False,'basis':'Existing fresh canonical publication with all 28 original ETF sources','publication':{k:packet[k] for k in ('generated_at','replay')}}
    claim={'contract':'sector-canonical-acceptance-request.v1','request_id':request,'started_at':started,'status':'claimed'}
    try:store.status_write(s3,BUCKET,key,claim,IfNoneMatch='*')
    except Exception as exc:
        if not store.conflict(exc):raise
        claim=json.loads(store.bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body']))
        assert claim['contract']=='sector-canonical-acceptance-request.v1' and claim['request_id']==request
    else:
        response,rejected=invoke_when_available(lam,{'FunctionName':'justhodl-daily-report-v3','InvocationType':'Event','Payload':model.encoded({})})
        assert response['StatusCode']==202;sent=True
        claim.update(status='accepted_async',lambda_http_status=202,throttle_rejections_before_acceptance=rejected);store.status_write(s3,BUCKET,key,claim)
    deadline=time.monotonic()+900
    while True:
        packet=ready(claim['started_at'])
        if packet:break
        assert time.monotonic()<deadline,'Expanded canonical publication absent; never repeat a claimed collector call'
        time.sleep(10)
    observed={**claim,'status':'source_publication_verified','verified_at':datetime.now(timezone.utc).isoformat(),'publication':{k:packet[k] for k in ('generated_at','replay')},'basis':'Pinned original-source publication observed after the durable claim; no fabricated synchronous result.'}
    store.status_write(s3,BUCKET,key,observed)
    return {**observed,'invoke_sent':sent}

def invoke_public(lam,s3,module,fn):
    request='chatgpt-'+fn+'-'+COMMIT[:12]+'-1';key=module.request_key(request);dispatch_key=module.request_key(request+'-dispatch');sent=False
    claim={'contract':'sector-native-dispatch.v1','request_id':request,'started_at':datetime.now(timezone.utc).isoformat(),'status':'claimed'}
    try:module.status_write(s3,BUCKET,dispatch_key,claim,IfNoneMatch='*')
    except Exception as exc:
        if not module.conflict(exc):raise
        claim=json.loads(module.bounded(s3.get_object(Bucket=BUCKET,Key=dispatch_key)['Body']))
        assert claim['request_id']==request and claim['contract']=='sector-native-dispatch.v1'
    else:
        response,rejected=invoke_when_available(lam,{'FunctionName':fn,'InvocationType':'Event','Payload':model.encoded({'request_id':request})})
        assert response['StatusCode']==202;sent=True
        claim.update(status='accepted_async',throttle_rejections_before_acceptance=rejected);module.status_write(s3,BUCKET,dispatch_key,claim)
    deadline=time.monotonic()+360;status=None
    while time.monotonic()<deadline:
        try:status=json.loads(module.bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body']))
        except Exception as exc:
            if not module.missing(exc):raise
        if status and status.get('status') in ('complete','failed'):break
        time.sleep(3)
    assert status and status.get('status')=='complete' and status.get('published') is True,'Native producer did not publish; inspect durable request without reinvoking'
    return {'request_id':request,'status_key':key,'invoke_sent':sent,'status':status}


def expected_commit(fn):
    if fn not in FUNCTIONS:raise ValueError('Unreviewed runtime')
    return COMMIT if fn in ('justhodl-sector-rotation','justhodl-sector-tilt') else BASE_COMMIT

def prior_failure():
    import ast
    path=ROOT/'aws/ops/reports/latest/ops_5966_sector_tilt_runtime_diagnosis.md'
    raw=path.read_bytes();lines=[line for line in raw.decode().splitlines() if line.startswith('|')]
    columns=[x.strip() for x in lines[0].split('|')[1:-1]];values=[x.strip() for x in lines[2].split('|')[1:-1]];row=dict(zip(columns,values))
    request=ast.literal_eval(row['request']);cfg=ast.literal_eval(row['configuration']);reports=ast.literal_eval(row['managed_runtime_reports'])
    assert request['execution_id']=='29b29ffe-ef65-4eff-9cf7-512a1fe565bb' and request['request_id']=='chatgpt-justhodl-sector-tilt-cc78b3e809e7-1'
    assert row['commit']==BASE_COMMIT and cfg['MemorySize']==256 and cfg['Timeout']==180
    assert any(v.get('status')=='timeout' and v.get('duration_ms')==180000 and v.get('max_memory_mb')==256 for v in reports)
    return {'diagnosis_report':str(path.relative_to(ROOT)),'report_sha256':model.sha(raw),'request':request,'managed_reports':reports,'new_release_has_new_request_identity':True}

def execution_profile(logs,fn,status):
    sys.path.insert(0,str(ROOT/'aws/ops/staged'))
    from ops_5966_sector_tilt_runtime_diagnosis import parse_runtime
    execution=status['execution_id'];assert re.fullmatch(r'[a-f0-9-]{36}',execution)
    start=model.clock(status['started_at']);deadline=time.monotonic()+45
    while True:
        page=logs.filter_log_events(logGroupName='/aws/lambda/'+fn,filterPattern='"'+execution+'"',startTime=int(start.timestamp()*1000)-5000,limit=50)
        values=[parse_runtime(row.get('message',''),execution) for row in page.get('events',[])]
        values=[v for v in values if v]
        if values:break
        assert time.monotonic()<deadline,'Managed execution profile unavailable; do not reinvoke a completed request'
        time.sleep(3)
    assert all(v.get('status') not in ('error','timeout') for v in values)
    assert all(v.get('memory_mb')==1024 and v.get('max_memory_mb',1024)<1024 and v.get('duration_ms',300000)<300000 for v in values)
    return {'execution_id':execution,'managed_reports':values,'completed_request':True}

def main():
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=310,connect_timeout=10,retries={'max_attempts':0},tcp_keepalive=True))
    s3=boto3.client('s3',region_name='us-east-1');events=boto3.client('events',region_name='us-east-1');scheduler=boto3.client('scheduler',region_name='us-east-1')
    with report('ops_5967_sector_native_recovery_acceptance') as r:
        subprocess.run([sys.executable,str(ROOT/'tests/test_sector_recovery_acceptance.py')],cwd=ROOT,check=True)
        subprocess.run([sys.executable,str(ROOT/'aws/lambdas/justhodl-sector-rotation/tests/run_tests.py')],cwd=ROOT,check=True)
        recovery=prior_failure();r.kv(verified_prior_terminal_failure=recovery)
        runtimes={fn:runtime(lam,fn) for fn in FUNCTIONS}
        r.kv(runtimes=runtimes,acceptance_consumer_invocations=0,private_account_reads=0,paid_ai_calls=0,notifications_sent=0,portfolio_writes=0)
        read=store.reader(s3,BUCKET);digest,size=AUDIT;key=model.PRIVATE+digest+'.bin';raw=read(key)
        assert len(raw)==size and model.sha(raw)==digest
        audit=json.loads(raw);protected_keys={key}
        for ref in (*audit['packets'].values(),*audit['source_artifacts'].values()):protected(ref,read);protected_keys.add(ref['key'])
        assert len(audit['packets'])==9 and len(audit['source_artifacts'])==37
        old_report=json.loads(protected(audit['packets']['data/report.json'],read))
        original=sector_market_replay.restore(old_report,model.SYMBOLS,lambda key:protected(audit['source_artifacts'][key],read))
        assert sum(v is not None for v in original.values())==24
        legacy={}
        for fn,name,size in [('justhodl-sector-rotation','legacy_sector_rotation.py',35346),('justhodl-sector-tilt','legacy_sector_tilt.py',18200)]:
            raw=subprocess.check_output(['git','show','4f6e67ecc:aws/lambdas/'+fn+'/source/lambda_function.py'],cwd=ROOT)
            assert (ROOT/'aws/lambdas'/fn/'source'/name).read_bytes()==raw and len(raw)==size
            legacy[fn]={'bytes':size,'sha256':model.sha(raw)}
        build=json.loads(public('build-manifest.json'));pages_commit=build['commit_sha']
        subprocess.run(['git','merge-base','--is-ancestor',PAGE_COMMIT,pages_commit],cwd=ROOT,check=True)
        subprocess.run(['git','diff','--quiet',PAGE_COMMIT,pages_commit,'--',*ASSETS],cwd=ROOT,check=True)
        for name in ASSETS:
            served=public(name);clean,count=re.subn(rb'<script\b[^>]*\bsrc="https://static\.cloudflareinsights\.com/beacon\.min\.js/v[a-f0-9]+"[^>]*></script>\n?',b'',served)
            assert count<=1 and model.sha(clean)==build['files_sha256'][name],'Built asset differs: '+name
            if name in ('rotation/index.html','sectors.html','sector-tilt.html'):assert b'jh-sector-research.js?v=20260921-native1' in served
        cadence=schedules(lam,s3,events,scheduler)
        for ref in cadence['retained_configurations']:protected(ref,read);protected_keys.add(ref['key'])
        r.kv(schedule=cadence,whole_predecessors=legacy,preflight_originals_replayed=24,pages_commit=pages_commit)
        canonical=canonical_refresh(lam,s3);r.kv(canonical_source_publication=canonical)
        requests={};publications={};replays={};profiles={};logs=boto3.client('logs',region_name='us-east-1')
        for fn,module,verify in [('justhodl-sector-rotation',store,replay_verify),('justhodl-sector-tilt',tilt,tilt_verify)]:
            request=invoke_public(lam,s3,module,fn);requests[fn]=request;r.kv(public_producer=fn,request=request)
            profiles[fn]=execution_profile(logs,fn,request['status']);r.kv(execution_profile=profiles[fn])
            scoped_read=module.reader(s3,BUCKET);raw=public(module.CURRENT);packet=json.loads(raw);status=request['status']
            assert packet['contract']==module.model.CONTRACT and model.clock(packet['generated_at'])>=model.clock(status['generated_at'])
            assert json.loads(scoped_read(module.CURRENT))==packet and packet['call'] is None and packet['portfolio_action']=='WAIT'
            assert all(packet[k] is False for k in model.PERMISSIONS) and datetime.now(timezone.utc)<model.clock(packet['source_valid_until'])
            requested=module.replay(status['replay'],scoped_read);assert requested['generated_at']==status['generated_at']
            replays[fn]=verify(packet,scoped_read)
            run=json.loads(scoped_read(packet['replay']['manifest_key']));inputs=module.checked(run['input'],'inputs',scoped_read)
            for ref in (inputs['market'],*inputs['legacy'].values()):
                if ref:protected(ref,read);protected_keys.add(ref['key'])
            if module is store:
                assert packet['quality']['replayed_sources']==28 and packet['quality']['within_age_ceiling']==28 and packet['quality']['core_reference_coverage']==11
                assert len(packet['ratios'])==14 and len(packet['benchmark_dates'])>=275 and packet['risk_sample']['status']=='available'
                assert len(packet['risk_sample']['return_rows'])==63 and packet['risk_sample']['statistics']['SPY']['beta_to_SPY']==1
                assert packet['quality']['independent_investment_votes']==0 and packet['dependency_graph']['independent_votes']==0
            else:
                assert len(packet['tilts'])==11 and packet['regime'] is None and packet['summary']['n_unavailable']==11
                assert packet['scenario_data']['status']=='available' and packet['scenario_data']['covariance_unit']=='percent_squared'
                assert not packet['summary']['top_buy_opportunities'] and not packet['summary']['top_fade_opportunities']
            publications[fn]={'generated_at':packet['generated_at'],'source_generated_at':packet['source_generated_at'],'source_valid_until':packet['source_valid_until'],'replay':packet['replay'],'quality':packet['quality'],'public_sha256':model.sha(raw),'public_bytes':len(raw)}
        for key in sorted(protected_keys):assert denied('https://justhodl.ai/'+key) and denied('https://'+BUCKET+'.s3.amazonaws.com/'+key),'Protected source readable anonymously'
        for fn in FUNCTIONS:
            cfg=lam.get_function_configuration(FunctionName=fn);receipt=json.loads(public('data/ops/releases/'+fn+'.json'))
            assert receipt['commit']==expected_commit(fn) and receipt['code_sha256']==cfg['CodeSha256']==runtimes[fn]['code_sha256']
        proof={'contract':'sector-native-acceptance.v1','verified_at':datetime.now(timezone.utc).isoformat(),'native_runtime_commit':COMMIT,'runtime_commits':{fn:expected_commit(fn) for fn in FUNCTIONS},'verified_prior_terminal_failure':recovery,'execution_profiles':profiles,'runtimes':runtimes,'pages_commit':pages_commit,'assets':{name:build['files_sha256'][name] for name in ASSETS},'publications':publications,'requests':requests,'canonical_source':canonical,'source_replays':replays,'schedule':cadence,'whole_predecessors_preserved':legacy,'protected_artifacts_checked':len(protected_keys),'whole_originals_anonymously_denied':True,'preflight_originals_replayed':24,'native_original_sources':28,'acceptance_public_producer_invocations':sum(int(x['invoke_sent']) for x in requests.values())+int(canonical['invoke_sent']),'acceptance_consumer_invocations':0,'private_account_reads':0,'paid_ai_calls':0,'notifications_sent':0,'portfolio_writes':0,'native_provider_requests':0,'canonical_provider_scope':'Existing reviewed default Daily Report public-source collector; added four declared ETF wrappers. No new provider, paid AI, account or notification route.','remaining':'Current retrieved split-adjusted prices, no dividends or historical first availability. Descriptive risk and user-entered exposure scenarios do not qualify future return, portfolio sizing or independent investment votes.'}
        key='data/sector-research-verification.json';s3.put_object(Bucket=BUCKET,Key=key,Body=model.encoded(proof),ContentType='application/json',CacheControl='no-store')
        assert json.loads(public(key))==proof
        r.kv(accepted=True,proof_key=key,source_replays=replays,publications=publications,protected_artifacts_checked=len(protected_keys))

if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
