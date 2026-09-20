"""Verify exact packages, preserved upstream evidence and two public producers.

No downstream consumer invocation, account read, provider request, paid AI,
notification, portfolio write or change of schedule/permissions.
"""
from pathlib import Path
from datetime import datetime,timezone
import base64,hashlib,io,json,re,subprocess,sys,time,urllib.request,urllib.error,zipfile
import boto3
from botocore.config import Config
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/checks'),str(ROOT/'aws/shared'),str(ROOT/'scripts'),str(ROOT/'tests')]
from ops_report import report
from release_package_evidence import shared_imports
import extremes_native_store as store
import extremes_native_model as model
from extremes_research import context as research_context
from replay_extremes_research import verify as replay_verify
BUCKET='justhodl-dashboard-live'
COMMIT='a346ce843feba7054b346738f3220ce6679c9751'
FUNCTIONS=('justhodl-capitulation','justhodl-market-extremes','justhodl-accumulation-radar','justhodl-ai-chat','justhodl-allocator','justhodl-cro-escalation','justhodl-forced-selling-bounce','justhodl-master-ranker','justhodl-pm-decision','justhodl-prepump-alerts-router','justhodl-vol-radar','justhodl-morning-intelligence','justhodl-calibration-fleet')
ASSETS=('capitulation.html','market-extremes.html','jh-extremes-research.js','intelligence/index.html')
AUDIT='c8db81907a5a46add7d0691a2ce042e9095688eb441763643e9dec54443bd491'

def archive_bytes(stream):
    try:raw=stream.read(64*1024*1024+1)
    finally:stream.close()
    assert len(raw)<=64*1024*1024,'AWS package inspection bound exceeded'
    return raw
def public(key):
    with urllib.request.urlopen(urllib.request.Request('https://justhodl.ai/'+key,headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=45) as r:return store.bounded(r)
def denied(url):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,method='HEAD',headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=25):return False
    except urllib.error.HTTPError as exc:return exc.code in (401,403,404)
def runtime(lam,fn):
    source=ROOT/'aws/lambdas'/fn/'source';cfgpath=source.parent/'config.json';configuration=json.loads(cfgpath.read_bytes()) if cfgpath.exists() else {}
    request={'FunctionName':fn}
    if configuration.get('release_validation'):request['Qualifier']='live'
    deployed=lam.get_function(**request);cfg=deployed['Configuration'];receipt=json.loads(public('data/ops/releases/'+fn+'.json'))
    assert receipt['commit']==COMMIT and receipt['code_sha256']==cfg['CodeSha256'],'Exact receipt required: '+fn
    assert cfg['State']=='Active' and cfg['LastUpdateStatus']=='Successful'
    raw=archive_bytes(urllib.request.urlopen(deployed['Code']['Location'],timeout=45))
    assert base64.b64encode(hashlib.sha256(raw).digest()).decode()==cfg['CodeSha256']
    files=[ROOT/p for p in subprocess.check_output(['git','ls-files',str(source.relative_to(ROOT))],cwd=ROOT,text=True).splitlines()]
    expected={p.relative_to(source).as_posix():p for p in files};expected.update({p.name:p for p in shared_imports(ROOT,files) if not (source/p.name).exists()})
    with zipfile.ZipFile(io.BytesIO(raw)) as z:
        for name,p in expected.items():assert z.read(name)==p.read_bytes(),'Packaged bytes differ: '+fn+'/'+name
    alias=None
    if request.get('Qualifier'):
        a=lam.get_alias(FunctionName=fn,Name='live');assert a['FunctionVersion']==cfg['Version'] and not (a.get('RoutingConfig') or {}).get('AdditionalVersionWeights')
        alias={'name':'live','version':cfg['Version'],'weighted_secondary_versions':0}
    if fn in ('justhodl-capitulation','justhodl-market-extremes'):assert cfg['MemorySize']==256 and cfg['Timeout']==60
    return {'commit':COMMIT,'code_sha256':cfg['CodeSha256'],'packaged_files_checked':len(expected),'all_packaged_sources_match':True,'alias':alias}
def schedules(lam,events,scheduler):
    cfg=lam.get_function_configuration(FunctionName='justhodl-capitulation');names=[]
    for page in events.get_paginator('list_rule_names_by_target').paginate(TargetArn=cfg['FunctionArn']):names.extend(page['RuleNames'])
    assert names==['capitulation-3h'];rule=events.describe_rule(Name=names[0]);targets=events.list_targets_by_rule(Rule=names[0])['Targets']
    selected=[t for t in targets if t.get('Arn')==cfg['FunctionArn']]
    assert rule['State']=='ENABLED' and rule['ScheduleExpression']=='cron(45 */3 * * ? *)' and len(selected)==1
    assert not json.loads(selected[0].get('Input') or '{}').get('request_id') and not selected[0].get('InputTransformer')
    m=scheduler.get_schedule(Name='justhodl-market-extremes-daily');cfg2=lam.get_function_configuration(FunctionName='justhodl-market-extremes')
    assert m['State']=='ENABLED' and m['ScheduleExpression']=='cron(0 23 * * ? *)' and m['ScheduleExpressionTimezone']=='UTC' and m['Target']['Arn']==cfg2['FunctionArn']
    assert not json.loads(m['Target'].get('Input') or '{}').get('request_id')
    return {'capitulation':{'name':names[0],'state':rule['State'],'expression':rule['ScheduleExpression'],'native_targets':1},
        'market-extremes':{'name':m['Name'],'state':m['State'],'expression':m['ScheduleExpression'],'timezone':'UTC','native_targets':1}}
def audit_replay(s3,preflight,read):
    summaries={}
    for engine in model.INPUTS:
        entries={}
        for name in model.INPUTS[engine]:
            source_key,contract,_=model.SOURCES[name];ref=preflight['packets'][source_key];raw=store.original(ref,read);p=json.loads(raw)
            entry={'source_key':source_key,'acquired_at':ref['acquired_at'],'status':'retained','packet':{k:ref[k] for k in ('key','sha256','bytes')},'upstream_identity_verified':False}
            if contract and model.identity(p,name):
                runkey=p['replay']['manifest_key'];runraw=store.bounded(s3.get_object(Bucket=BUCKET,Key=runkey)['Body']);run=json.loads(runraw);outkey=run['output']['key']
                assert store.upstream_path(name,outkey,'outputs');outraw=store.bounded(s3.get_object(Bucket=BUCKET,Key=outkey)['Body']);store.verify_upstream(name,p,runraw,outraw)
                entry.update(upstream_identity_verified=True,upstream_run={'source_key':runkey,**store.retain_original(s3,BUCKET,runraw)},upstream_output={'source_key':outkey,**store.retain_original(s3,BUCKET,outraw)})
            entries[name]=entry
        inputs={'contract':'extremes-native-inputs.v1','engine':engine,'started_at':min(v['acquired_at'] for v in entries.values()),'generated_at':preflight['generated_at'],'sources':entries}
        out=store.compile_output(inputs,read)
        assert out['measurements'] and all(out[k] is False for k in model.PERMISSIONS) and out['capitulation_score'] is None
        assert out['pd_settlement_fails']['scopes']['ust_ex_tips']['combined_bn']==233.045 and out['pd_settlement_fails']['scopes']['treasury_incl_tips']['combined_bn']==262.569
        summaries[engine]={'output_sha256':model.sha(model.encoded(out)),'quality':out['quality'],'measurements':len(out['measurements']),
            'overlap_groups':len(out['dependency_graph']['same_series_date_overlaps']),'contexts':{k:v['reason'] for k,v in out['eligibility'].items()}}
    return summaries
def invoke_public(lam,s3,engine):
    request_id='chatgpt-extremes-native-'+COMMIT[:12]+'-'+engine+'-1';key=store.request_key(engine,request_id);invoked=False
    try:status=json.loads(store.bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body']))
    except Exception as exc:
        if not store.missing(exc):raise
        response=lam.invoke(FunctionName='justhodl-'+engine,InvocationType='Event',Payload=model.encoded({'request_id':request_id}));assert response['StatusCode']==202;invoked=True;status=None
    deadline=time.monotonic()+180
    while time.monotonic()<deadline:
        try:status=json.loads(store.bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body']))
        except Exception as exc:
            if not store.missing(exc):raise
        if status and status.get('status') in ('complete','failed'):break
        time.sleep(3)
    assert status and status.get('status')=='complete' and status.get('published') is True,'Native producer request did not publish: '+engine
    return {'request_id':request_id,'status_key':key,'invoke_sent':invoked,'status':status}

def main():
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=45,connect_timeout=10,retries={'max_attempts':0},tcp_keepalive=True))
    s3=boto3.client('s3',region_name='us-east-1');events=boto3.client('events',region_name='us-east-1');scheduler=boto3.client('scheduler',region_name='us-east-1')
    with report('ops_5938_extremes_native_acceptance') as r:
        for test in ('extremes_native_test_support.py','extremes_consumer_test_support.py'):subprocess.run([sys.executable,str(ROOT/'tests'/test)],cwd=ROOT,check=True)
        from extremes_native_test_support import packet as fixture_packet
        _,_,fixture=fixture_packet();portable=fixture['replay']['output_sha256'];assert portable=='cdd96035d70c42065d1510169af00939d5c6399d254123c8cfd3984c0a4b5e39'
        runtimes={fn:runtime(lam,fn) for fn in FUNCTIONS};r.kv(runtimes=runtimes,acceptance_consumer_invocations=0,private_account_reads=0,paid_ai_calls=0,notifications_sent=0,portfolio_writes=0)
        cadence=schedules(lam,events,scheduler);read=store.reader(s3,BUCKET)
        raw=read(model.PRIVATE+AUDIT+'.bin');assert model.sha(raw)==AUDIT and len(raw)==21154;preflight=json.loads(raw)
        for ref in preflight['packets'].values():store.original(ref,read)
        for engine,n in (('capitulation',12305),('market-extremes',17286)):
            legacy=(ROOT/'aws/lambdas'/('justhodl-'+engine)/'source'/('legacy_'+engine.replace('-','_')+'.py')).read_bytes()
            prior=subprocess.check_output(['git','show','13b531a9e:aws/lambdas/justhodl-'+engine+'/source/lambda_function.py'],cwd=ROOT)
            assert legacy==prior and len(legacy)==n
        reconstructed=audit_replay(s3,preflight,read);r.kv(preflight_reconstruction=reconstructed,schedules=cadence)
        histories={engine:store.bounded(s3.get_object(Bucket=BUCKET,Key='data/'+engine+'-history.json')['Body']) for engine in model.INPUTS}
        for raw in histories.values():store.retain_original(s3,BUCKET,raw)
        build=json.loads(public('build-manifest.json'));pages_commit=build['commit_sha']
        subprocess.run(['git','merge-base','--is-ancestor',COMMIT,pages_commit],cwd=ROOT,check=True);subprocess.run(['git','diff','--quiet',COMMIT,pages_commit,'--',*ASSETS],cwd=ROOT,check=True)
        for name in ASSETS:
            served=public(name);clean,count=re.subn(rb'<script\b[^>]*\bsrc="https://static\.cloudflareinsights\.com/beacon\.min\.js/v[a-f0-9]+"[^>]*></script>\n?',b'',served)
            assert count<=1 and model.sha(clean)==build['files_sha256'][name],'Built asset differs: '+name
            if name in ('capitulation.html','market-extremes.html'):assert b'jh-extremes-research.js?v=20260920-native1' in served and b'/nav.js' not in served
        packets={};requests={};replays={};hashes={}
        for engine in ('capitulation','market-extremes'):
            result=invoke_public(lam,s3,engine);requests[engine]=result;r.kv(public_producer_request={engine:result})
            raw=public(store.current(engine));p=json.loads(raw);status=result['status']
            assert p['generated_at']==status['generated_at'] and p['replay']==status['replay'] and p['generated_at']>preflight['generated_at']
            assert p['engine']==engine and p['call'] is None and p['quality']['status']=='partial' and all(p[k] is False for k in model.PERMISSIONS)
            assert p['capitulation_score'] is None and p['posture'] is None and p['cycle_position'] is None and p['decision']['eligible_votes']==0
            assert json.loads(read(store.current(engine)))==p;replays[engine]=replay_verify(p,read);assert research_context(p)['available']
            assert store.bounded(s3.get_object(Bucket=BUCKET,Key='data/'+engine+'-history.json')['Body'])==histories[engine]
            assert p['pd_settlement_fails']['scopes']['ust_ex_tips']['combined_bn']==233.045 and p['pd_settlement_fails']['scopes']['treasury_incl_tips']['combined_bn']==262.569
            first=next(e['packet'] for e in p['input_evidence'].values() if e.get('packet'))
            assert denied('https://justhodl.ai/'+first['key']) and denied('https://'+BUCKET+'.s3.amazonaws.com/'+first['key'])
            packets[engine]=p;hashes[engine]=model.sha(raw)
        assert packets['capitulation']['dependency_graph']['same_series_date_overlaps']
        assert packets['market-extremes']['contexts']['capitulation']['upstream_replay']==packets['capitulation']['replay']
        for name in ('valuation','retail','vrp'):assert packets['market-extremes']['eligibility'][name]['research_context_available'] is False
        proof={'contract':'extremes-native-acceptance.v1','verified_at':datetime.now(timezone.utc).isoformat(),'runtime_commit':COMMIT,'runtimes':runtimes,'pages_commit':pages_commit,
            'portable_reference_digest':portable,'assets':{name:build['files_sha256'][name] for name in ASSETS},'public_sha256':hashes,'requests':requests,
            'source_replay':replays,'preflight_reconstruction':reconstructed,'schedules':cadence,
            'publications':{e:{'generated_at':p['generated_at'],'replay':p['replay'],'quality':p['quality'],'measurement_count':len(p['measurements']),
                'dependency_graph':p['dependency_graph'],'pd_settlement_fails':p['pd_settlement_fails']} for e,p in packets.items()},
            'legacy_history_sha256':{e:model.sha(raw) for e,raw in histories.items()},'legacy_history_unchanged':True,'whole_originals_anonymously_denied':True,
            'acceptance_public_producer_invocations':sum(int(v['invoke_sent']) for v in requests.values()),'acceptance_consumer_invocations':0,'provider_requests':0,'private_account_reads':0,'paid_ai_calls':0,'notifications_sent':0,'portfolio_writes':0,
            'remaining':'Retained upstream publication synthesis; upstream provider parsers are not re-executed here. No validated top/bottom forecast, probability, score, allocation or execution authority.'}
        key='data/extremes-research-verification.json';s3.put_object(Bucket=BUCKET,Key=key,Body=model.encoded(proof),ContentType='application/json',CacheControl='no-store');assert json.loads(public(key))==proof
        r.kv(accepted=True,proof_key=key,source_replay=replays,legacy_history_unchanged=True)

if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
