"""Verify five exact packages, canonical originals and three public research producers.
No account reads, downstream recommendation consumers, paid AI or notifications.
"""
from pathlib import Path
from datetime import datetime,timezone
import base64,hashlib,io,json,re,subprocess,sys,time,urllib.request,urllib.error,zipfile
import boto3
from botocore.config import Config
ROOT=Path(__file__).resolve().parents[3];SOURCE=ROOT/'aws/lambdas/justhodl-vrp/source'
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/checks'),str(ROOT/'aws/shared'),str(ROOT/'scripts'),str(SOURCE)]
from ops_report import report
from release_package_evidence import shared_imports
import vrp_research_store as store
import vrp_research_model as model
import extremes_native_store as extremes_store
import extremes_native_model as extremes_model
from vrp_research import context as research_context
from replay_vrp_research import verify as replay_verify
from replay_extremes_research import verify as extremes_verify
BUCKET='justhodl-dashboard-live'
COMMIT='09fc3ae7e3db3625fa5881aeede5cdf9f90b275f'
FUNCTIONS=('justhodl-vrp','justhodl-capitulation','justhodl-market-extremes','justhodl-vol-radar','justhodl-calibration-fleet')
ASSETS=('vrp.html','jh-vrp-research.js','jh-extremes-research.js','capitulation.html','market-extremes.html')
AUDIT='48d83c73582df5886046fb7d1ecc0802609e932c525ccf1eb73aa2efaa529283'

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
    if fn in ('justhodl-capitulation','justhodl-market-extremes','justhodl-vrp'):assert cfg['MemorySize']==256 and cfg['Timeout']==60
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
    v=scheduler.get_schedule(Name='justhodl-vrp-daily');cfg3=lam.get_function_configuration(FunctionName='justhodl-vrp')
    assert v['State']=='ENABLED' and v['ScheduleExpression']=='cron(30 22 * * ? *)' and v['ScheduleExpressionTimezone']=='UTC' and v['Target']['Arn']==cfg3['FunctionArn']
    assert not json.loads(v['Target'].get('Input') or '{}').get('request_id')
    return {'capitulation':{'name':names[0],'state':rule['State'],'expression':rule['ScheduleExpression'],'native_targets':1},
        'market-extremes':{'name':m['Name'],'state':m['State'],'expression':m['ScheduleExpression'],'timezone':'UTC','native_targets':1},
        'vrp':{'name':v['Name'],'state':v['State'],'expression':v['ScheduleExpression'],'timezone':'UTC','native_targets':1}}

def invoke_public(lam,s3,engine):
    request='chatgpt-vrp-native-'+COMMIT[:12]+'-'+engine+'-1'
    key=store.request_key(request) if engine=='vrp' else extremes_store.request_key(engine,request);sent=False;status=None
    try:status=json.loads(store.bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body']))
    except Exception as exc:
        if not store.missing(exc):raise
        response=lam.invoke(FunctionName='justhodl-'+engine,InvocationType='Event',Payload=model.encoded({'request_id':request}));assert response['StatusCode']==202;sent=True
    deadline=time.monotonic()+180
    while time.monotonic()<deadline:
        try:status=json.loads(store.bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body']))
        except Exception as exc:
            if not store.missing(exc):raise
        if status and status.get('status') in ('complete','failed'):break
        time.sleep(3)
    assert status and status.get('status')=='complete' and status.get('published') is True,'Public producer did not publish: '+engine
    return {'request_id':request,'status_key':key,'invoke_sent':sent,'status':status}

def main():
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=45,connect_timeout=10,retries={'max_attempts':0},tcp_keepalive=True))
    s3=boto3.client('s3',region_name='us-east-1');events=boto3.client('events',region_name='us-east-1');scheduler=boto3.client('scheduler',region_name='us-east-1')
    with report('ops_5940_vrp_native_acceptance') as r:
        subprocess.run([sys.executable,str(SOURCE.parent/'tests/run_tests.py')],cwd=ROOT,check=True)
        for test in ('extremes_native_test_support.py','extremes_consumer_test_support.py'):
            subprocess.run([sys.executable,str(ROOT/'tests'/test)],cwd=ROOT,check=True)
        sys.path.insert(0,str(SOURCE.parent/'tests'));from native_vrp_tests import fixture
        c,i,_,_=fixture();portable=model.sha(model.encoded(store.compile_output(i,store.reader(c,'b'))))
        assert portable=='bbd793cde3ec047dfe3680796c856508e4f9159edeb9be0364adb1b8c49bc510'
        runtimes={fn:runtime(lam,fn) for fn in FUNCTIONS};r.kv(runtimes=runtimes,acceptance_consumer_invocations=0,private_account_reads=0,provider_requests=0,paid_ai_calls=0,notifications_sent=0,portfolio_writes=0)
        cadence=schedules(lam,events,scheduler);read=store.reader(s3,BUCKET);xread=extremes_store.reader(s3,BUCKET)
        raw=read(model.PRIVATE+AUDIT+'.bin');assert len(raw)==9759 and model.sha(raw)==AUDIT;preflight=json.loads(raw)
        for ref in (*preflight['packets'].values(),*preflight['canonical_originals'].values()):
            raw=read(ref['key']);assert len(raw)==ref['bytes'] and model.sha(raw)==ref['sha256']
        legacy=subprocess.check_output(['git','show','f3835b961:aws/lambdas/justhodl-vrp/source/lambda_function.py'],cwd=ROOT)
        assert (SOURCE/'legacy_vrp.py').read_bytes()==legacy and len(legacy)==17852
        oldinputs={'contract':'vrp-native-inputs.v1','generated_at':preflight['generated_at'],
            'macro':{**preflight['packets'][store.SOURCES[0]],'source_key':store.SOURCES[0]},
            'legacy':{k:{**preflight['packets'][k],'source_key':k} for k in store.SOURCES[1:]}}
        def audit_read(key):
            if key in preflight['canonical_originals']:
                ref=preflight['canonical_originals'][key];raw=read(ref['key']);assert model.sha(raw)==ref['sha256'] and len(raw)==ref['bytes'];return raw
            return read(key)
        prior=store.compile_output(oldinputs,audit_read)
        assert prior['quality']['within_age_ceiling']==3 and prior['measurements']['SP500']['history_coverage']['numeric_rows']==2513
        assert all(g['available'] for g in prior['descriptive_gaps'].values()) and prior['expost_research']['rows']
        reconstruction={'output_sha256':model.sha(model.encoded(prior)),'quality':prior['quality'],'retained_original_artifacts':len(preflight['canonical_originals']),
            'history_coverage':{s:m['history_coverage'] for s,m in prior['measurements'].items()},'descriptive_gaps':prior['descriptive_gaps'],
            'expost_observations':prior['expost_research']['observations']}
        r.kv(preflight_original_reconstruction=reconstruction,schedules=cadence)
        histories={e:store.bounded(s3.get_object(Bucket=BUCKET,Key='data/'+e+'-history.json')['Body']) for e in ('vrp','capitulation','market-extremes')}
        for e,raw in histories.items():
            target=model.PRIVATE+model.sha(raw)+'.bin';store.immutable(s3,BUCKET,target,raw,'application/octet-stream')
        build=json.loads(public('build-manifest.json'));pages_commit=build['commit_sha']
        subprocess.run(['git','merge-base','--is-ancestor',COMMIT,pages_commit],cwd=ROOT,check=True);subprocess.run(['git','diff','--quiet',COMMIT,pages_commit,'--',*ASSETS],cwd=ROOT,check=True)
        for name in ASSETS:
            served=public(name);clean,count=re.subn(rb'<script\b[^>]*\bsrc="https://static\.cloudflareinsights\.com/beacon\.min\.js/v[a-f0-9]+"[^>]*></script>\n?',b'',served)
            assert count<=1 and model.sha(clean)==build['files_sha256'][name],'Built asset differs: '+name
            if name=='vrp.html':assert b'jh-vrp-research.js?v=20260920-native1' in served
        packets={};requests={};replays={};hashes={}
        for engine in ('vrp','capitulation','market-extremes'):
            result=invoke_public(lam,s3,engine);requests[engine]=result;r.kv(public_producer_request={engine:result})
            raw=public('data/'+engine+'.json');p=json.loads(raw);status=result['status']
            assert p['generated_at']==status['generated_at'] and p['replay']==status['replay'] and p['generated_at']>preflight['generated_at']
            assert p['call'] is None and all(p[k] is False for k in model.PERMISSIONS)
            if engine=='vrp':
                assert p['contract']==model.CONTRACT and p['quality']['within_age_ceiling']==3 and p['regime'] is None
                assert all(v is None for v in p['vrp'].values()) and all(g['available'] for g in p['descriptive_gaps'].values())
                assert p['expost_research']['rows'] and research_context(p)['available']
                assert json.loads(read(store.CURRENT))==p;replays[engine]=replay_verify(p,read)
            else:
                assert p['contract']==extremes_model.CONTRACT and p['engine']==engine and p['posture'] is None and p['capitulation_score'] is None
                assert json.loads(xread(extremes_store.current(engine)))==p;replays[engine]=extremes_verify(p,xread)
                assert p['decision']['eligible_votes']==0 and all(isinstance(m['label'],str) for m in p['measurements'])
            assert store.bounded(s3.get_object(Bucket=BUCKET,Key='data/'+engine+'-history.json')['Body'])==histories[engine]
            packets[engine]=p;hashes[engine]=model.sha(raw)
        m=packets['market-extremes'];assert m['eligibility']['vrp']['research_context_available'] is True and m['eligibility']['vrp']['measurement_count']==3
        assert m['eligibility']['vrp']['upstream_replay']==packets['vrp']['replay']
        assert m['contexts']['capitulation']['upstream_replay']==packets['capitulation']['replay']
        for name in ('valuation','retail'):assert m['eligibility'][name]['research_context_available'] is False
        run=json.loads(read(packets['vrp']['replay']['manifest_key']));inputs=store.checked(run['input'],'inputs',read)
        for item in (inputs['macro'],*inputs['legacy'].values()):
            if item:assert denied('https://justhodl.ai/'+item['key']) and denied('https://'+BUCKET+'.s3.amazonaws.com/'+item['key'])
        proof={'contract':'vrp-native-acceptance.v1','verified_at':datetime.now(timezone.utc).isoformat(),'runtime_commit':COMMIT,'runtimes':runtimes,'pages_commit':pages_commit,
            'portable_reference_digest':portable,'assets':{name:build['files_sha256'][name] for name in ASSETS},'public_sha256':hashes,'requests':requests,
            'source_replay':replays,'preflight_reconstruction':reconstruction,'schedules':cadence,
            'publications':{e:{'generated_at':p['generated_at'],'replay':p['replay'],'quality':p['quality'],'measurement_count':len(p['measurements'])} for e,p in packets.items()},
            'native_vrp':{'measurements':packets['vrp']['measurements'],'trailing_realized':packets['vrp']['trailing_realized'],
                'descriptive_gaps':packets['vrp']['descriptive_gaps'],'gap_history_statistics':packets['vrp']['gap_history']['statistics'],
                'latest_expost':packets['vrp']['expost_research']['rows'][-1]},
            'legacy_history_sha256':{e:model.sha(raw) for e,raw in histories.items()},'legacy_history_unchanged':True,'whole_originals_anonymously_denied':True,
            'acceptance_public_producer_invocations':sum(int(v['invoke_sent']) for v in requests.values()),'acceptance_consumer_invocations':0,
            'provider_requests':0,'private_account_reads':0,'paid_ai_calls':0,'notifications_sent':0,'portfolio_writes':0,
            'remaining':'Current-vintage close-sampled research. No synchronized intraday/official-session qualification, expected premium, strategy profit, hedge price or portfolio authority.'}
        for key,contract in (('data/vrp-research-verification.json','vrp-native-acceptance.v1'),('data/extremes-research-verification.json','extremes-native-acceptance.v1')):
            doc={**proof,'contract':contract};s3.put_object(Bucket=BUCKET,Key=key,Body=model.encoded(doc),ContentType='application/json',CacheControl='no-store');assert json.loads(public(key))==doc
        r.kv(accepted=True,proof_key='data/vrp-research-verification.json',source_replay=replays,legacy_history_unchanged=True)

if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
