"""Verify twelve exact packages, attention originals and three public research producers.
No account reads, downstream recommendation consumers, paid AI or notifications.
"""
from pathlib import Path
from datetime import datetime,timezone
import base64,hashlib,io,json,re,subprocess,sys,time,urllib.request,urllib.error,zipfile
import boto3
from botocore.config import Config
ROOT=Path(__file__).resolve().parents[3];SOURCE=ROOT/'aws/lambdas/justhodl-retail-sentiment/source'
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/checks'),str(ROOT/'aws/shared'),str(ROOT/'scripts'),str(SOURCE)]
from ops_report import report
from release_package_evidence import shared_imports
import retail_research_store as store
import retail_research_model as model
import extremes_native_store as extremes_store
import extremes_native_model as extremes_model
from retail_research import context as research_context
from replay_retail_research import verify as replay_verify
from replay_extremes_research import verify as extremes_verify
BUCKET='justhodl-dashboard-live'
COMMIT='40985851f0da6944f103334c884cbec1c5824029'
FUNCTIONS=('justhodl-ai-chat', 'justhodl-best-setups', 'justhodl-capitulation', 'justhodl-convergence-radar', 'justhodl-cycle-clock', 'justhodl-digest-trends-ai', 'justhodl-hot-stocks-digest', 'justhodl-market-extremes', 'justhodl-morning-intelligence', 'justhodl-prediction-snapshotter', 'justhodl-regime-composite', 'justhodl-retail-sentiment')
ASSETS=('retail/index.html', 'jh-retail-research.js', 'jh-extremes-research.js', 'capitulation.html', 'market-extremes.html', 'classic-dashboard.html', 'chart-pro.html', 'digest-trends.html', 'assets/jh-workspaces.js')
AUDIT="d154fd1a83ecec7307e3d99615ed7ca95483e7840a3d0159ce8ce9a36d0d5086"

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
    if fn=='justhodl-retail-sentiment':assert cfg['MemorySize']==512 and cfg['Timeout']==180
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
    rcfg=lam.get_function_configuration(FunctionName='justhodl-retail-sentiment');rnames=[]
    for page in events.get_paginator('list_rule_names_by_target').paginate(TargetArn=rcfg['FunctionArn']):rnames.extend(page['RuleNames'])
    assert rnames==['justhodl-retail-sentiment-30min']
    rr=events.describe_rule(Name=rnames[0]);rt=events.list_targets_by_rule(Rule=rnames[0])['Targets']
    assert rr['State']=='ENABLED' and rr['ScheduleExpression']=='cron(10 19 * * ? *)'
    selected=[t for t in rt if t.get('Arn')==rcfg['FunctionArn']];assert len(selected)==1
    assert not json.loads(selected[0].get('Input') or '{}').get('request_id') and not selected[0].get('InputTransformer')
    return {'retail-sentiment':{'name':rnames[0],'state':rr['State'],'expression':rr['ScheduleExpression'],'native_targets':1},
        'capitulation':{'name':names[0],'state':rule['State'],'expression':rule['ScheduleExpression'],'native_targets':1},
        'market-extremes':{'name':m['Name'],'state':m['State'],'expression':m['ScheduleExpression'],'timezone':'UTC','native_targets':1}}

def invoke_public(lam,s3,engine):
    request='chatgpt-retail-native-'+COMMIT[:12]+'-'+engine+'-1'
    key=store.request_key(request) if engine=='retail-sentiment' else extremes_store.request_key(engine,request);sent=False;status=None
    try:status=json.loads(store.bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body']))
    except Exception as exc:
        if not store.missing(exc):raise
        response=lam.invoke(FunctionName='justhodl-'+engine,InvocationType='Event',Payload=model.encoded({'request_id':request}));assert response['StatusCode']==202;sent=True
    deadline=time.monotonic()+240
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
    with report('ops_5942_retail_native_acceptance') as r:
        subprocess.run([sys.executable,str(SOURCE.parent/'tests/run_tests.py')],cwd=ROOT,check=True)
        for test in ('extremes_native_test_support.py','retail_consumer_test_support.py'):
            subprocess.run([sys.executable,str(ROOT/'tests'/test)],cwd=ROOT,check=True)
        sys.path.insert(0,str(SOURCE.parent/'tests'));from native_retail_tests import fixture
        c,i=fixture(100);portable=model.sha(model.encoded(store.compile_output(i,store.reader(c,'b'))))
        assert portable=='d2332898fd8b89e329ff73e1183c4cf2e226fcdd86669eb37ae33dce419fea62'
        runtimes={fn:runtime(lam,fn) for fn in FUNCTIONS};r.kv(runtimes=runtimes,acceptance_consumer_invocations=0,private_account_reads=0,paid_ai_calls=0,notifications_sent=0,portfolio_writes=0)
        cadence=schedules(lam,events,scheduler);read=store.reader(s3,BUCKET);xread=extremes_store.reader(s3,BUCKET)
        raw=read(model.PRIVATE+AUDIT+'.bin');assert len(raw)==17534 and model.sha(raw)==AUDIT;preflight=json.loads(raw)
        for ref in preflight['packets'].values():
            raw=read(ref['key']);assert len(raw)==ref['bytes'] and model.sha(raw)==ref['sha256']
        legacy=subprocess.check_output(['git','show','369b495ec:aws/lambdas/justhodl-retail-sentiment/source/lambda_function.py'],cwd=ROOT)
        assert (SOURCE/'legacy_retail_sentiment.py').read_bytes()==legacy and len(legacy)==47266
        pages=[]
        for item in preflight['provider_pages']:
            url=item['request_url'];a=re.fullmatch(r'https://apewisdom.io/api/v1.0/filter/([^/]+)/page/([12])',url)
            if a:kind,identity,page='apewisdom',a[1],int(a[2])
            else:kind,identity,page='stocktwits','trending' if '/trending/' in url else url.rsplit('/',1)[-1][:-5],1
            assert url==model.source_url(kind,identity,page)
            ref=item['original'];raw=read(ref['key']);assert len(raw)==ref['bytes'] and model.sha(raw)==ref['sha256']
            pages.append({'kind':kind,'identity':identity,'page':page,'request_url':url,'acquired_at':item['acquired_at'],'status':'received','http_status':item['http_status'],'original':ref,'raw':raw})
        context={k:{'status':'retained_unqualified_context','measurement_eligible':False,'original':v} for k,v in preflight['packets'].items()}
        prior=model.compute(pages,context,preflight['generated_at'])
        assert prior['quality']['community_samples_available']==4 and prior['communities']['all-stocks']['eligible_symbols']==200
        reconstruction={'output_sha256':model.sha(model.encoded(prior)),'quality':prior['quality'],'original_responses':len(pages),
            'scope':'All seven original audit responses parsed; five community pages, trending and one public stream. Not a full native 25-stream acquisition.',
            'sample_counts':{k:{f:v[f] for f in ('eligible_symbols','reported_symbol_count','baseline_status_counts')} for k,v in prior['communities'].items()}}
        r.kv(preflight_original_reconstruction=reconstruction,schedules=cadence)
        history_keys=('data/retail-attention-history.json','data/capitulation-history.json','data/market-extremes-history.json')
        histories={key:store.bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body']) for key in history_keys}
        for raw in histories.values():store.original(s3,BUCKET,raw)
        build=json.loads(public('build-manifest.json'));pages_commit=build['commit_sha']
        subprocess.run(['git','merge-base','--is-ancestor',COMMIT,pages_commit],cwd=ROOT,check=True);subprocess.run(['git','diff','--quiet',COMMIT,pages_commit,'--',*ASSETS],cwd=ROOT,check=True)
        for name in ASSETS:
            served=public(name);clean,count=re.subn(rb'<script\b[^>]*\bsrc="https://static\.cloudflareinsights\.com/beacon\.min\.js/v[a-f0-9]+"[^>]*></script>\n?',b'',served)
            assert count<=1 and model.sha(clean)==build['files_sha256'][name],'Built asset differs: '+name
            if name=='retail/index.html':assert b'jh-retail-research.js?v=20260920-native1' in served
        packets={};requests={};replays={};hashes={}
        for engine in ('retail-sentiment','capitulation','market-extremes'):
            result=invoke_public(lam,s3,engine);requests[engine]=result;r.kv(public_producer_request={engine:result})
            raw=public('data/'+engine+'.json');p=json.loads(raw);status=result['status']
            assert p['generated_at']==status['generated_at'] and p['replay']==status['replay'] and p['generated_at']>preflight['generated_at']
            assert p['call'] is None and all(p[k] is False for k in model.PERMISSIONS)
            if engine=='retail-sentiment':
                assert p['contract']==model.CONTRACT and p['quality']['community_samples_available']==4 and p['market_regime'] is None
                assert p['top_30_by_mentions']==[] and p['recent_alerts']==[] and p['signals_logged']==0 and research_context(p)['available']
                assert json.loads(read(store.CURRENT))==p;replays[engine]=replay_verify(p,read)
            else:
                assert p['contract']==extremes_model.CONTRACT and p['engine']==engine and p['posture'] is None and p['capitulation_score'] is None
                assert json.loads(xread(extremes_store.current(engine)))==p;replays[engine]=extremes_verify(p,xread)
                assert p['decision']['eligible_votes']==0
            packets[engine]=p;hashes[engine]=model.sha(raw)
        for key,raw in histories.items():assert store.bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body'])==raw
        m=packets['market-extremes'];assert m['eligibility']['retail']['research_context_available'] is True and m['eligibility']['retail']['measurement_count']==0
        assert m['eligibility']['retail']['upstream_replay']==packets['retail-sentiment']['replay']
        assert m['contexts']['capitulation']['upstream_replay']==packets['capitulation']['replay']
        assert m['eligibility']['valuation']['research_context_available'] is False
        rp=packets['retail-sentiment'];run=json.loads(read(rp['replay']['manifest_key']));inputs=store.checked(run['input'],'inputs',read)
        refs=[p['original'] for p in inputs['collection']['pages'] if p['original']]+[p['original'] for p in inputs['collection']['context_evidence'].values() if p.get('original')]
        for ref in refs:assert denied('https://justhodl.ai/'+ref['key']) and denied('https://'+BUCKET+'.s3.amazonaws.com/'+ref['key'])
        proof={'contract':'retail-native-acceptance.v1','verified_at':datetime.now(timezone.utc).isoformat(),'runtime_commit':COMMIT,'runtimes':runtimes,'pages_commit':pages_commit,
            'portable_reference_digest':portable,'assets':{name:build['files_sha256'][name] for name in ASSETS},'public_sha256':hashes,'requests':requests,
            'source_replay':replays,'preflight_reconstruction':reconstruction,'schedules':cadence,
            'publications':{e:{'generated_at':p['generated_at'],'replay':p['replay'],'quality':p['quality'],'measurement_count':len(p.get('measurements',[]))} for e,p in packets.items()},
            'native_retail':{'collection':rp['collection'],'freshness':rp['freshness'],'communities':{k:{f:v[f] for f in ('eligible_symbols','reported_symbol_count','sample_mentions','baseline_status_counts','population_complete')} for k,v in rp['communities'].items()},
                'streams':len(rp['stocktwits']['streams']),'available_streams':sum(v['available'] for v in rp['stocktwits']['streams'])},
            'legacy_history_sha256':{key:model.sha(raw) for key,raw in histories.items()},'legacy_history_unchanged':True,'whole_originals_anonymously_denied':True,
            'acceptance_public_producer_invocations':sum(int(v['invoke_sent']) for v in requests.values()),'acceptance_consumer_invocations':0,
            'producer_provider_requests':rp['collection']['provider_requests'],'private_account_reads':0,'paid_ai_calls':0,'notifications_sent':0,'portfolio_writes':0,
            'remaining':'Unknown vendor cutoffs, overlapping ranked samples and unmatched issuer identities. No retail flow, representative survey, price confirmation, forecast or position sizing qualification.'}
        for key,contract in (('data/retail-research-verification.json','retail-native-acceptance.v1'),('data/extremes-research-verification.json','extremes-native-acceptance.v1')):
            doc={**proof,'contract':contract};s3.put_object(Bucket=BUCKET,Key=key,Body=model.encoded(doc),ContentType='application/json',CacheControl='no-store');assert json.loads(public(key))==doc
        r.kv(accepted=True,proof_key='data/retail-research-verification.json',source_replay=replays,legacy_history_unchanged=True)

if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
