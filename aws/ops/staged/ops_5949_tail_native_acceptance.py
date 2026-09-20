"""Verify exact Tail packages, original replay and public page; invoke one research producer.

No account consumers, paid AI, notifications, portfolio writes or schedule changes.
"""
from pathlib import Path
from datetime import datetime,timezone
import base64,hashlib,io,json,re,subprocess,sys,time,urllib.request,urllib.error,zipfile
import boto3
from botocore.config import Config
ROOT=Path(__file__).resolve().parents[3];SOURCE=ROOT/'aws/lambdas/justhodl-tail-risk/source'
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/checks'),str(ROOT/'aws/shared'),str(ROOT/'scripts'),str(SOURCE)]
from ops_report import report
from release_package_evidence import shared_imports
import tail_research_store as store
import tail_research_model as model
from tail_research import context as research_context
from replay_tail_research import verify as replay_verify
BUCKET='justhodl-dashboard-live'
COMMIT='0a57c7e6a049dca79b03119e8c6e61164ba9b54e'
PAGE_COMMIT='3cb7313a8d2b7bbae1e28801b7e72f0d48e84c0b'
FUNCTIONS=('justhodl-cycle-clock','justhodl-implied-prob','justhodl-jh-fusion','justhodl-jhsignal-bridge','justhodl-katlin','justhodl-quantum-desk','justhodl-strategist','justhodl-stress-index','justhodl-tail-risk')
ASSETS=('tail-risk.html','jh-tail-research.js')
AUDITS={'predecessor':('4a847e428b26a51db1295a66c84a7463c12fb1e8f255be0227f2ce70405ab9d4',2465),
        'options':('e049f3117439e07288df06d92ebb9b5cc017bf8cd5e21f885b5b2fa286cb812f',5007)}

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
    if fn=='justhodl-tail-risk':assert cfg['MemorySize']==512 and cfg['Timeout']==180
    return {'commit':COMMIT,'code_sha256':cfg['CodeSha256'],'packaged_files_checked':len(expected),'all_packaged_sources_match':True,'alias':alias}

def schedule(lam,events):
    cfg=lam.get_function_configuration(FunctionName='justhodl-tail-risk');names=[]
    for page in events.get_paginator('list_rule_names_by_target').paginate(TargetArn=cfg['FunctionArn']):names.extend(page['RuleNames'])
    assert names==['justhodl-tail-risk-daily'];rule=events.describe_rule(Name=names[0]);targets=events.list_targets_by_rule(Rule=names[0])['Targets']
    selected=[t for t in targets if t.get('Arn')==cfg['FunctionArn']]
    assert rule['State']=='ENABLED' and rule['ScheduleExpression']=='cron(0 13 ? * TUE-SAT *)' and len(selected)==1
    assert not json.loads(selected[0].get('Input') or '{}').get('request_id') and not selected[0].get('InputTransformer')
    return {'name':names[0],'state':rule['State'],'expression':rule['ScheduleExpression'],'native_targets':1}

def invoke_public(lam,s3):
    request='chatgpt-tail-native-'+COMMIT[:12]+'-1';key=store.request_key(request);sent=False;status=None
    try:status=json.loads(store.bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body']))
    except Exception as exc:
        if not store.missing(exc):raise
        response=lam.invoke(FunctionName='justhodl-tail-risk',InvocationType='Event',Payload=model.encoded({'request_id':request}));assert response['StatusCode']==202;sent=True
    deadline=time.monotonic()+240
    while time.monotonic()<deadline:
        try:status=json.loads(store.bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body']))
        except Exception as exc:
            if not store.missing(exc):raise
        if status and status.get('status') in ('complete','failed'):break
        time.sleep(3)
    assert status and status.get('status')=='complete' and status.get('published') is True,'Native public producer did not publish'
    return {'request_id':request,'status_key':key,'invoke_sent':sent,'status':status}

def main():
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=45,connect_timeout=10,retries={'max_attempts':0},tcp_keepalive=True))
    s3=boto3.client('s3',region_name='us-east-1');events=boto3.client('events',region_name='us-east-1')
    with report('ops_5949_tail_native_acceptance') as r:
        subprocess.run([sys.executable,str(SOURCE.parent/'tests/run_tests.py')],cwd=ROOT,check=True)
        subprocess.run([sys.executable,str(ROOT/'tests/tail_consumer_test_support.py')],cwd=ROOT,check=True)
        sys.path.insert(0,str(SOURCE.parent/'tests'));from native_tail_tests import fixture
        client,inputs=fixture();portable=model.sha(model.encoded(store.compile_output(inputs,store.reader(client,'b'))))
        assert portable=='d34e063bf90ca41f225273339c774c1f04d647d0a50626ca7c2db354521ae5f0'
        runtimes={fn:runtime(lam,fn) for fn in FUNCTIONS}
        r.kv(runtimes=runtimes,acceptance_consumer_invocations=0,private_account_reads=0,acceptance_provider_requests=0,paid_ai_calls=0,notifications_sent=0,portfolio_writes=0)
        cadence=schedule(lam,events);read=store.reader(s3,BUCKET);audits={};protected=set()
        for label,(digest,size) in AUDITS.items():
            key=model.PRIVATE+digest+'.bin';raw=read(key);assert len(raw)==size and model.sha(raw)==digest
            audits[label]=json.loads(raw);protected.add(key)
        for ref in audits['predecessor']['packets'].values():store.verified_original(ref,read);protected.add(ref['key'])
        legacy=subprocess.check_output(['git','show','86959aebe:aws/lambdas/justhodl-tail-risk/source/lambda_function.py'],cwd=ROOT)
        assert (SOURCE/'legacy_tail_risk.py').read_bytes()==legacy and len(legacy)==16030
        prior=audits['options'];reconstruction={}
        for symbol,item in prior['pages'].items():
            raw=store.verified_original(item['original'],read);protected.add(item['original']['key']);doc=model.decode(raw)
            assert item['request_url']==model.initial_url(symbol,prior['started_at']) and item['http_status']==200
            rows=[];excluded={}
            for index,row in enumerate(doc['results']):
                result,why=model.contract(row,symbol,prior['started_at'],{**item,'page':1},index)
                if result:rows.append(result)
                else:excluded[why]=excluded.get(why,0)+1
            assert len(doc['results'])==250 and rows and doc.get('next_url')
            assert all(x['provider_iv_observed_at'] is None and x['quote']['midpoint'] is None for x in rows)
            reconstruction[symbol]={'received_rows':len(doc['results']),'eligible_identity_rows':len(rows),'excluded':excluded,
                'with_vendor_iv':sum(x['provider_implied_volatility'] is not None for x in rows),'with_qualified_quote':0,
                'original':item['original'],'parsed_output_sha256':model.sha(model.encoded(rows)),'pagination_complete':False}
        r.kv(preflight_reconstruction=reconstruction,schedule=cadence)
        history=read('data/tail-risk-history.json');store.original(s3,BUCKET,history)
        build=json.loads(public('build-manifest.json'));pages_commit=build['commit_sha']
        subprocess.run(['git','merge-base','--is-ancestor',PAGE_COMMIT,pages_commit],cwd=ROOT,check=True)
        subprocess.run(['git','diff','--quiet',COMMIT,pages_commit,'--',*ASSETS],cwd=ROOT,check=True)
        for name in ASSETS:
            served=public(name);clean,count=re.subn(rb'<script\b[^>]*\bsrc="https://static\.cloudflareinsights\.com/beacon\.min\.js/v[a-f0-9]+"[^>]*></script>\n?',b'',served)
            assert count<=1 and model.sha(clean)==build['files_sha256'][name],'Built asset differs: '+name
            if name=='tail-risk.html':assert b'jh-tail-research.js?v=20260920-native1' in served
        request=invoke_public(lam,s3);r.kv(public_producer_request=request)
        raw=public(store.CURRENT);packet=json.loads(raw);status=request['status']
        assert packet['contract']==model.CONTRACT and packet['generated_at']==status['generated_at'] and packet['replay']==status['replay']
        assert packet['generated_at']>prior['generated_at'] and json.loads(read(store.CURRENT))==packet
        assert packet['call'] is None and packet['portfolio_action']=='WAIT' and all(packet[k] is False for k in model.PERMISSIONS)
        assert all(packet[k] is None for k in ('system_tail_gauge','tail_regime','tail_valuation','score','regime'))
        assert research_context(packet)['available'] and packet['quality']['eligible_identity_rows']>0
        assert 0<packet['collection']['provider_requests']<=36 and packet['collection']['source_bytes']<=model.MAX_TOTAL_BYTES
        for row in packet['indices']:
            assert all(row[k] is None for k in ('p_drop_5','p_drop_10','p_drop_20','tail_stress','rn_skew','rn_kurt'))
            for term in row['terms']:
                for selection in term['selections'].values():
                    c=selection['contract']
                    if c:assert c['provider_iv_observed_at'] is None and c['provider_greek_observed_at'] is None and selection['actual_delta_error']<=selection['maximum_delta_error']
        replay=replay_verify(packet,read);assert read('data/tail-risk-history.json')==history
        run=json.loads(read(packet['replay']['manifest_key']));inputs=store.checked(run['input'],'inputs',read)
        for chain in inputs['collection']['chains'].values():
            for page in chain['pages']:
                if page.get('original'):protected.add(page['original']['key'])
        for context in inputs['collection']['context_evidence'].values():
            if context.get('original'):protected.add(context['original']['key'])
        for key in sorted(protected):assert denied('https://justhodl.ai/'+key) and denied('https://'+BUCKET+'.s3.amazonaws.com/'+key),'Original publicly readable'
        proof={'contract':'tail-native-acceptance.v1','verified_at':datetime.now(timezone.utc).isoformat(),'runtime_commit':COMMIT,
            'runtimes':runtimes,'pages_commit':pages_commit,'assets':{name:build['files_sha256'][name] for name in ASSETS},'portable_reference_digest':portable,
            'publication':{'generated_at':packet['generated_at'],'replay':packet['replay'],'quality':packet['quality'],'collection':packet['collection']},
            'public_sha256':model.sha(raw),'request':request,'source_replay':replay,'preflight_reconstruction':reconstruction,'schedule':cadence,
            'legacy_history_sha256':model.sha(history),'legacy_history_unchanged':True,'whole_originals_anonymously_denied':True,'protected_artifacts_checked':len(protected),
            'acceptance_public_producer_invocations':int(request['invoke_sent']),'acceptance_consumer_invocations':0,'acceptance_provider_requests':0,
            'private_account_reads':0,'paid_ai_calls':0,'notifications_sent':0,'portfolio_writes':0,
            'remaining':'Captured vendor-model research only. Independent IV clocks, synchronized/executable quotes, density, physical probability and hedge/portfolio qualification remain unestablished.'}
        key='data/tail-research-verification.json';s3.put_object(Bucket=BUCKET,Key=key,Body=model.encoded(proof),ContentType='application/json',CacheControl='no-store')
        assert json.loads(public(key))==proof
        r.kv(accepted=True,proof_key=key,source_replay=replay,legacy_history_unchanged=True,protected_artifacts_checked=len(protected))

if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
