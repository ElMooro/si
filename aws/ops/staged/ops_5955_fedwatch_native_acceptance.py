"""Verify exact FedWatch packages, original replay and public page; invoke one reviewed public research producer.

No account consumers, paid AI, notifications, portfolio writes or account changes; deduplicates the reviewed public schedule.
"""
from pathlib import Path
from datetime import datetime,timezone
import base64,hashlib,io,json,re,subprocess,sys,time,urllib.request,urllib.error,zipfile
import boto3
from botocore.config import Config
ROOT=Path(__file__).resolve().parents[3];SOURCE=ROOT/'aws/lambdas/justhodl-fedwatch-rate-probability/source'
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/checks'),str(ROOT/'aws/shared'),str(ROOT/'scripts'),str(SOURCE)]
from ops_report import report
from release_package_evidence import shared_imports
import fedwatch_research_store as store
import fedwatch_research_model as model
from replay_fedwatch_research import verify as replay_verify
BUCKET='justhodl-dashboard-live'
COMMIT='659bc75001226d2731f8edc2cac9bf96db2ee9f2'
PAGE_COMMIT=COMMIT
FUNCTIONS=('justhodl-fedwatch-rate-probability','justhodl-cycle-clock','justhodl-katlin','justhodl-fomc-reaction')
ASSETS=('fedwatch.html','jh-fedwatch-research.js')
AUDIT=('9ad573405acdb4419e4779cdd83ade529965f28fc506452f5d70f68c20df0181',10855)
QUOTES=('59f9814e9da536fc25e835681868809e26cc1e994847ab7cd9e8e1f6634f00bf',8780)
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
    if fn=='justhodl-fedwatch-rate-probability':assert cfg['MemorySize']==512 and cfg['Timeout']==180
    return {'commit':COMMIT,'code_sha256':cfg['CodeSha256'],'packaged_files_checked':len(expected),'all_packaged_sources_match':True,'alias':alias}

def protected(ref,read):
    assert ref['key']==model.PRIVATE+ref['sha256']+'.bin'
    raw=read(ref['key']);assert len(raw)==ref['bytes'] and model.sha(raw)==ref['sha256']
    return raw

def invoke_public(lam,s3):
    request='chatgpt-fedwatch-native-'+COMMIT[:12]+'-1';key=store.request_key(request);sent=False;status=None
    try:status=json.loads(store.bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body']))
    except Exception as exc:
        if not store.missing(exc):raise
        response=lam.invoke(FunctionName='justhodl-fedwatch-rate-probability',InvocationType='Event',Payload=model.encoded({'request_id':request}));assert response['StatusCode']==202;sent=True
    deadline=time.monotonic()+300
    while time.monotonic()<deadline:
        try:status=json.loads(store.bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body']))
        except Exception as exc:
            if not store.missing(exc):raise
        if status and status.get('status') in ('complete','failed'):break
        time.sleep(3)
    assert status and status.get('status')=='complete' and status.get('published') is True,'Native public producer did not publish'
    return {'request_id':request,'status_key':key,'invoke_sent':sent,'status':status}

def schedule(lam,events,scheduler,s3):
    fn='justhodl-fedwatch-rate-probability';name=fn+'-daily';cfg=lam.get_function_configuration(FunctionName=fn)
    names=[]
    for page in events.get_paginator('list_rule_names_by_target').paginate(TargetArn=cfg['FunctionArn']):names.extend(page['RuleNames'])
    assert names==[name],'Unexpected public classic schedule inventory'
    rule=events.describe_rule(Name=name);targets=events.list_targets_by_rule(Rule=name)['Targets']
    assert rule['State']=='ENABLED' and rule['ScheduleExpression']=='cron(0 23 * * ? *)' and len(targets)==1
    assert targets[0]['Arn']==cfg['FunctionArn'] and not targets[0].get('InputTransformer')
    assert not json.loads(targets[0].get('Input') or '{}').get('request_id')
    old=scheduler.get_schedule(Name=name,GroupName='default')
    assert old['Target']['Arn']==cfg['FunctionArn'] and old['ScheduleExpression']=='cron(0 23 * * ? *)'
    assert old['ScheduleExpressionTimezone']=='UTC' and old['State'] in ('ENABLED','DISABLED')
    assert not json.loads(old['Target'].get('Input') or '{}').get('request_id')
    raw=json.dumps(old,sort_keys=True,separators=(',',':'),default=lambda v:v.isoformat()).encode()
    retained=store.original(s3,BUCKET,raw)
    fields=('Name','GroupName','ScheduleExpression','ScheduleExpressionTimezone','StartDate','EndDate','Description','KmsKeyArn','Target','FlexibleTimeWindow','ActionAfterCompletion')
    preserved={k:old[k] for k in fields if k in old}
    changed=old['State']=='ENABLED'
    if changed:scheduler.update_schedule(**preserved,State='DISABLED')
    actual=scheduler.get_schedule(Name=name,GroupName='default')
    assert actual['State']=='DISABLED' and all(actual.get(k)==v for k,v in preserved.items())
    assert events.describe_rule(Name=name)['State']=='ENABLED'
    return {'classic':{'name':name,'state':'ENABLED','expression':rule['ScheduleExpression'],'native_targets':1},
        'duplicate_scheduler':{'name':name,'group':'default','state':'DISABLED','changed':changed,'original':retained,'all_other_settings_preserved':True}}

def main():
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=210,connect_timeout=10,retries={'max_attempts':0},tcp_keepalive=True))
    s3=boto3.client('s3',region_name='us-east-1');events=boto3.client('events',region_name='us-east-1');scheduler=boto3.client('scheduler',region_name='us-east-1')
    with report('ops_5955_fedwatch_native_acceptance') as r:
        subprocess.run([sys.executable,str(SOURCE.parent/'tests/run_tests.py')],cwd=ROOT,check=True)
        subprocess.run([sys.executable,str(ROOT/'tests/fedwatch_consumer_test_support.py')],cwd=ROOT,check=True)
        sys.path.insert(0,str(SOURCE.parent/'tests'));from fedwatch_fixture import fixture
        client,inputs,_,_=fixture();portable=model.sha(model.encoded(store.compile_output(inputs,store.reader(client,'b'))))
        assert portable=='10f2cc04f4a724fb7bac4b8d52d945d0df5fd0174894cd159338face08123184'
        runtimes={fn:runtime(lam,fn) for fn in FUNCTIONS}
        r.kv(runtimes=runtimes,acceptance_consumer_invocations=0,private_account_reads=0,paid_ai_calls=0,notifications_sent=0,portfolio_writes=0)
        read=store.reader(s3,BUCKET);protected_keys=set()
        def manifest(pair):
            digest,size=pair;ref={'key':model.PRIVATE+digest+'.bin','sha256':digest,'bytes':size};raw=protected(ref,read);protected_keys.add(ref['key']);return json.loads(raw)
        audit=manifest(AUDIT);quotes=manifest(QUOTES)
        for ref in (*audit['packets'].values(),*audit['canonical_originals'].values()):protected(ref,read);protected_keys.add(ref['key'])
        old_macro=json.loads(protected(audit['packets']['data/report-measurements.json'],read))
        old_restored=canonical_fred_replay.restore(old_macro,model.SERIES,lambda key:protected(audit['canonical_originals'][key],read))
        assert sum(v is not None for v in old_restored.values())==4
        calendar_page=quotes['pages']['calendar'];cal=model.calendar(protected(calendar_page['original'],read),calendar_page,quotes['started_at'])
        assert cal['available'] and next(m for m in cal['meetings'] if m['year']==2027 and m['month_text']=='June')['end_date']=='2027-06-09'
        old_quotes={}
        for item in model.plan(quotes['started_at'])[:4]:
            page=quotes['pages'][item['symbol']];value=model.chart(protected(page['original'],read),page,item)
            assert value['available'] and value['latest_bar']['original_row_index']>=0 and value['meeting_probabilities'] is None
            old_quotes[item['symbol']]={'bars':len(value['bars']),'latest_bar':value['latest_bar'],'provider_market_mark':value['provider_market_mark']}
        for page in quotes['pages'].values():protected_keys.add(page['original']['key'])
        legacy=subprocess.check_output(['git','show','74f967b1b:aws/lambdas/justhodl-fedwatch-rate-probability/source/lambda_function.py'],cwd=ROOT)
        assert (SOURCE/'legacy_fedwatch.py').read_bytes()==legacy and len(legacy)==17403
        build=json.loads(public('build-manifest.json'));pages_commit=build['commit_sha']
        subprocess.run(['git','merge-base','--is-ancestor',PAGE_COMMIT,pages_commit],cwd=ROOT,check=True)
        subprocess.run(['git','diff','--quiet',COMMIT,pages_commit,'--',*ASSETS],cwd=ROOT,check=True)
        for name in ASSETS:
            served=public(name);clean,count=re.subn(rb'<script\b[^>]*\bsrc="https://static\.cloudflareinsights\.com/beacon\.min\.js/v[a-f0-9]+"[^>]*></script>\n?',b'',served)
            assert count<=1 and model.sha(clean)==build['files_sha256'][name],'Built asset differs: '+name
            if name=='fedwatch.html':assert b'jh-fedwatch-research.js?v=20260920-native1' in served
        r.kv(preflight_policy_originals_replayed=4,preflight_contracts_replayed=old_quotes,pages_commit=pages_commit)
        cadence=schedule(lam,events,scheduler,s3);protected_keys.add(cadence['duplicate_scheduler']['original']['key']);r.kv(schedule=cadence)
        request=invoke_public(lam,s3);r.kv(public_producer_request=request)
        raw=public(store.CURRENT);packet=json.loads(raw);status=request['status']
        assert packet['contract']==model.CONTRACT and packet['generated_at']==status['generated_at'] and packet['replay']==status['replay']
        assert packet['generated_at']>audit['metadata']['data/fedwatch.json']['generated_at'] and json.loads(read(store.CURRENT))==packet
        assert packet['call'] is None and packet['portfolio_action']=='WAIT' and all(packet[k] is False for k in model.PERMISSIONS)
        assert packet['quality']['dated_contracts_within_age_ceiling']>=4 and len(packet['contracts'])==12 and packet['calendar']['available']
        assert packet['next_6mo_summary']['scenario'] is None and packet['n_meetings_with_data']==0
        assert all(q['meeting_probabilities'] is None and q['official_settlement_verified'] is False for q in packet['contracts'])
        replay=replay_verify(packet,read)
        run=json.loads(read(packet['replay']['manifest_key']));inputs=store.checked(run['input'],'inputs',read)
        for ref in (inputs['macro'],*inputs['legacy'].values()):
            if ref:protected(ref,read);protected_keys.add(ref['key'])
        for page in [inputs['collection']['calendar'],*inputs['collection']['quotes'].values()]:
            if page.get('original'):protected(page['original'],read);protected_keys.add(page['original']['key'])
        for key in sorted(protected_keys):assert denied('https://justhodl.ai/'+key) and denied('https://'+BUCKET+'.s3.amazonaws.com/'+key),'Protected source publicly readable'
        for fn in FUNCTIONS:
            cfg=lam.get_function_configuration(FunctionName=fn);receipt=json.loads(public('data/ops/releases/'+fn+'.json'))
            assert receipt['commit']==COMMIT and receipt['code_sha256']==cfg['CodeSha256']==runtimes[fn]['code_sha256']
        assert scheduler.get_schedule(Name='justhodl-fedwatch-rate-probability-daily',GroupName='default')['State']=='DISABLED'
        proof={'contract':'fedwatch-native-acceptance.v1','verified_at':datetime.now(timezone.utc).isoformat(),'runtime_commit':COMMIT,
            'runtimes':runtimes,'pages_commit':pages_commit,'assets':{name:build['files_sha256'][name] for name in ASSETS},'portable_reference_digest':portable,
            'publication':{'generated_at':packet['generated_at'],'source_generated_at':packet['source_generated_at'],'replay':packet['replay'],'quality':packet['quality']},
            'public_sha256':model.sha(raw),'request':request,'source_replay':replay,'schedule':cadence,
            'preflight_policy_originals_replayed':4,'preflight_contract_originals_replayed':4,'preflight_calendar_replayed':True,
            'whole_originals_anonymously_denied':True,'protected_artifacts_checked':len(protected_keys),
            'acceptance_public_producer_invocations':int(request['invoke_sent']),'acceptance_consumer_invocations':0,
            'private_account_reads':0,'paid_ai_calls':0,'notifications_sent':0,'portfolio_writes':0,
            'remaining':'Dated descriptive contract research. Official settlements, synchronized curves, effective-date/branching probability model, calibration and portfolio recommendations remain unqualified.'}
        key='data/fedwatch-research-verification.json';s3.put_object(Bucket=BUCKET,Key=key,Body=model.encoded(proof),ContentType='application/json',CacheControl='no-store')
        assert json.loads(public(key))==proof
        r.kv(accepted=True,proof_key=key,source_replay=replay,protected_artifacts_checked=len(protected_keys))

if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
