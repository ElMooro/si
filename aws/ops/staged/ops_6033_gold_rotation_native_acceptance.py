"""Exact release, one durable native request, replay, public assets and privacy."""
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import json,re,subprocess,sys,time,urllib.request
import boto3
from botocore.config import Config
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/staged'),str(ROOT/'aws/ops/checks'),str(ROOT/'aws/shared')]
from ops_report import report
from acceptance_invoke import invoke_when_available
from ops_5975_etf_constituent_source_preflight import runtime
from ops_5998_option_population_retained_acceptance import denied_with_retry
from ops_5966_sector_tilt_runtime_diagnosis import parse_runtime
from gold_rotation_arithmetic_audit import independent
import gold_rotation_model as model
import gold_rotation_store as store
BUCKET='justhodl-dashboard-live';FUNCTION='justhodl-gold-equity-rotation'
QUALIFIED={'key':model.PRIVATE+'b47f903dcc01cdfb94c824843392243dec315cc869fb49b07469d13c0cc6ee8f.bin',
    'sha256':'b47f903dcc01cdfb94c824843392243dec315cc869fb49b07469d13c0cc6ee8f','bytes':1874}
PROOF='data/gold-rotation-research-verification.json'
ASSETS=('gold-rotation.html','retail-edges.html','market-evidence.html','jh-gold-rotation-research.js','jh-gold-rotation-page.js','jh-option-research.js','jh-option-research.css')

def public(key):
    with urllib.request.urlopen(urllib.request.Request('https://justhodl.ai/'+key,headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=40) as response:return store.bounded(response)
def current(s3):
    packet=model.strict(store.bounded(s3.get_object(Bucket=BUCKET,Key=model.CURRENT)['Body']))
    if packet.get('engine')=='gold-equity-rotation' and packet.get('version') in ('1.0','1.0.0') and not packet.get('contract'):return None
    assert packet.get('contract')==model.CONTRACT and all(packet.get(k) is False for k in model.FLAGS)
    assert model.digest({k:v for k,v in packet.items() if k!='replay'})==packet['replay']['output_sha256']
    return packet
def completed_request(s3,packet):
    items=[]
    for page in s3.get_paginator('list_objects_v2').paginate(Bucket=BUCKET,Prefix=model.PRIVATE+'requests/'):
        items.extend(page.get('Contents',[]));assert len(items)<=1000,'Bounded completed-request discovery'
    for item in sorted(items,key=lambda x:x['LastModified'],reverse=True)[:100]:
        value=model.strict(store.bounded(s3.get_object(Bucket=BUCKET,Key=item['Key'])['Body']))
        if value.get('status')=='complete' and value.get('published') is True and value.get('replay')==packet['replay']:
            assert value.get('execution_id'),'Actual AWS execution identity required'
            return {'status_key':item['Key'],'status':value,'invoke_sent':False,'adopted_completed_native_request':True}
    raise AssertionError('Inspect current publication request; never blindly invoke again')
def invoke(lam,s3,commit,wait_seconds=210):
    existing=current(s3)
    if existing is not None:return completed_request(s3,existing)
    request='chatgpt-gold-rotation-native-'+commit[:12]+'-1';key=store.request_key(request);dispatch=store.request_key(request+'-dispatch')
    claim={'contract':'gold-rotation-native-dispatch.v1','request_id':request,'started_at':store.now(),'status':'claimed'};sent=False
    try:store.status_write(s3,BUCKET,dispatch,claim,IfNoneMatch='*')
    except Exception as exc:
        if not store.conflict(exc):raise
        prior=model.strict(store.bounded(s3.get_object(Bucket=BUCKET,Key=dispatch)['Body']))
        assert prior['contract']==claim['contract'] and prior['request_id']==request
    else:
        response,rejected=invoke_when_available(lam,{'FunctionName':FUNCTION,'InvocationType':'Event','Payload':model.encoded({'request_id':request})},wait_seconds=45)
        assert response['StatusCode']==202;sent=True
        claim.update(status='accepted_async',throttle_rejections_before_acceptance=rejected);store.status_write(s3,BUCKET,dispatch,claim)
    status=None;deadline=time.monotonic()+wait_seconds
    while time.monotonic()<deadline:
        try:status=model.strict(store.bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body']))
        except Exception as exc:
            if not store.missing(exc):raise
        if status and status.get('status') in ('complete','failed'):break
        time.sleep(3)
    assert status and status['status']=='complete','Inspect retained attempt; never blindly reinvoke'
    result={'request_id':request,'status_key':key,'dispatch_key':dispatch,'status':status,'invoke_sent':sent}
    packet=current(s3);assert packet is not None
    if packet['replay']!=status['replay']:
        assert model.clock(packet['generated_at'])>model.clock(status['generated_at'])
        adopted=completed_request(s3,packet);adopted.update(invoke_sent=sent,earlier_completed_request=result);return adopted
    assert status['published'] is True
    return result
def profile(logs,status):
    execution=status['execution_id'];assert re.fullmatch('[a-f0-9-]{36}',execution)
    start=model.clock(status['generated_at']);deadline=time.monotonic()+45
    while True:
        events=logs.filter_log_events(logGroupName='/aws/lambda/'+FUNCTION,filterPattern='"'+execution+'"',startTime=int(start.timestamp()*1000)-240000,limit=50).get('events',[])
        values=[parse_runtime(row.get('message',''),execution) for row in events];values=[v for v in values if v]
        if values:break
        assert time.monotonic()<deadline,'Read completed execution report; do not reinvoke';time.sleep(3)
    assert all(v.get('status') not in ('error','timeout') and v.get('memory_mb')==512 and v.get('max_memory_mb',512)<512 and v.get('duration_ms',180000)<180000 for v in values)
    return {'execution_id':execution,'managed_reports':values}

def main():
    s3=boto3.client('s3',region_name='us-east-1');lam=boto3.client('lambda',region_name='us-east-1',config=Config(connect_timeout=10,read_timeout=60,retries={'max_attempts':0}))
    events=boto3.client('events',region_name='us-east-1');scheduler=boto3.client('scheduler',region_name='us-east-1');read=store.reader(s3,BUCKET)
    with report('ops_6033_gold_rotation_native_acceptance') as r:
        subprocess.run([sys.executable,str(ROOT/'aws/lambdas'/FUNCTION/'tests/run_tests.py')],cwd=ROOT,check=True)
        commit=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/'+FUNCTION+'/source','aws/lambdas/'+FUNCTION+'/config.json'],cwd=ROOT,text=True).strip()
        assert re.fullmatch('[a-f0-9]{40}',commit)
        actual=runtime(lam,s3,events,scheduler,FUNCTION);assert actual['receipt']=={'status':'matched','commit':commit}
        consumers={}
        for name in ('justhodl-master-allocator','justhodl-signal-board','justhodl-morning-intelligence'):
            consumers[name]=runtime(lam,s3,events,scheduler,name)
            assert consumers[name]['receipt']=={'status':'matched','commit':commit},name
        qualified=model.strict(model.original(QUALIFIED,read));assert qualified['contract']=='gold-rotation-qualified-candidate.v1'
        store.replay(qualified['replay'],read)
        for field in ('timeout','memory_mb','runtime','handler','architectures','role','ephemeral_storage_mb','schedules'):assert actual[field]==qualified['runtime'][field],field
        build=json.loads(public('build-manifest.json'));pages_commit=build['commit_sha']
        subprocess.run(['git','merge-base','--is-ancestor',commit,pages_commit],cwd=ROOT,check=True)
        subprocess.run(['git','diff','--quiet','HEAD',pages_commit,'--',*ASSETS],cwd=ROOT,check=True)
        for name in ASSETS:
            raw=public(name);clean,n=re.subn(rb'<script\b[^>]*\bsrc="https://static\.cloudflareinsights\.com/beacon\.min\.js/v[a-f0-9]+"[^>]*></script>\n?',b'',raw)
            assert n<=1 and model.sha(clean)==build['files_sha256'][name],name
            if name=='gold-rotation.html':assert b'/jh-gold-rotation-research.js' in raw and b'/jh-gold-rotation-page.js' in raw
            if name=='market-evidence.html':assert b'/gold-rotation.html' in raw
        r.kv(commit=commit,runtime=actual,pages_commit=pages_commit,qualified_candidate=qualified['replay'])
        request=invoke(lam,s3,commit);r.kv(completed_request=request)
        execution=profile(boto3.client('logs',region_name='us-east-1'),request['status'])
        raw=public(model.CURRENT);packet=model.strict(raw);assert packet==current(s3) and packet['replay']==request['status']['replay']
        output=store.replay(packet['replay'],read);assert output=={k:v for k,v in packet.items() if k!='replay'}
        run=store.verified_run(packet['replay'],read);inputs=store.checked(run['input'],'inputs',read)
        for key in (packet['replay']['manifest_key'],run['input']['key'],run['output']['key'],*(v['key'] for v in run['compilers'].values())):assert public(key)==read(key),key
        counts=independent(output,inputs,read)
        predecessor=model.original(inputs['predecessor'],read)
        assert output['retained_predecessor']['sha256']==model.sha(predecessor)
        assert s3.head_object(Bucket=BUCKET,Key=model.CURRENT)['CacheControl']=='no-store'
        with urllib.request.urlopen(urllib.request.Request('https://justhodl.ai/'+model.CURRENT,method='HEAD',headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=30) as response:assert 'no-store' in response.headers.get('Cache-Control','')
        protected={QUALIFIED['key'],request['status_key'],inputs['predecessor']['key'],*(v['original']['key'] for v in inputs['captures'].values() if v['original'])}
        if request.get('dispatch_key'):protected.add(request['dispatch_key'])
        def deny(key):assert denied_with_retry('https://justhodl.ai/'+key) and denied_with_retry('https://'+BUCKET+'.s3.amazonaws.com/'+key)
        with ThreadPoolExecutor(max_workers=4) as pool:
            for _ in pool.map(deny,sorted(protected)):pass
        assert runtime(lam,s3,events,scheduler,FUNCTION)==actual
        proof={'contract':'gold-rotation-native-acceptance.v1','generated_at':store.now(),'commit':commit,'runtime_package':actual,
            'consumer_runtime_packages':consumers,'request':request,'execution_profile':execution,'qualified_candidate':qualified['replay'],
            'publication':{'key':model.CURRENT,'sha256':model.sha(raw),'bytes':len(raw),'replay':packet['replay'],'generated_at':packet['generated_at']},
            'source_original_replay_matches':True,'complete_predecessor_retained':True,'arithmetic_independently_checked':True,
            'source_original_replay_performed_by_this_acceptance':True,'counts':counts,'originals_anonymously_denied':True,
            'protected_artifacts_checked':len(protected),'pages_commit':pages_commit,'assets':{name:build['files_sha256'][name] for name in ASSETS},
            'producer_invocations_this_acceptance':int(request['invoke_sent']),'acceptance_provider_requests':0,'producer_provider_requests':32 if request['invoke_sent'] else 0,'consumer_invocations':0,
            'private_account_reads':0,'paid_ai_calls':0,'notifications_sent':0,'portfolio_writes':0,'schedules_changed':0}
        body=model.encoded(proof);s3.put_object(Bucket=BUCKET,Key=PROOF,Body=body,ContentType='application/json',CacheControl='no-store')
        assert public(PROOF)==body
        r.kv(proof_key=PROOF,publication=proof['publication'],counts=counts,execution_profile=execution,
            protected_artifacts_checked=len(protected),producer_invocations_this_acceptance=proof['producer_invocations_this_acceptance'],
            acceptance_provider_requests=0,producer_provider_requests=proof['producer_provider_requests'],consumer_invocations=0,private_account_reads=0,paid_ai_calls=0,notifications_sent=0,portfolio_writes=0,schedules_changed=0)

if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
