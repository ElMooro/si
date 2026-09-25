"""One durable FINRA research invocation; exact packages, replay, public UI and privacy."""
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from fractions import Fraction
import json,re,subprocess,sys,time,urllib.request
import boto3
from botocore.config import Config
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/staged'),str(ROOT/'aws/ops/checks'),str(ROOT/'aws/shared'),str(ROOT/'scripts')]
from ops_report import report
from acceptance_invoke import invoke_when_available
from ops_5975_etf_constituent_source_preflight import runtime
from ops_5998_option_population_retained_acceptance import denied_with_retry
from ops_5966_sector_tilt_runtime_diagnosis import parse_runtime
from shared_dependents import dependents
from release_package_evidence import check_packages
import ops_6042_offexchange_reported_issue_capture as audit
import offexchange_research_model as model
import offexchange_research_store as store
import offexchange_producer as producer
import offexchange_context as boundary
BUCKET='justhodl-dashboard-live';FUNCTION='justhodl-dark-pool';PROOF='data/offexchange-research-verification.json'
BASELINE={'key':model.PRIVATE+'a87413d775af2e211170e707bc3869fbb143d89d5bf5fc3ffc5cf8c381dc75b6.bin','sha256':'a87413d775af2e211170e707bc3869fbb143d89d5bf5fc3ffc5cf8c381dc75b6','bytes':26448}
CANDIDATE={'manifest_key':model.PREFIX+'runs/a4de25e2370113a31187224b03f053bd198008dc86eef2ff71f0e9a0572c5a1a.json','output_sha256':'fa9ca95c57bb990a91bdd16b398ac8685a66f12c3c1f54ae7664789a2436bd76'}
ASSETS=('dark-pool.html','offexchange-research.html','jh-offexchange-research.js','jh-offexchange-page.js','jh-option-research.css','market-evidence.html')
SHARED=('aws/shared/offexchange_context.py','aws/shared/offexchange_producer.py','aws/shared/jh_adapters.py','aws/shared/holdings_derived_boundary.py')

def public(key,method='GET'):
    response=urllib.request.urlopen(urllib.request.Request('https://justhodl.ai/'+key,method=method,headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=40)
    headers=dict(response.headers);return store.bounded(response),headers

def current(s3):
    packet=model.strict(store.bounded(s3.get_object(Bucket=BUCKET,Key=model.CURRENT)['Body']))
    if packet.get('version')=='2.6.0' and not packet.get('contract'):return None
    assert boundary.context(packet)['native_reference_available'],'Unrecognized or promoted current publication'
    return packet

def completed_request(s3,packet):
    items=[]
    for page in s3.get_paginator('list_objects_v2').paginate(Bucket=BUCKET,Prefix=model.PRIVATE+'requests/'):
        items.extend(page.get('Contents',[]));assert len(items)<=2000,'Bounded native request discovery'
    for item in sorted(items,key=lambda x:x['LastModified'],reverse=True)[:100]:
        value=model.strict(store.bounded(s3.get_object(Bucket=BUCKET,Key=item['Key'])['Body']));result=value.get('result',{})
        if value.get('status')=='complete' and result.get('published') is True and result.get('replay')==packet['replay']:
            assert value.get('execution_id')
            return {'status_key':item['Key'],'status':value,'invoke_sent':False,'adopted_completed_native_request':True}
    raise AssertionError('Inspect current native request; never blindly invoke again')

def invoke(lam,s3,commit,wait_seconds=320):
    existing=current(s3)
    if existing is not None:return completed_request(s3,existing)
    request='chatgpt-offexchange-native-'+commit[:12]+'-1';key=producer.request_key(request);dispatch=producer.request_key(request+'-dispatch');sent=False
    claim={'contract':'offexchange-native-dispatch.v1','request_id':request,'started_at':producer.now(),'status':'claimed'}
    try:producer.journal(s3,BUCKET,dispatch,claim,True)
    except Exception as exc:
        if not store.conflict(exc):raise
        prior=model.strict(producer.raw_read(s3,BUCKET,dispatch));assert prior['contract']==claim['contract'] and prior['request_id']==request
    else:
        response,rejected=invoke_when_available(lam,{'FunctionName':FUNCTION,'InvocationType':'Event','Payload':model.encoded({'request_id':request})},wait_seconds=45)
        assert response['StatusCode']==202;sent=True;claim.update(status='accepted_async',throttle_rejections_before_acceptance=rejected);producer.journal(s3,BUCKET,dispatch,claim)
    status=None;deadline=time.monotonic()+wait_seconds
    while time.monotonic()<deadline:
        try:status=model.strict(producer.raw_read(s3,BUCKET,key))
        except Exception as exc:
            if not producer.missing(exc):raise
        if status and status.get('status') in ('complete','failed'):break
        time.sleep(3)
    assert status and status['status']=='complete','Inspect retained attempt; never blindly reinvoke'
    result={'request_id':request,'status_key':key,'dispatch_key':dispatch,'status':status,'invoke_sent':sent}
    packet=current(s3);assert packet is not None and status['result']['published'] is True
    if packet['replay']!=status['result']['replay']:
        assert model.clock(packet['generated_at'])>model.clock(status['result']['generated_at'])
        adopted=completed_request(s3,packet);adopted.update(invoke_sent=sent,earlier_completed_request=result);return adopted
    return result

def profile(logs,status):
    execution=status['execution_id'];assert re.fullmatch('[a-f0-9-]{36}',execution)
    start=model.clock(status['result']['generated_at']);deadline=time.monotonic()+45
    while True:
        events=logs.filter_log_events(logGroupName='/aws/lambda/'+FUNCTION,filterPattern='"'+execution+'"',startTime=int(start.timestamp()*1000)-350000,limit=50).get('events',[])
        values=[parse_runtime(row.get('message',''),execution) for row in events];values=[v for v in values if v]
        if values:break
        assert time.monotonic()<deadline,'Read completed execution report; do not reinvoke';time.sleep(3)
    assert all(v.get('status') not in ('error','timeout') and v.get('memory_mb')==1024 and v.get('max_memory_mb',1024)<1024 and v.get('duration_ms',300000)<300000 for v in values)
    return {'execution_id':execution,'managed_reports':values}

def selection_proof(inputs,read):
    """Independently reconcile chosen periods with the retained discovery rows."""
    advertised={}
    for dataset,capture in inputs['partition_discovery'].items():
        assert capture['url']=='https://api.finra.org/partitions/group/otcMarket/name/'+dataset
        assert capture['http_status']==200 and capture['status']=='response_retained'
        doc=model.strict(model.original(capture['original'],read));values={}
        for row in doc['availablePartitions']:
            period,tier=row['partitions'];values.setdefault(tier,set()).add(period)
        advertised[dataset]=values
    expected=set()
    for tier in ('T1','T2'):
        period=max(advertised['weeklySummary'][tier])
        for code in ('ATS_W_SMBL','OTC_W_SMBL'):expected.add(('weeklySummary',code,period,tier))
    for period in sorted(advertised['monthlySummary']['NMS'],reverse=True)[:2]:expected.add(('monthlySummary','OTC_M_SMBL_FIRM',period,'NMS'))
    actual={(p['partition']['dataset'],p['partition']['code'],p['partition']['period'],p['partition']['tier']) for p in inputs['partitions'].values()}
    assert actual==expected and len(actual)==6
    assert inputs['daily'] in inputs['daily_candidates'] and len(inputs['daily_candidates'])<=3
    assert all(c['http_status'] in (403,404,204) for c in inputs['daily_candidates'][:-1])
    return {'selected_partitions_equal_latest_advertised_periods':True,'partition_count':6,'daily_attempts':len(inputs['daily_candidates'])}

def main():
    s3=boto3.client('s3',region_name='us-east-1');lam=boto3.client('lambda',region_name='us-east-1',config=Config(connect_timeout=10,read_timeout=60,retries={'max_attempts':0}))
    events=boto3.client('events',region_name='us-east-1');scheduler=boto3.client('scheduler',region_name='us-east-1');read=store.reader(s3,BUCKET)
    with report('ops_6045_offexchange_native_acceptance') as r:
        subprocess.run([sys.executable,str(ROOT/'aws/lambdas'/FUNCTION/'tests/run_tests.py')],cwd=ROOT,check=True)
        commit=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/'+FUNCTION+'/source','aws/lambdas/'+FUNCTION+'/config.json'],cwd=ROOT,text=True).strip();assert re.fullmatch('[a-f0-9]{40}',commit)
        actual=runtime(lam,s3,events,scheduler,FUNCTION);assert actual['receipt']=={'status':'matched','commit':commit}
        baseline=model.strict(model.original(BASELINE,read))['runtime']
        for field in ('timeout','memory_mb','runtime','handler','architectures','role','ephemeral_storage_mb','schedules'):assert actual[field]==baseline[field],field
        store.verified_run(CANDIDATE,read)
        consumers={}
        for name in dependents(ROOT,SHARED):
            if name==FUNCTION:continue
            consumers[name]=runtime(lam,s3,events,scheduler,name);assert consumers[name]['receipt']=={'status':'matched','commit':commit},name
        alias_packages=check_packages(lam,ROOT,['justhodl-katlin'])
        assert len(alias_packages)==1 and alias_packages[0]['pass'] and alias_packages[0]['qualifier']=='live'
        assert alias_packages[0]['code_sha256']==consumers['justhodl-katlin']['code_sha256'],'Active alias must carry the same qualified package'
        build=json.loads(public('build-manifest.json')[0]);pages_commit=build['commit_sha']
        subprocess.run(['git','merge-base','--is-ancestor',commit,pages_commit],cwd=ROOT,check=True)
        subprocess.run(['git','diff','--quiet','HEAD',pages_commit,'--',*ASSETS],cwd=ROOT,check=True)
        for name in ASSETS:
            raw=public(name)[0];clean,n=re.subn(rb'<script\b[^>]*\bsrc="https://static\.cloudflareinsights\.com/beacon\.min\.js/v[a-f0-9]+"[^>]*></script>\n?',b'',raw)
            assert n<=1 and model.sha(clean)==build['files_sha256'][name],name
            if name.endswith('.html') and name!='market-evidence.html':assert b'/jh-offexchange-research.js' in raw and b'/jh-offexchange-page.js' in raw
        r.kv(commit=commit,runtime=actual,consumers_checked=len(consumers),pages_commit=pages_commit)
        request=invoke(lam,s3,commit);r.kv(completed_request=request)
        execution=profile(boto3.client('logs',region_name='us-east-1'),request['status'])
        raw,headers=public(model.CURRENT);packet=model.strict(raw);assert packet==current(s3) and packet['replay']==request['status']['result']['replay']
        compiled=store.replay(packet['replay'],read);assert compiled['packet']=={k:v for k,v in packet.items() if k!='replay'}
        run=store.verified_run(packet['replay'],read);inputs=store.checked(run['input'],'inputs',read)
        selection=selection_proof(inputs,read)
        arithmetic=audit.qualify(s3,inputs['partitions']);daily=store.measurements.cnms(model.original(inputs['daily']['original'],read),inputs['daily_date'])
        for row in daily:audit.independent_ratio(row['short_volume_pct'],Fraction(row['short_volume_shares'])*100,Fraction(row['total_volume_shares']))
        keys={packet['replay']['manifest_key'],run['input']['key'],run['output']['key'],*(v['key'] for v in run['compilers'].values()),*(v['key'] for v in packet['record_shards'].values())}
        for key in sorted(keys):assert public(key)[0]==read(key),key
        assert s3.head_object(Bucket=BUCKET,Key=model.CURRENT)['CacheControl']=='no-store'
        for method in ('GET','HEAD'):assert 'no-store' in {k.lower():v for k,v in public(model.CURRENT,method)[1].items()}.get('cache-control','')
        protected={BASELINE['key'],request['status_key'],inputs['predecessor']['key'],inputs['daily']['original']['key']}
        if request.get('dispatch_key'):protected.add(request['dispatch_key'])
        captures=[*inputs['partition_discovery'].values(),*inputs['daily_candidates']]
        for part in inputs['partitions'].values():captures.extend(part['pages'])
        protected.update(v['original']['key'] for v in captures)
        def deny(key):assert denied_with_retry('https://justhodl.ai/'+key) and denied_with_retry('https://'+BUCKET+'.s3.amazonaws.com/'+key)
        with ThreadPoolExecutor(max_workers=4) as pool:
            for _ in pool.map(deny,sorted(protected)):pass
        assert runtime(lam,s3,events,scheduler,FUNCTION)==actual
        proof={'contract':'offexchange-native-acceptance.v1','generated_at':producer.now(),'commit':commit,'runtime_package':actual,'consumer_runtime_packages':consumers,'active_alias_packages':alias_packages,
            'request':request,'execution_profile':execution,'qualified_candidate':CANDIDATE,
            'publication':{'key':model.CURRENT,'sha256':model.sha(raw),'bytes':len(raw),'replay':packet['replay'],'generated_at':packet['generated_at']},
            'source_original_replay_matches':True,'complete_predecessor_retained':True,'arithmetic_independently_checked':True,
            'counts':packet['counts'],'partition_selection':selection,'partition_arithmetic':arithmetic,'daily_ratio_checks':len(daily),'public_artifacts_checked':len(keys),
            'originals_anonymously_denied':True,'protected_artifacts_checked':len(protected),'pages_commit':pages_commit,'assets':{name:build['files_sha256'][name] for name in ASSETS},
            'producer_invocations_this_acceptance':int(request['invoke_sent']),'acceptance_provider_requests':0,'producer_provider_requests':len(captures) if request['invoke_sent'] else 0,
            'consumer_invocations':0,'private_account_reads':0,'paid_ai_calls':0,'notifications_sent':0,'portfolio_writes':0,'schedules_changed':0}
        body=model.encoded(proof);s3.put_object(Bucket=BUCKET,Key=PROOF,Body=body,ContentType='application/json',CacheControl='no-store');assert public(PROOF)[0]==body
        r.kv(proof_key=PROOF,publication=proof['publication'],counts=packet['counts'],execution_profile=execution,public_artifacts_checked=len(keys),protected_artifacts_checked=len(protected),
            consumer_packages_checked=len(consumers),producer_invocations_this_acceptance=int(request['invoke_sent']),acceptance_provider_requests=0,producer_provider_requests=proof['producer_provider_requests'],
            consumer_invocations=0,private_account_reads=0,paid_ai_calls=0,notifications_sent=0,portfolio_writes=0,schedules_changed=0)
if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
