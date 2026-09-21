"""Accept an already completed scheduled option capture; never invoke/recollect."""
from pathlib import Path
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import json,re,sys,time
import boto3
from botocore.config import Config
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/staged'),str(ROOT/'aws/shared')]
from ops_report import report
from ops_5994_option_flow_native_acceptance import public,runtime_commit,execution_profile
from ops_5975_etf_constituent_source_preflight import runtime,denied
from ops_5993_option_flow_recovered_qualification import verify_records,original_keys
import option_flow_research as model
import option_flow_store as store
BUCKET='justhodl-dashboard-live'
FUNCTION='justhodl-polygon-options-flow'
COMMIT='12e863c6f583819cf0ece3f8ba1a915e6a530939'
PROOF='data/option-flow-scheduled-verification.json'


def scheduled_request(s3,packet):
    prefix=store.PRIVATE+'requests/'
    candidates=[]
    pages=s3.get_paginator('list_objects_v2').paginate(Bucket=BUCKET,Prefix=prefix,Delimiter='/',
        PaginationConfig={'PageSize':1000,'MaxItems':10000})
    for page in pages:
        for item in page.get('Contents',[]):
            key=item['Key']
            if re.fullmatch(re.escape(prefix)+r'[a-f0-9]{64}\.json',key):candidates.append(item)
    # Read only this producer's recent durable attempts, never account artifacts.
    for item in sorted(candidates,key=lambda x:x['LastModified'],reverse=True)[:100]:
        raw=store.bounded(s3.get_object(Bucket=BUCKET,Key=item['Key'])['Body']);status=json.loads(raw)
        if status.get('contract')!='option-flow-request.v1' or status.get('replay')!=packet['replay']:continue
        assert status.get('status')=='complete' and status.get('published') is True
        assert status.get('compatibility_published') is True and 'recovered_from' not in status
        assert status['provider_requests_this_execution']==status['provider_requests']>0
        assert status['generated_at']==packet['generated_at']
        assert model.clock(status['started_at'])>model.clock('2026-09-21T14:14:00Z')
        assert re.fullmatch(r'[a-f0-9-]{36}',status['request_id'])
        assert all(status.get(k)==0 for k in ('private_account_reads','paid_ai_calls','signals_emitted','notifications_sent','portfolio_writes'))
        return item['Key'],status
    raise AssertionError('No matching completed automatic capture; do not invoke again')


def main():
    s3=boto3.client('s3',region_name='us-east-1',config=Config(max_pool_connections=24))
    lam=boto3.client('lambda',region_name='us-east-1')
    events=boto3.client('events',region_name='us-east-1');scheduler=boto3.client('scheduler',region_name='us-east-1')
    with report('ops_5995_option_flow_scheduled_acceptance') as r:
        assert runtime_commit()==COMMIT
        actual=runtime(lam,s3,events,scheduler,FUNCTION)
        assert actual['receipt']=={'status':'matched','commit':COMMIT}
        prior=json.loads(public('data/option-flow-research-verification.json'))
        assert prior['commit']==COMMIT and prior['runtime_package']==actual
        read=store.reader(s3,BUCKET)
        raw=public(model.CURRENT);packet=json.loads(raw)
        assert read(model.CURRENT)==raw
        assert model.clock(packet['generated_at'])>model.clock(prior['publication']['generated_at'])
        key,status=scheduled_request(s3,packet)
        profile=execution_profile(boto3.client('logs',region_name='us-east-1'),status)
        r.kv(commit=COMMIT,publication_clock=packet['generated_at'],replay=packet['replay'],execution_profile=profile)
        started=time.monotonic()
        run=store.verified_run(packet['replay'],read)
        inputs=model.checked(run['input'],read,'inputs')
        replay=store.replay(packet['replay'],read)
        assert replay=={k:v for k,v in packet.items() if k!='replay'}
        assert inputs['provider_requests']==status['provider_requests']
        counts=Counter()
        for symbol,entry in packet['chains'].items():
            summary=model.checked(entry['chain'],read,'chains')
            counts.update(verify_records(inputs['chains'][symbol],summary,read))
        assert counts['retained_rows']==packet['quality']['returned_rows']
        assert all(packet.get(k) is False for k in model.PERMISSIONS)
        assert packet['call'] is None and packet['portfolio_action']=='WAIT' and packet['independent_investment_votes']==0
        protected=original_keys(inputs)|{key}
        def check(k):assert denied('https://justhodl.ai/'+k) and denied('https://'+BUCKET+'.s3.amazonaws.com/'+k)
        with ThreadPoolExecutor(max_workers=8) as pool:
            for _ in pool.map(check,sorted(protected)):pass
        # A later valid capture may advance while the large retained run replays.
        # Verify current alias against its current head; never roll it backwards.
        latest=json.loads(public(model.CURRENT));alias=json.loads(public(model.LEGACY))
        assert model.clock(latest['generated_at'])>=model.clock(packet['generated_at'])
        store.verified_run(latest['replay'],read)
        assert alias==store.compatibility(latest)
        assert runtime(lam,s3,events,scheduler,FUNCTION)==actual
        capture_s=(model.clock(inputs['generated_at'])-model.clock(status['started_at'])).total_seconds()
        duration=profile['managed_reports'][-1]['duration_ms']/1000
        proof={'contract':'option-flow-scheduled-acceptance.v1','generated_at':store.now(),'commit':COMMIT,
            'runtime_package':actual,'scheduled_execution_id':status['execution_id'],'execution_profile':profile,
            'publication':{'key':model.CURRENT,'sha256':model.sha(raw),'bytes':len(raw),'generated_at':packet['generated_at'],'replay':packet['replay']},
            'latest_after_audit':{'generated_at':latest['generated_at'],'replay':latest['replay']},
            'scheduled_source_requests':inputs['provider_requests'],'source_bytes':inputs['source_bytes'],
            'selected_underlyings':len(packet['chains']),'quality':packet['quality'],'independent_source_checks':dict(counts),
            'source_phase_seconds':round(capture_s,3),'total_execution_seconds':duration,
            'post_capture_execution_seconds':round(duration-capture_s,3),'timeout_headroom_seconds':round(900-duration,3),
            'audit_seconds':round(time.monotonic()-started,3),'protected_artifacts_checked':len(protected),
            'producer_invocations_this_audit':0,'provider_requests_this_audit':0,'schedule_changes':0,
            'private_account_reads':0,'paid_ai_calls':0,'notifications_sent':0,'signals_emitted':0,'portfolio_writes':0}
        proof_raw=model.encoded(proof)
        s3.put_object(Bucket=BUCKET,Key=PROOF,Body=proof_raw,ContentType='application/json',CacheControl='no-store')
        assert public(PROOF)==proof_raw
        r.kv(proof_key=PROOF,publication=proof['publication'],independent_source_checks=dict(counts),
            scheduled_source_requests=inputs['provider_requests'],selected_underlyings=len(packet['chains']),
            source_phase_seconds=proof['source_phase_seconds'],post_capture_execution_seconds=proof['post_capture_execution_seconds'],
            timeout_headroom_seconds=proof['timeout_headroom_seconds'],protected_artifacts_checked=len(protected),
            producer_invocations_this_audit=0,provider_requests_this_audit=0,schedule_changes=0)


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
