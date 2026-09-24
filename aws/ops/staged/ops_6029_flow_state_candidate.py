"""Qualify the retained Cross-Asset Flow composition without native publication."""
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import subprocess,sys,time
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/staged'),str(ROOT/'aws/ops/checks'),str(ROOT/'aws/shared')]
from ops_report import report
from ops_5975_etf_constituent_source_preflight import runtime
from ops_5998_option_population_retained_acceptance import denied_with_retry
from flow_state_composition_audit import independent
import flow_state_model as model
import flow_state_store as store
BUCKET='justhodl-dashboard-live';FUNCTION='justhodl-cross-asset-flow-state'
BASELINE={'key':model.PRIVATE+'333e9687f2a5631aba0b92c7355224f077da256f22854fc2aafff979dad4cf00.bin',
    'sha256':'333e9687f2a5631aba0b92c7355224f077da256f22854fc2aafff979dad4cf00','bytes':29001}
REQUEST='chatgpt-flow-state-candidate-6029';STATUS=store.request_key(REQUEST)

def main():
    import resource
    s3=boto3.client('s3',region_name='us-east-1');lam=boto3.client('lambda',region_name='us-east-1')
    events=boto3.client('events',region_name='us-east-1');scheduler=boto3.client('scheduler',region_name='us-east-1');read=store.reader(s3,BUCKET)
    with report('ops_6029_flow_state_candidate') as r:
        for name in ('test_flow_state_model.py','test_flow_state_store.py','test_flow_state_composition_audit.py'):
            subprocess.run([sys.executable,str(ROOT/'tests'/name)],cwd=ROOT,check=True)
        actual=runtime(lam,s3,events,scheduler,FUNCTION)
        try:prior=model.strict(store.bounded(s3.get_object(Bucket=BUCKET,Key=STATUS)['Body']))
        except Exception as exc:
            if not store.missing(exc):raise
            prior=None
        if prior:
            assert prior['status']=='complete','Inspect incomplete qualification; no redispatch'
            qualified=model.strict(model.original(prior['qualified'],read));qref=prior['qualified'];r.kv(adopted_completed_request=True)
        else:
            store.status_write(s3,BUCKET,STATUS,{'status':'claimed','request_id':REQUEST,'generated_at':store.now()},IfNoneMatch='*')
            baseline=model.strict(model.original(BASELINE,read));assert baseline['contract']=='flow-state-source-preflight.v1'
            assert baseline['runtime']==actual,'Native source package or runtime changed after preflight'
            inputs={'contract':'flow-state-inputs.v1','generated_at':store.now(),'captures':baseline['captures']}
            start=time.monotonic();output=model.compile_output(inputs,read);ref=store.retain(s3,BUCKET,inputs,output)
            assert store.replay(ref,read)==output;elapsed=time.monotonic()-start
            parents={key:model.strict(model.original(inputs['captures'][key]['original'],read)) for key in model.PARENTS}
            counts=independent(output,parents);peak=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            r.kv(replay=ref,counts=counts,retention_and_replay_seconds=round(elapsed,3),peak_rss_kib=peak)
            assert elapsed<45 and peak<224*1024,'Unchanged 60-second/256-MB runtime qualification failed'
            qualified={'contract':'flow-state-qualified-candidate.v1','replay':ref,'generated_at':output['generated_at'],
                'baseline':BASELINE,'runtime':actual,'counts':counts,'retention_and_replay_seconds':round(elapsed,3),'peak_rss_kib':peak,
                'source_original_replay_performed_by_this_qualification':False,'publication_performed':False}
            qref=store.protect(s3,BUCKET,model.encoded(qualified))
            store.status_write(s3,BUCKET,STATUS,{'status':'complete','request_id':REQUEST,'qualified':qref})
        run=store.verified_run(qualified['replay'],read);inputs=store.checked(run['input'],'inputs',read)
        protected={STATUS,BASELINE['key'],qref['key'],*(r['original']['key'] for r in inputs['captures'].values())}
        def deny(key):assert denied_with_retry('https://justhodl.ai/'+key) and denied_with_retry('https://'+BUCKET+'.s3.amazonaws.com/'+key)
        with ThreadPoolExecutor(max_workers=4) as pool:
            for _ in pool.map(deny,sorted(protected)):pass
        assert runtime(lam,s3,events,scheduler,FUNCTION)==actual
        r.kv(qualified=qref,candidate=qualified,protected_artifacts_checked=len(protected),originals_anonymously_denied=True,
            provider_requests=0,engine_invocations=0,public_head_writes=0,private_account_reads=0,paid_ai_calls=0,
            notifications_sent=0,portfolio_writes=0,schedules_changed=0)

if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
