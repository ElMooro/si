"""Qualify exact retained Gold source arithmetic before replacing its producer."""
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import subprocess,sys,time
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/staged'),str(ROOT/'aws/ops/checks'),str(ROOT/'aws/shared')]
from ops_report import report
from ops_5975_etf_constituent_source_preflight import runtime
from ops_5998_option_population_retained_acceptance import denied_with_retry
from gold_rotation_arithmetic_audit import independent
import gold_rotation_model as model
import gold_rotation_store as store
BUCKET='justhodl-dashboard-live';FUNCTION='justhodl-gold-equity-rotation'
BASELINE={'key':model.PRIVATE+'9707f0816f32dd54b4dce40aa139ed4e9cc2f11f57745d9a319c29f759cc42c9.bin',
 'sha256':'9707f0816f32dd54b4dce40aa139ed4e9cc2f11f57745d9a319c29f759cc42c9','bytes':42229}
REQUEST='chatgpt-gold-rotation-candidate-6032';STATUS=store.request_key(REQUEST)

def main():
    import resource
    s3=boto3.client('s3',region_name='us-east-1');lam=boto3.client('lambda',region_name='us-east-1')
    events=boto3.client('events',region_name='us-east-1');scheduler=boto3.client('scheduler',region_name='us-east-1');read=store.reader(s3,BUCKET)
    with report('ops_6032_gold_rotation_candidate') as r:
        for name in ('test_gold_rotation_model.py','test_gold_rotation_store.py','test_gold_rotation_arithmetic_audit.py'):
            subprocess.run([sys.executable,str(ROOT/'tests'/name)],cwd=ROOT,check=True)
        actual=runtime(lam,s3,events,scheduler,FUNCTION)
        try:prior=model.strict(store.bounded(s3.get_object(Bucket=BUCKET,Key=STATUS)['Body']))
        except Exception as exc:
            if not store.missing(exc):raise
            prior=None
        if prior:
            assert prior['status']=='complete','Inspect retained incomplete qualification; never silently rebuild it'
            qref=prior['qualified'];qualified=model.strict(model.original(qref,read));r.kv(adopted_completed_request=True)
        else:
            store.status_write(s3,BUCKET,STATUS,{'status':'claimed','request_id':REQUEST,'generated_at':store.now()},IfNoneMatch='*')
            baseline=model.strict(model.original(BASELINE,read));assert baseline['contract']=='gold-rotation-source-preflight.v1' and baseline['runtime']==actual
            inputs={'contract':'gold-rotation-inputs.v1','generated_at':store.now(),'range':baseline['range'],'predecessor':baseline['predecessor'],'captures':baseline['captures']}
            started=time.monotonic();output=model.compile_output(inputs,read);ref=store.retain(s3,BUCKET,inputs,output)
            assert store.replay(ref,read)==output;elapsed=time.monotonic()-started
            r.kv(replay=ref,quality=output['quality'],source_failures={k:v['source_failures'] for k,v in output['instruments'].items()})
            counts=independent(output,inputs,read);peak=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            r.kv(counts=counts,retention_and_replay_seconds=round(elapsed,3),peak_rss_kib=peak)
            assert elapsed<150 and peak<480*1024,'Existing 180-second/512-MB qualification failed'
            assert counts['instruments']==8 and counts['source_endpoints']==32 and output['quality']['instruments_with_history']==8
            qualified={'contract':'gold-rotation-qualified-candidate.v1','generated_at':output['generated_at'],'replay':ref,'baseline':BASELINE,'runtime':actual,
                'counts':counts,'retention_and_replay_seconds':round(elapsed,3),'peak_rss_kib':peak,'source_original_replay_performed':True,'publication_performed':False}
            qref=store.protect(s3,BUCKET,model.encoded(qualified));store.status_write(s3,BUCKET,STATUS,{'status':'complete','request_id':REQUEST,'qualified':qref})
        run=store.verified_run(qualified['replay'],read);inputs=store.checked(run['input'],'inputs',read)
        protected={STATUS,BASELINE['key'],qref['key'],inputs['predecessor']['key'],*(v['original']['key'] for v in inputs['captures'].values() if v['original'])}
        def deny(key):assert denied_with_retry('https://justhodl.ai/'+key) and denied_with_retry('https://'+BUCKET+'.s3.amazonaws.com/'+key)
        with ThreadPoolExecutor(max_workers=4) as pool:
            for _ in pool.map(deny,sorted(protected)):pass
        assert runtime(lam,s3,events,scheduler,FUNCTION)==actual
        r.kv(qualified=qref,candidate=qualified,protected_artifacts_checked=len(protected),originals_anonymously_denied=True,
            provider_requests=0,engine_invocations=0,public_head_writes=0,private_account_reads=0,paid_ai_calls=0,notifications_sent=0,portfolio_writes=0,schedules_changed=0)

if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
