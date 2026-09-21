"""Qualify native futures retention/replay without replacing the live producer."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import json,subprocess,sys,time
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/staged'),str(ROOT/'aws/shared')]
from ops_report import report
import ops_6009_futures_original_source_preflight as baseline
import ops_6013_futures_holiday_calendar_candidate as calculation
import futures_research_model as model
import futures_research_store as store
BUCKET=baseline.BUCKET
QUALIFIED={'key':model.PRIVATE+'980a2ce2c991ecdb3116fe72bd3d99bb1f1d3dd70ab8f58bf51dcd5918fa1cae.bin',
    'sha256':'980a2ce2c991ecdb3116fe72bd3d99bb1f1d3dd70ab8f58bf51dcd5918fa1cae','bytes':33279}
REQUEST='chatgpt-ops-6014-futures-native-candidate'


def candidate_inputs(source,expected):
    return {'contract':'futures-original-inputs.v1','generated_at':expected['generated_at'],'sources':source['sources'],
        'provider_requests':source['provider_requests'],'source_bytes':source['provider_response_bytes'],
        'predecessors':{model.LEGACY:{k:source['predecessors'][model.LEGACY][k] for k in ('source_key','original','acquired_at')},model.CURRENT:None}}


def main():
    import resource
    s3=boto3.client('s3',region_name='us-east-1');lam=boto3.client('lambda',region_name='us-east-1')
    events=boto3.client('events',region_name='us-east-1');scheduler=boto3.client('scheduler',region_name='us-east-1')
    read=store.reader(s3,BUCKET);key=store.request_key(REQUEST)
    with report('ops_6014_futures_native_candidate') as r:
        for name in ('futures_research_model','futures_session_calendar','futures_research_store','futures_calculation_candidate','futures_native_candidate'):
            subprocess.run([sys.executable,str(ROOT/'tests'/('test_'+name+'.py'))],cwd=ROOT,check=True)
        accepted=json.loads(model.checked_original(QUALIFIED,read))
        assert accepted['privacy_verified'] is True and accepted['original_source_replay_verified'] is True
        source=json.loads(model.checked_original(accepted['source_audit'],read));expected=json.loads(model.checked_original(accepted['output'],read))
        for module in store.COMPILERS:
            if module.__name__ in accepted['compilers']:
                assert model.checked_original(accepted['compilers'][module.__name__],read)==Path(module.__file__).read_bytes()
        runtime=baseline.runtime(lam,s3,events,scheduler,baseline.FUNCTION);assert runtime==source['runtime']
        assert baseline.get(s3,model.LEGACY)==model.checked_original(source['predecessors'][model.LEGACY]['original'],read)
        try:s3.head_object(Bucket=BUCKET,Key=model.CURRENT)
        except Exception as exc:
            if not store.missing(exc):raise
        else:raise RuntimeError('Native futures head exists; inspect current publication before migration')
        try:prior=json.loads(baseline.get(s3,key))
        except Exception as exc:
            if not store.missing(exc):raise
            prior=None
        compilers={m.__name__:model.ref(Path(m.__file__).read_bytes(),'compilers') for m in store.COMPILERS}
        if prior:
            assert prior['status']=='complete' and prior['compilers']==compilers,'Inspect incomplete native candidate; do not discard its evidence'
            identity=prior['replay'];output=store.replay(identity,read)
            inputs=store.checked(store.verified_run(identity,read)['input'],'inputs',read)
        else:
            store.status_write(s3,BUCKET,key,{'status':'claimed','compilers':compilers,'source_qualification':QUALIFIED},IfNoneMatch='*')
            inputs=candidate_inputs(source,expected)
        def checkpoint(**fields):store.status_write(s3,BUCKET,key,{'status':'running','compilers':compilers,'source_qualification':QUALIFIED,**fields})
        start=time.monotonic()
        def emit(kind,doc):
            raw=model.encoded(doc);ref=model.ref(raw,kind);store.immutable(s3,BUCKET,ref,raw,read);return ref
        compiled=store.compile_output(inputs,read,emit)
        assert {k:v for k,v in compiled.items() if k!='predecessors'}==expected
        if prior:assert compiled==output
        output=compiled;identity=store.retain(s3,BUCKET,inputs,output,read,checkpoint)
        elapsed=round(time.monotonic()-start,3);counts=calculation.independent(output,source['sources'],read)
        assert elapsed < 45, 'Retain/replay must fit the reserved native execution budget'
        assert resource.getrusage(resource.RUSAGE_SELF).ru_maxrss < 350*1024, 'Leave native runtime memory headroom'
        assert baseline.runtime(lam,s3,events,scheduler,baseline.FUNCTION)==runtime
        assert baseline.get(s3,model.LEGACY)==model.checked_original(source['predecessors'][model.LEGACY]['original'],read)
        run=store.verified_run(identity,read)
        candidate={'contract':'futures-native-candidate.v1','source_qualification':QUALIFIED,'source_audit':accepted['source_audit'],
            'replay':identity,'generated_at':output['generated_at'],'candidate_seconds':elapsed,'counts':counts,
            'peak_rss_kib':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,'predecessor_runtime':runtime,
            'original_source_replay_verified':True,'independent_rational_reconciliation':True,'compilers':compilers,
            'provider_requests':0,'engine_invocations':0,'public_head_writes':0,'private_account_reads':0,
            'paid_ai_calls':0,'notifications_sent':0,'portfolio_writes':0,'schedules_changed':0}
        ref=store.protect(s3,BUCKET,model.encoded(candidate),read)
        r.kv(retained_candidate=ref,candidate_replay=identity,counts=counts,candidate_seconds=elapsed)
        store.status_write(s3,BUCKET,key,{'status':'complete','compilers':compilers,'replay':identity,'candidate':ref})
        protected={key,QUALIFIED['key'],accepted['source_audit']['key'],ref['key'],
            inputs['predecessors'][model.LEGACY]['original']['key'],*(p['original']['key'] for s in inputs['sources'].values() for p in s['pages'] if p.get('original'))}
        def deny(key):assert baseline.denied_with_retry('https://justhodl.ai/'+key) and baseline.denied_with_retry('https://'+BUCKET+'.s3.amazonaws.com/'+key)
        with ThreadPoolExecutor(max_workers=4) as pool:
            for _ in pool.map(deny,sorted(protected)):pass
        candidate.update(privacy_verified=True,protected_artifacts_checked=len(protected)+1,retained_candidate=ref)
        final=store.protect(s3,BUCKET,model.encoded(candidate),read);deny(final['key'])
        r.kv(accepted_candidate=final,candidate_replay=identity,counts=counts,candidate_seconds=elapsed,
            peak_rss_kib=candidate['peak_rss_kib'],protected_artifacts_checked=len(protected)+1,originals_anonymously_denied=True,
            provider_requests=0,engine_invocations=0,public_head_writes=0,private_account_reads=0,
            paid_ai_calls=0,notifications_sent=0,portfolio_writes=0,schedules_changed=0)


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
