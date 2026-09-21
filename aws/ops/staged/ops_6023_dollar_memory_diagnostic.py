"""Read-only phase profiling of the already-retained 6022 candidate.

Fresh subprocesses separate production reconstruction from independent-audit
overhead. No replay request is dispatched and no S3 object is written.
"""
from pathlib import Path
import json,subprocess,sys,time
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/staged'),str(ROOT/'aws/ops/checks'),str(ROOT/'aws/shared')]
from ops_report import report
from dollar_original_audit import independent
import ops_6022_dollar_original_candidate as candidate
store=candidate.store;model=store.model;BUCKET=candidate.BUCKET


def profile(mode,s3=None,memory=None):
    assert mode in ('production_replay','independent_audit')
    if memory is None:
        import resource
        memory=lambda:resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    s3=s3 or boto3.client('s3',region_name='us-east-1');read=store.reader(s3,BUCKET);stages=[];started=time.monotonic()
    def stage(name):stages.append({'phase':name,'seconds':round(time.monotonic()-started,3),'peak_rss_kib':memory()})
    status=json.loads(candidate.baseline.get(s3,candidate.STATUS));ref=status['candidate_replay']
    assert status['request_id']==candidate.REQUEST,'Exact rejected request required'
    stage('original_request_read')
    run=store.verified_run(ref,read);inputs=store.checked(run['input'],'inputs',read);stage('recorded_input_loaded')
    out=store.compile_output(inputs,read);stage('first_original_compilation')
    assert out==store.checked(run['output'],'outputs',read) and model.sha(model.encoded(out))==ref['output_sha256']
    stage('recorded_output_compared')
    if mode=='production_replay':
        assert store.replay(ref,read)==out;stage('second_original_replay')
        counts=None
    else:
        packet=json.loads(store.original(inputs['captures']['data/report-measurements.json']['original'],read));stage('audit_canonical_loaded')
        originals=store.canonical_fred_replay.restore(packet,store.catalog.SERIES,read);stage('audit_originals_restored')
        counts=independent(out,originals,packet,store.catalog);stage('independent_arithmetic_checked')
    return {'mode':mode,'candidate_replay':ref,'generated_at':out['generated_at'],'stages':stages,'counts':counts,
        'provider_requests':0,'engine_invocations':0,'s3_writes':0,'private_account_reads':0}


def main():
    with report('ops_6023_dollar_memory_diagnostic') as r:
        results=[]
        for mode in ('production_replay','independent_audit'):
            result=subprocess.run([sys.executable,str(Path(__file__).resolve()),'--profile',mode],cwd=ROOT,check=True,capture_output=True,text=True)
            results.append(json.loads(result.stdout))
        r.kv(profiles=results,existing_timeout_seconds=180,existing_memory_mb=256,qualification_budget_rss_kib=224*1024,
            candidate_not_promoted=True,provider_requests=0,engine_invocations=0,s3_writes=0,public_head_writes=0,
            private_account_reads=0,paid_ai_calls=0,notifications_sent=0,portfolio_writes=0,schedules_changed=0)


if __name__=='__main__':
    try:
        if len(sys.argv)==3 and sys.argv[1]=='--profile':print(json.dumps(profile(sys.argv[2]),sort_keys=True))
        else:main()
    except Exception:sys.exit(1)
