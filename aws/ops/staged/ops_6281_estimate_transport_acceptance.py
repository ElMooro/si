"""Read-only Estimate package, compiler-bound publication and own public history."""
from pathlib import Path
import hashlib,json,subprocess,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/ops/staged','aws/shared')]
from ops_report import report
from market_runtime_evidence import runtime,bounded
from ops_6232_sec_search_research_acceptance import check_runtime
from ops_6272_summary_context_acceptance import expected_commit
import ops_6243_estimate_observations_acceptance as original
FN=original.FN;KEY=original.KEY;BUCKET=original.BUCKET


def source_files():
    paths=[ROOT/('aws/lambdas/'+FN+'/source/'+name) for name in ('lambda_function.py','estimate_observations.py')]
    paths += [ROOT/('aws/shared/'+name) for name in ('benzinga.py','managed_secret.py')]
    return {p.name:{'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()} for p in paths for raw in [p.read_bytes()]}


def publication(raw,prior_raw=None):
    p=original.compiler().strict(raw)
    if not isinstance(p,dict):raise ValueError('Whole current publication required')
    if p.get('measurement_contract')!=original.compiler().CONTRACT or p.get('version') in ('3.1.0','3.2.0'):
        return {'status':'pending_original_schedule_compiler_bound_publication','bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest(),
            'generated_at':p.get('generated_at'),'version':p.get('version'),'current_compiler_publication_verified':False,'investment_authority':False}
    if p.get('source_files')!=source_files():raise ValueError('Exact four-file compiler identity differs')
    if p.get('estimate_transport')!={'credential_location':'header','follow_redirects':False,'automatic_retries':False,'timeout_seconds':12,'max_response_bytes':1024*1024}:
        raise ValueError('Declared original estimate transport bounds differ')
    result=original.publication(raw,prior_raw)
    result.update(current_compiler_publication_verified=True,compiler_files=4,calendar_originals_verified=False)
    return result


def main():
    subprocess.run([sys.executable,str(ROOT/'aws/lambdas'/FN/'tests/run_tests.py')],cwd=ROOT,check=True)
    baseline=json.loads((ROOT/'docs/audit/2026-09-27/estimate-revisions-original-baseline.json').read_bytes())['actual_producers'][FN]
    clients=[boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler')];s3=clients[1]
    with report('ops_6281_estimate_transport_acceptance') as r:
        commit=expected_commit(FN);before=runtime(*clients,FN);check_runtime(before,baseline,commit,4)
        obj=s3.get_object(Bucket=BUCKET,Key=KEY);raw=bounded(obj['Body'])
        if obj.get('ContentLength')!=len(raw):raise ValueError('Whole current publication required')
        p=original.compiler().strict(raw);prior=None;archived=False
        if p.get('measurement_contract')==original.compiler().CONTRACT:
            if original.read_public_archive(s3,original.archive_ref(raw))!=raw:raise ValueError('Current archive differs')
            archived=True
            if p.get('previous_publication') is not None:prior=original.read_public_archive(s3,p['previous_publication'])
        result=publication(raw,prior)
        if runtime(*clients,FN)!=before:raise ValueError('Runtime changed during acceptance')
        r.kv(expected_commit=commit,actual_runtime=before,native_publication=result,current_archive_verified=archived,
            native_invocations=0,provider_requests=0,private_state_reads=0,consumer_output_reads=0,account_reads=0,learning_log_reads=0,
            public_writes=0,history_writes=0,schedule_changes=0,scope='Four complete native/compiler sources and original runtime/schedules. Declared public estimate source and own immutable history only; calendar originals, first releases, partial-run history continuity and investment authority remain unqualified.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
