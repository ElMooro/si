"""Read-only complete EPS package, original route, source history and progress replay."""
from pathlib import Path
import hashlib,json,subprocess,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/ops/staged','aws/shared')]
from ops_report import report
from market_runtime_evidence import runtime,bounded
from ops_6232_sec_search_research_acceptance import check_runtime
from ops_6272_summary_context_acceptance import expected_commit
import ops_6284_eps_baselines_acceptance as retained
original=retained.original;FN=retained.FN;KEY=retained.KEY;BUCKET=retained.BUCKET


def publication(raw,prior_raw=None,read_source=None):
    m=original.compiler();p=m.strict(raw)
    if not isinstance(p,dict):raise ValueError('Whole public packet required')
    if p.get('measurement_contract')!=m.CONTRACT or p.get('version') in ('1.1.0','1.1.1','1.2.0'):
        return {'status':'pending_original_fanout_resumption_publication','bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest(),
            'generated_at':p.get('generated_at'),'version':p.get('version'),'current_compiler_publication_verified':False,'investment_authority':False}
    if p.get('version')!='1.3.0':raise ValueError('Recognized resumption compiler required')
    progress=p.get('acquisition_progress');selected=p['universe_membership']['selected_symbols'];records=p['request_records']
    if progress is None:raise ValueError('Complete actual acquisition progress required')
    m.validate_acquisition_progress(selected,records,progress)
    plan=m.acquisition_plan(selected,m.strict(prior_raw) if prior_raw else None)
    if progress!=m.acquisition_progress(plan,progress['visited_request_indices'],progress['stop_reason'],progress['retained_provider_bytes']):
        raise ValueError('Acquisition ordering does not replay from whole prior publication')
    result=retained.publication(raw,prior_raw,read_source)
    result.update(status='published_eps_sources_and_resumption_replayed',selected_occurrences=len(selected),
        visited_occurrences=progress['visited_occurrences'],pending_occurrences=progress['pending_occurrences'],
        complete_selected_visit_cycle=progress['cycle_complete'],acquisition_stop_reason=progress['stop_reason'],
        annual_arrays_received=sum(a.get('endpoint')=='analyst-estimates' and a.get('status')=='received' and isinstance(m.original(a),list) for row in records for a in row['acquisitions']),
        whole_source_universe_coverage_verified=False,historical_runtime_stop_independently_verified=False,investment_authority=False)
    return result


def main():
    subprocess.run([sys.executable,str(ROOT/'aws/lambdas'/FN/'tests/run_tests.py')],cwd=ROOT,check=True)
    baseline=json.loads((ROOT/'docs/audit/2026-09-27/eps-revision-velocity-original-baseline.json').read_bytes())['actual_producers'][FN]
    prior_route=json.loads((ROOT/'docs/audit/2026-09-27/eps-target-code-acceptance.json').read_bytes())['fanout_route']
    clients={n:boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler')};args=[clients[n] for n in ('lambda','s3','events','scheduler')]
    with report('ops_6285_eps_resumption_acceptance') as r:
        commit=expected_commit(FN);before=runtime(*args,FN);check_runtime(before,baseline,commit,3)
        route=original.fanout(clients)
        if any(route[k]!=prior_route[k] for k in ('matching_ticks','routes')):raise ValueError('Original EPS scheduled route differs')
        obj=clients['s3'].get_object(Bucket=BUCKET,Key=KEY);raw=bounded(obj['Body'])
        if obj.get('ContentLength')!=len(raw):raise ValueError('Whole current publication required')
        p=original.compiler().strict(raw);prior=None;archived=False
        if p.get('measurement_contract')==original.compiler().CONTRACT:
            if original.read_public_archive(clients['s3'],original.archive_ref(raw))!=raw:raise ValueError('Current archive differs')
            archived=True
            if p.get('previous_publication') is not None:prior=original.read_public_archive(clients['s3'],p['previous_publication'])
        result=publication(raw,prior,retained.source_reader(clients['s3']))
        if runtime(*args,FN)!=before or original.fanout(clients)!=route:raise ValueError('Runtime or original fanout changed during acceptance')
        r.kv(expected_commit=commit,actual_runtime=before,fanout_route=route,native_publication=result,current_archive_verified=archived,
            native_invocations=0,provider_requests=0,private_state_reads=0,consumer_output_reads=0,account_reads=0,learning_log_reads=0,
            public_writes=0,history_writes=0,schedule_changes=0,scope='Complete three-file EPS compiler and original fanout. Whole public head/prior history, strictly owned retained originals and exact selected acquisition progress only. No invocation, private/consumer reads, schedule change or investment authority.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
