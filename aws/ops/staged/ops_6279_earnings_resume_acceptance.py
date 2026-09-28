"""Read-only exact Earnings package, original fanout and public acquisition replay."""
from pathlib import Path
import hashlib,json,subprocess,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/ops/staged','aws/shared')]
from ops_report import report
from market_runtime_evidence import runtime,bounded
from ops_6232_sec_search_research_acceptance import check_runtime
import ops_6254_earnings_event_acceptance as original
FN=original.FN;KEY=original.KEY;BUCKET=original.BUCKET


def publication(raw,prior_raw=None):
    m=original.compiler();p=m.strict(raw)
    if not isinstance(p,dict):raise ValueError('Whole public packet required')
    if p.get('measurement_contract')!=m.CONTRACT or p.get('version')=='1.1.0':
        return {'status':'pending_original_fanout_resumed_publication','bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest(),
            'generated_at':p.get('generated_at'),'version':p.get('version'),'new_queue_live_verified':False,'investment_authority':False}
    out=original.publication(raw,prior_raw)
    previous=m.strict(prior_raw) if prior_raw is not None else None
    plan=m.acquisition_plan(p['universe_membership']['selected'],previous);progress=p.get('acquisition_progress')
    if not isinstance(progress,dict):raise ValueError('Resumable acquisition progress required')
    visited=progress.get('visited_request_indices');total=0
    for i,row in enumerate(p['request_records']):
        group=row['acquisitions']
        unattempted=group[0].get('status')=='not_attempted_runtime_rate_or_size_limit'
        if not isinstance(visited,list) or unattempted==(i in visited):raise ValueError('Visited acquisition outcomes differ')
        total+=sum(a.get('original_bytes',0) for a in group)
    if progress!=m.acquisition_progress(plan,visited,progress.get('stop_reason'),total):raise ValueError('Acquisition progress replay differs')
    if progress['stop_reason']=='source_byte_budget' and total<8*1024*1024:raise ValueError('Source byte limit not reached')
    if progress['stop_reason']=='provider_rate_limit' and not any(a.get('status')=='rate_limited' for row in p['request_records'] for a in row['acquisitions']):raise ValueError('Rate limit outcome missing')
    out.update(new_queue_live_verified=True,planned_occurrences=len(plan['planned_request_indices']),visited_occurrences=len(visited),
        remaining_occurrences=progress['pending_occurrences'],received_earnings=sum(r['acquisitions'][0].get('status')=='received' for r in p['request_records']),
        whole_universe_current_coverage_qualified=False,stop_reason=progress['stop_reason'],retained_provider_bytes=total)
    return out


def main():
    subprocess.run([sys.executable,str(ROOT/'aws/lambdas'/FN/'tests/run_tests.py')],cwd=ROOT,check=True)
    baseline=json.loads((ROOT/'docs/audit/2026-09-27/earnings-pead-original-baseline.json').read_bytes())['actual_producers'][FN]
    expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/'+FN],cwd=ROOT,text=True).strip()
    clients={n:boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler')};args=[clients[n] for n in ('lambda','s3','events','scheduler')]
    with report('ops_6279_earnings_resume_acceptance') as r:
        before=runtime(*args,FN);check_runtime(before,baseline,expected,3);route=original.fanout(clients)
        obj=clients['s3'].get_object(Bucket=BUCKET,Key=KEY);raw=bounded(obj['Body'])
        if obj.get('ContentLength')!=len(raw):raise ValueError('Whole current public packet required')
        p=original.compiler().strict(raw);prior=None;archived=False
        if p.get('measurement_contract')==original.compiler().CONTRACT:
            if original.read_public_archive(clients['s3'],original.archive_ref(raw))!=raw:raise ValueError('Current archive differs')
            archived=True
            if p.get('previous_publication') is not None:prior=original.read_public_archive(clients['s3'],p['previous_publication'])
        result=publication(raw,prior)
        if runtime(*args,FN)!=before or original.fanout(clients)!=route:raise ValueError('Runtime or fanout changed during acceptance')
        r.kv(expected_commit=expected,actual_runtime=before,fanout_route=route,native_publication=result,current_archive_verified=archived,
             native_invocations=0,provider_requests=0,private_state_reads=0,consumer_output_reads=0,account_reads=0,learning_log_reads=0,
             public_writes=0,history_writes=0,schedule_changes=0,scope='Exact full producer package and original enabled fanout. Declared public producer and its own immutable source/history only. No whole-universe freshness or investment qualification.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
