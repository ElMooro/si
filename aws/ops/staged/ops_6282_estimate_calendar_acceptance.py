"""Read-only exact Estimate compiler, original calendar and own public history."""
from pathlib import Path
from datetime import timedelta
import hashlib,json,subprocess,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/ops/staged','aws/shared')]
from ops_report import report
from market_runtime_evidence import runtime,bounded
from ops_6232_sec_search_research_acceptance import check_runtime
from ops_6272_summary_context_acceptance import expected_commit
import ops_6281_estimate_transport_acceptance as transport
original=transport.original
FN=original.FN;KEY=original.KEY;BUCKET=original.BUCKET


def publication(raw,prior_raw=None):
    m=original.compiler();p=m.strict(raw)
    if not isinstance(p,dict):raise ValueError('Whole current publication required')
    if p.get('measurement_contract')!=m.CONTRACT or p.get('version') in ('3.1.0','3.2.0','3.2.1'):
        return {'status':'pending_original_schedule_calendar_publication','bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest(),
            'generated_at':p.get('generated_at'),'version':p.get('version'),'current_compiler_publication_verified':False,
            'calendar_originals_verified':False,'investment_authority':False}
    result=transport.publication(raw,prior_raw)
    a=p.get('calendar_acquisition');projected=m.calendar_projection(a)
    if p.get('calendar_original_http_retained') is not True or p.get('calendar_rows')!=projected['rows'] or p.get('calendar_evidence')!={k:v for k,v in projected.items() if k!='rows'}:
        raise ValueError('Whole calendar projection and exclusion replay differs')
    if p.get('calendar_transport')!={'credential_location':'header','follow_redirects':False,'automatic_retries':False,'timeout_seconds':15,'max_response_bytes':8*1024*1024}:
        raise ValueError('Calendar request transport scope differs')
    start=m.clock(p['acquisition_started_at']);finish=m.clock(p['generated_at']);received=m.clock(a['received_at'])
    if received is None or not start<=received<=finish:raise ValueError('Calendar receipt outside run clocks')
    if type(p.get('horizon_days')) is not int or p['horizon_days']!=75 or type(p.get('min_importance')) is not int or p['min_importance']!=2 or type(p.get('request_limit')) is not int or p['request_limit']!=280:
        raise ValueError('Original calendar window, filter or enrichment limit differs')
    if a['start_date']!=p['checked_as_of'] or a['end_date']!=(m.day(p['checked_as_of'])+timedelta(days=75)).isoformat() or a['min_importance']!=2:
        raise ValueError('Calendar request dates or filter differ')
    rows=p['calendar_rows'];ordered=sorted(range(len(rows)),key=lambda i:((m.day(rows[i]['date'])-start.date()).days,-rows[i]['importance']))[:280]
    if [r['calendar_index'] for r in p['request_records']]!=ordered:raise ValueError('Actual selected calendar order differs')
    result.update(calendar_originals_verified=True,calendar_received_occurrences=projected['received_occurrences'],
        calendar_excluded_occurrences=len(projected['excluded']),calendar_pagination=projected['pagination_status'],
        provider_universe_complete=False,market_session_qualified=False,first_release_history_verified=False,investment_authority=False)
    return result


def main():
    subprocess.run([sys.executable,str(ROOT/'aws/lambdas'/FN/'tests/run_tests.py')],cwd=ROOT,check=True)
    baseline=json.loads((ROOT/'docs/audit/2026-09-27/estimate-revisions-original-baseline.json').read_bytes())['actual_producers'][FN]
    clients=[boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler')];s3=clients[1]
    with report('ops_6282_estimate_calendar_acceptance') as r:
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
            public_writes=0,history_writes=0,schedule_changes=0,scope='Four complete sources and original runtime/schedules. Declared public estimate source and own history only. Whole single calendar response and filtering replay; no additional pages, market-session or first-release qualification, private state or investment authority.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
