"""Read-only exact Estimate Revisions package, public originals and observation replay.

No provider/native invocation, private revision state, consumer/account/learning
reads, schedule changes, publication writes or notifications.
"""
from pathlib import Path
import hashlib,importlib.util,json,re,subprocess,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/ops/staged','aws/shared')]
from ops_report import report
from market_runtime_evidence import runtime,bounded
from ops_6232_sec_search_research_acceptance import check_runtime
FN='justhodl-estimate-revisions';KEY='data/estimate-revisions.json';BUCKET='justhodl-dashboard-live'


def compiler():
    p=ROOT/'aws/lambdas'/FN/'source/estimate_observations.py'
    spec=importlib.util.spec_from_file_location('isolated_estimate_observations',p);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m


def archive_ref(raw):
    return {'key':'data/estimate-revisions/history/'+hashlib.sha256(raw).hexdigest()+'.json','sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw)}


def read_public_archive(s3,ref):
    if not isinstance(ref,dict) or not re.fullmatch(r'data/estimate-revisions/history/[a-f0-9]{64}\.json',str(ref.get('key',''))):
        raise ValueError('Only declared public estimate history permitted')
    obj=s3.get_object(Bucket=BUCKET,Key=ref['key']);raw=bounded(obj['Body'])
    if obj.get('ContentLength')!=len(raw) or archive_ref(raw)!=ref:raise ValueError('Whole immutable archive differs')
    return raw


def publication(raw,prior_raw=None):
    m=compiler();p=m.strict(raw)
    if not isinstance(p,dict):raise ValueError('Whole packet required')
    out={'status':'pending_original_schedule_publication','bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest(),'generated_at':p.get('generated_at'),'version':p.get('version')}
    if p.get('measurement_contract')!=m.CONTRACT:return out
    if any(p.get(k) is not False for k in ('calls_eligible','forecast_qualified','sizing_eligible','execution_eligible','private_state_read_or_written')) or p.get('call') is not None:
        raise ValueError('Explicit research-only permission required')
    if any(p.get(k)!=[] for k in ('top_picks','upward_revisions','downward_revisions','estimate_strength_leaders')) or p.get('direction_map')!={} or p.get('by_ticker')!={} or p.get('signals_logged')!=0 or p.get('notifications_sent')!=0:
        raise ValueError('No unqualified score/direction publication')
    stamp=m.clock(p.get('generated_at'));started=m.clock(p.get('acquisition_started_at'))
    if stamp is None or started is None or stamp<started or m.day(p.get('checked_as_of'))!=started.date():raise ValueError('Publication clocks invalid')
    if p.get('previous_publication')!=(archive_ref(prior_raw) if prior_raw is not None else None):raise ValueError('Complete previous public packet differs')
    previous=m.strict(prior_raw) if prior_raw is not None else {};prior={}
    if previous.get('measurement_contract')==m.CONTRACT:
        if m.clock(previous.get('generated_at')) is None or m.clock(previous['generated_at'])>=started:raise ValueError('Prior publication time invalid')
        for row in previous['request_records']:prior.setdefault(row.get('ticker'),[]).append(row)
    records=p['request_records'];calendar=p['calendar_rows'];not_selected=p['not_selected_calendar_indices']
    if not isinstance(records,list) or not isinstance(calendar,list) or len(records)!=p['n_requested_occurrences'] or len(calendar)!=p['n_tracked']:
        raise ValueError('Whole populations differ')
    indices=[];count=0
    for i,row in enumerate(records):
        index=row['calendar_index']
        if type(index) is not int or not 0<=index<len(calendar):raise ValueError('Calendar occurrence invalid')
        event=calendar[index] if isinstance(calendar[index],dict) else {}
        if row['ticker']!=event.get('ticker'):raise ValueError('Request symbol changed')
        old=prior.get(row['ticker'],[]);a=row['acquisition']
        if a.get('status')=='received' and (m.clock(a.get('received_at')) is None or not started<=m.clock(a['received_at'])<=stamp):raise ValueError('Acquisition not bounded by run clocks')
        expected=m.dossier(row['ticker'],a,p['checked_as_of'],old[0].get('acquisition') if len(old)==1 else None)
        expected.update(request_index=i,calendar_index=index)
        if row!=expected:raise ValueError('Full observation replay differs')
        count+=len(row['observations']);indices.append(index)
    if len(set(indices))!=len(indices) or sorted(indices+not_selected)!=list(range(len(calendar))):raise ValueError('Selection loses or duplicates a calendar occurrence')
    if count!=p['n_estimate_observations'] or sum(r['acquisition'].get('status')=='received' for r in records)!=p['n_fmp_enriched']:raise ValueError('Observation counts differ')
    out.update(status='published_original_estimate_observations_replayed',calendar_occurrences=len(calendar),request_occurrences=len(records),estimate_observations=count,calendar_originals_verified=False,first_release_history_verified=False,investment_authority=False)
    return out


def main():
    subprocess.run([sys.executable,str(ROOT/'aws/lambdas'/FN/'tests/run_tests.py')],cwd=ROOT,check=True)
    original=json.loads((ROOT/'docs/audit/2026-09-27/estimate-revisions-original-baseline.json').read_bytes())['actual_producers'][FN]
    expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/'+FN],cwd=ROOT,text=True).strip()
    clients={n:boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler')};args=[clients[n] for n in ('lambda','s3','events','scheduler')]
    with report('ops_6243_estimate_observations_acceptance') as r:
        before=runtime(*args,FN);check_runtime(before,original,expected,4)
        obj=clients['s3'].get_object(Bucket=BUCKET,Key=KEY);raw=bounded(obj['Body'])
        if obj.get('ContentLength')!=len(raw):raise ValueError('Whole current publication required')
        p=compiler().strict(raw);prior=None;archived=False
        if p.get('measurement_contract')==compiler().CONTRACT:
            if read_public_archive(clients['s3'],archive_ref(raw))!=raw:raise ValueError('Current archive differs')
            archived=True
            if p.get('previous_publication') is not None:prior=read_public_archive(clients['s3'],p['previous_publication'])
        result=publication(raw,prior)
        if runtime(*args,FN)!=before:raise ValueError('Runtime changed during acceptance')
        r.kv(expected_commit=expected,actual_runtime=before,native_publication=result,current_archive_verified=archived,
             native_invocations=0,provider_requests=0,private_state_reads=0,consumer_output_reads=0,account_reads=0,
             learning_log_reads=0,public_writes=0,history_writes=0,schedule_changes=0,
             scope='Exact complete package and unchanged schedules. Whole current public response bytes and previous public snapshot; no private revision state, calendar-original replay, first-release history or investment authority.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
