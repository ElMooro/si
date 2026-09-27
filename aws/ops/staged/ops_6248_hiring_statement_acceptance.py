"""Read-only whole Hiring package, original cadence and public statement replay.

No native/provider invocation, consumer/account/learning reads, schedule changes,
publication writes or notifications. Only declared public producer/history keys.
"""
from pathlib import Path
import hashlib, importlib.util, json, re, subprocess, sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/ops/staged','aws/shared')]
from ops_report import report
from market_runtime_evidence import runtime,bounded
from ops_6232_sec_search_research_acceptance import check_runtime
FN='justhodl-hiring-velocity';KEY='data/hiring-velocity.json';BUCKET='justhodl-dashboard-live'


def compiler():
    p=ROOT/'aws/lambdas'/FN/'source/hiring_observations.py'
    spec=importlib.util.spec_from_file_location('isolated_hiring_observations',p);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m


def archive_ref(raw):
    return {'key':'data/hiring-velocity/history/'+hashlib.sha256(raw).hexdigest()+'.json','sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw)}


def read_public_archive(s3,ref):
    if not isinstance(ref,dict) or not re.fullmatch(r'data/hiring-velocity/history/[a-f0-9]{64}\.json',str(ref.get('key',''))):
        raise ValueError('Only declared public Hiring history permitted')
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
    if any(p.get(k)!=[] for k in ('top_50','expansion_inflections','double_confirmed')) or p.get('signals_logged')!=0 or p.get('notifications_sent')!=0 or p.get('n_scored')!=0:
        raise ValueError('No unqualified scores or alerts permitted')
    stamp=m.clock(p.get('generated_at'));started=m.clock(p.get('acquisition_started_at'))
    if stamp is None or started is None or stamp<started or m.day(p.get('checked_as_of'))!=started.date():raise ValueError('Publication clocks invalid')
    if p.get('previous_publication')!=(archive_ref(prior_raw) if prior_raw is not None else None):raise ValueError('Whole previous public packet differs')
    if prior_raw:
        previous=m.strict(prior_raw)
        if previous.get('measurement_contract')==m.CONTRACT and (m.clock(previous.get('generated_at')) is None or m.clock(previous['generated_at'])>=started):raise ValueError('Previous clock invalid')
    def acquisition(a):
        if a.get('status') in ('received','invalid_original'):
            received=m.clock(a.get('received_at'))
            if received is None or not started<=received<=stamp:raise ValueError('Acquisition clock outside run')
        return m.original(a)
    universe=acquisition(p['universe_acquisition'])
    if not isinstance(universe,dict) or not isinstance(universe.get('stocks'),list):raise ValueError('Whole universe required')
    stocks=universe['stocks'];acquisition(p['bagger_context_acquisition'])
    if p.get('cap_buckets')!=['micro','mid','nano','small']:raise ValueError('Original universe scope changed')
    indices=[i for i,r in enumerate(stocks) if isinstance(r,dict) and r.get('cap_bucket') in p['cap_buckets']]
    limit=p.get('event_limit')
    if limit is not None and (type(limit) is not int or limit<0):raise ValueError('Invalid event limit')
    if limit:indices=indices[:limit]
    records=p.get('request_records')
    if not isinstance(records,list) or len(records)!=len(indices) or p.get('n_selected_occurrences')!=len(indices) or p.get('universe_occurrences')!=len(stocks):raise ValueError('Whole request population differs')
    if p.get('unselected_universe_indices')!=[i for i in range(len(stocks)) if i not in set(indices)]:raise ValueError('Universe occurrence lost')
    employees=0;incomes=0;scanned=0;errors=0
    for i,(r,index) in enumerate(zip(records,indices)):
        captures=r['acquisitions']
        if not isinstance(captures,list) or not captures:raise ValueError('Acquisitions required')
        endpoints=[a.get('endpoint') for a in captures]
        if endpoints not in (['historical-employee-count'],['historical-employee-count','income-statement'],['historical-employee-count','employee-count'],['historical-employee-count','employee-count','income-statement']):raise ValueError('Unexpected provider request scope')
        for a in captures:acquisition(a)
        expected=m.dossier(stocks[index],captures,p['checked_as_of']);expected.update(request_index=i,universe_index=index)
        if r!=expected:raise ValueError('Whole statement replay differs')
        employees+=len(r['employee_observations']);incomes+=len(r['income_observations'])
        scanned+=any(a['status'] not in ('not_attempted_runtime_rate_or_size_limit','invalid_symbol_not_requested','credential_unavailable') for a in captures)
        errors+=any(a['status'] in ('unavailable','rate_limited','invalid_original','response_exceeds_bound','credential_echo_withheld') for a in captures)
    if any(p.get(k)!=v for k,v in [('n_employee_observations',employees),('n_income_observations',incomes),('n_scanned',scanned),('n_errors',errors)]):raise ValueError('Observation/acquisition counts differ')
    out.update(status='published_workforce_originals_replayed',universe_occurrences=len(stocks),request_occurrences=len(records),employee_observations=employees,income_observations=incomes,first_release_history_verified=False,organic_hiring_verified=False,investment_authority=False)
    return out


def main():
    subprocess.run([sys.executable,str(ROOT/'aws/lambdas'/FN/'tests/run_tests.py')],cwd=ROOT,check=True)
    original=json.loads((ROOT/'docs/audit/2026-09-27/hiring-velocity-original-baseline.json').read_bytes())['actual_producers'][FN]
    expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/'+FN],cwd=ROOT,text=True).strip()
    clients={n:boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler')};args=[clients[n] for n in ('lambda','s3','events','scheduler')]
    with report('ops_6248_hiring_statement_acceptance') as r:
        before=runtime(*args,FN);r.kv(actual_runtime=before);check_runtime(before,original,expected,2)
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
             scope='Exact complete package and unchanged weekly cadence. Whole public response bytes and previous public snapshot; no private state, organic hiring, first-release history or investment authority.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
