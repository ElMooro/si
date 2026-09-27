"""Read-only exact Capex package and complete published accounting projections.

No provider calls, native invocation, consumer/account/learning reads, writes,
notifications, schedule change or original HTTP replay claim.
"""
from pathlib import Path
import importlib.util
import json
import subprocess
import sys
import boto3

ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/ops/staged','aws/shared')]
from ops_report import report
from market_runtime_evidence import runtime,bounded
from ops_6232_sec_search_research_acceptance import check_runtime
from sec_atom_model import strict,sha,clock

FN='justhodl-capex-pulse';KEY='data/capex-pulse.json';BUCKET='justhodl-dashboard-live'
HYPERSCALERS=['AMZN','MSFT','GOOGL','META','AAPL','NVDA','ORCL','AVGO']


def compiler():
    p=ROOT/'aws/lambdas'/FN/'source/capex_measurements.py'
    spec=importlib.util.spec_from_file_location('isolated_capex_measurements',p)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module


def publication(raw):
    packet=strict(raw)
    if not isinstance(packet,dict) or not isinstance(packet.get('rows'),list):raise ValueError('Whole Capex population required')
    result={'status':'pending_original_schedule_publication','bytes':len(raw),'sha256':sha(raw),
            'generated_at':packet.get('generated_at'),'version':packet.get('version')}
    m=compiler()
    if packet.get('measurement_contract')!=m.CONTRACT:return result
    if clock(packet.get('generated_at')) is None or packet.get('call') is not None or any(
            packet.get(k) is not False for k in ('calls_eligible','forecast_qualified','sizing_eligible','execution_eligible')):
        raise ValueError('Explicit research publication required')
    observations=0
    rows=packet['rows']
    if packet.get('n')!=len(rows):raise ValueError('Population differs')
    for row in rows:
        expected=m.dossier(row['ticker'],row['provider_response'],row['source_context'],row['checked_as_of'])
        if any(row.get(k)!=v for k,v in expected.items()):raise ValueError('Complete row projection differs')
        observations+=len(expected['observations'])
    sectors={r['sector'] for r in rows}
    if set(packet['sectors'])!=sectors:raise ValueError('Sector population differs')
    groups=[(packet['market'],rows),(packet['hyperscalers'],[r for r in rows if r['ticker'] in HYPERSCALERS])]
    groups += [(packet['sectors'][s],[r for r in rows if r['sector']==s]) for s in sectors]
    for actual,population_rows in groups:
        expected=m.aggregate(population_rows)
        if any(actual.get(k)!=v for k,v in expected.items()):raise ValueError('Complete calendar cohort projection differs')
    survey=(packet.get('macro_intentions') or {}).get('philly_future_capex')
    if survey is not None:
        expected=m.intentions(survey['original_response'],packet['generated_at'][:10])
        if survey!=expected:raise ValueError('Survey calendar projection differs')
    result.update(status='published_accounting_projections_reproduced',issuer_rows=len(rows),observations=observations,
                  provider_originals_replayed=False,original_sec_filings_replayed=False,point_in_time_availability_verified=False)
    return result


def main():
    subprocess.run([sys.executable,str(ROOT/'aws/lambdas'/FN/'tests/run_tests.py')],cwd=ROOT,check=True)
    original=json.loads((ROOT/'docs/audit/2026-09-27/accounting-original-baseline.json').read_bytes())['actual_producers'][FN]
    expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/'+FN],cwd=ROOT,text=True).strip()
    clients={n:boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler')}
    args=[clients[n] for n in ('lambda','s3','events','scheduler')]
    with report('ops_6237_capex_measurements_acceptance') as r:
        before=runtime(*args,FN);check_runtime(before,original,expected,3)
        obj=clients['s3'].get_object(Bucket=BUCKET,Key=KEY);raw=bounded(obj['Body'])
        if obj.get('ContentLength')!=len(raw):raise ValueError('Whole publication required')
        result=publication(raw)
        if runtime(*args,FN)!=before:raise ValueError('Runtime changed during acceptance')
        r.kv(expected_commit=expected,actual_runtime=before,native_publication=result,
             native_invocations=0,provider_requests=0,consumer_output_reads=0,account_reads=0,
             credential_reads=0,learning_log_reads=0,public_writes=0,history_writes=0,schedule_changes=0,
             scope='Exact complete package, unchanged schedule, whole published rows and calendar-cohort reproduction. No original HTTP/SEC replay, first-release availability or investment authority.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
