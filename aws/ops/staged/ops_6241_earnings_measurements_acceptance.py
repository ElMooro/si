"""Read-only exact Earnings Quality package and complete published observation projections.

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

FN='justhodl-earnings-quality';KEY='data/earnings-quality.json';BUCKET='justhodl-dashboard-live'


def compiler():
    p=ROOT/'aws/lambdas'/FN/'source/earnings_measurements.py'
    spec=importlib.util.spec_from_file_location('isolated_earnings_measurements',p)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module


def publication(raw):
    packet=strict(raw)
    if not isinstance(packet,dict):raise ValueError('Whole research packet required')
    result={'status':'pending_original_schedule_publication','bytes':len(raw),'sha256':sha(raw),
            'generated_at':packet.get('generated_at',packet.get('as_of')),'version':packet.get('version')}
    m=compiler()
    if packet.get('measurement_contract')!=m.CONTRACT:return result
    if clock(packet.get('generated_at')) is None or packet.get('call') is not None or packet.get('signals_logged')!=0 or packet.get('notifications_sent')!=0 or any(
            packet.get(k) is not False for k in ('calls_eligible','forecast_qualified','sizing_eligible','execution_eligible')):
        raise ValueError('Explicit research publication required')
    if any(packet.get(k)!=[] for k in ('all_ranked','top_20_high_quality','top_10_low_quality_avoid','trade_tickets')):
        raise ValueError('Unqualified rank/ticket promotion')
    rows=packet['issuer_rows'];plan=packet['universe_plan']
    if not isinstance(rows,list) or len(rows)!=packet['n_received'] or [r.get('ticker') for r in rows]!=plan['requested']:
        raise ValueError('Complete requested population differs')
    observations=0
    for i,row in enumerate(rows):
        expected=m.dossier(row['ticker'],row['acquisitions'],packet['checked_as_of'])
        expected['request_index']=i
        if row!=expected:raise ValueError('Whole reported accounting projection differs')
        observations+=sum(len(v) for v in row['statement_observations'].values())
    if packet['n_aligned']!=sum(r['windows']['current']['aligned'] for r in rows):raise ValueError('Aligned count differs')
    result.update(status='published_accounting_projections_reproduced',company_occurrences=len(rows),observations=observations,
                  provider_originals_replayed=False,original_sec_filings_replayed=False,point_in_time_availability_verified=False)
    return result


def main():
    subprocess.run([sys.executable,str(ROOT/'aws/lambdas'/FN/'tests/run_tests.py')],cwd=ROOT,check=True)
    original=json.loads((ROOT/'docs/audit/2026-09-27/earnings-quality-original-baseline.json').read_bytes())['actual_producers'][FN]
    expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/'+FN],cwd=ROOT,text=True).strip()
    clients={n:boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler')}
    args=[clients[n] for n in ('lambda','s3','events','scheduler')]
    with report('ops_6241_earnings_measurements_acceptance') as r:
        before=runtime(*args,FN);check_runtime(before,original,expected,2)
        obj=clients['s3'].get_object(Bucket=BUCKET,Key=KEY);raw=bounded(obj['Body'])
        if obj.get('ContentLength')!=len(raw):raise ValueError('Whole publication required')
        result=publication(raw)
        if runtime(*args,FN)!=before:raise ValueError('Runtime changed during acceptance')
        r.kv(expected_commit=expected,actual_runtime=before,native_publication=result,
             native_invocations=0,provider_requests=0,consumer_output_reads=0,account_reads=0,
             credential_reads=0,learning_log_reads=0,public_writes=0,history_writes=0,schedule_changes=0,
             scope='Exact complete package, unchanged schedule, whole published rows and dated observation reproduction. No original HTTP/SEC replay, first-release availability or investment authority.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
