"""Read-only complete Buyback package/cadence and published statement arithmetic.

No provider requests, native invocations, consumer-output/account/learning reads,
public writes, schedule changes, execution, messages or original-HTTP replay claim.
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

FN='justhodl-buyback-engine';KEY='data/buyback-engine.json';BUCKET='justhodl-dashboard-live'


def same(left,right):
    return json.dumps(left,sort_keys=True,ensure_ascii=False,allow_nan=False,separators=(',',':'))==json.dumps(right,sort_keys=True,ensure_ascii=False,allow_nan=False,separators=(',',':'))


def compiler():
    p=ROOT/'aws/lambdas'/FN/'source/buyback_measurements.py'
    spec=importlib.util.spec_from_file_location('isolated_buyback_measurements',p)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module


def publication(raw):
    packet=strict(raw)
    if not isinstance(packet,dict) or not isinstance(packet.get('tickers'),dict):raise ValueError('Whole Buyback ticker ledger required')
    result={'status':'pending_original_schedule_publication','bytes':len(raw),'sha256':sha(raw),
            'generated_at':packet.get('generated_at'),'version':packet.get('version')}
    m=compiler()
    if packet.get('measurement_contract')!=m.CONTRACT:return result
    if (clock(packet.get('generated_at')) is None or packet.get('call') is not None
            or any(packet.get(k) is not False for k in ('calls_eligible','forecast_qualified','sizing_eligible','execution_eligible'))
            or packet.get('quality',{}).get('provider_originals_replayed') is not False):
        raise ValueError('Explicit research-only output required')
    observations=0
    for symbol,row in packet['tickers'].items():
        source=row['provider_responses']
        expected=m.dossier(symbol,source['profile'],source['cash_flow'],source['key_metrics'],source['enterprise_values'],row['checked_as_of'])
        if any(k not in row or not same(row[k],v) for k,v in expected.items()):raise ValueError('Complete published statement projection differs')
        if row.get('buyback_score') is not None or row.get('class')!='RESEARCH_ONLY' or row.get('high_conviction_pump') is not False or row.get('auth_pct_mcap') is not None:
            raise ValueError('Source arithmetic was promoted into an event or forecast')
        observations+=len(expected['measurements']['cashflow_observations'])
    if (type(packet.get('n_scored')) is not int or packet['n_scored']!=0 or
            type(packet.get('n_research_rows')) is not int or packet['n_research_rows']!=len(packet['tickers']) or
            packet.get('high_conviction_pumps')!=[]):
        raise ValueError('Declared research population or forecast exclusion differs')
    result.update(status='published_statement_arithmetic_reproduced',issuer_rows=len(packet['tickers']),
                  cashflow_observations=observations,provider_originals_replayed=False,point_in_time_availability_verified=False)
    return result


def main():
    subprocess.run([sys.executable,str(ROOT/'aws/lambdas'/FN/'tests/run_tests.py')],cwd=ROOT,check=True)
    original=json.loads((ROOT/'docs/audit/2026-09-27/accounting-original-baseline.json').read_bytes())['actual_producers'][FN]
    expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/'+FN],cwd=ROOT,text=True).strip()
    clients={n:boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler')}
    args=[clients[n] for n in ('lambda','s3','events','scheduler')]
    with report('ops_6236_buyback_measurements_acceptance') as r:
        before=runtime(*args,FN);check_runtime(before,original,expected,6)
        obj=clients['s3'].get_object(Bucket=BUCKET,Key=KEY);raw=bounded(obj['Body'])
        if obj.get('ContentLength')!=len(raw):raise ValueError('Whole published statement packet required')
        publication_result=publication(raw)
        if runtime(*args,FN)!=before:raise ValueError('Actual runtime changed during read-only acceptance')
        r.kv(expected_commit=expected,actual_runtime=before,native_publication=publication_result,
             native_invocations=0,provider_requests=0,consumer_output_reads=0,account_reads=0,
             credential_reads=0,learning_log_reads=0,public_writes=0,history_writes=0,schedule_changes=0,
             scope='Exact package and original schedule; whole published provider-record arithmetic. No original-HTTP/SEC replay, complete issuer universe, corporate-action adjustment or investment qualification.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
