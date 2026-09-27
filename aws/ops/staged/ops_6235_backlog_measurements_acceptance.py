"""Read-only exact Backlog package/cadence and published-observation arithmetic.

No original-provider replay claim, provider request, native invocation, account
read, consumer output, notification, public write or schedule change.
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
from market_runtime_evidence import runtime, bounded
from ops_6232_sec_search_research_acceptance import check_runtime
from sec_atom_model import strict, sha, clock

FN='justhodl-backlog';KEY='data/backlog.json';BUCKET='justhodl-dashboard-live'


def compiler():
    p=ROOT/'aws/lambdas'/FN/'source/backlog_measurements.py'
    spec=importlib.util.spec_from_file_location('isolated_backlog_measurements',p)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module


def check_concept(c, m):
    if c.get('contract')==m.CONTRACT:
        original=dict(c['source_metadata']);units={}
        for row in c['observations']:
            values=units.setdefault(row['source_unit'],[])
            if row['source_index']!=len(values):raise ValueError('Contiguous complete source observations required')
            values.append(row['source'])
        if original.get('_source_status')!=404:original['units']=units
        rebuilt=m.compile_concept(original,c['cik'],c['tag'],c['checked_as_of'])
        if any(c.get(key)!=value for key,value in rebuilt.items()):raise ValueError('Published concept calculation differs')
    elif c.get('latest') is not None or c.get('observations') or c.get('qoq') is not None or c.get('yoy') is not None:
        raise ValueError('Unreviewed concept cannot supply measurements')
    for alternate in c.get('alternate_concepts',[]):check_concept(alternate,m)


def publication(raw):
    packet=strict(raw)
    if not isinstance(packet,dict) or not isinstance(packet.get('by_ticker'),dict):raise ValueError('Whole Backlog ledger required')
    result={'status':'pending_original_schedule_publication','bytes':len(raw),'sha256':sha(raw),
            'generated_at':packet.get('generated_at'),'version':packet.get('version')}
    m=compiler()
    if packet.get('measurement_contract')!=m.CONTRACT:return result
    if (clock(packet.get('generated_at')) is None or packet.get('call') is not None
            or any(packet.get(k) is not False for k in ('calls_eligible','forecast_qualified','sizing_eligible'))
            or packet.get('quality',{}).get('provider_originals_replayed') is not False):
        raise ValueError('Explicit research-only publication required')
    counts={'rebuilt_rows':0,'retained_legacy_rows':0,'observations':0}
    for ticker,row in packet['by_ticker'].items():
        if not isinstance(row,dict) or row.get('ticker')!=ticker:raise ValueError('Declared issuer ledger differs')
        if row.get('measurement_contract')==m.CONTRACT:
            for c in row['measurements'].values():
                check_concept(c,m);counts['observations']+=len(c.get('observations',[]))
            expected=m.row_from_concepts(ticker,row['cik'],{k:row.get(k) for k in ('sector','cap_bucket','group')},row['measurements'])
            if any(row.get(k)!=v for k,v in expected.items()):raise ValueError('Published issuer projection differs')
            counts['rebuilt_rows']+=1
        else:
            if not isinstance(row.get('legacy_unverified_original'),dict):raise ValueError('Whole legacy observation required')
            for k in ('rpo','rpo_qoq','rpo_yoy','deferred_rev','deferred_qoq','deferred_yoy','eps','eps_qoq','eps_yoy','rev_yoy','ev_to_rpo','rpo_minus_rev_growth','demand_accelerating','deferred_accelerating'):
                if row.get(k) is not None:raise ValueError('Unverified legacy measurement remains active')
            counts['retained_legacy_rows']+=1
    if packet.get('ledger_size')!=len(packet['by_ticker']):raise ValueError('Whole ledger count differs')
    result.update(status='published_observation_arithmetic_reproduced',**counts,
                  provider_originals_replayed=False,point_in_time_availability_verified=False)
    return result


def main():
    subprocess.run([sys.executable,str(ROOT/'aws/lambdas'/FN/'tests/run_tests.py')],cwd=ROOT,check=True)
    original=json.loads((ROOT/'docs/audit/2026-09-27/accounting-original-baseline.json').read_bytes())['actual_producers'][FN]
    expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/'+FN],cwd=ROOT,text=True).strip()
    clients={n:boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler')}
    args=[clients[n] for n in ('lambda','s3','events','scheduler')]
    with report('ops_6235_backlog_measurements_acceptance') as r:
        before=runtime(*args,FN);check_runtime(before,original,expected,3)
        obj=clients['s3'].get_object(Bucket=BUCKET,Key=KEY);raw=bounded(obj['Body'])
        if obj.get('ContentLength')!=len(raw):raise ValueError('Whole publication required')
        result=publication(raw)
        if runtime(*args,FN)!=before:raise ValueError('Runtime changed during read-only check')
        r.kv(expected_commit=expected,actual_runtime=before,native_publication=result,
             native_invocations=0,provider_requests=0,consumer_output_reads=0,account_reads=0,
             credential_reads=0,learning_log_reads=0,public_writes=0,history_writes=0,schedule_changes=0,
             scope='Complete package and original cadence; arithmetic from published concept observations. Whole HTTP originals, filing vintage completeness, 52/53-week calendars and investment qualification remain open.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
