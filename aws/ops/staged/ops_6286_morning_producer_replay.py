"""Read-only normal-publication replay for three declared public producers.

Consumer packages have advanced independently since the original checks. This
operation verifies only the producers and their own immutable originals/history;
it grants no consumer, forecast or investment qualification.
"""
from pathlib import Path
import json,subprocess,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/ops/staged','aws/shared')]
from ops_report import report
from market_runtime_evidence import runtime,bounded
from ops_6232_sec_search_research_acceptance import check_runtime
from ops_6272_summary_context_acceptance import expected_commit
import ops_6262_price_compression_acceptance as compression
import ops_6264_momentum_price_acceptance as momentum
import ops_6260_activist_filings_acceptance as filings

SPECS=(
    (compression,'volatility-squeeze-original-baseline.json','price-compression-code-acceptance.json'),
    (momentum,'momentum-breakout-original-baseline.json','momentum-price-code-acceptance.json'),
    (filings,'activist-filings-original-baseline.json','activist-filings-code-acceptance.json'),
)


def replay(op,s3):
    obj=s3.get_object(Bucket=op.BUCKET,Key=op.KEY);raw=bounded(obj['Body'])
    if obj.get('ContentLength')!=len(raw):raise ValueError('Whole current public publication required')
    p=op.compiler().strict(raw)
    if not isinstance(p,dict) or p.get('measurement_contract')!=op.compiler().CONTRACT:
        raise ValueError('Normal research publication is not yet available')
    if op.read_public_archive(s3,op.archive_ref(raw))!=raw:raise ValueError('Current immutable archive differs')
    prior=op.read_public_archive(s3,p['previous_publication']) if p.get('previous_publication') is not None else None
    return op.publication(raw,prior,op.read_sources(s3,p))


def accept(op,baseline,prior,clients):
    args=[clients[n] for n in ('lambda','s3','events','scheduler')]
    commit=expected_commit(op.FN);before=runtime(*args,op.FN)
    check_runtime(before,baseline['actual_producers'][op.FN],commit,3)
    route=op.fanout(clients)
    if any(route[k]!=prior['fanout_route'][k] for k in ('matching_ticks','routes')):
        raise ValueError('Original scheduled producer route differs')
    settings=op.producer_settings(clients['lambda']) if hasattr(op,'producer_settings') else None
    if settings is not None and settings!=baseline['producer_settings']:
        raise ValueError('Original producer controls differ')
    publication=replay(op,clients['s3'])
    if runtime(*args,op.FN)!=before or op.fanout(clients)!=route:
        raise ValueError('Producer package, runtime or schedule changed during replay')
    if settings is not None and op.producer_settings(clients['lambda'])!=settings:
        raise ValueError('Producer controls changed during replay')
    return {'function':op.FN,'key':op.KEY,'expected_commit':commit,'actual_runtime':before,
            'fanout_route':route,'producer_settings':settings,'native_publication':publication,
            'current_archive_verified':True,'consumer_qualification':False,'investment_authority':False}


def main():
    clients={n:boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler')}
    with report('ops_6286_morning_producer_replay') as r:
        for op,baseline_name,prior_name in SPECS:
            subprocess.run([sys.executable,str(ROOT/'aws/lambdas'/op.FN/'tests/run_tests.py')],cwd=ROOT,check=True)
            directory=ROOT/'docs/audit/2026-09-28'
            result=accept(op,json.loads((directory/baseline_name).read_bytes()),json.loads((directory/prior_name).read_bytes()),clients)
            r.kv(**{op.FN:result})
        r.kv(native_invocations=0,provider_requests=0,private_state_reads=0,consumer_output_reads=0,
             account_reads=0,learning_log_reads=0,public_writes=0,history_writes=0,schedule_changes=0,
             scope='Three exact public producers, original enabled routes, whole originals and prior/current archives only. No consumer package/output acceptance or predictive qualification.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
