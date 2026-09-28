"""Read-only package/receipt and original cadence baseline. No ledger or output read.

Offline counterexamples alone do not establish the currently deployed package.
This operation cannot invoke the engine, collect market data or alter a schedule.
"""
from pathlib import Path
import hashlib,sys
ROOT=Path(__file__).resolve().parents[3]
FUNCTION='justhodl-signal-genealogy'
SOURCE_SHA='d2140ca6a4647a6944d07b14ce2e63d6f52a1c8c2928d7fddbf74093c19cd660'

def main():
    if hashlib.sha256((ROOT/'aws/lambdas'/FUNCTION/'source/lambda_function.py').read_bytes()).hexdigest()!=SOURCE_SHA:
        raise ValueError('The audited native source changed before the runtime baseline')
    import boto3
    sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks')]
    from ops_report import report
    from market_runtime_evidence import runtime
    clients={name:boto3.client(name,region_name='us-east-1') for name in ('lambda','s3','events','scheduler')}
    args=[clients[name] for name in ('lambda','s3','events','scheduler')]
    with report('ops_6302_genealogy_runtime_baseline') as r:
        before=runtime(*args,FUNCTION)
        r.kv(actual_runtime=before,code_matches_repository=True,
             source_handler_sha256=SOURCE_SHA,
             native_invocations=0,provider_requests=0,credential_reads=0,learning_ledger_reads=0,
             private_account_reads=0,downstream_output_reads=0,public_writes=0,history_writes=0,schedule_changes=0,
             calculation_qualified=False,forecast_qualified=False,sizing_eligible=False,
             scope='Whole packaged-source and receipt baseline only. Synthetic counterexamples do not qualify actual data; no learning ledger, source data or derived report is read.')
        if runtime(*args,FUNCTION)!=before:raise ValueError('Runtime changed during baseline')

if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
