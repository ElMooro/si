"""Capture only one complete deployed consumer package and unchanged schedules.

No consumer/private output or account reads, native/provider/model invocation,
schedule changes or public writes. Whole code is retained privately.
"""
from pathlib import Path
from datetime import datetime,timezone
import hashlib,json,subprocess,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/ops/staged')]
from ops_report import report
from ops_6204_shipping_consumer_baseline import runtime,retain,encode
import retained_access_evidence as access
FN='justhodl-prepump-summary'
PIN='e84e046020dd28190702e51ebccde27bf37e77ef0499eb445b221307260872c7'


def source_check(raw):
    if not isinstance(raw,bytes) or hashlib.sha256(raw).hexdigest()!=PIN:raise ValueError('Reviewed whole summary source required')
    return {'bytes':len(raw),'sha256':PIN,'imported_or_executed':False}


def main():
    for test in ('tests/ops/test_prepump_summary_original.py','tests/test_shipping_consumer_baseline.py'):
        subprocess.run([sys.executable,str(ROOT/test)],cwd=ROOT,check=True)
    checked=source_check((ROOT/'aws/lambdas'/FN/'source/lambda_function.py').read_bytes())
    clients=[boto3.client(name,region_name='us-east-1') for name in ('lambda','s3','events','scheduler')]
    with report('ops_6271_prepump_summary_original_baseline') as r:
        actual=runtime(*clients,FN)
        if actual.get('status')!='whole_actual_package_retained':raise ValueError('Whole deployed consumer required')
        baseline={'contract':'prepump-summary-original-baseline.v1','captured_at':datetime.now(timezone.utc).isoformat(),
                  'source_check':checked,'actual_consumer':actual,'consumer_output_reads':0}
        ref=retain(clients[1],encode(baseline))
        privacy=access.summarize([access.check(ref['key']),access.check(actual['whole_zip']['key'])])
        if not privacy['all_denied']:raise ValueError('Complete original code must remain private')
        summary={k:v for k,v in actual.items() if k not in ('inventory','repository_sources')}
        summary.update({k:actual['inventory'][k] for k in ('code_matches_repository','source_files_checked','source_differences')})
        r.kv(baseline=ref,source_check=checked,actual_consumer=summary,**privacy,
             native_invocations=0,provider_requests=0,model_requests=0,consumer_output_reads=0,
             private_context_reads=0,account_reads=0,learning_log_reads=0,schedule_changes=0,public_writes=0,
             scope='One complete deployed consumer package and original runtime/schedules only. No output, private context or account reads; no investment qualification.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
