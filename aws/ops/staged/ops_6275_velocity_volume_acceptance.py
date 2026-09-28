"""Read-only exact Velocity package/runtime acceptance; no output or learning-record reads."""
from pathlib import Path
import json,subprocess,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/ops/staged','aws/shared')]
from ops_report import report
from market_runtime_evidence import runtime
from ops_6232_sec_search_research_acceptance import check_runtime
from ops_6272_summary_context_acceptance import expected_commit
FUNCTION='justhodl-velocity-acceleration'


def check(before,baseline,commit):
    check_runtime(before,baseline,commit,6)
    return {'status':'exact_package_and_original_schedule_verified','native_output_verified':False,
            'consumer_decisions_qualified':False,'investment_authority':False}


def main():
    clients=[boto3.client(name,region_name='us-east-1') for name in ('lambda','s3','events','scheduler')]
    baseline=json.loads((ROOT/'docs/audit/2026-09-28/momentum-leaders-original-baseline.json').read_bytes())['actual_producers'][FUNCTION]
    with report('ops_6275_velocity_volume_acceptance') as r:
        subprocess.run([sys.executable,str(ROOT/'aws/lambdas'/FUNCTION/'tests/run_tests.py')],cwd=ROOT,check=True)
        commit=expected_commit(FUNCTION);before=runtime(*clients,FUNCTION);result=check(before,baseline,commit)
        if runtime(*clients,FUNCTION)!=before:raise ValueError('Runtime changed during acceptance')
        r.kv(expected_commit=commit,actual_package=before,acceptance=result,native_invocations=0,provider_requests=0,
             model_requests=0,consumer_output_reads=0,private_context_reads=0,account_reads=0,learning_log_reads=0,
             schedule_changes=0,public_writes=0,scope='Complete Velocity package, original runtime/schedules and synthetic replay; no private or downstream consumer-output qualification.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
