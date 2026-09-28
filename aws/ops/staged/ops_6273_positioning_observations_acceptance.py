"""Read-only Positioning/Apex package acceptance; no consumer packet or native I/O."""
from pathlib import Path
import json,subprocess,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/ops/staged','aws/shared')]
from ops_report import report
from market_runtime_evidence import runtime
from ops_6232_sec_search_research_acceptance import check_runtime
from ops_6272_summary_context_acceptance import expected_commit
FUNCTIONS={'justhodl-pump-positioning':4,'justhodl-apex-fusion':2}


def check(before,baseline,commit,count):
    check_runtime(before,baseline,commit,count)
    return {'status':'exact_package_and_original_schedule_verified','native_output_verified':False,
            'consumer_decisions_qualified':False,'investment_authority':False}


def main():
    clients=[boto3.client(name,region_name='us-east-1') for name in ('lambda','s3','events','scheduler')]
    baselines=json.loads((ROOT/'docs/audit/2026-09-28/momentum-leaders-original-baseline.json').read_bytes())['actual_producers']
    with report('ops_6273_positioning_observations_acceptance') as r:
        packages={}
        for function,count in FUNCTIONS.items():
            subprocess.run([sys.executable,str(ROOT/'aws/lambdas'/function/'tests/run_tests.py')],cwd=ROOT,check=True)
            commit=expected_commit(function);before=runtime(*clients,function)
            result=check(before,baselines[function],commit,count)
            if runtime(*clients,function)!=before:raise ValueError('Runtime changed during acceptance')
            packages[function]={'expected_commit':commit,'actual_package':before,'acceptance':result}
        r.kv(packages=packages,native_invocations=0,provider_requests=0,model_requests=0,consumer_output_reads=0,
             private_context_reads=0,account_reads=0,learning_log_reads=0,schedule_changes=0,public_writes=0,
             scope='Complete Positioning and Apex packages, original runtime/schedules, synthetic observations and narrow input-exclusion regression. No consumer-output, portfolio or broader Apex qualification.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
