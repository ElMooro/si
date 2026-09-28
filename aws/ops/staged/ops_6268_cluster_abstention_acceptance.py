"""Read-only exact Catalyst Clusters package/runtime acceptance.

No producer or consumer output/state/account reads, native/provider invocation,
schedule change, notification, paid AI or publication write. Behaviour checks use
isolated synthetic inputs. Native output qualification is outside this operation.
"""
from pathlib import Path
import json,subprocess,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/ops/staged','aws/shared')]
from ops_report import report
from market_runtime_evidence import runtime
from ops_6232_sec_search_research_acceptance import check_runtime
FN='justhodl-catalyst-clusters'


def check(before,baseline,commit):
    check_runtime(before,baseline,commit,2)
    return {'status':'exact_package_and_original_schedule_verified',
            'synthetic_behaviour_verified':True, 'native_output_verified':False,
            'consumer_qualification':False,'investment_authority':False}


def main():
    subprocess.run([sys.executable,str(ROOT/'aws/lambdas'/FN/'tests/run_tests.py')],cwd=ROOT,check=True)
    baseline=json.loads((ROOT/'docs/audit/2026-09-28/momentum-leaders-original-baseline.json').read_bytes())['actual_producers'][FN]
    expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/'+FN],cwd=ROOT,text=True).strip()
    args=[boto3.client(name,region_name='us-east-1') for name in ('lambda','s3','events','scheduler')]
    with report('ops_6268_cluster_abstention_acceptance') as r:
        before=runtime(*args,FN);result=check(before,baseline,expected)
        if runtime(*args,FN)!=before:raise ValueError('Package or schedule changed during acceptance')
        r.kv(expected_commit=expected,actual_package=before,acceptance=result,
             native_invocations=0,provider_requests=0,consumer_output_reads=0,account_reads=0,
             private_state_reads=0,learning_log_reads=0,public_writes=0,schedule_changes=0,
             scope='Exact complete package, original runtime/schedule and synthetic regression only. No native output or portfolio qualification.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
