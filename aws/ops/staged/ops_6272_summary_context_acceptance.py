"""Read-only two-package acceptance; no consumer outputs, private context or invocations."""
from pathlib import Path
import json,subprocess,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/ops/staged','aws/shared')]
from ops_report import report
from market_runtime_evidence import runtime
from release_package_evidence import shared_imports
from ops_6232_sec_search_research_acceptance import check_runtime
FUNCTIONS=('justhodl-prepump-summary','justhodl-pump-radar-brief')


def expected_commit(function):
    source=ROOT/'aws/lambdas'/function/'source';paths=list(source.glob('*.py'))
    paths.extend(shared_imports(ROOT,paths));paths.append(source.parent/'config.json')
    return subprocess.check_output(['git','log','-1','--format=%H','--',*[p.relative_to(ROOT).as_posix() for p in sorted(set(paths))]],cwd=ROOT,text=True).strip()


def check(before,baseline,commit):
    check_runtime(before,baseline,commit,3)
    return {'status':'exact_package_and_original_schedule_verified','native_output_verified':False,
            'consumer_decisions_qualified':False,'investment_authority':False}


def main():
    clients=[boto3.client(name,region_name='us-east-1') for name in ('lambda','s3','events','scheduler')]
    summary=json.loads((ROOT/'docs/audit/2026-09-28/prepump-summary-original-baseline.json').read_bytes())['actual_consumer']
    brief=json.loads((ROOT/'docs/audit/2026-09-28/momentum-leaders-original-baseline.json').read_bytes())['actual_producers']['justhodl-pump-radar-brief']
    with report('ops_6272_summary_context_acceptance') as r:
        results={}
        for function,baseline in zip(FUNCTIONS,(summary,brief)):
            subprocess.run([sys.executable,str(ROOT/'aws/lambdas'/function/'tests/run_tests.py')],cwd=ROOT,check=True)
            commit=expected_commit(function);before=runtime(*clients,function);result=check(before,baseline,commit)
            if runtime(*clients,function)!=before:raise ValueError('Runtime changed during acceptance')
            results[function]={'expected_commit':commit,'actual_package':before,'acceptance':result}
        r.kv(packages=results,native_invocations=0,provider_requests=0,model_requests=0,consumer_output_reads=0,
             private_context_reads=0,account_reads=0,learning_log_reads=0,schedule_changes=0,public_writes=0,
             scope='Complete Summary and Brief packages, original runtime/schedules and synthetic replay only. No native output or portfolio qualification.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
