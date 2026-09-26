"""Read-only package proof and strict complete replay of the existing Risk Gate head."""
from pathlib import Path
import json,subprocess,sys
import boto3

ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/shared')]
from ops_report import report
from market_runtime_evidence import runtime

FUNCTION='justhodl-risk-gate'
EXPECTED='16b164d2ab27cdf31dfdbd586e9c79114e3ff456'


def main():
    lam,s3,events,scheduler=(boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler'))
    with report('ops_6157_risk_gate_public_replay_verify') as r:
        for path in ('tests/test_risk_gate_public_replay.py','aws/lambdas/justhodl-risk-gate/tests/run_tests.py',
                     'aws/lambdas/justhodl-risk-gate/tests/test_risk_gate_research_store.py'):
            subprocess.run([sys.executable,str(ROOT/path)],cwd=ROOT,check=True)
        before=runtime(lam,s3,events,scheduler,FUNCTION)
        if before['receipt']!={'status':'matched','commit':EXPECTED}:
            raise ValueError('Exact intended native release required')
        proof=json.loads(subprocess.check_output([sys.executable,str(ROOT/'scripts/replay_risk_gate_research.py')],
            cwd=ROOT,text=True,timeout=1800))
        if proof.get('replayed') is not True or proof.get('calls_eligible') is not False or proof.get('sizing_eligible') is not False:
            raise ValueError('Complete typed original-source research-only replay required')
        after=runtime(lam,s3,events,scheduler,FUNCTION)
        if before!=after:raise ValueError('Runtime changed during verification')
        r.kv(expected_commit=EXPECTED,actual_runtime=before,public_original_replay=proof,
            producer_invocations=0,consumer_invocations=0,provider_requests=0,private_account_reads=0,
            public_writes=0,notifications_sent=0,schedules_changed=0,
            scope='Existing native public output and retained original FRED/ECB evidence; no new engine deployment, acquisition, forecast or sizing qualification.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
