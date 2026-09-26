"""Read-only proof of every deployed allocator interpretation boundary."""
from pathlib import Path
import subprocess,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks')]
from ops_report import report
from market_runtime_evidence import runtime
FUNCTIONS=('justhodl-master-allocator','justhodl-quantum-desk','justhodl-regime-conditional-router','justhodl-streaming-fanout')


def main():
    lam,s3,events,scheduler=(boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler'))
    with report('ops_6159_master_allocation_boundary_verify') as r:
        subprocess.run([sys.executable,str(ROOT/'tests/test_master_allocation_authority.py')],cwd=ROOT,check=True)
        rows=[]
        for function in FUNCTIONS:
            expected=subprocess.check_output(['git','log','-1','--format=%H','--',
                'aws/lambdas/'+function+'/source/lambda_function.py'],cwd=ROOT,text=True).strip()
            before=runtime(lam,s3,events,scheduler,function)
            if len(expected)!=40 or before['receipt']!={'status':'matched','commit':expected}:
                raise ValueError('Exact intended package and receipt required')
            after=runtime(lam,s3,events,scheduler,function)
            if before!=after:raise ValueError('Package or timing changed during verification')
            rows.append({'function':function,'expected_commit':expected,'runtime':before})
        r.kv(packages=rows,code_and_receipt_verified=True,producer_invocations=0,consumer_invocations=0,
            provider_requests=0,public_writes=0,private_account_reads=0,notifications_sent=0,schedules_changed=0,
            new_normal_publication_verified=False,live_machine_target_verified=False,
            scope='Exact deployed interpretation boundaries only; normal public and machine-readable allocator output remains separate acceptance.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
