"""Read-only source proof; normal scheduled outputs are a separate acceptance."""
from pathlib import Path
import json,subprocess,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks')]
from ops_report import report
from market_runtime_evidence import runtime,bounded
BUCKET='justhodl-dashboard-live'


def main():
    lam,s3,events,scheduler=(boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler'))
    with report('ops_6158_sentinel_context_release_verify') as r:
        rows=[]
        for function,key in (('justhodl-us10y-sentinel','data/us10y-sentinel.json'),
                             ('justhodl-master-allocator','data/master-allocation.json')):
            subprocess.run([sys.executable,str(ROOT/'aws/lambdas'/function/'tests/run_tests.py')],cwd=ROOT,check=True)
            expected=subprocess.check_output(['git','log','-1','--format=%H','--',
                'aws/lambdas/'+function+'/source/lambda_function.py'],cwd=ROOT,text=True).strip()
            before=runtime(lam,s3,events,scheduler,function)
            if len(expected)!=40 or before['receipt']!={'status':'matched','commit':expected}:
                raise ValueError('Exact intended release receipt required')
            packet=json.loads(bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body']))
            after=runtime(lam,s3,events,scheduler,function)
            if before!=after:raise ValueError('Native package or schedule changed during verification')
            rows.append({'function':function,'expected_commit':expected,'runtime':before,
                'current_publication_clock':packet.get('generated_at',packet.get('as_of')),
                'current_schema':packet.get('schema'),'new_output_verified':False})
        r.kv(packages=rows,code_and_receipt_verified=True,producer_invocations=0,consumer_invocations=0,
            provider_requests=0,public_writes=0,private_account_reads=0,notifications_sent=0,schedules_changed=0,
            scope='Exact deployed source proof only; normal new-code outputs and original-source Sentinel replay remain separate.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
