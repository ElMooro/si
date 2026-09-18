"""Exercise real provider parsing/math with an isolated synthetic SPY holding."""
import json
from pathlib import Path
import subprocess
import sys
import time
import boto3
from botocore.config import Config
ROOT=Path(__file__).resolve().parents[3]
sys.path.insert(0,str(ROOT/'aws/ops'))
from ops_report import report


def main():
    s3=boto3.client('s3',region_name='us-east-1')
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=340,retries={'max_attempts':0}))
    fn='justhodl-portfolio-risk'; bucket='justhodl-dashboard-live'
    expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/'+fn+'/source/lambda_function.py'],text=True).strip()
    with report('ops_5718_risk_market_adapter_verify') as r:
        deadline=time.monotonic()+1200
        while True:
            receipt=json.loads(s3.get_object(Bucket=bucket,Key='data/ops/releases/'+fn+'.json')['Body'].read())
            if receipt['commit']==expected: break
            assert time.monotonic()<deadline,'exact receipt unavailable'
            time.sleep(15)
        assert lam.get_function_configuration(FunctionName=fn)['CodeSha256']==receipt['code_sha256']
        result=lam.invoke(FunctionName=fn,Payload=b'{"validation_only":true,"suppress_alerts":true}')
        response=json.loads(result['Payload'].read())
        assert not result.get('FunctionError') and response.get('statusCode')==200,'market probe failed'
        proof=json.loads(response['body'])
        assert proof['validation_only'] is True and proof['ok'] is True and proof['writes']==0 and proof['private_account_reads']==0
        assert proof['sample_count']>=60 and proof['replay']=='reproduced' and proof['sizing_eligible'] is False
        r.kv(commit=expected,source=proof['source'],sample_count=proof['sample_count'],as_of=proof['as_of'],
             original_response_sha256=proof['original_response_sha256'],replay=proof['replay'],
             account_reads=0,artifact_writes=0,fixture='synthetic SPY holding')
        r.ok('Exact release and real provider response validated; synthetic risk reproduced without account reads or writes')


if __name__=='__main__':
    try: main()
    except Exception: sys.exit(1)
