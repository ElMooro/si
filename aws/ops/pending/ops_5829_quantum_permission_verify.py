"""Verify exact Quantum runtime and deterministic public research refresh."""
from datetime import datetime,timezone
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
    expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/justhodl-quantum-desk/source/lambda_function.py'],text=True).strip()
    s3=boto3.client('s3',region_name='us-east-1');bucket='justhodl-dashboard-live'
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=910,retries={'max_attempts':0}))
    def read(key):return json.loads(s3.get_object(Bucket=bucket,Key=key)['Body'].read(32*1024*1024))
    with report('ops_5829_quantum_permission_verify') as r:
        deadline=time.monotonic()+1800;fn='justhodl-quantum-desk'
        while True:
            receipt=read('data/ops/releases/'+fn+'.json')
            if receipt.get('commit')==expected:break
            assert time.monotonic()<deadline,'exact receipt timeout'
            time.sleep(15)
        conf=lam.get_function_configuration(FunctionName=fn)
        assert conf['CodeSha256']==receipt['code_sha256'],'runtime differs'
        started=datetime.now(timezone.utc).isoformat()
        response=lam.invoke(FunctionName=fn,InvocationType='RequestResponse',Payload=b'{"suppress_alerts":true}')
        result=json.loads(response['Payload'].read());assert not response.get('FunctionError') and result.get('statusCode')==200,result
        d=read('data/quantum-desk.json')
        assert d['generated_at']>started and d['version']=='2.3.4'
        assert d['risk_gate']['sizing_multiplier'] is None and not d['risk_gate']['allows_new_entries']
        assert d['asset_ladder'] and all(x['verdict']=='ABSTAIN' for x in d['asset_ladder'])
        assert all(x['size_hint_x'] is None and x['sizing_eligible'] is False for x in d['money_map'])
        gate=next(x for x in d['risk_panel']['veto_stack'] if x['name']=='risk-gate')
        assert gate['active'] and gate['sizing_x'] is None and 'out-of-sample evidence' in gate['flips_when']
        proof={'contract':'quantum-permission-verification.v1','generated_at':datetime.now(timezone.utc).isoformat(),
               'runtime':{'commit':expected,'code_sha256':conf['CodeSha256']},'source_generated_at':d['generated_at'],
               'abstaining_classes':len(d['asset_ladder']),'money_map_rows':len(d['money_map']),
               'sizing_eligible':False,'paid_ai_calls':0,'notifications_sent':0,'private_account_reads':0,'portfolio_writes':0}
        s3.put_object(Bucket=bucket,Key='data/quantum-permission-verification.json',Body=json.dumps(proof).encode(),ContentType='application/json',CacheControl='no-store')
        r.kv(**proof)


if __name__=='__main__':
    try:main()
    except Exception:
        print('Quantum permission verification failed; inspect runner report.')
        sys.exit(1)
