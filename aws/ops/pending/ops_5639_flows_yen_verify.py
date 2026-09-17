"""Refresh producer before radar, then verify long-history yen measurements."""
import hashlib
import json
import subprocess
import sys
import time
from datetime import datetime,timezone
from pathlib import Path
import boto3
from botocore.config import Config
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from ops_report import report
BUCKET='justhodl-dashboard-live'
TARGETS=[('justhodl-etf-fund-flows','b68d0cb4cc972bee62a59378b9132edb022d4a04','etf-flows/measurements.json','etf-dated-flows.v2'),
         ('justhodl-capital-flow-radar','12907314717f4dc7bee82c1a058a15d865bb5ada','data/capital-flow-radar.json','dated-etf-radar.v2'),
         ('justhodl-yen-carry','60b4695be9ac6f35e8291b4151226442b03a139e','data/yen-carry.json','yen-dated-measurements.v2')]


def main():
    s3=boto3.client('s3',region_name='us-east-1')
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=650,retries={'max_attempts':0}))
    def read(key):return json.loads(s3.get_object(Bucket=BUCKET,Key=key)['Body'].read())
    with report('ops_5639_flows_yen_verify') as r:
        for fn,commit,key,method in TARGETS:
            end=time.monotonic()+600
            while True:
                try:receipt=read(f'data/ops/releases/{fn}.json')
                except s3.exceptions.NoSuchKey:receipt={}
                if receipt.get('commit')==commit:break
                if time.monotonic()>end:raise RuntimeError('Pinned receipt absent: '+fn)
                time.sleep(15)
            assert lam.get_function_configuration(FunctionName=fn)['CodeSha256']==receipt['code_sha256']
            for name,meta in receipt['source'].items():
                raw=subprocess.check_output(['git','show',f'{commit}:aws/lambdas/{fn}/source/{name}'])
                assert hashlib.sha256(raw).hexdigest()==meta['sha256']
            started=datetime.now(timezone.utc)
            result=lam.invoke(FunctionName=fn,InvocationType='RequestResponse',Payload=b'{"suppress_alerts":true}')
            body=json.loads(result['Payload'].read())
            assert not result.get('FunctionError') and body.get('statusCode')==200,'Lambda invocation failed'
            obj=s3.get_object(Bucket=BUCKET,Key=key);doc=json.loads(obj['Body'].read())
            assert obj['LastModified']>=started and doc['methodology_version']==method
            assert doc['call'] is None and doc['execution_eligible'] is False
            if fn=='justhodl-etf-fund-flows':
                fresh=[m for m in doc['metrics'] if m['quality']['status']=='fresh']
                assert fresh and all(m['date_basis']=='effective_date' for m in fresh)
                assert receipt['source']['lambda_function.py']['bytes']>90000
                r.kv(large_source_bytes=receipt['source']['lambda_function.py']['bytes'],fresh_funds=len(fresh),total_funds=len(doc['metrics']),sample=fresh[0])
            elif fn=='justhodl-capital-flow-radar':
                assert all(c['pump_probability'] is None and c['net_flow_5d_usd'] is None for c in doc['complexes'])
                assert doc['pump_setups']==[] and doc['party_over_alerts']==[]
                assert doc['quality']['complete_complexes']>0,'No complete matched complexes'
                r.kv(complete_complexes=doc['quality']['complete_complexes'],total_complexes=doc['n_complexes'])
            else:
                assert doc['unwind_risk_score'] is None and doc['boj_injection_score'] is None
                assert doc['positioning']['history_weeks']>=156,'Long CFTC history unavailable'
                assert doc['positioning']['quality']['status']=='fresh'
                assert doc['positioning']['whole_carry_trade_size'] is None
                assert doc['observations']['usdjpy']['quality']['status']=='fresh'
                r.kv(cftc_reports=doc['positioning']['history_weeks'],baseline_weeks=doc['positioning']['baseline_weeks'],cftc_current=doc['positioning']['current'],funding_proxy=doc['carry_width']['front_end_proxy'])
            r.kv(function=fn,commit=commit,code_sha256=receipt['code_sha256'],generated_at=doc['generated_at'],quality=doc['quality'])
            r.ok(fn+': source bytes, AWS code and new measurements verified')
    return 0


if __name__=='__main__':sys.exit(main())
