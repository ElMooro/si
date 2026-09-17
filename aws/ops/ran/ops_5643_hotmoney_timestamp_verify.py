"""Verify and invoke the corrected hot-money publication timestamp release."""
import hashlib
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
import boto3
from botocore.config import Config
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report

FN='justhodl-hot-money'
COMMIT='417b5067b6f8650c655ffb50ad7f848a51725419'
BUCKET='justhodl-dashboard-live'


def main():
    s3=boto3.client('s3',region_name='us-east-1')
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=650,retries={'max_attempts':0}))
    with report('ops_5643_hotmoney_timestamp_verify') as r:
        deadline=time.monotonic()+600
        while True:
            receipt=json.loads(s3.get_object(Bucket=BUCKET,Key=f'data/ops/releases/{FN}.json')['Body'].read())
            if receipt.get('commit')==COMMIT:break
            if time.monotonic()>deadline:raise RuntimeError('Pinned hot-money receipt absent')
            time.sleep(15)
        assert lam.get_function_configuration(FunctionName=FN)['CodeSha256']==receipt['code_sha256']
        for name,meta in receipt['source'].items():
            raw=subprocess.check_output(['git','show',f'{COMMIT}:aws/lambdas/{FN}/source/{name}'])
            assert len(raw)==meta['bytes'] and hashlib.sha256(raw).hexdigest()==meta['sha256']
        started=datetime.now(timezone.utc)
        result=lam.invoke(FunctionName=FN,InvocationType='RequestResponse',Payload=b'{"suppress_alerts":true}')
        body=json.loads(result['Payload'].read())
        assert not result.get('FunctionError') and body.get('ok')
        obj=s3.get_object(Bucket=BUCKET,Key='data/hot-money.json');doc=json.loads(obj['Body'].read())
        assert obj['LastModified']>=started
        assert doc['methodology_version']=='exchange-observations.v2'
        stamp=datetime.fromisoformat(doc['quality']['publication_date'])
        assert stamp.tzinfo and stamp>=started
        assert doc['quality']['publication_date']==doc['generated_at']
        assert len(doc['quality']['observation_date'])==10 and doc['quality']['status']=='fresh'
        r.kv(function=FN,commit=COMMIT,code_sha256=receipt['code_sha256'],quality=doc['quality'],generated_at=doc['generated_at'])
        r.ok('Exact release and fresh timezone-aware publication verified; exchange observation date retained separately')
    return 0


if __name__=='__main__':sys.exit(main())
