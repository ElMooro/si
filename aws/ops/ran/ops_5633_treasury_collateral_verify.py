"""Prove the corrected collateral engine and canonical fails on AWS."""
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
import boto3
from botocore.config import Config
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report

BUCKET = 'justhodl-dashboard-live'
FN = 'justhodl-treasury-rehypo'
COMMIT = '96eec553e5d23ee026faedb0ce4634934acb0c1c'


def main():
    s3 = boto3.client('s3', region_name='us-east-1')
    lam = boto3.client('lambda', region_name='us-east-1', config=Config(read_timeout=650, retries={'max_attempts':0}))
    def read(key):
        return json.loads(s3.get_object(Bucket=BUCKET,Key=key)['Body'].read())
    with report('ops_5633_treasury_collateral_verify') as r:
        deadline=time.monotonic()+720
        while True:
            receipt=read(f'data/ops/releases/{FN}.json')
            if receipt.get('commit')==COMMIT: break
            if time.monotonic()>deadline: raise RuntimeError('Pinned release receipt did not arrive')
            time.sleep(15)
        cfg=lam.get_function_configuration(FunctionName=FN)
        assert cfg['CodeSha256']==receipt['code_sha256'], 'AWS code differs from receipt'
        started=datetime.now(timezone.utc)
        response=lam.invoke(FunctionName=FN,InvocationType='RequestResponse',Payload=b'{"suppress_alerts":true}')
        body=json.loads(response['Payload'].read())
        assert not response.get('FunctionError') and body.get('ok'), 'Primary dealer invoke failed'
        obj=s3.get_object(Bucket=BUCKET,Key='data/treasury-rehypo.json')
        doc=json.loads(obj['Body'].read())
        assert obj['LastModified']>=started, 'No new publication'
        assert doc['methodology_version']=='collateral-measurement.v2'
        assert doc['legs']['velocity']['latest'] is None
        assert doc['legs']['specialness']['latest_bps'] is None
        assert doc['legs']['rrp_drain_4w']['score_contribution']==0
        assert doc['treasury_fails']==read('data/settlement-fails.json')['treasury']
        assert doc['quality']['status']=='fresh', 'Required collateral inputs unavailable'
        assert doc['call'] is None and doc['execution_eligible'] is False
        history=read('data/treasury-rehypo-long.json')
        assert history['methodology_version']==doc['methodology_version']
        assert all(set(row['z'])=={'fails','sofr_iorb'} for row in history['weekly'])
        r.kv(function=FN,commit=COMMIT,code_sha256=receipt['code_sha256'],generated_at=doc['generated_at'],
             quality=doc['quality'],fails_gross_bn=doc['treasury_fails']['gross_bn'],
             composite=doc['composite'],history_start=history['actual_start'],history_weeks=history['n_weekly'])
        r.ok('Exact AWS release and canonical fails proven; invalid velocity/specialness removed; old history excluded')
    return 0


if __name__=='__main__':
    sys.exit(main())
