"""Prove the primary-dealer source, refresh it and reconcile canonical fails."""
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
FN = 'justhodl-nyfed-pd'
COMMIT = '91000c63572375f7b3e1cb36a896a9d3fb7f52e5'


def main():
    s3 = boto3.client('s3', region_name='us-east-1')
    lam = boto3.client('lambda', region_name='us-east-1', config=Config(read_timeout=650, retries={'max_attempts':0}))
    def read(key):
        return json.loads(s3.get_object(Bucket=BUCKET,Key=key)['Body'].read())
    with report('ops_5632_primary_dealers_verify') as r:
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
        obj=s3.get_object(Bucket=BUCKET,Key='data/nyfed-primary-dealer.json')
        doc=json.loads(obj['Body'].read())
        assert obj['LastModified']>=started, 'No new publication'
        assert doc['methodology_version']=='fr2004-measurement.v2'
        assert doc['net_treasury_total_b'] is None and doc['call'] is None
        assert doc['corporate']['turnover_velocity'] is None
        assert doc['settlement_fails']['treasury']==read('data/settlement-fails.json')['treasury'], 'Fails scope differs between desks'
        assert doc['corporate']['quality']['status']=='fresh', 'Corporate data not current'
        assert all(row['weekly_b'] is None and row['daily_average_b'] is not None for row in doc['transactions'].values())
        r.kv(function=FN,commit=COMMIT,code_sha256=receipt['code_sha256'],generated_at=doc['generated_at'],
             observation_date=doc['as_of'],corporate_net_bonds_b=doc['corporate']['net_bonds_b'],
             quality=doc['quality'],fails=doc['settlement_fails']['treasury']['gross_bn'],financing_quality=(doc.get('financing') or {}).get('quality'))
        r.ok('AWS source verified, new FR2004 publication and canonical fails equality proven')
    return 0


if __name__=='__main__':
    sys.exit(main())
