"""Prove ECB unit conversion and corrected exchange-ledger quality on AWS."""
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
TARGETS=[('justhodl-ecb-history','5457c02bf51d5331cb9033b7affd4595dcf1479f','data/ecb-hist/_manifest.json'),
         ('justhodl-hot-money','f9df3ac219c267ff2cb650ffc6fa3d1bca73c04b','data/hot-money.json')]


def main():
    s3=boto3.client('s3',region_name='us-east-1')
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=650,retries={'max_attempts':0}))
    def read(key):return json.loads(s3.get_object(Bucket=BUCKET,Key=key)['Body'].read())
    with report('ops_5638_ecb_hotmoney_verify') as r:
        for fn,commit,key in TARGETS:
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
            assert not result.get('FunctionError'),'Lambda invocation failed'
            obj=s3.get_object(Bucket=BUCKET,Key=key);doc=json.loads(obj['Body'].read())
            assert obj['LastModified']>=started
            if fn.endswith('ecb-history'):
                assert body['statusCode']==200 and doc['methodology_version']=='ecb-dated-units.v2'
                rows={x['id']:x for x in doc['series']}
                for sid in ['ilm_mp_lending','ilm_usd_claims','total_assets']:
                    row=rows[sid]
                    assert row['unit']=='EUR_bn' and row['source_metadata']['source_unit_multipliers']==[6]
                    assert row['quality']['status']=='fresh'
                    r.kv(series=sid,value=row['latest'],unit=row['unit'],date=row['latest_date'],quality=row['quality'])
                assert rows['ilm_mp_lending']['latest']<rows['total_assets']['latest']
                assert all(row.get('latest') is None for row in rows.values() if row.get('quality',{}).get('status')!='fresh')
                r.kv(series_count=doc['n'],fresh_series=doc['quality']['fresh_series'],errors=doc['errors'])
            else:
                assert doc['methodology_version']=='exchange-observations.v2'
                tw=doc['countries']['taiwan']
                assert tw['status']=='LIVE' and tw['sum_5d_bn'] is None and tw['z_60d'] is None
                assert 'korea' not in doc['countries']
                r.kv(twse_latest=tw['latest_bn'],otc_latest=tw['otc']['latest_bn'],combined=tw['combined'],windows=tw['windows'])
            r.kv(function=fn,commit=commit,code_sha256=receipt['code_sha256'],generated_at=doc['generated_at'],quality=doc['quality'])
            r.ok(fn+': exact source, AWS code and corrected public data verified')
    return 0


if __name__=='__main__':sys.exit(main())
