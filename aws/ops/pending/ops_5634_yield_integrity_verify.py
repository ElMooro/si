"""Verify and refresh daily yield measurements and quarantined equity studies."""
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
import boto3
from botocore.config import Config
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from ops_report import report
BUCKET='justhodl-dashboard-live'
TARGETS=(('justhodl-us10y-sentinel','5d0c09d4de5aaee9895aed269ce0004f5202522d','data/us10y-sentinel.json'),
         ('justhodl-yield-curve','7e33d1b52a6521e4596b8fd7f9ede0debc6b5d12','data/yield-curve.json'))


def main():
    s3=boto3.client('s3',region_name='us-east-1')
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=650,retries={'max_attempts':0}))
    def read(key):return json.loads(s3.get_object(Bucket=BUCKET,Key=key)['Body'].read())
    with report('ops_5634_yield_integrity_verify') as r:
        for fn,commit,key in TARGETS:
            deadline=time.monotonic()+720
            while True:
                receipt=read(f'data/ops/releases/{fn}.json')
                if receipt.get('commit')==commit:break
                if time.monotonic()>deadline:raise RuntimeError('Pinned release receipt did not arrive')
                time.sleep(15)
            cfg=lam.get_function_configuration(FunctionName=fn)
            assert cfg['CodeSha256']==receipt['code_sha256']
            started=datetime.now(timezone.utc)
            response=lam.invoke(FunctionName=fn,InvocationType='RequestResponse',Payload=b'{"suppress_alerts":true}')
            body=json.loads(response['Payload'].read())
            assert not response.get('FunctionError') and body.get('statusCode')==200
            obj=s3.get_object(Bucket=BUCKET,Key=key);doc=json.loads(obj['Body'].read())
            assert obj['LastModified']>=started
            assert doc['quality']['status']=='fresh', 'Current yield observations unavailable'
            assert doc['call'] is None and doc['execution_eligible'] is False
            if fn.endswith('sentinel'):
                assert doc['methodology_version']=='daily-price-episodes.v2'
                assert doc['yields_driving_stocks'] is None
                for study in doc['episode_study'].values():
                    assert study['return_basis']=='price-only; excludes dividends'
                    assert study['n_valid_3m']>=3 or study['median_spx_3m'] is None
                    assert study['median_spx_3m'] is None or abs(study['median_spx_3m'])<=100
                r.kv(valid_episode_counts={k:v['n_valid_3m'] for k,v in doc['episode_study'].items()},level=doc['level'])
            else:
                assert doc['methodology_version']=='dated-curve.v2'
                assert len(doc['curve_points'])==11 and len({p['date'] for p in doc['curve_points']})==1
                assert doc['fed_context']['FED_FUNDS_RATE_EFFECTIVE']['series_id']=='DFF'
                assert doc['term_premium_source']!='proxy'
                r.kv(curve_date=doc['as_of_date'],regime=doc['regime'],term_premium_source=doc['term_premium_source'])
            r.kv(function=fn,commit=commit,code_sha256=receipt['code_sha256'],generated_at=doc['generated_at'],quality=doc['quality'])
            r.ok('Exact source and new measurement publication verified on AWS')
    return 0


if __name__=='__main__':sys.exit(main())
