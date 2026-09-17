"""Refresh two repaired engines and prove current measurement outputs on the runner."""
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report

COMMIT = '21e4e262ff991ad4127e4afbad9d239516905ead'
BUCKET = 'justhodl-dashboard-live'
TARGETS = [('justhodl-liquidity-agent','liquidity-data.json'),
           ('justhodl-euro-fragmentation','data/euro-fragmentation.json')]


def main():
    s3=boto3.client('s3',region_name='us-east-1')
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=220,connect_timeout=10,retries={'max_attempts':0}))
    with report('ops_5628_liquidity_fragmentation_verify') as r:
        r.heading('Liquidity history and fragmentation cache release proof')
        for fn,key in TARGETS:
            deadline=time.monotonic()+12*60
            while True:
                receipt=json.loads(s3.get_object(Bucket=BUCKET,Key=f'data/ops/releases/{fn}.json')['Body'].read())
                if receipt.get('commit')==COMMIT:break
                if time.monotonic()>deadline:raise RuntimeError('Expected source receipt did not arrive for '+fn)
                time.sleep(15)
            config=lam.get_function_configuration(FunctionName=fn)
            assert config['CodeSha256']==receipt['code_sha256'], 'AWS code differs from release receipt'
            started=datetime.now(timezone.utc)
            response=lam.invoke(FunctionName=fn,InvocationType='RequestResponse',Payload=b'{"suppress_alerts":true}')
            body=json.loads(response['Payload'].read() or b'{}')
            assert not response.get('FunctionError') and body.get('statusCode')==200, 'Invocation failed: '+fn
            obj=s3.get_object(Bucket=BUCKET,Key=key);feed=json.loads(obj['Body'].read())
            assert obj['LastModified']>=started, 'Refresh did not publish a new object'
            q=feed.get('quality') or {}
            r.kv(function=fn,commit=COMMIT,code_sha256=receipt['code_sha256'],generated_at=feed.get('generated_at'),
                 quality_status=q.get('status'),missing=q.get('missing'),source_bytes=receipt.get('source',{}).get('lambda_function.py',{}).get('bytes'))
            assert q.get('status')=='fresh', 'Required observations unavailable: '+str(q.get('missing'))
            if fn=='justhodl-liquidity-agent':
                assert feed['meta']['agent_version']=='2.1.2'
                assert feed['spy_signal']['direction'] is None and feed['spy_signal']['confidence'] is None
                assert feed['regime']['history_status']=='available', '13 calendar weeks of aligned history unavailable'
                assert feed['regime']['history_observations']>=14
                assert feed['money_supply']['monetary_base']>1000, 'Monetary-base unit check failed'
                assert feed['catalog']['credit']['BAMLC0A0CM']['unit']=='%'
                assert feed['part4']['dxy']['level']==feed['catalog']['dollar']['DTWEXBGS']['value']
                r.kv(history_observations=feed['regime']['history_observations'],regime=feed['regime']['trend'],
                     monetary_base_usd_bn=feed['money_supply']['monetary_base'],rrp_4w_change_bn=feed['components']['rrp_analysis'].get('4w_change_bn'))
            else:
                assert feed.get('ok') is True and feed['fragmentation']['score_0_100'] is not None
                assert feed['countries']['DE']['yield_10y_pct'] is not None
                r.kv(bund_yield_pct=feed['countries']['DE']['yield_10y_pct'],yield_as_of=feed['countries']['DE']['yield_as_of'],
                     fragmentation_score=feed['fragmentation']['score_0_100'])
            r.ok('Exact source release, refreshed data and measurement contract verified')
    return 0


if __name__=='__main__':
    try:sys.exit(main())
    except Exception as exc:
        print('Refresh failed:',type(exc).__name__,str(exc)[:250])
        sys.exit(1)
