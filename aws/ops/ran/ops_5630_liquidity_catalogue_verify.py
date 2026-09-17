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

COMMIT = 'fecbd472d6c1588e04c4230f45c2ac505a2105c2'
BUCKET = 'justhodl-dashboard-live'
TARGETS = [('justhodl-liquidity-agent','liquidity-data.json')]


def main():
    s3=boto3.client('s3',region_name='us-east-1')
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=220,connect_timeout=10,retries={'max_attempts':0}))
    with report('ops_5630_liquidity_catalogue_verify') as r:
        r.heading('Final liquidity catalogue and source release proof')
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
                assert feed['meta']['agent_version']=='2.1.3'
                assert feed['spy_signal']['direction'] is None and feed['spy_signal']['confidence'] is None
                assert feed['regime']['history_status']=='available', '13 calendar weeks of aligned history unavailable'
                assert feed['regime']['history_observations']>=14
                assert feed['money_supply']['monetary_base']>1000, 'Monetary-base unit check failed'
                assert feed['catalog']['credit']['BAMLC0A0CM']['unit']=='%'
                assert feed['part4']['dxy']['level']==feed['catalog']['dollar']['DTWEXBGS']['value']
                reserves=feed['catalog']['reserves']['TRESEGUSM052N']
                assert 1 < reserves['value'] < 1000 and 'Excluding Gold' in reserves['label']
                assert feed['catalog']['money_supply']['WCURCIR']['value']>1000
                assert 'CURRCIR' not in feed['catalog']['money_supply']
                assert feed['reserves']['excess_bn'] is None
                r.kv(international_reserves_ex_gold_usd_bn=reserves['value'], currency_usd_bn=feed['catalog']['money_supply']['WCURCIR']['value'])
                r.kv(history_observations=feed['regime']['history_observations'],regime=feed['regime']['trend'],
                     monetary_base_usd_bn=feed['money_supply']['monetary_base'],rrp_4w_change_bn=feed['components']['rrp_analysis'].get('4w_change_bn'))
            r.ok('Exact source release, refreshed data and measurement contract verified')
    return 0


if __name__=='__main__':
    try:sys.exit(main())
    except Exception as exc:
        print('Refresh failed:',type(exc).__name__,str(exc)[:250])
        sys.exit(1)
