"""Refresh the bounded BLS collector, retain history and replay current public CPI."""
import gzip
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / 'aws/ops'), str(ROOT / 'aws/shared')]
from ops_report import report
from evidence_store import read_verified
BUCKET = 'justhodl-dashboard-live'


def main():
    s3 = boto3.client('s3', region_name='us-east-1')
    lam = boto3.client('lambda', region_name='us-east-1', config=Config(read_timeout=920, retries={'max_attempts': 0}))
    expected = subprocess.check_output(['git', 'log', '-1', '--format=%H', '--',
        'aws/lambdas/justhodl-usgov-direct/source/lambda_function.py'], text=True).strip()
    def read(key):
        raw = s3.get_object(Bucket=BUCKET, Key=key)['Body'].read()
        return json.loads(gzip.decompress(raw) if key.endswith('.gz') else raw)
    def invoke(fn, event):
        result = lam.invoke(FunctionName=fn, InvocationType='RequestResponse', Payload=json.dumps(event).encode())
        payload = json.loads(result['Payload'].read())
        assert not result.get('FunctionError') and payload.get('statusCode', 200) < 400, fn+' failed'
        return json.loads(payload.get('body', '{}'))
    with report('ops_5715_bls_current_vintage_verify') as r:
        deadline = time.monotonic()+1500
        for fn in ('justhodl-usgov-direct', 'justhodl-warm-bridge'):
            while True:
                try: receipt = read('data/ops/releases/'+fn+'.json')
                except ClientError as exc:
                    if exc.response['Error']['Code'] not in ('NoSuchKey', '404'): raise
                    receipt = {}
                if receipt.get('commit') == expected: break
                assert time.monotonic() < deadline, fn+' exact receipt not available'
                time.sleep(15)
            assert lam.get_function_configuration(FunctionName=fn)['CodeSha256'] == receipt['code_sha256']
            r.log(fn+' exact receipt and runtime hash: '+expected)
        started = datetime.now(timezone.utc).replace(microsecond=0)
        key = 'data/warm/usgov/bls/CUUR0000SA0.json.gz'
        previous = read(key)
        old_periods = {(row['year'], row['period']) for row in previous['data']}
        response = invoke('justhodl-usgov-direct', {'feed': 'bls'})
        statuses = response['bls'].get('results') or {}
        assert statuses.get('CUUR0000SA0', {}).get('status') == 'updated', 'CPI collector did not update'
        current = read(key)
        assert old_periods <= {(row['year'], row['period']) for row in current['data']}, 'historical periods were lost'
        raw = json.loads(read_verified(s3, BUCKET, current['evidence']))
        source = next(row for row in raw['Results']['series'] if row['seriesID'] == 'CUUR0000SA0')
        latest = max((row for row in source['data'] if row['period'] in {'M%02d' % m for m in range(1,13)}), key=lambda row: (row['year'], row['period']))
        assert (int(latest['year']), int(latest['period'][1:])) >= (2026, 8), 'CPI current monthly observation still absent'
        previous_bytes = json.loads(read_verified(s3, BUCKET, current['prior_warehouse_evidence']))
        assert previous_bytes == previous, 'prior warehouse vintage not retained'
        invoke('justhodl-warm-bridge', {})
        hot = read('data/bls-macro.json')
        assert datetime.fromisoformat(hot['generated_at']) >= started
        cpi = hot['cpi_headline']
        assert cpi['value'] == float(latest['value']) and cpi['as_of'] == latest['year']+'-'+latest['period']
        assert cpi['freshness']['status'] == 'fresh' and cpi['provider_evidence'] == current['evidence']
        read_verified(s3, BUCKET, cpi['provider_evidence'])
        r.kv(commit=expected, generated_at=hot['generated_at'], cpi_value=cpi['value'], cpi_period=cpi['as_of'],
             cpi_unit=cpi['unit'], observation_age_days=cpi['freshness']['age_days'],
             old_periods_retained=len(old_periods), total_observations=current['n_obs'],
             collector_window=current['retrieval_window'], public_quality=hot['quality']['status'])
        r.log('Collector statuses: '+json.dumps(statuses, sort_keys=True))
        r.ok('Current CPI replayed from original BLS bytes; old history and prior vintage retained')


if __name__ == '__main__':
    try: main()
    except Exception: sys.exit(1)
