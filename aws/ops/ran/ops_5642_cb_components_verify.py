"""Verify the exact CB release and refresh its measurement contract without alerts."""
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

BUCKET = 'justhodl-dashboard-live'
FUNCTION = 'justhodl-cb-injection'
COMMIT = '57c9bc3e897e8feb6af3e53c9f9107cda5e0c3c7'


def main():
    s3 = boto3.client('s3', region_name='us-east-1')
    lam = boto3.client('lambda', region_name='us-east-1',
                       config=Config(read_timeout=300, retries={'max_attempts': 0}))
    with report('ops_5642_cb_components_verify') as r:
        deadline = time.monotonic() + 600
        while True:
            try:
                receipt = json.loads(s3.get_object(Bucket=BUCKET, Key=f'data/ops/releases/{FUNCTION}.json')['Body'].read())
            except s3.exceptions.NoSuchKey:
                receipt = {}
            if receipt.get('commit') == COMMIT:
                break
            if time.monotonic() > deadline:
                raise RuntimeError('Pinned central-bank release receipt absent')
            time.sleep(15)
        assert lam.get_function_configuration(FunctionName=FUNCTION)['CodeSha256'] == receipt['code_sha256']
        for name, meta in receipt['source'].items():
            raw = subprocess.check_output(['git', 'show', f'{COMMIT}:aws/lambdas/{FUNCTION}/source/{name}'])
            assert len(raw) == meta['bytes'] and hashlib.sha256(raw).hexdigest() == meta['sha256']
        started = datetime.now(timezone.utc)
        result = lam.invoke(FunctionName=FUNCTION, InvocationType='RequestResponse', Payload=b'{"suppress_alerts":true}')
        body = json.loads(result['Payload'].read())
        assert not result.get('FunctionError') and body.get('statusCode') == 200, 'Lambda invocation failed'
        obj = s3.get_object(Bucket=BUCKET, Key='data/cb-injection.json')
        doc = json.loads(obj['Body'].read())
        assert obj['LastModified'] >= started
        assert doc['methodology_version'] == 'cb-component-measurements.v2'
        assert doc['call'] is None and doc['execution_eligible'] is False
        assert doc['global_injection_impulse']['score'] is None
        banks = {b['cb']: b for b in doc['central_banks']}
        for name in ('Fed', 'ECB'):
            d = banks[name]['decomposition']
            r.kv(bank=name, decomposition=d)
            assert d['status'] == 'partial_attribution', name + ': component coverage incomplete'
            assert abs(d['reconciliation_residual']) < 0.000001
            assert d['net_injection_estimate'] is None and d['fx_valuation_change_1m'] is None
        for name in ('BOJ', 'SNB'):
            assert banks[name]['policy_rate_pct'] is None
            assert banks[name]['decomposition']['net_injection_estimate'] is None
        r.kv(function=FUNCTION, commit=COMMIT, code_sha256=receipt['code_sha256'],
             generated_at=doc['generated_at'], quality=doc['quality'], errors=doc['errors'])
        r.ok('Source bytes, AWS code, fresh dated components and stock reconciliation verified')
    return 0


if __name__ == '__main__':
    sys.exit(main())
