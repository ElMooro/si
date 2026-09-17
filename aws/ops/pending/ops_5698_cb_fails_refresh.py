"""Refresh the already deployed Grok CB settlement context and prove its release."""
import hashlib
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
import boto3
from botocore.config import Config
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report


def main():
    fn = 'justhodl-cb-injection'
    commit = '4e28f560c1bc460b2623e17bd06bea78e5f3d038'
    bucket = 'justhodl-dashboard-live'
    s3 = boto3.client('s3', region_name='us-east-1')
    lam = boto3.client('lambda', region_name='us-east-1', config=Config(read_timeout=360, retries={'max_attempts': 0}))
    def read(key): return json.loads(s3.get_object(Bucket=bucket, Key=key)['Body'].read())
    with report('ops_5698_cb_fails_refresh') as r:
        receipt = read('data/ops/releases/' + fn + '.json')
        assert receipt['commit'] == commit, 'Exact CB release receipt missing'
        assert lam.get_function_configuration(FunctionName=fn)['CodeSha256'] == receipt['code_sha256']
        for name, meta in receipt['source'].items():
            raw = subprocess.check_output(['git', 'show', commit + ':aws/lambdas/' + fn + '/source/' + name])
            assert len(raw) == meta['bytes'] and hashlib.sha256(raw).hexdigest() == meta['sha256']
        started = datetime.now(timezone.utc)
        response = lam.invoke(FunctionName=fn, InvocationType='RequestResponse', Payload=b'{"suppress_alerts":true}')
        body = json.loads(response['Payload'].read())
        assert not response.get('FunctionError') and body.get('statusCode') == 200, 'CB invoke failed'
        doc = read('data/cb-injection.json'); sf = read('data/settlement-fails.json')['treasury']
        assert datetime.fromisoformat(doc['generated_at']) >= started
        pd = doc['pd_settlement_fails']
        for dest, src in [('as_of','as_of'), ('ftd_bn','ftd_bn'), ('ftr_bn','ftr_bn'), ('combined_bn','gross_bn')]:
            assert pd[dest] == sf[src], dest + ': Treasury context mismatch'
        assert pd['unit'] == 'usd_bn' and doc['global_injection_impulse']['score'] is None
        assert doc['call'] is None and doc['execution_eligible'] is False
        r.kv(function=fn, commit=commit, generated_at=doc['generated_at'], scope='treasury_incl_tips', **pd)
        r.ok('Grok source and live AWS hash verified; fresh CB packet includes Treasury gross FTD/FTR without changing injection attribution')


if __name__ == '__main__':
    try: main()
    except Exception: sys.exit(1)
