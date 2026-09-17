"""Invoke and verify the exact assembled release; never infer deploy from green CI."""
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
FN = 'justhodl-crisis-composite'
KEY = 'data/crisis-composite.json'
BATCH = 'chatgpt-20260917T232504Z-defcon-canonical-alias'


def main():
    bucket = 'justhodl-dashboard-live'
    s3 = boto3.client('s3', region_name='us-east-1')
    lam = boto3.client('lambda', region_name='us-east-1', config=Config(read_timeout=900, retries={'max_attempts':0}))
    commit = subprocess.check_output(['git','log','-1','--format=%H','--',
        'aws/ops/patchers/batch/_receipts/' + BATCH + '.json'],text=True).strip()
    assert len(commit)==40, 'Assembled batch commit absent'
    def read(key): return json.loads(s3.get_object(Bucket=bucket, Key=key)['Body'].read())
    with report('ops_5702_defcon_alias_verify') as r:
        deadline = time.monotonic()+900
        while True:
            receipt = read('data/ops/releases/' + FN + '.json')
            if receipt['commit']==commit: break
            assert time.monotonic()<deadline, 'Pinned release receipt missing'
            time.sleep(15)
        assert lam.get_function_configuration(FunctionName=FN)['CodeSha256']==receipt['code_sha256']
        for filename, meta in receipt['source'].items():
            raw=subprocess.check_output(['git','show',commit+':aws/lambdas/'+FN+'/source/'+filename])
            assert hashlib.sha256(raw).hexdigest()==meta['sha256'] and len(raw)==meta['bytes']
        started=datetime.now(timezone.utc)
        response=lam.invoke(FunctionName=FN,InvocationType='RequestResponse',Payload=b'{"suppress_alerts":true}')
        result=json.loads(response['Payload'].read())
        assert not response.get('FunctionError') and result.get('statusCode',200)<400, 'Engine invoke failed'
        doc=read(KEY)
        assert datetime.fromisoformat(doc['generated_at'].replace('Z','+00:00'))>=started
        assert doc==read('data/defcon.json'), 'DEFCON alias diverged from canonical packet'
        r.kv(function=FN,commit=commit,generated_at=doc['generated_at'],
             alias='data/defcon.json',canonical=KEY,defcon_level=doc['defcon_level'])
        r.ok('Exact release, AWS code hash, source hashes and fresh canonical/DEFCON alias parity verified')


if __name__=='__main__':
    try: main()
    except Exception: sys.exit(1)
