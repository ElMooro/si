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
FN = 'justhodl-ai-brief'
KEY = 'data/ai-brief.json'
BATCH = 'chatgpt-20260917T233349Z-calls-provider-free-brief'


def main():
    bucket = 'justhodl-dashboard-live'
    s3 = boto3.client('s3', region_name='us-east-1')
    lam = boto3.client('lambda', region_name='us-east-1', config=Config(read_timeout=900, retries={'max_attempts':0}))
    commit = subprocess.check_output(['git','log','-1','--format=%H','--',
        'aws/ops/patchers/batch/_receipts/' + BATCH + '.json'],text=True).strip()
    assert len(commit)==40, 'Assembled batch commit absent'
    def read(key): return json.loads(s3.get_object(Bucket=bucket, Key=key)['Body'].read())
    with report('ops_5704_calls_free_verify') as r:
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
        assert datetime.fromisoformat(doc['generated_at'].replace('Z','+00:00'))>=started.replace(microsecond=0)
        public=read('data/ai-brief-public.json')
        assert public['generation_method']=='warehouse_deterministic_v1'
        assert public['paid_api_calls']==0 and public['model'] is None
        assert public['brief_md']==doc['brief_md'] and len(public['brief_md'])>=120
        assert '**DECISIVE CALL: WAIT**' in public['brief_md'][-500:]
        assert not public.get('error') and public['coverage']['available']>0
        assert public['call_verb']=='WAIT' and public['sizing_eligible'] is False
        assert 'snapshot' not in public and public['visibility']=='public_projection'
        assert doc['visibility']=='private' and 'snapshot' in doc
        decision=doc['decision']
        assert decision['call_verb']=='WAIT' and decision['candidate_verb']=='WAIT'
        assert decision['decision_status']=='ABSTAIN' and decision['sizing_eligible'] is False
        rows=read('data/decisive-call-history.json')['snapshots']
        assert any(row==decision for row in rows)
        r.kv(function=FN,commit=commit,generated_at=doc['generated_at'],
             brief_chars=len(public['brief_md']),paid_api_calls=public['paid_api_calls'],
             call_verb=decision['call_verb'],decision_status=decision['decision_status'],
             public_key='data/ai-brief-public.json',fields=public['coverage']['fields'],
             fresh_fields=public['coverage']['fresh'],eligible_votes=public['coverage']['eligible_votes'])
        r.ok('Pinned release, live AWS/source hashes, substantive provider-free brief, private/public separation and WAIT ledger row verified')


if __name__=='__main__':
    try: main()
    except Exception: sys.exit(1)
