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
FN = 'justhodl-alpha-compass'
KEY = 'data/alpha-compass.json'
BATCH = 'chatgpt-20260917T231647Z-alpha-quality-pd-context'


def main():
    bucket = 'justhodl-dashboard-live'
    s3 = boto3.client('s3', region_name='us-east-1')
    lam = boto3.client('lambda', region_name='us-east-1', config=Config(read_timeout=900, retries={'max_attempts':0}))
    commit = subprocess.check_output(['git','log','-1','--format=%H','--',
        'aws/ops/patchers/batch/_receipts/' + BATCH + '.json'],text=True).strip()
    assert len(commit)==40, 'Assembled batch commit absent'
    def read(key): return json.loads(s3.get_object(Bucket=bucket, Key=key)['Body'].read())
    with report('ops_5700_alpha_pd_verify') as r:
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
        doc=read(KEY); source=read('data/settlement-fails.json')
        assert datetime.fromisoformat(doc['generated_at'].replace('Z','+00:00'))>=started
        pd=doc['pd_settlement_fails']
        assert pd['scope_id']=='treasury_incl_tips' and pd['unit']=='usd_bn' and pd['calls_eligible'] is False
        for dest,src in [('ftd_bn','ftd_bn'),('ftr_bn','ftr_bn'),('combined_bn','gross_bn'),('as_of','as_of')]:
            assert pd[dest]==source['treasury'][src], 'Gross scope mismatch: '+dest
        hd=pd['ust_ex_tips']
        assert hd['scope_id']=='ust_ex_tips'
        for field in ('ftd_bn','ftr_bn','combined_bn','as_of'):
            assert hd[field]==source['headline'][field], 'Headline scope mismatch: '+field
        if FN=='justhodl-alpha-compass': assert isinstance(doc.get('quality'),dict)
        if FN=='justhodl-liquidity-reversal': assert doc['generated_at']==doc['as_of']
        r.kv(function=FN,commit=commit,generated_at=doc['generated_at'],gross_as_of=pd['as_of'],
             gross_ftd=pd['ftd_bn'],gross_ftr=pd['ftr_bn'],gross_total=pd['combined_bn'],
             headline_as_of=hd['as_of'],headline_total=hd['combined_bn'],engine_quality=doc.get('quality',{}).get('status'))
        r.ok('Exact release, live AWS code hash, source hashes and freshly invoked public output verified; scopes remain separate')


if __name__=='__main__':
    try: main()
    except Exception: sys.exit(1)
