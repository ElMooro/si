"""Verify every raw-archive importer and a real >400KB Lambda capture/readback."""
import ast
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time
from datetime import datetime,timezone
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/shared')]
from ops_report import report
from raw_snapshot import snapshot_receipt,read_snapshot,CONTRACT

def importers():
    result=[]
    for source in (ROOT/'aws/lambdas').glob('*/source/*.py'):
        tree=ast.parse(source.read_text(encoding='utf-8'))
        if any(isinstance(n,ast.ImportFrom) and n.module=='raw_snapshot' or
               isinstance(n,ast.Import) and any(a.name=='raw_snapshot' for a in n.names) for n in ast.walk(tree)):
            result.append(source.parent.parent.name)
    return sorted(set(result))

def main():
    bucket='justhodl-dashboard-live';functions=importers()
    assert len(functions)>=17,'unexpected importer coverage'
    expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/shared/raw_snapshot.py'],text=True).strip()
    s3=boto3.client('s3',region_name='us-east-1')
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=920,retries={'max_attempts':0}))
    def read(key):return json.loads(s3.get_object(Bucket=bucket,Key=key)['Body'].read())
    with report('ops_5727_raw_capture_replay_verify') as r:
        deadline=time.monotonic()+2400
        for fn in functions:
            while True:
                try:receipt=read('data/ops/releases/'+fn+'.json')
                except ClientError as exc:
                    if exc.response['Error']['Code'] not in ('404','NoSuchKey'):raise
                    receipt={}
                if receipt.get('commit')==expected:break
                assert time.monotonic()<deadline,'exact receipt missing for '+fn
                time.sleep(15)
            assert lam.get_function_configuration(FunctionName=fn)['CodeSha256']==receipt['code_sha256']
            r.log(fn+' exact release/runtime matched')
        response=lam.invoke(FunctionName='justhodl-plumbing-panel',InvocationType='RequestResponse',
                            Payload=b'{"snapshot_validation_only":true,"suppress_alerts":true}')
        result=json.loads(response['Payload'].read())
        assert not response.get('FunctionError') and result.get('statusCode')==200,'live archive probe failed'
        assert result['kind']=='real_provider_capture_probe' and result['derived_data_writes']==0
        receipt=result['receipt'];independent=snapshot_receipt(receipt['key'])
        assert independent==receipt and receipt['contract']==CONTRACT and receipt['captured_bytes_verified'] is True
        raw=read_snapshot(receipt['key']);assert raw is not None and len(raw)>400000
        assert len(raw)==receipt['bytes'] and hashlib.sha256(raw).hexdigest()==receipt['sha256']
        doc=json.loads(raw);assert len(doc['data'])==result['source_rows']
        proof={'schema_version':'raw-capture-verification.v1','generated_at':datetime.now(timezone.utc).isoformat(),
               'commit':expected,'runtime_importers_verified':functions,'live_probe_engine':'justhodl-plumbing-panel',
               'capture':receipt,'source_rows':result['source_rows'],'derived_data_writes':0,'paid_api_calls':0,
               'scope':'Exact importer runtimes and one actual complete Treasury response supplied by the deployed probe; not a whole-fleet historical completeness audit',
               'historical_archives_upgraded':False,'sizing_eligible':False}
        s3.put_object(Bucket=bucket,Key='data/raw-snapshot-verification.json',Body=json.dumps(proof,sort_keys=True).encode(),
                      ContentType='application/json',CacheControl='no-cache')
        r.kv(commit=expected,importers_verified=len(functions),generated_at=proof['generated_at'],
             captured_bytes=receipt['bytes'],sha256=receipt['sha256'],source_rows=result['source_rows'],
             raw_key=receipt['key'],derived_data_writes=0,legacy_archive_mutations=0,paid_api_calls=0)
        r.ok('Every importer runtime matches; deployed Lambda captured a complete real >400KB response, independently read and verified without derived-data writes')

if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
