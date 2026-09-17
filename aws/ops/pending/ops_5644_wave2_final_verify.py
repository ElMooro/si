"""Final Wave 2 AWS code/source readback and CB registered archive refresh."""
import hashlib
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
import boto3
from botocore.config import Config
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from ops_report import report
BUCKET='justhodl-dashboard-live'
TARGETS=[('justhodl-nyfed-pd', '91000c63572375f7b3e1cb36a896a9d3fb7f52e5', 'data/nyfed-primary-dealer.json'), ('justhodl-treasury-rehypo', '96eec553e5d23ee026faedb0ce4634934acb0c1c', 'data/treasury-rehypo.json'), ('justhodl-yield-curve', '7e33d1b52a6521e4596b8fd7f9ede0debc6b5d12', 'data/yield-curve.json'), ('justhodl-us10y-sentinel', 'b0faf65ceb835d4b61a38ad91bad5120b46da3d7', 'data/us10y-sentinel.json'), ('manufacturing-global-agent', '3dfe21a032c8f0c27652a6810af2b6ea61b3c6f0', 'data/manufacturing.json'), ('justhodl-macro-nowcast', '66279f5110242b234c59e2732b2b48816cb3703a', 'data/macro-nowcast.json'), ('justhodl-ecb-history', '5457c02bf51d5331cb9033b7affd4595dcf1479f', 'data/ecb-hist/_manifest.json'), ('justhodl-hot-money', '417b5067b6f8650c655ffb50ad7f848a51725419', 'data/hot-money.json'), ('justhodl-etf-fund-flows', 'b68d0cb4cc972bee62a59378b9132edb022d4a04', 'etf-flows/measurements.json'), ('justhodl-capital-flow-radar', '12907314717f4dc7bee82c1a058a15d865bb5ada', 'data/capital-flow-radar.json'), ('justhodl-yen-carry', '60b4695be9ac6f35e8291b4151226442b03a139e', 'data/yen-carry.json'), ('justhodl-cb-injection', '35e34961021e67556241e24e81c8871b35364204', 'data/cb-injection.json')]


def main():
    s3=boto3.client('s3',region_name='us-east-1')
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=300,retries={'max_attempts':0}))
    def read(key):return json.loads(s3.get_object(Bucket=BUCKET,Key=key)['Body'].read())
    with report('ops_5644_wave2_final_verify') as r:
        for fn,commit,key in TARGETS:
            deadline=time.monotonic()+600
            while True:
                receipt=read(f'data/ops/releases/{fn}.json')
                if receipt.get('commit')==commit:break
                if time.monotonic()>deadline:raise RuntimeError('Pinned receipt absent: '+fn)
                time.sleep(15)
            cfg=lam.get_function_configuration(FunctionName=fn)
            assert cfg['CodeSha256']==receipt['code_sha256'],fn+': AWS code hash differs'
            for name,meta in receipt['source'].items():
                raw=subprocess.check_output(['git','show',f'{commit}:aws/lambdas/{fn}/source/{name}'])
                assert len(raw)==meta['bytes'] and hashlib.sha256(raw).hexdigest()==meta['sha256'],fn+': source differs'
            if fn=='justhodl-cb-injection':
                started=datetime.now(timezone.utc)
                response=lam.invoke(FunctionName=fn,InvocationType='RequestResponse',Payload=b'{"suppress_alerts":true}')
                body=json.loads(response['Payload'].read())
                assert not response.get('FunctionError') and body.get('statusCode')==200
            obj=s3.get_object(Bucket=BUCKET,Key=key);doc=json.loads(obj['Body'].read())
            assert (datetime.now(timezone.utc)-obj['LastModified']).total_seconds()<26*3600,fn+': stale object'
            if fn=='justhodl-cb-injection':
                assert obj['LastModified']>=started
                assert doc['methodology_version']=='cb-component-measurements.v2'
                assert doc['call'] is None and doc['global_injection_impulse']['score'] is None
                snapshot=read('data/cb-injection/snapshots/'+doc['generated_at'][:10]+'.json')
                assert snapshot['generated_at']==doc['generated_at'] and snapshot['impulse'] is None and snapshot['unwind_risk'] is None
                for bank in doc['central_banks'][:2]:
                    d=bank['decomposition']
                    assert d['status']=='partial_attribution' and abs(d['reconciliation_residual'])<1e-6
                r.ok('Registered CB snapshot restored with measured components and null legacy forecasts')
            r.kv(function=fn,commit=commit,code_sha256=receipt['code_sha256'],source_files=len(receipt['source']),
                 source_bytes=sum(m['bytes'] for m in receipt['source'].values()),generated_at=doc.get('generated_at'),quality=doc.get('quality'))
            r.ok(fn+': exact commit, source byte lengths/digests and live AWS CodeSha256 verified')
    return 0


if __name__=='__main__':sys.exit(main())
