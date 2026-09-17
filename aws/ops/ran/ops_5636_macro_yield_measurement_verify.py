"""Refresh four pinned measurement releases; inspect ECB unit metadata."""
import csv
import hashlib
import io
import json
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
import boto3
from botocore.config import Config
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from ops_report import report
BUCKET='justhodl-dashboard-live'
TARGETS=[
 ('manufacturing-global-agent','3dfe21a032c8f0c27652a6810af2b6ea61b3c6f0','data/manufacturing.json','manufacturing-measurement.v2'),
 ('justhodl-macro-nowcast','66279f5110242b234c59e2732b2b48816cb3703a','data/macro-nowcast.json','monthly-measurement.v2'),
 ('justhodl-yield-curve','7e33d1b52a6521e4596b8fd7f9ede0debc6b5d12','data/yield-curve.json','dated-curve.v2'),
 ('justhodl-us10y-sentinel','b0faf65ceb835d4b61a38ad91bad5120b46da3d7','data/us10y-sentinel.json','daily-price-episodes.v2')]


def main():
    s3=boto3.client('s3',region_name='us-east-1')
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=650,retries={'max_attempts':0}))
    def read(key):return json.loads(s3.get_object(Bucket=BUCKET,Key=key)['Body'].read())
    with report('ops_5636_macro_yield_measurement_verify') as r:
        for series in ['ILM/W.U2.C.A050000.U2.EUR','ILM/W.U2.C.A030000.U2.Z06','ILM/W.U2.C.T000000.Z5.Z01','CISS/D.U2.Z0Z.4F.EC.SS_CIN.IDX']:
            try:
                req=urllib.request.Request('https://data-api.ecb.europa.eu/service/data/'+series+'?format=csvdata&lastNObservations=1',headers={'User-Agent':'Mozilla/5.0'})
                with urllib.request.urlopen(req,timeout=30) as response:rows=list(csv.DictReader(io.StringIO(response.read().decode())))
                r.kv(ecb_series=series,metadata=[{k:v for k,v in row.items() if k in ('TIME_PERIOD','OBS_VALUE','UNIT','UNIT_MULT','FREQ','OBS_STATUS','TITLE')} for row in rows])
            except Exception as exc:r.kv(ecb_series=series,probe_error=type(exc).__name__)
        errors=[]
        for fn,commit,key,method in TARGETS:
            try:
                deadline=time.monotonic()+540
                while True:
                    try:receipt=read(f'data/ops/releases/{fn}.json')
                    except s3.exceptions.NoSuchKey:receipt={}
                    if receipt.get('commit')==commit:break
                    if time.monotonic()>deadline:raise RuntimeError('Pinned release receipt absent')
                    time.sleep(15)
                cfg=lam.get_function_configuration(FunctionName=fn)
                assert cfg['CodeSha256']==receipt['code_sha256'],'AWS hash mismatch'
                for name,meta in receipt['source'].items():
                    raw=subprocess.check_output(['git','show',f'{commit}:aws/lambdas/{fn}/source/{name}'])
                    assert hashlib.sha256(raw).hexdigest()==meta['sha256'],'Source receipt mismatch'
                started=datetime.now(timezone.utc)
                result=lam.invoke(FunctionName=fn,InvocationType='RequestResponse',Payload=b'{"suppress_alerts":true}')
                body=json.loads(result['Payload'].read())
                assert not result.get('FunctionError') and body.get('statusCode')==200, 'Invoke failed: '+str(body)[:500]
                obj=s3.get_object(Bucket=BUCKET,Key=key);doc=json.loads(obj['Body'].read())
                assert obj['LastModified']>=started and doc['methodology_version']==method
                assert doc['call'] is None and doc['execution_eligible'] is False
                if fn=='manufacturing-global-agent':
                    assert doc['analysis']['ism_level'] is None and doc['analysis']['us_cycle']=='UNKNOWN'
                    assert doc['recommendations']==[]
                    assert 'FRANCE_PMI' not in doc['global_manufacturing']
                elif fn=='justhodl-macro-nowcast':
                    assert doc['global_confidence']['composite_z'] is None and not doc['return_study']['dividends_included']
                    assert doc['quality']['status']=='fresh' or doc['normalized_score'] is None
                    r.kv(nowcast_regime=doc['regime'],score=doc['normalized_score'],coverage=doc['coverage_pct'])
                elif fn=='justhodl-yield-curve':
                    assert doc['quality']['status']=='fresh'
                    assert len(doc['curve_points'])==11 and len({p['date'] for p in doc['curve_points']})==1
                    assert doc['fed_context']['FED_FUNDS_RATE_EFFECTIVE']['series_id']=='DFF'
                    assert doc['term_premium_source']!='proxy'
                else:
                    assert doc['quality']['status']=='fresh' and doc['yields_driving_stocks'] is None
                    for study in doc['episode_study'].values():
                        assert study['n_valid_3m']>=3 or study['median_spx_3m'] is None
                        assert study['median_spx_3m'] is None or abs(study['median_spx_3m'])<=100
                    r.kv(episodes=doc['episode_study'],yield_level=doc['level'])
                r.kv(function=fn,commit=commit,code_sha256=receipt['code_sha256'],generated_at=doc['generated_at'],quality=doc['quality'])
                r.ok(fn+': pinned source, live AWS hash and corrected publication verified')
            except Exception as exc:
                errors.append(fn);r.kv(function=fn,error=str(exc)[:600]);r.fail(fn+' verification failed')
        if errors:raise RuntimeError('Unverified releases: '+', '.join(errors))
    return 0


if __name__=='__main__':sys.exit(main())
