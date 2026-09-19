"""Read official ALFRED response shapes; retain exact originals without credentials."""
from datetime import datetime, timezone
import gzip
import hashlib
import json
from pathlib import Path
import sys
import urllib.parse
import urllib.request
import urllib.error
import boto3

ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/shared')]
from ops_report import report
from managed_secret import managed_secret


def main():
    # Use the working producer's existing managed provider configuration; never print it.
    env=boto3.client('lambda',region_name='us-east-1').get_function_configuration(FunctionName='justhodl-daily-report-v3').get('Environment',{}).get('Variables',{})
    key=env.get('FRED_API_KEY') or env.get('FRED_KEY') or managed_secret(('FRED_KEY','FRED_API_KEY'),('/justhodl/fred/api-key',))
    del env
    assert key, 'FRED managed source credential unavailable'
    client=boto3.client('s3',region_name='us-east-1');bucket='justhodl-dashboard-live'
    today=datetime.now(timezone.utc).date().isoformat();proof={}
    with report('ops_5828_vintage_source_probe') as r:
        for sid in ('WALCL','WTREGEN','RRPONTSYD','GDPC1','PAYEMS'):
            proof[sid]={}
            for endpoint in ('series','series/observations'):
                params={'series_id':sid,'file_type':'json','realtime_start':'1776-07-04','realtime_end':'9999-12-31'}
                if endpoint.endswith('observations'):
                    params.update(observation_start='2000-01-01',observation_end=today,output_type=1,units='lin',sort_order='asc',limit=1000,offset=0)
                public='https://api.stlouisfed.org/fred/'+endpoint+'?'+urllib.parse.urlencode(params)
                req=urllib.request.Request(public+'&api_key='+urllib.parse.quote(key),headers={'User-Agent':'JustHodl-source-audit/1.0'})
                try:
                    with urllib.request.urlopen(req,timeout=30) as response:raw=response.read(16*1024*1024+1)
                except urllib.error.HTTPError as exc:
                    message=exc.read(2048).decode('utf-8','replace').replace(key,'[REDACTED]')
                    raise ValueError('FRED '+sid+' '+endpoint+' HTTP '+str(exc.code)+': '+message[:500]) from None
                assert len(raw)<=16*1024*1024,'source bound exceeded'
                doc=json.loads(raw);sha=hashlib.sha256(raw).hexdigest()
                dest='data/vintage-research/probe-originals/'+sha+'.json.gz'
                try:client.put_object(Bucket=bucket,Key=dest,Body=gzip.compress(raw,mtime=0),ContentType='application/gzip',IfNoneMatch='*')
                except Exception as exc:
                    if getattr(exc,'response',{}).get('Error',{}).get('Code') not in ('412','PreconditionFailed'):raise
                assert gzip.decompress(client.get_object(Bucket=bucket,Key=dest)['Body'].read())==raw
                rows=doc.get('observations',doc.get('seriess',[]))
                item={'request_url':public,'key':dest,'sha256':sha,'bytes':len(raw),
                      'response_fields':{k:v for k,v in doc.items() if k not in ('seriess','observations')},
                      'returned_rows':len(rows),'sample':rows[:2]+rows[-2:]}
                proof[sid][endpoint]=item
                r.kv(series=sid,endpoint=endpoint,returned_rows=len(rows),response_count=doc.get('count'),key=dest)
        out={'contract':'vintage-source-probe.v1','generated_at':datetime.now(timezone.utc).isoformat(),'series':proof,
             'scope':'Read-only source-shape audit; no current engine or portfolio output changed.'}
        client.put_object(Bucket=bucket,Key='data/vintage-source-probe-verification.json',Body=json.dumps(out).encode(),ContentType='application/json',CacheControl='no-store')


if __name__=='__main__':
    try:main()
    except Exception:
        print('Vintage source probe failed; see redacted runner report.')
        sys.exit(1)
