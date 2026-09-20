"""Preserve public JSI products and inspect original FRED sources; no producers invoked."""
from datetime import datetime, timezone
from pathlib import Path
import base64, hashlib, io, json, sys, urllib.request, urllib.error, urllib.parse, zipfile
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/shared')]
from ops_report import report
from evidence_store import capture, read_verified
SERIES=('VIXCLS','NFCI','KCFSI','STLFSI4','BAMLH0A0HYM2','T10Y2Y','BAMLC0A0CM','WRESBAL','WALCL','RRPONTSYD','SOFR','IORB','NASDAQCOM')


def denied(url):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,method='HEAD',headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=25): return False
    except urllib.error.HTTPError as exc: return exc.code in (401,403,404)


def main():
    s3=boto3.client('s3',region_name='us-east-1');lam=boto3.client('lambda',region_name='us-east-1')
    bucket,fn='justhodl-dashboard-live','justhodl-stress-index'
    with report('ops_5905_stress_research_preflight') as r:
        cfg=lam.get_function_configuration(FunctionName=fn)
        with urllib.request.urlopen(lam.get_function(FunctionName=fn)['Code']['Location'],timeout=45) as response: archive=response.read(32*1024*1024+1)
        assert len(archive)<=32*1024*1024 and base64.b64encode(hashlib.sha256(archive).digest()).decode()==cfg['CodeSha256']
        with zipfile.ZipFile(io.BytesIO(archive)) as z: source=z.read('lambda_function.py')
        assert source==(ROOT/'aws/lambdas'/fn/'source/lambda_function.py').read_bytes()
        r.kv(runtime_code_sha256=cfg['CodeSha256'],runtime_source_bytes=len(source),source_matches=True,
             engine_invocations=0,private_account_reads=0,paid_ai_calls=0,notifications_sent=0,portfolio_writes=0,
             timeout=cfg['Timeout'],memory=cfg['MemorySize'])
        saved=[]
        for leaf in ('jsi','jsi-history','jsi-overlay-history','jsi-calibration'):
            raw=s3.get_object(Bucket=bucket,Key='data/'+leaf+'.json')['Body'].read(16*1024*1024+1)
            assert len(raw)<=16*1024*1024
            with urllib.request.urlopen(urllib.request.Request('https://justhodl.ai/data/'+leaf+'.json',headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=30) as response:
                assert json.loads(response.read())==json.loads(raw)
            sha=hashlib.sha256(raw).hexdigest();key='audit-private/20260909-originals/stress-index/'+sha+'.bin'
            try:s3.put_object(Bucket=bucket,Key=key,Body=raw,IfNoneMatch='*',ContentType='application/octet-stream',CacheControl='no-store')
            except Exception as exc:
                if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) not in ('PreconditionFailed','412','ConditionalRequestConflict','409'):raise
            assert s3.get_object(Bucket=bucket,Key=key)['Body'].read()==raw
            assert denied('https://'+bucket+'.s3.amazonaws.com/'+key) and denied('https://justhodl.ai/'+key)
            saved.append({'product':leaf,'sha256':sha,'bytes':len(raw),'anonymous_denied':True})
        r.kv(whole_preceding_products=saved)
        fred=cfg.get('Environment',{}).get('Variables',{}).get('FRED_KEY')
        if not fred:fred=boto3.client('ssm',region_name='us-east-1').get_parameter(Name='/justhodl/fred/api-key',WithDecryption=True)['Parameter']['Value']
        today=str(datetime.now(timezone.utc).date()); probes=[]
        for sid in SERIES:
            base=dict(series_id=sid,api_key=fred,file_type='json',realtime_start=today,realtime_end=today)
            for kind in ('definition','observations'):
                query=base.copy();path='/fred/series'
                if kind=='observations':
                    path+='/observations';query.update(observation_start='1990-01-01',observation_end=today,units='lin',output_type=1,sort_order='asc',limit=20000,offset=0)
                url='https://api.stlouisfed.org'+path+'?'+urllib.parse.urlencode(query)
                try:
                    with urllib.request.urlopen(urllib.request.Request(url,headers={'User-Agent':'JustHodl-stress-research-preflight/1.0'}),timeout=25) as response:raw=response.read(8*1024*1024+1)
                    assert len(raw)<=8*1024*1024 and fred.encode() not in raw
                    doc=json.loads(raw);ref=capture(s3,bucket,'fred',url,raw);assert read_verified(s3,bucket,ref)==raw
                    rows=doc.get('observations') or []
                    meta=(doc.get('seriess') or [{}])[0]
                    probes.append({'name':kind+':'+sid,'evidence':ref,'rows':len(rows),'count':doc.get('count'),
                        'definition':{k:meta.get(k) for k in ('id','title','units','frequency','frequency_short','seasonal_adjustment','last_updated','observation_start','observation_end')},
                        'latest':rows[-1] if rows else None})
                except Exception as exc:probes.append({'name':kind+':'+sid,'failure':type(exc).__name__})
        r.kv(native_probes=probes,mutable_products_written=False)
        scheduler=boto3.client('scheduler',region_name='us-east-1');schedules=[]
        for page in scheduler.get_paginator('list_schedules').paginate():
            for entry in page.get('Schedules',[]):
                if fn not in (entry.get('Target') or {}).get('Arn','') and 'jsi' not in entry['Name'] and 'stress-index' not in entry['Name']:continue
                schedule=scheduler.get_schedule(Name=entry['Name'],GroupName=entry['GroupName'])
                schedules.append({k:schedule.get(k) for k in ('Name','State','ScheduleExpression','ScheduleExpressionTimezone')})
        r.kv(schedules=schedules)


if __name__=='__main__':
    try:main()
    except Exception:
        print('Stress research preflight failed; inspect the committed report before retrying.')
        sys.exit(1)
