"""Preserve the live public risk product and inspect native sources without invoking producers."""
from datetime import datetime,timedelta,timezone
from pathlib import Path
import base64,hashlib,io,json,sys,urllib.request,urllib.error,urllib.parse,zipfile
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/shared')]
from ops_report import report
from evidence_store import capture,read_verified
from massive import get_massive_key,MASSIVE_BASE


def denied(url):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,method='HEAD',headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=25):return False
    except urllib.error.HTTPError as exc:return exc.code in (401,403,404)


def main():
    s3=boto3.client('s3',region_name='us-east-1');lam=boto3.client('lambda',region_name='us-east-1')
    bucket,fn='justhodl-dashboard-live','justhodl-risk-regime'
    with report('ops_5902_risk_regime_preflight') as r:
        cfg=lam.get_function_configuration(FunctionName=fn)
        with urllib.request.urlopen(lam.get_function(FunctionName=fn)['Code']['Location'],timeout=45) as response:archive=response.read(32*1024*1024+1)
        assert len(archive)<=32*1024*1024 and base64.b64encode(hashlib.sha256(archive).digest()).decode()==cfg['CodeSha256']
        with zipfile.ZipFile(io.BytesIO(archive)) as z:source=z.read('lambda_function.py')
        assert source==(ROOT/'aws/lambdas'/fn/'source/lambda_function.py').read_bytes()
        r.kv(runtime_code_sha256=cfg['CodeSha256'],runtime_source_bytes=len(source),source_matches=True,
             engine_invocations=0,private_account_reads=0,paid_ai_calls=0,notifications_sent=0,portfolio_writes=0,
             timeout=cfg['Timeout'],memory=cfg['MemorySize'])
        raw=s3.get_object(Bucket=bucket,Key='data/risk-regime.json')['Body'].read(32*1024*1024+1)
        assert len(raw)<=32*1024*1024
        with urllib.request.urlopen(urllib.request.Request('https://justhodl.ai/data/risk-regime.json',headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=30) as response:
            assert json.loads(response.read())==json.loads(raw)
        sha=hashlib.sha256(raw).hexdigest();key='audit-private/20260909-originals/risk-regime/'+sha+'.bin'
        try:s3.put_object(Bucket=bucket,Key=key,Body=raw,IfNoneMatch='*',ContentType='application/octet-stream',CacheControl='no-store')
        except Exception as exc:
            if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) not in ('PreconditionFailed','412','ConditionalRequestConflict','409'):raise
        assert s3.get_object(Bucket=bucket,Key=key)['Body'].read()==raw
        assert denied('https://'+bucket+'.s3.amazonaws.com/'+key) and denied('https://justhodl.ai/'+key)
        r.kv(whole_preceding_product={'sha256':sha,'bytes':len(raw),'anonymous_denied':True})
        fred=cfg.get('Environment',{}).get('Variables',{}).get('FRED_KEY')
        if not fred:fred=boto3.client('ssm',region_name='us-east-1').get_parameter(Name='/justhodl/fred/api-key',WithDecryption=True)['Parameter']['Value']
        massive=get_massive_key();today=datetime.now(timezone.utc).date();probes=[]
        def fetch(label,provider,url,token=None,secret=None):
            try:
                headers={'User-Agent':'JustHodl-risk-native-preflight/1.0'}
                if token:headers['Authorization']='Bearer '+token
                with urllib.request.urlopen(urllib.request.Request(url,headers=headers),timeout=25) as response:raw=response.read(4*1024*1024+1)
                assert len(raw)<=4*1024*1024
                # Retain exact successful bodies only when no authentication token is reflected.
                for value in (token,secret):
                    if value and value.encode() in raw:raise ValueError('credential reflected in response')
                document=json.loads(raw);ref=capture(s3,bucket,provider,url,raw)
                assert read_verified(s3,bucket,ref)==raw
                result=document.get('results') or []
                probes.append({'name':label,'evidence':ref,'rows':len(result),'fields':sorted(result[0]) if result else sorted(document),
                    'has_next_page':bool(document.get('next_url'))})
                return document
            except Exception as exc:probes.append({'name':label,'failure':type(exc).__name__});return None
        for sid in ('VIXCLS','VXVCLS','BAMLH0A0HYM2'):
            query={'series_id':sid,'api_key':fred,'file_type':'json','realtime_start':str(today),'realtime_end':str(today)}
            fetch('fred-definition:'+sid,'fred','https://api.stlouisfed.org/fred/series?'+urllib.parse.urlencode(query),secret=fred)
            query.update(observation_start=str(today-timedelta(days=800)),observation_end=str(today),units='lin',output_type=1,sort_order='asc',limit=10000,offset=0)
            fetch('fred-observations:'+sid,'fred','https://api.stlouisfed.org/fred/series/observations?'+urllib.parse.urlencode(query),secret=fred)
        if massive:
            for symbol in ('SPY','HYG'):
                quote=fetch('previous:'+symbol,'polygon',MASSIVE_BASE+'/v2/aggs/ticker/'+symbol+'/prev?adjusted=true',token=massive)
                close=((quote or {}).get('results') or [{}])[0].get('c')
                if isinstance(close,(int,float)) and close>0:
                    params={'strike_price.gte':f'{close*.88:.2f}','strike_price.lte':f'{close*1.12:.2f}',
                        'expiration_date.gte':str(today+timedelta(days=21)),'expiration_date.lte':str(today+timedelta(days=45)),
                        'sort':'ticker','order':'asc','limit':250}
                    fetch('options:'+symbol,'polygon',MASSIVE_BASE+'/v3/snapshot/options/'+symbol+'?'+urllib.parse.urlencode(params),token=massive)
            fetch('fx:AUDJPY','polygon',MASSIVE_BASE+f'/v2/aggs/ticker/C:AUDJPY/range/1/day/{today-timedelta(days=65)}/{today-timedelta(days=1)}?adjusted=true&sort=asc&limit=50000',token=massive)
        r.kv(native_probes=probes,mutable_products_written=False)
        scheduler=boto3.client('scheduler',region_name='us-east-1');schedules=[]
        for page in scheduler.get_paginator('list_schedules').paginate(NamePrefix='justhodl-risk-regime'):
            for entry in page.get('Schedules',[]):
                schedule=scheduler.get_schedule(Name=entry['Name'],GroupName=entry['GroupName'])
                schedules.append({k:schedule.get(k) for k in ('Name','State','ScheduleExpression','ScheduleExpressionTimezone')})
        r.kv(schedules=schedules)


if __name__=='__main__':
    try:main()
    except Exception:
        print('Risk preflight failed; inspect its committed report before continuing.')
        sys.exit(1)
