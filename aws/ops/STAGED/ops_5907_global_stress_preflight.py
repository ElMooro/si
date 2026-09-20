"""Preserve public Global Stress products and inspect provider definitions; no producer invocation."""
from datetime import datetime,timedelta,timezone
from pathlib import Path
import base64,hashlib,io,json,sys,time,urllib.request,urllib.error,urllib.parse,zipfile
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/shared')]
from ops_report import report
from evidence_store import capture,read_verified
SYMBOLS=('SPY','FEZ','EWU','EWJ','MCHI','INDA','EWY','EEM','IEF','LQD','HYG','BWX','EMB','GLD')
FRED=('BAMLH0A1HYBB','BAMLH0A2HYB','BAMLH0A3HYC','BAMLEMCBPIOAS','BAMLEMHBHYCRPIOAS','DGS10')


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,req,fp,code,msg,headers,newurl):return None


def denied(url):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,method='HEAD',headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=25):return False
    except urllib.error.HTTPError as exc:return exc.code in (401,403,404)


def main():
    s3=boto3.client('s3',region_name='us-east-1');lam=boto3.client('lambda',region_name='us-east-1')
    bucket,fn='justhodl-dashboard-live','justhodl-global-stress'
    with report('ops_5907_global_stress_preflight') as r:
        cfg=lam.get_function_configuration(FunctionName=fn)
        with urllib.request.urlopen(lam.get_function(FunctionName=fn)['Code']['Location'],timeout=45) as response:archive=response.read(32*1024*1024+1)
        assert len(archive)<=32*1024*1024 and base64.b64encode(hashlib.sha256(archive).digest()).decode()==cfg['CodeSha256']
        with zipfile.ZipFile(io.BytesIO(archive)) as z:source=z.read('lambda_function.py')
        assert source==(ROOT/'aws/lambdas'/fn/'source/lambda_function.py').read_bytes()
        r.kv(runtime_code_sha256=cfg['CodeSha256'],runtime_source_bytes=len(source),source_matches=True,
             engine_invocations=0,private_account_reads=0,notifications_sent=0,portfolio_writes=0,paid_ai_calls=0,
             timeout=cfg['Timeout'],memory=cfg['MemorySize'])
        saved=[]
        for leaf in ('global-stress','global-stress-history','gsi-dim-history','gsi-calibration','gsi-horizons'):
            try:raw=s3.get_object(Bucket=bucket,Key='data/'+leaf+'.json')['Body'].read(32*1024*1024+1)
            except Exception as exc:
                if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) not in ('NoSuchKey','404'):raise
                saved.append({'product':leaf,'absent':True});continue
            assert len(raw)<=32*1024*1024
            with urllib.request.urlopen(urllib.request.Request('https://justhodl.ai/data/'+leaf+'.json',headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=30) as response:assert json.loads(response.read())==json.loads(raw)
            sha=hashlib.sha256(raw).hexdigest();key='audit-private/20260909-originals/global-stress/'+sha+'.bin'
            try:s3.put_object(Bucket=bucket,Key=key,Body=raw,IfNoneMatch='*',ContentType='application/octet-stream',CacheControl='no-store')
            except Exception as exc:
                if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) not in ('PreconditionFailed','412','ConditionalRequestConflict','409'):raise
            assert s3.get_object(Bucket=bucket,Key=key)['Body'].read()==raw
            assert denied('https://'+bucket+'.s3.amazonaws.com/'+key) and denied('https://justhodl.ai/'+key)
            saved.append({'product':leaf,'sha256':sha,'bytes':len(raw),'anonymous_denied':True})
        r.kv(whole_preceding_products=saved)
        env=cfg.get('Environment',{}).get('Variables',{});ssm=boto3.client('ssm',region_name='us-east-1')
        fred=env.get('FRED_KEY') or ssm.get_parameter(Name='/justhodl/fred/api-key',WithDecryption=True)['Parameter']['Value']
        fmp=env.get('FMP_KEY')
        if not fmp:
            fmp=ssm.get_parameter(Name='/justhodl/fmp-api-key',WithDecryption=True)['Parameter']['Value']
        assert fmp,'Existing FMP credential required'
        today=datetime.now(timezone.utc).date();probes=[];deadline=time.monotonic()+180
        opener=urllib.request.build_opener(NoRedirect())
        def fetch(name,provider,url,headers=None,wire=None):
            try:
                if time.monotonic()>=deadline:raise TimeoutError('preflight provider budget exhausted')
                with opener.open(urllib.request.Request(wire or url,headers={'User-Agent':'JustHodl-global-stress-preflight/1.0',**(headers or {})}),timeout=max(1,min(20,deadline-time.monotonic()))) as response:raw=response.read(8*1024*1024+1)
                assert len(raw)<=8*1024*1024 and all(key.encode() not in raw for key in (fred,fmp) if key)
                doc=json.loads(raw);ref=capture(s3,bucket,provider,url,raw);assert read_verified(s3,bucket,ref)==raw
                rows=doc if isinstance(doc,list) else doc.get('observations') or doc.get('seriess') or []
                dates=sorted(str(row.get('date')) for row in rows if isinstance(row,dict) and row.get('date'))
                first=rows[0] if rows and isinstance(rows[0],dict) else {}
                probes.append({'name':name,'request_url':url,'evidence':ref,'rows':len(rows),'fields':sorted(first),
                    'first_date':dates[0] if dates else None,'last_date':dates[-1] if dates else None,
                    'definition':{k:first.get(k) for k in ('id','title','units','frequency','frequency_short','seasonal_adjustment','last_updated','observation_start','observation_end')} if name.startswith('definition:') else None,
                    'identity':{k:first.get(k) for k in ('symbol','companyName','currency','exchange','isEtf','isin','cusip')} if name.startswith('profile:') else None})
            except Exception as exc:probes.append({'name':name,'failure':type(exc).__name__,'http_status':getattr(exc,'code',None)})
        for symbol in SYMBOLS:
            fetch('profile:'+symbol,'fmp','https://financialmodelingprep.com/stable/profile?'+urllib.parse.urlencode({'symbol':symbol}),{'apikey':fmp})
            for kind in ('full','dividend-adjusted'):
                url='https://financialmodelingprep.com/stable/historical-price-eod/'+kind+'?'+urllib.parse.urlencode({'symbol':symbol,'from':str(today-timedelta(days=800)),'to':str(today-timedelta(days=1))})
                fetch(kind+':'+symbol,'fmp',url,{'apikey':fmp})
        for sid in FRED:
            base=dict(series_id=sid,file_type='json',realtime_start=str(today),realtime_end=str(today))
            for kind in ('definition','observations'):
                q=base.copy();path='/fred/series'
                if kind=='observations':path+='/observations';q.update(observation_start=str(today-timedelta(days=800)),observation_end=str(today),units='lin',output_type=1,sort_order='asc',limit=10000,offset=0)
                url='https://api.stlouisfed.org'+path+'?'+urllib.parse.urlencode(q)
                fetch(kind+':'+sid,'fred',url,wire=url+'&api_key='+urllib.parse.quote(fred,safe=''))
        r.kv(native_probes=probes,mutable_products_written=False,existing_credential_only=True)
        scheduler=boto3.client('scheduler',region_name='us-east-1');schedule=scheduler.get_schedule(Name='justhodl-global-stress-6h',GroupName='default')
        r.kv(schedule={k:schedule.get(k) for k in ('Name','State','ScheduleExpression','ScheduleExpressionTimezone')},
            next_work='Native identity and separate dividend-adjusted/price-only histories, exact-date return alignment, descriptive risk metrics, original-source replay and consumer qualification.')


if __name__=='__main__':
    try:main()
    except Exception:
        print('Global Stress preflight failed; inspect the committed report before retrying. No engine was invoked.')
        sys.exit(1)
