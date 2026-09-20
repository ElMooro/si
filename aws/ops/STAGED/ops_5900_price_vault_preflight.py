"""Inspect actual quote shapes and preserve the whole public vault; no producer invocation."""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import base64, hashlib, io, json, sys, urllib.error, urllib.parse, urllib.request, zipfile
import boto3

ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/shared'),str(ROOT/'aws/lambdas/justhodl-tradingview/source')]
from ops_report import report
from evidence_store import capture, read_verified
from fred_level_io import immutable, body


def denied(url):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,method='HEAD',headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=25):return False
    except urllib.error.HTTPError as exc:return exc.code in (401,403,404)


def main():
    bucket,fn='justhodl-dashboard-live','justhodl-tradingview'
    lam=boto3.client('lambda',region_name='us-east-1');s3=boto3.client('s3',region_name='us-east-1')
    with report('ops_5900_price_vault_preflight') as r:
        cfg=lam.get_function_configuration(FunctionName=fn)
        url=lam.get_function(FunctionName=fn)['Code']['Location']
        with urllib.request.urlopen(url,timeout=45) as response:archive=response.read(32*1024*1024+1)
        assert len(archive)<=32*1024*1024 and base64.b64encode(hashlib.sha256(archive).digest()).decode()==cfg['CodeSha256']
        with zipfile.ZipFile(io.BytesIO(archive)) as z:
            for name in ('lambda_function.py','fred_level_io.py','fred_level_model.py'):
                assert z.read(name)==(ROOT/'aws/lambdas'/fn/'source'/name).read_bytes()
        r.kv(runtime_code_sha256=cfg['CodeSha256'],runtime_sources_match=True,producer_invocations=0,
             private_account_reads=0,paid_ai_calls=0,notifications_sent=0,portfolio_writes=0)
        raw=body(s3.get_object(Bucket=bucket,Key='data/tradingview.json'))
        with urllib.request.urlopen(urllib.request.Request('https://justhodl.ai/data/tradingview.json',headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=45) as response:
            assert json.loads(response.read(32*1024*1024+1))==json.loads(raw)
        key='audit-private/20260909-originals/price-vault/'+hashlib.sha256(raw).hexdigest()+'.bin'
        ref=immutable(s3,bucket,key,raw,private=True)
        assert denied('https://'+bucket+'.s3.amazonaws.com/'+key) and denied('https://justhodl.ai/'+key)
        r.kv(whole_preceding_product={**ref,'anonymous_denied':True},symbols=len(json.loads(raw)['symbols']))
        # Existing provider credentials stay in runner memory; never emit environment/config.
        env=cfg.get('Environment',{}).get('Variables',{})
        fmp=env.get('FMP_KEY') or env.get('FMP_API_KEY')
        if not fmp:
            fmp=boto3.client('ssm',region_name='us-east-1').get_parameter(Name='/justhodl/fmp/api-key',WithDecryption=True)['Parameter']['Value']
        poly=env.get('POLYGON_KEY')
        now=datetime.now(timezone.utc);end=(now-timedelta(days=1)).date();start=end-timedelta(days=10)
        probes=[]
        def fetch(label,provider,url):
            try:
                req=urllib.request.Request(url,headers={'User-Agent':'Mozilla/5.0' if provider=='yahoo' else 'JustHodl-price-audit/1.0'})
                with urllib.request.urlopen(req,timeout=25) as response:content=response.read(4*1024*1024+1)
                assert len(content)<=4*1024*1024
                parsed=json.loads(content);receipt=capture(s3,bucket,provider,url,content)
                assert read_verified(s3,bucket,receipt)==content
                probes.append({'name':label,'evidence':receipt,'shape':'list' if isinstance(parsed,list) else 'object',
                    'fields':sorted(parsed[0] if isinstance(parsed,list) and parsed else parsed) if parsed else []})
            except Exception as exc:
                probes.append({'name':label,'failure':type(exc).__name__})
        fetch('fmp-quotes','fmp','https://financialmodelingprep.com/stable/batch-quote?'+urllib.parse.urlencode({'symbols':'AAPL,MSFT,NVDA','apikey':fmp}))
        fetch('fmp-profile','fmp','https://financialmodelingprep.com/stable/profile?'+urllib.parse.urlencode({'symbol':'AAPL','apikey':fmp}))
        for symbol in ('DX-Y.NYB','^MOVE','GC=F','BTC-USD','000001.SS','USCA'):
            fetch('yahoo:'+symbol,'yahoo','https://query1.finance.yahoo.com/v8/finance/chart/'+urllib.parse.quote(symbol,safe='')+'?range=5d&interval=1d')
        if poly:
            fetch('polygon-prev:EFS','polygon','https://api.polygon.io/v2/aggs/ticker/EFS/prev?'+urllib.parse.urlencode({'adjusted':'true','apiKey':poly}))
            fetch('polygon-range:EFS','polygon',f'https://api.polygon.io/v2/aggs/ticker/EFS/range/1/day/{start}/{end}?'+urllib.parse.urlencode({'adjusted':'true','sort':'desc','limit':50000,'apiKey':poly}))
        else:probes.append({'name':'polygon','failure':'ExistingCredentialUnavailable'})
        r.kv(public_source_probes=probes,mutable_vault_written=False)


if __name__=='__main__':
    try:main()
    except Exception:
        print('Price vault preflight failed; inspect the committed report before any refresh.')
        sys.exit(1)
