"""Read configured market-data sources; retain exact responses and report selected schema fields."""
from concurrent.futures import ThreadPoolExecutor,as_completed
from datetime import datetime,timezone
from pathlib import Path
import ast,hashlib,json,sys,time,urllib.error,urllib.parse,urllib.request
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/ops')]
from managed_secret import managed_secret
from evidence_store import capture,read_verified
from ops_report import report

SAMPLE_SYMBOLS=('AAPL','SPY','HDV','INDA','NVDA','BRK-B')
PATHS=('profile','historical-price-eod/light','historical-price-eod/non-split-adjusted','dividends','splits','ratios-ttm','key-metrics-ttm')
SELECTED={'symbol','currency','exchange','exchangeShortName','isEtf','isFund','isActivelyTrading','country',
 'date','price','close','adjClose','volume','recordDate','paymentDate','declarationDate','adjDividend','dividend','yield','frequency',
 'numerator','denominator','dividendYieldTTM','dividendYieldPercentageTTM','buybackYieldTTM','netBuybackYieldTTM'}

def main():
    s3=boto3.client('s3',region_name='us-east-1');bucket='justhodl-dashboard-live'
    fmp=managed_secret(('FMP_KEY','FMP_API_KEY'),('/justhodl/fmp/api-key',))
    fred=managed_secret(('FRED_API_KEY','FRED_KEY'),('/justhodl/fred/api-key',))
    assert fmp and fred,'configured provider credentials unavailable'
    tree=ast.parse((ROOT/'aws/lambdas/justhodl-carry-surface/source/lambda_function.py').read_text())
    constants={n.targets[0].id:ast.literal_eval(n.value) for n in tree.body if isinstance(n,ast.Assign) and isinstance(n.targets[0],ast.Name) and n.targets[0].id in ('FX_UNIVERSE','FIXED_INCOME','EQUITY_UNIVERSE')}
    ids=sorted(set(constants['FX_UNIVERSE'].values())|{v[0] for v in constants['FIXED_INCOME'].values()})
    requests={}
    for symbol in SAMPLE_SYMBOLS:
        for path in PATHS:
            url='https://financialmodelingprep.com/stable/'+path+'?'+urllib.parse.urlencode({'symbol':symbol})
            requests[('fmp',symbol,path)]=(url,'apikey',fmp)
    for sid in ids:
        url='https://api.stlouisfed.org/fred/series?'+urllib.parse.urlencode({'series_id':sid,'file_type':'json'})
        requests[('fred',sid,'definition')]=(url,'api_key',fred)
    def fetch(item):
        url,key_name,key=item;actual=url+'&'+urllib.parse.urlencode({key_name:key})
        try:
            with urllib.request.build_opener().open(urllib.request.Request(actual,headers={'User-Agent':'JustHodl carry source audit','Accept':'application/json'}),timeout=25) as response:raw=response.read(8*1024*1024+1)
            if not raw or len(raw)>8*1024*1024:raise ValueError('response size bound')
            if len(key)>=12 and key.encode() in raw:raise ValueError('credential echoed')
            doc=json.loads(raw);ref=capture(s3,bucket,'carry-probe',url,raw,datetime.now(timezone.utc))
            assert read_verified(s3,bucket,ref)==raw
            return doc,ref,None
        except Exception as exc:return None,None,('HTTP_'+str(exc.code) if isinstance(exc,urllib.error.HTTPError) else type(exc).__name__)
    with report('ops_5851_carry_original_source_probe') as r:
        r.kv(equity_universe=len(constants['EQUITY_UNIVERSE']),fred_series=len(ids),sample_symbols=SAMPLE_SYMBOLS,
             production_engines_invoked=0,paid_ai_calls=0,notifications_sent=0,private_account_reads=0,portfolio_writes=0)
        successes=0
        # Four bounded workers, small fixed sample. No market orders or engine execution.
        with ThreadPoolExecutor(max_workers=4) as pool:
            jobs={pool.submit(fetch,v):k for k,v in requests.items()}
            for task in as_completed(jobs):
                provider,identity,kind=jobs[task];doc,ref,error=task.result()
                if error:r.kv(provider=provider,identity=identity,kind=kind,status=error);continue
                successes+=1
                if provider=='fred':
                    rows=doc.get('seriess',[]) if isinstance(doc,dict) else []
                    selected=[{k:v for k,v in item.items() if k in {'id','title','units','frequency','frequency_short','observation_start','observation_end','last_updated','seasonal_adjustment'}} for item in rows]
                    r.kv(provider=provider,identity=identity,kind=kind,rows=len(rows),selected=selected,evidence=ref)
                else:
                    rows=doc if isinstance(doc,list) else []
                    samples=[{k:v for k,v in item.items() if k in SELECTED} for item in rows[:3] if isinstance(item,dict)]
                    r.kv(provider=provider,identity=identity,kind=kind,rows=len(rows),fields=sorted(rows[0]) if rows and isinstance(rows[0],dict) else [],
                         newest=rows[0].get('date') if rows else None,oldest=rows[-1].get('date') if rows else None,selected=samples,evidence=ref)
        assert successes>0,'all source requests failed'

if __name__=='__main__':
    try:main()
    except Exception:
        print('Carry source probe failed; inspect sanitized runner report.')
        sys.exit(1)
