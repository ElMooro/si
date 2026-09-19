"""Retain ETF issuer originals and dated vendor fields; no engine invocation."""
from datetime import datetime,timezone
from pathlib import Path
from urllib.parse import urlencode
import ast,csv,io,json,sys,urllib.request,urllib.error
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/ops')]
from evidence_store import capture,read_verified
from managed_secret import managed_secret
from ops_report import report

SOURCE=ROOT/'aws/lambdas/justhodl-etf-true-flows/source/lambda_function.py'
CONSTANTS={}
for statement in ast.parse(SOURCE.read_text()).body:
    if isinstance(statement,ast.Assign):
        for target in statement.targets:
            if isinstance(target,ast.Name) and target.id in ('ISHARES_SCREENER','SSGA_FUNDFINDER','PROSHARES_HIST','PROSHARES','ETFS'):
                CONSTANTS[target.id]=ast.literal_eval(statement.value)

def acquire(s3,bucket,url,key=None):
    actual=url+('&'+urlencode({'apikey':key}) if key else '')
    with urllib.request.urlopen(urllib.request.Request(actual,headers={'User-Agent':'JustHodl original ETF research audit (ops@justhodl.ai)'}),timeout=45) as response:
        raw=response.read(24*1024*1024+1);headers={name:response.headers.get(name) for name in ('Content-Type','Last-Modified','ETag','Date')}
    if not raw or len(raw)>24*1024*1024:raise ValueError('source response bound')
    if key and key.encode() in raw:raise ValueError('credential echoed')
    at=datetime.now(timezone.utc);ref={'url':url,'acquired_at':at.isoformat(),'response_headers':headers,'evidence':capture(s3,bucket,'etf_original',url,raw,at)}
    assert read_verified(s3,bucket,ref['evidence'])==raw,'retained original differs'
    return ref,raw

def json_summary(doc):
    samples=[];matched=0;visited=0
    wanted={'IVV','TLT','EWT','SPY','QQQ','TQQQ','SQQQ','UVXY'}
    def walk(node):
        nonlocal matched,visited
        visited+=1
        if visited>2000000:raise ValueError('JSON structure bound')
        if isinstance(node,dict):
            ticker=next((node.get(k) for k in ('localExchangeTicker','fundTicker','ticker','symbol') if isinstance(node.get(k),str)),None)
            if ticker:
                matched+=1
                if ticker in wanted:
                    samples.append({'ticker':ticker,'record':node})
            for value in node.values():walk(value)
        elif isinstance(node,list):
            for value in node:walk(value)
    walk(doc)
    return {'shape':type(doc).__name__,'root_keys':list(doc)[:25] if isinstance(doc,dict) else None,
            'ticker_records':matched,'samples':samples[:10],
            'unmatched_sample':doc[:2] if isinstance(doc,list) and not samples else None}

def csv_summary(raw):
    rows=list(csv.reader(io.StringIO(raw.decode('utf-8-sig'))))
    assert rows and len(rows)<200000,'CSV response bound'
    return {'rows':len(rows)-1,'header':rows[0],'first_rows':rows[1:4],'last_rows':rows[-3:]}

def main():
    s3=boto3.client('s3',region_name='us-east-1');bucket='justhodl-dashboard-live'
    key=managed_secret(('FMP_KEY','FMP_API_KEY'),('/justhodl/fmp/api-key',));assert key,'configured vendor credential unavailable'
    urls=[('ishares_screener',CONSTANTS['ISHARES_SCREENER'],'json',False),
          ('ssga_fundfinder',CONSTANTS['SSGA_FUNDFINDER'],'json',False),
          ('proshares_current','https://accounts.profunds.com/etfdata/historical_nav.csv','csv',False),
          ('proshares_splits','https://accounts.profunds.com/etfdata/etf_splits.csv','csv',False),
          ('ishares_ivv_page','https://www.ishares.com/us/products/239726/ishares-core-sp-500-etf','html',False),
          ('ishares_ivv_download','https://www.blackrock.com/varnish-api/blk-one01-product-data/product-data/api/v1/get-fund-document?appSubType=ISHARES&appType=PRODUCT_PAGE&component=fundDownload&locale=en_US&portfolioId=239726&targetSite=us-ishares&userType=individual','document',False)]
    for ticker in sorted(CONSTANTS['PROSHARES']):
        urls.append(('proshares_'+ticker,CONSTANTS['PROSHARES_HIST'].format(t=ticker),'csv',False))
    for ticker in ('SPY','QQQ','IVV','TQQQ'):
        for endpoint in ('shares-float','etf/info','quote'):
            urls.append(('vendor_'+ticker+'_'+endpoint.replace('/','_'),'https://financialmodelingprep.com/stable/'+endpoint+'?'+urlencode({'symbol':ticker}),'json',True))
    errors={};refs={};summaries={}
    with report('ops_5866_etf_original_source_probe') as r:
        r.kv(production_engines_invoked=0,paid_ai_calls=0,notifications_sent=0,private_account_reads=0,portfolio_writes=0,
             configured_universe=len({v for members in CONSTANTS['ETFS'].values() for v in members}))
        for label,url,kind,vendor in urls:
            try:
                ref,raw=acquire(s3,bucket,url,key if vendor else None);refs[label]=ref
                summary=json_summary(json.loads(raw)) if kind=='json' else csv_summary(raw) if kind=='csv' else {'bytes':len(raw),'prefix':raw[:100].decode('utf-8','replace')}
                summaries[label]=summary;r.kv(source=label,original=ref,summary=summary)
            except Exception as exc:errors[label]='HTTP_'+str(exc.code) if isinstance(exc,urllib.error.HTTPError) else type(exc).__name__
        r.kv(source_status_codes=errors,retained_responses=len(refs),
             audit_scope='Original ETF source identity, dates, units and corporate-action evidence only. No production packet, classification or portfolio changed.')
        assert 'ishares_screener' in refs and 'proshares_splits' in refs,'required native source unavailable'
        assert any(k.startswith('proshares_') and k not in ('proshares_splits','proshares_current') for k in refs),'native histories unavailable'

if __name__=='__main__':
    try:main()
    except Exception:
        print('ETF original-source audit failed; inspect sanitized committed report.')
        sys.exit(1)
