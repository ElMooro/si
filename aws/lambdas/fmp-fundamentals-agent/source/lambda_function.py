"""Public FMP market snapshot with explicit provider coverage and safe errors."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
import math
import time
import urllib.error
import urllib.parse
import urllib.request

from managed_secret import managed_secret

BASE = 'https://financialmodelingprep.com/stable/'
WATCH = ['AAPL','MSFT','GOOGL','AMZN','NVDA','META','TSLA','JPM','V','XOM','JNJ','WMT',
         'MA','HD','CVX','LLY','AVGO','NFLX','AMD','VRT','PLTR','SMCI','ARM','MSTR']
INDICES = ['^GSPC','^IXIC','^DJI','^VIX','^TRN']
# The production Function URL owns CORS. Returning it here as well produces
# duplicate allow-origin headers, which browsers reject (verified by ops5271).
HEADERS = {'Content-Type':'application/json','Cache-Control':'no-store'}
CACHE = None
CACHE_UNTIL = 0


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args):return None


def fetch_rows(endpoint, params, api_key):
    diagnostic = {'endpoint':endpoint,'status':'UNAVAILABLE','row_count':0}
    if not api_key:return [],{**diagnostic,'reason':'MANAGED_CREDENTIAL_UNAVAILABLE'}
    query=urllib.parse.urlencode({**params,'apikey':api_key})
    request=urllib.request.Request(BASE+endpoint+'?'+query,headers={'User-Agent':'JustHodl-FMP/20260915'})
    try:
        raw=None
        for attempt in range(3):          # 429/5xx: bounded backoff (2 s, 5 s) honouring Retry-After; then the last-good snapshot serves
            try:
                with urllib.request.build_opener(NoRedirect).open(request,timeout=15) as response:
                    raw=response.read(8_000_001)
                break
            except urllib.error.HTTPError as exc:
                if exc.code in (429,500,502,503,504) and attempt<2:
                    wait=exc.headers.get('Retry-After') if exc.headers else None
                    try: wait=min(float(wait),8.0) if wait else (2.0 if attempt==0 else 5.0)
                    except ValueError: wait=2.0 if attempt==0 else 5.0
                    exc.close();time.sleep(wait);continue
                raise
        if len(raw)>8_000_000:return [],{**diagnostic,'reason':'RESPONSE_SIZE_LIMIT'}
        def reject(value):raise ValueError('non_finite')
        doc=json.loads(raw,parse_constant=reject)
        if not isinstance(doc,list) or any(not isinstance(row,dict) for row in doc):
            return [],{**diagnostic,'reason':'PROVIDER_SCHEMA_OR_ENTITLEMENT_ERROR'}
        json.dumps(doc,allow_nan=False)
        return doc,{**diagnostic,'status':'AVAILABLE' if doc else 'EMPTY','row_count':len(doc)}
    except urllib.error.HTTPError as exc:
        code=exc.code;exc.close()
        return [],{**diagnostic,'reason':'PROVIDER_HTTP_ERROR','http_status':code}
    except Exception:
        return [],{**diagnostic,'reason':'PROVIDER_REQUEST_OR_JSON_FAILED'}


LAST_GOOD_KEY='data/fmp-market-snapshot-last-good.json'
_BUCKET='justhodl-dashboard-live'


def _with_last_good(document):
    """READY/PARTIAL snapshots are stored as the last-good copy; an UNAVAILABLE one (e.g. provider 429 at the top of the
    hour) returns the last-good copy with an explicit degraded label -- never a page of zeros pretending the market is empty."""
    try:
        import boto3
        s3=boto3.client('s3')
        if document.get('status') in ('READY','PARTIAL') and document.get('quotes_ok',0)>0:
            s3.put_object(Bucket=_BUCKET,Key=LAST_GOOD_KEY,Body=json.dumps(document,allow_nan=False).encode(),ContentType='application/json',CacheControl='max-age=60')
            return document
        if document.get('status')=='UNAVAILABLE':
            prior=json.loads(s3.get_object(Bucket=_BUCKET,Key=LAST_GOOD_KEY)['Body'].read())
            codes=sorted({str(v.get('http_status')) for v in (document.get('source_health') or {}).values() if v.get('http_status')})
            prior['degraded']={'live_status':'UNAVAILABLE','live_attempt_at':document.get('generated_at'),'provider_http':codes,
                               'note':'provider rejected the live pull (HTTP %s); showing the last good snapshot from %s' % (','.join(codes) or '?',prior.get('generated_at'))}
            prior['status']='PARTIAL';prior['live_source_health']=document.get('source_health')
            return prior
    except Exception:
        pass
    return document


def snapshot():
    key=managed_secret(('FMP_API_KEY','FMP_KEY'),('/justhodl/fmp/api-key',))
    now=datetime.now(timezone.utc)
    requests={
        'watchlist_quotes':('batch-quote',{'symbols':','.join(WATCH)}),
        'index_quotes':('batch-quote',{'symbols':','.join(INDICES)}),
        'sector_performance':('sector-performance-snapshot',{'date':now.date().isoformat()}),
        'gainers':('biggest-gainers',{}),'losers':('biggest-losers',{}),'actives':('most-actives',{}),
    }
    with ThreadPoolExecutor(max_workers=6) as pool:
        values=list(pool.map(lambda args:fetch_rows(*args,key),requests.values()))
    results=dict(zip(requests,values));sources={name:value[1] for name,value in results.items()}
    quotes={row['symbol']:row for row in results['watchlist_quotes'][0]
            if isinstance(row.get('symbol'),str) and row['symbol'] in WATCH}
    def valid_quote(row):
        price,stamp=row.get('price'),row.get('timestamp')
        return (type(price) in (int,float) and math.isfinite(price) and price>0
                and type(stamp) in (int,float) and math.isfinite(stamp) and 0<stamp<=now.timestamp()+300)
    valid_symbols={symbol for symbol,row in quotes.items() if valid_quote(row)}
    available=sum(row['status']=='AVAILABLE' for row in sources.values())
    status='READY' if available==len(sources) and len(valid_symbols)==len(WATCH) else 'PARTIAL' if available else 'UNAVAILABLE'
    stamp=datetime.now(timezone.utc).isoformat()
    return {'schema_version':'fmp-market-snapshot.v2','agent':'fmp-fundamentals-agent','ts':stamp,
        'generated_at':stamp,'status':status,'watchlist_quotes':quotes,
        'index_quotes':results['index_quotes'][0],'sector_performance':results['sector_performance'][0],
        'movers':{name:results[name][0] for name in ('gainers','losers','actives')},
        'watchlist':WATCH,'quotes_received':len(quotes),'quotes_ok':len(valid_symbols),'quotes_err':len(WATCH)-len(valid_symbols),
        'invalid_quote_symbols':[symbol for symbol in quotes if symbol not in valid_symbols],
        'missing_quote_symbols':[symbol for symbol in WATCH if symbol not in quotes],
        'source_health':sources,'sector_requested_date':now.date().isoformat(),
        'timestamp_scope':'Response collection time; individual provider observation fields are preserved.',
        'execution_eligible':False}


def lambda_handler(event,context):
    global CACHE,CACHE_UNTIL
    event=event if isinstance(event,dict) else {}
    method=(event.get('requestContext') or {}).get('http',{}).get('method',event.get('httpMethod','GET'))
    path=event.get('rawPath','/')
    if method=='OPTIONS':return {'statusCode':204,'headers':HEADERS,'body':''}
    if method!='GET':return {'statusCode':405,'headers':HEADERS,'body':json.dumps({'error':'method_not_allowed'})}
    if path=='/health':
        return {'statusCode':200,'headers':HEADERS,'body':json.dumps({'status':'HANDLER_READY',
            'provider_data_verified':False,'agent':'fmp-fundamentals-agent','watchlist':len(WATCH)})}
    if path not in ('','/'):
        return {'statusCode':404,'headers':HEADERS,'body':json.dumps({'error':'unknown_route'})}
    try:
        if CACHE is None or time.monotonic()>=CACHE_UNTIL:
            document=snapshot()
            document=_with_last_good(document)
            CACHE=document;CACHE_UNTIL=time.monotonic()+60
        return {'statusCode':200,'headers':HEADERS,'body':json.dumps(CACHE,allow_nan=False)}
    except Exception:
        # Never emit exception text, URLs, credential values or a stack trace.
        return {'statusCode':503,'headers':HEADERS,'body':json.dumps({'agent':'fmp-fundamentals-agent',
            'status':'UNAVAILABLE','error':'snapshot_unavailable'})}
