"""Observed ETF quotes and explicitly limited sector-ETF breadth."""
import json
import math
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone
from managed_secret import managed_secret
from public_provider_json import ProviderError, error_metadata, get_json

ENDPOINTS={'market_overview':['SPY','QQQ','IWM','DIA','VTI'],
           'sector_etfs':['XLF','XLK','XLE','XLV','XLI','XLY','XLP','XLU','XLRE','XLB','XLC']}
_CACHE=None
_CACHE_AT=0

def numeric(value):
    try:
        n=float(value)
        return n if not isinstance(value,bool) and math.isfinite(n) else None
    except (ValueError,TypeError):return None

def fetch_quote(symbol,key):
    try:
        data=get_json('https://www.alphavantage.co/query?'+urllib.parse.urlencode({'function':'GLOBAL_QUOTE','symbol':symbol,'apikey':key}))
        if any(k in data for k in ('Error Message','Note','Information')):raise ProviderError('PROVIDER_REJECTED_OR_RATE_LIMITED')
        q=data.get('Global Quote')
        if not isinstance(q,dict) or q.get('01. symbol')!=symbol:raise ProviderError('PROVIDER_SCHEMA_INVALID')
        price=numeric(q.get('05. price'));day=q.get('07. latest trading day')
        if price is None or price<=0:raise ProviderError('PROVIDER_PRICE_UNAVAILABLE')
        try:observed=date.fromisoformat(day)
        except (ValueError,TypeError):raise ProviderError('PROVIDER_OBSERVATION_DATE_INVALID')
        if observed>datetime.now(timezone.utc).date():raise ProviderError('PROVIDER_OBSERVATION_DATE_INVALID')
        volume=numeric(q.get('06. volume'));previous=numeric(q.get('08. previous close'))
        return {'symbol':symbol,'status':'OBSERVED','price':price,'change':numeric(q.get('09. change')),
                'change_percent':numeric(str(q.get('10. change percent','')).removesuffix('%')),
                'volume':int(volume) if volume is not None and volume>=0 and volume.is_integer() else None,
                'latest_trading_day':day,'previous_close':previous if previous is not None and previous>0 else None,
                'provider_quote':q,'freshness_status':'CALENDAR_AND_SESSION_UNVERIFIED'}
    except ProviderError as exc:return {'symbol':symbol,'status':'UNAVAILABLE',**error_metadata(exc)}
    except Exception:return {'symbol':symbol,'status':'UNAVAILABLE','error':'PROVIDER_SCHEMA_INVALID'}

def analyze_market_breadth(data):
    rows=list(data.get('sector_etfs',{}).values());expected=len(ENDPOINTS['sector_etfs'])
    usable=[r for r in rows if r.get('status')=='OBSERVED' and numeric(r.get('change')) is not None]
    days={r['latest_trading_day'] for r in usable};complete=len(usable)==expected and len(days)==1
    advancing=sum(r['change']>0 for r in usable);declining=sum(r['change']<0 for r in usable)
    unchanged=sum(r['change']==0 for r in usable)
    ratio=advancing/(advancing+declining) if complete and advancing+declining else None
    direction=('POSITIVE' if ratio>0.7 else 'NEGATIVE' if ratio<=0.3 else 'NEUTRAL') if ratio is not None else ('UNCHANGED' if complete else 'UNKNOWN')
    return {'market_breadth':direction,'breadth_ratio':ratio,'advancing_sectors':advancing,'declining_sectors':declining,
            'unchanged_sectors':unchanged,'observed_sectors':len(usable),'expected_sectors':expected,
            'same_observation_date':len(days)==1,'observation_dates':sorted(days),
            'scope':'11 sector ETFs; not exchange-wide advance/decline breadth',
            'freshness_status':'CALENDAR_AND_SESSION_UNVERIFIED','execution_eligible':False}

def response(status,body):
    # Production Function URL CORS is verified in runner receipt 5280.
    return {'statusCode':status,'headers':{'Content-Type':'application/json','Cache-Control':'no-store'},'body':json.dumps(body,allow_nan=False)}

def lambda_handler(event,context):
    global _CACHE,_CACHE_AT
    event=event if isinstance(event,dict) else {}
    path=event.get('rawPath',event.get('path','/'))
    if path=='/health':return response(200,{'agent':'alphavantage-market-agent','status':'HANDLER_READY','provider_data_verified':False})
    if path!='/':return response(404,{'error':'unknown_route'})
    if _CACHE is not None and time.monotonic()-_CACHE_AT<300:return response(200,{**_CACHE,'served_from_warm_cache':True})
    try:
        key=managed_secret(('AV_KEY','ALPHAVANTAGE_KEY','ALPHA_VANTAGE_API_KEY','ALPHAVANTAGE_API_KEY'),('/justhodl/alphavantage/api-key',))
        symbols=[s for group in ENDPOINTS.values() for s in group]+['VIX']
        if key:
            with ThreadPoolExecutor(max_workers=4) as pool:rows=dict(zip(symbols,pool.map(lambda s:fetch_quote(s,key),symbols)))
        else:rows={s:{'symbol':s,'status':'UNAVAILABLE','error':'PROVIDER_CREDENTIAL_UNAVAILABLE'} for s in symbols}
        results={group:{s:rows[s] for s in names} for group,names in ENDPOINTS.items()}
        vix=rows['VIX']
        results['sentiment']={'vix':vix.get('price'),'vix_quote':vix,'fear_greed':None,
                              'scope':'VIX quote only; no Fear & Greed index supplied'}
        count=sum(r['status']=='OBSERVED' for r in rows.values());stamp=datetime.now(timezone.utc).isoformat()
        body={'agent':'alphavantage-market-agent','status':'READY' if count==len(symbols) else 'PARTIAL' if count else 'UNAVAILABLE',
              'generated_at':stamp,'timestamp':stamp,'market_data':results,'analysis':analyze_market_breadth(results),
              'coverage':{'expected_quotes':len(symbols),'observed_quotes':count},'recommendations':[],
              'execution_eligible':False,'served_from_warm_cache':False}
        _CACHE=body;_CACHE_AT=time.monotonic()
        return response(200,body)
    except Exception:return response(503,{'agent':'alphavantage-market-agent','status':'UNAVAILABLE','error':'snapshot_unavailable'})
