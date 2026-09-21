"""Bounded, original FX quote-bar capture. No regime, flow or carry inference."""
from collections import Counter
from datetime import date, datetime, timezone
from threading import Lock
import time, urllib.request, urllib.error, urllib.parse
from option_snapshot_capture import decode, bounded, NoRedirect, now, sha, numeric_state

CONTRACT = 'fx-quote-capture.v1'
MAX_PAGE_BYTES = 4*1024*1024
MAX_PAGES = 4
PAIRS = {
    'EUR_USD': 'C:EURUSD', 'USD_JPY': 'C:USDJPY', 'GBP_USD': 'C:GBPUSD',
    'USD_CHF': 'C:USDCHF', 'USD_CAD': 'C:USDCAD', 'AUD_USD': 'C:AUDUSD',
    'NZD_USD': 'C:NZDUSD', 'USD_NOK': 'C:USDNOK', 'AUD_JPY': 'C:AUDJPY',
    'EUR_JPY': 'C:EURJPY', 'NZD_JPY': 'C:NZDJPY', 'USD_CNH': 'C:USDCNH',
    'USD_BRL': 'C:USDBRL', 'USD_MXN': 'C:USDMXN', 'USD_ZAR': 'C:USDZAR',
    'USD_KRW': 'C:USDKRW', 'USD_TRY': 'C:USDTRY', 'XAU_USD': 'C:XAUUSD',
    'XAG_USD': 'C:XAGUSD',
}


def path(pair, start, end):
    if pair not in PAIRS: raise ValueError('Reviewed FX identity required')
    a, b = date.fromisoformat(start), date.fromisoformat(end)
    if a.isoformat()!=start or b.isoformat()!=end or not 0 <= (b-a).days <= 180:
        raise ValueError('Bounded explicit calendar window required')
    return '/v2/aggs/ticker/'+PAIRS[pair]+'/range/1/day/'+start+'/'+end


def canonical(url, pair, start, end):
    if not isinstance(url, str) or len(url)>12000: raise ValueError('Bounded cursor required')
    p=urllib.parse.urlsplit(url)
    if (p.scheme!='https' or p.netloc not in ('api.massive.com','api.polygon.io') or p.fragment
            or urllib.parse.unquote(p.path)!=path(pair,start,end) or p.username or p.password):
        raise ValueError('Unreviewed FX request destination')
    clean=[];seen=set()
    for key,value in urllib.parse.parse_qsl(p.query,keep_blank_values=True):
        if key in seen or not value or len(value)>10000: raise ValueError('Ambiguous FX cursor')
        seen.add(key)
        if key=='apiKey': continue
        if key not in ('cursor','limit','sort','adjusted'): raise ValueError('FX request scope changed')
        if key=='limit' and (not value.isdigit() or not 1<=int(value)<=50000): raise ValueError('FX page bound changed')
        if key=='sort' and value!='asc': raise ValueError('FX order changed')
        if key=='adjusted' and value!='true': raise ValueError('FX adjustment request changed')
        clean.append((key,value))
    if not clean: raise ValueError('Explicit FX query required')
    return urllib.parse.urlunsplit(('https',p.netloc,path(pair,start,end),urllib.parse.urlencode(sorted(clean)),''))


def initial(pair,start,end):
    return canonical('https://api.massive.com'+path(pair,start,end)+'?adjusted=true&sort=asc&limit=50000',pair,start,end)


class Budget:
    def __init__(self, requests=76, byte_limit=64*1024*1024):
        self.requests=0;self.bytes=0;self.max_requests=requests;self.max_bytes=byte_limit;self.lock=Lock()
    def request(self):
        with self.lock:
            if self.requests>=self.max_requests: raise ValueError('FX request budget exhausted')
            self.requests+=1
    def received(self,size):
        with self.lock:
            self.bytes+=size
            if self.bytes>self.max_bytes: raise ValueError('FX original byte budget exhausted')


def request(url,pair,start,end,secret,deadline,retain,budget,opener=None):
    if not isinstance(secret,str) or not secret: raise ValueError('Existing managed source configuration required')
    if canonical(url,pair,start,end)!=url: raise ValueError('Canonical FX request required')
    if time.monotonic()>=deadline: return {'status':'time_budget','acquired_at':now(),'http_status':None,'original':None},None
    budget.request();status=None
    req=urllib.request.Request(url+'&apiKey='+urllib.parse.quote(secret,safe=''),headers={
        'User-Agent':'JustHodl-FX-Original-Research/1.0','Accept':'application/json','Accept-Encoding':'identity'})
    try:
        response=(opener or urllib.request.build_opener(NoRedirect())).open(req,timeout=min(15,max(1,deadline-time.monotonic())))
        status=response.status;raw=bounded(response,MAX_PAGE_BYTES)
    except urllib.error.HTTPError as exc: status=exc.code;raw=bounded(exc,MAX_PAGE_BYTES)
    except (OSError,ValueError,TimeoutError):
        return {'status':'transport_or_body_failure','acquired_at':now(),'http_status':status,'original':None},None
    if secret.encode() in raw: raise ValueError('Credential reflection rejected before retention')
    budget.received(len(raw))
    return {'status':'received','acquired_at':now(),'http_status':status,'original':retain(raw)},raw


def envelope(raw,pair):
    doc=decode(raw)
    if (not isinstance(doc,dict) or doc.get('ticker')!=PAIRS[pair] or doc.get('status') not in ('OK','DELAYED')
            or not isinstance(doc.get('results'),list) or len(doc['results'])>500):
        raise ValueError('Unqualified FX response identity or row count')
    count=doc.get('resultsCount')
    if count is not None and (type(count) is not int or count!=len(doc['results'])):
        raise ValueError('FX returned count differs')
    return doc


def collect(pair,start,end,secret,deadline,retain,budget,checkpoint,opener=None):
    url=initial(pair,start,end);pages=[];seen=set();started=now();stop='page_limit'
    for index in range(MAX_PAGES):
        identity=sha(url.encode())
        if identity in seen: stop='pagination_cycle';break
        seen.add(identity)
        meta,raw=request(url,pair,start,end,secret,deadline,retain,budget,opener)
        meta.update(page=index+1,request_url=url,request_sha256=identity);pages.append(meta)
        checkpoint({'contract':CONTRACT,'pair':pair,'started_at':started,'pages':pages,'stop':'collecting'})
        if meta['status']!='received': stop=meta['status'];break
        if meta['http_status']!=200: stop='provider_http_failure';break
        try: doc=envelope(raw,pair)
        except (ValueError,UnicodeDecodeError): stop='invalid_provider_envelope';break
        if not doc.get('next_url'):
            limit=int(dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(url).query)).get('limit','5000'))
            stop='base_aggregate_limit_may_bind' if type(doc.get('queryCount')) is int and doc['queryCount']>=limit else 'complete_returned_pagination'
            break
        try: url=canonical(doc['next_url'],pair,start,end)
        except ValueError: stop='invalid_pagination_address';break
    result={'contract':CONTRACT,'pair':pair,'provider_ticker':PAIRS[pair],'from':start,'to':end,
        'started_at':started,'completed_at':now(),'pages':pages,'stop':stop,
        'pagination_complete':stop=='complete_returned_pagination','capture_is_atomic':False,
        'full_calendar_coverage_verified':False,'bar_finality_independently_verified':False,
        'exchange_trades_or_volume_observed':False}
    checkpoint(result);return result


def summarize(rows):
    valid=[r for r in rows if isinstance(r,dict)]
    timestamps=[r.get('t') for r in valid if type(r.get('t')) is int and 946684800000<=r['t']<=4102444800000]
    def stamp(t):return datetime.fromtimestamp(t/1000,timezone.utc).isoformat()
    return {'returned_rows':len(rows),'object_rows':len(valid),'fields':dict(sorted(Counter(k for r in valid for k in r).items())),
        'close_states':dict(Counter(numeric_state(r,'c') for r in valid)),
        'plausible_window_start_count':len(timestamps),'distinct_window_start_count':len(set(timestamps)),
        'duplicate_timestamp_rows':sum(n-1 for n in Counter(timestamps).values() if n>1),
        'first_window_start':stamp(min(timestamps)) if timestamps else None,
        'last_window_start':stamp(max(timestamps)) if timestamps else None,
        'window_start_is_not_quote_or_close_timestamp':True,'no_return_or_regime_computed':True}
