"""Retain bounded futures catalogs, sessions and dated-contract bars before inference."""
from collections import Counter
from datetime import date
from threading import Lock
import re, time, urllib.request, urllib.error, urllib.parse
from option_snapshot_capture import decode, bounded, NoRedirect, now, sha, numeric_state

CONTRACT = 'futures-source-capture.v1'
PRODUCTS = {'ES':'XCME','NQ':'XCME','CL':'XNYM','GC':'XCEC','SI':'XCEC','HG':'XCEC','NG':'XNYM'}
MAX_PAGES = 4
MAX_PAGE_BYTES = 4*1024*1024


class Budget:
    def __init__(self,requests=168,byte_limit=64*1024*1024):
        self.requests=0;self.bytes=0;self.max_requests=requests;self.max_bytes=byte_limit;self.lock=Lock()
    def request(self):
        with self.lock:
            if self.requests>=self.max_requests:raise ValueError('Futures request budget exhausted')
            self.requests+=1
    def received(self,size):
        with self.lock:
            self.bytes+=size
            if self.bytes>self.max_bytes:raise ValueError('Futures original byte budget exhausted')


def dates(start, end):
    a,b=date.fromisoformat(start),date.fromisoformat(end)
    if a.isoformat()!=start or b.isoformat()!=end or not 0<=(b-a).days<=120:
        raise ValueError('Explicit bounded futures date window required')


def spec(kind, product, start, end, ticker=None):
    dates(start,end)
    if product not in PRODUCTS: raise ValueError('Reviewed futures product required')
    query={'limit':'1000'}
    if kind in ('products','contracts'):
        query.update(product_code=product,date=end,type='single',sort='date.asc')
        if kind=='contracts':query['active']='true'
        path='/futures/v1/'+kind
    elif kind=='schedules':
        path='/futures/v1/schedules';query.update(product_code=product,trading_venue=PRODUCTS[product],
            **{'session_end_date.gte':start,'session_end_date.lte':end,'sort':'session_end_date.asc'})
    elif kind=='bars':
        if not isinstance(ticker,str) or not re.fullmatch(re.escape(product)+r'[FGHJKMNQUVXZ](?:\d{1,2}|\d{4})',ticker):
            raise ValueError('Dated futures identity required; never use an equity endpoint')
        path='/futures/v1/aggs/'+ticker
        query.update(resolution='1session',sort='window_start.asc',**{'window_start.gte':start,'window_start.lte':end})
    else:raise ValueError('Reviewed futures dataset required')
    return {'kind':kind,'product':product,'ticker':ticker,'from':start,'to':end,'path':path,'query':query}


def canonical(url,scope):
    expected=spec(scope['kind'],scope['product'],scope['from'],scope['to'],scope['ticker'])
    if expected!=scope or not isinstance(url,str) or len(url)>12000:raise ValueError('Bounded original scope required')
    p=urllib.parse.urlsplit(url)
    if (p.scheme!='https' or p.netloc not in ('api.massive.com','api.polygon.io') or p.fragment
            or p.path!=scope['path'] or p.username or p.password):raise ValueError('Unreviewed futures destination')
    clean={}
    for key,value in urllib.parse.parse_qsl(p.query,keep_blank_values=True):
        if key in clean or not value or len(value)>10000:raise ValueError('Ambiguous futures query')
        if key!='apiKey' and key!='cursor' and scope['query'].get(key)!=value:raise ValueError('Futures scope changed')
        clean[key]=value
    clean.pop('apiKey',None)
    if not clean or ('cursor' not in clean and clean!=scope['query']):raise ValueError('Complete query or cursor required')
    return urllib.parse.urlunsplit(('https',p.netloc,p.path,urllib.parse.urlencode(sorted(clean.items())),''))


def initial(scope):return canonical('https://api.massive.com'+scope['path']+'?'+urllib.parse.urlencode(scope['query']),scope)


def request(url,scope,secret,deadline,retain,budget,opener=None):
    if not isinstance(secret,str) or not secret:raise ValueError('Existing managed provider configuration required')
    if canonical(url,scope)!=url:raise ValueError('Canonical futures request required')
    if time.monotonic()>=deadline:return {'status':'time_budget','acquired_at':now(),'http_status':None,'original':None},None
    budget.request();status=None
    req=urllib.request.Request(url+'&apiKey='+urllib.parse.quote(secret,safe=''),headers={
        'User-Agent':'JustHodl-Futures-Original-Research/1.0','Accept':'application/json','Accept-Encoding':'identity'})
    try:
        response=(opener or urllib.request.build_opener(NoRedirect())).open(req,timeout=min(15,max(1,deadline-time.monotonic())))
        status=response.status;raw=bounded(response,MAX_PAGE_BYTES)
    except urllib.error.HTTPError as exc:status=exc.code;raw=bounded(exc,MAX_PAGE_BYTES)
    except (OSError,ValueError,TimeoutError):
        return {'status':'transport_or_body_failure','acquired_at':now(),'http_status':status,'original':None},None
    if secret.encode() in raw:raise ValueError('Credential reflection refused before retention')
    budget.received(len(raw))
    return {'status':'received','acquired_at':now(),'http_status':status,'original':retain(raw)},raw


def envelope(raw):
    doc=decode(raw)
    if not isinstance(doc,dict) or doc.get('status') not in ('OK','DELAYED') or not isinstance(doc.get('results'),list) or len(doc['results'])>1000:
        raise ValueError('Unqualified futures response envelope')
    return doc


def collect(scope,secret,deadline,retain,budget,checkpoint,opener=None):
    url=initial(scope);pages=[];seen=set();started=now();stop='page_limit'
    for index in range(MAX_PAGES):
        identity=sha(url.encode())
        if identity in seen:stop='pagination_cycle';break
        seen.add(identity)
        meta,raw=request(url,scope,secret,deadline,retain,budget,opener)
        meta.update(page=index+1,request_url=url,request_sha256=identity);pages.append(meta)
        checkpoint({'contract':CONTRACT,'scope':scope,'started_at':started,'pages':pages,'stop':'collecting'})
        if meta['status']!='received':stop=meta['status'];break
        if meta['http_status']!=200:stop='provider_http_failure';break
        try:doc=envelope(raw)
        except (ValueError,UnicodeDecodeError):stop='invalid_provider_envelope';break
        if not doc.get('next_url'):stop='complete_returned_pagination';break
        try:url=canonical(doc['next_url'],scope)
        except ValueError:stop='invalid_pagination_address';break
    result={'contract':CONTRACT,'scope':scope,'started_at':started,'completed_at':now(),'pages':pages,'stop':stop,
        'pagination_complete':stop=='complete_returned_pagination','capture_is_atomic':False,
        'market_calendar_completeness_verified':False,'bar_finality_independently_verified':False}
    checkpoint(result);return result


def select_contracts(rows,product,asof,complete):
    """No inferred identities, lexical expiry, price-band identity or truncated front rank."""
    date.fromisoformat(asof)
    if product not in PRODUCTS:raise ValueError('Reviewed product required')
    accepted=[];rejected=Counter();identities=Counter()
    for i,row in enumerate(rows):
        if not isinstance(row,dict):rejected['nonobject']+=1;continue
        ticker=row.get('ticker')
        try:spec('bars',product,asof,asof,ticker)
        except ValueError:rejected['undated_or_foreign_ticker']+=1;continue
        identities[ticker]+=1
        if (row.get('product_code')!=product or row.get('trading_venue')!=PRODUCTS[product]
                or row.get('type')!='single' or row.get('date')!=asof or row.get('active') is not True):
            rejected['unqualified_identity_or_vintage']+=1;continue
        try:
            first,last,settles=(date.fromisoformat(row.get(k)) for k in ('first_trade_date','last_trade_date','settlement_date'))
            if not first<=date.fromisoformat(asof)<=last<=settles:raise ValueError('Invalid contract chronology')
        except (ValueError,TypeError):rejected['unqualified_trade_or_settlement_dates']+=1;continue
        accepted.append({'ticker':ticker,'source_row_index':i,'last_trade_date':last.isoformat(),'settlement_date':settles.isoformat()})
    duplicated=sorted(k for k,n in identities.items() if n>1)
    accepted.sort(key=lambda row:(row['last_trade_date'],row['settlement_date'],row['ticker']))
    qualified=bool(complete and not duplicated and not rejected)
    return {'selected':accepted[:3] if qualified else [],'selection_qualified':qualified,
        'selection_basis':'earliest last trade date among complete active single-contract catalog; not liquidity rank',
        'qualified_contracts':len(accepted),'returned_rows':len(rows),'rejected':dict(rejected),'duplicate_tickers':duplicated}


def summarize(rows,kind):
    valid=[r for r in rows if isinstance(r,dict)]
    result={'returned_rows':len(rows),'object_rows':len(valid),'fields':dict(sorted(Counter(k for r in valid for k in r).items())),
        'no_returns_curve_signals_or_portfolio_decisions_computed':True}
    if kind=='products':
        fields=('product_code','date','trading_venue','type','price_quotation','unit_of_measure',
            'unit_of_measure_qty','trade_currency_code','settlement_currency_code','settlement_method','settlement_type')
        result['returned_specifications']=[{k:(str(r[k]) if k=='unit_of_measure_qty' and r.get(k) is not None else r.get(k))
            for k in fields} for r in valid]
    if kind=='schedules':
        result['event_types']=dict(Counter(str(r.get('event')) for r in valid))
    if kind=='bars':
        times=[r.get('window_start') for r in valid if type(r.get('window_start')) is int]
        result.update(numeric_states={k:dict(Counter(numeric_state(r,k) for r in valid)) for k in
            ('open','high','low','close','settlement_price','volume','dollar_volume','transactions')},
            tickers=dict(Counter(str(r.get('ticker')) for r in valid)),
            session_dates=sorted(set(r['session_end_date'] for r in valid if isinstance(r.get('session_end_date'),str))),
            integer_window_start_count=len(times),distinct_window_start_count=len(set(times)),
            first_window_start_ns=str(min(times)) if times else None,last_window_start_ns=str(max(times)) if times else None,
            close_is_not_settlement=True,dollar_volume_is_not_notional=True)
    return result
