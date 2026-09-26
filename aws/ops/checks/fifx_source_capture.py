"""Bounded original-source inspection; no market score or investment authority."""
from datetime import date,datetime,timezone
from decimal import Decimal,InvalidOperation
import csv,hashlib,io,json,urllib.parse,urllib.request,urllib.error

SERIES=('VIXCLS','DGS10','DEXUSEU','DEXJPUS','DEXUSUK','DTWEXBGS')
SYMBOLS=('^MOVE','^KS11','^HSI','^N225','^GDAXI','^FTSE','^FCHI','000001.SS','^BSESN','^BVSP','^AXJO','^VHSI')
MAX=8*1024*1024

def request_plan(stamp):
    clock=datetime.fromisoformat(stamp.replace('Z','+00:00'))
    if clock.tzinfo is None:raise ValueError('Acquisition cutoff needs a timezone')
    end=int(clock.timestamp());day=clock.astimezone(timezone.utc).date().isoformat()
    plan={sid:{'provider':'fred_csv','series_id':sid,'url':'https://fred.stlouisfed.org/graph/fredgraph.csv?'+
        urllib.parse.urlencode({'id':sid,'cosd':'1988-01-01','coed':day})} for sid in SERIES}
    # Reuse MOVE's complete retained native response; do not acquire it twice.
    for symbol in SYMBOLS[1:]:
        params={'range':'2y','interval':'1d'} if symbol=='^VHSI' else {'period1':315532800,'period2':end,'interval':'1d'}
        plan[symbol]={'provider':'yahoo_chart','symbol':symbol,'url':'https://query1.finance.yahoo.com/v8/finance/chart/'+urllib.parse.quote(symbol,safe='')+'?'+urllib.parse.urlencode(params)}
    return plan

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs):return None

def acquire(spec):
    request=urllib.request.Request(spec['url'],headers={'User-Agent':'Mozilla/5.0 (JustHodl original-source review)','Accept':'application/json,text/csv'})
    opener=urllib.request.build_opener(NoRedirect)
    try:response=opener.open(request,timeout=35)
    except urllib.error.HTTPError as error:response=error
    try:
        raw=response.read(MAX+1);status=response.code
        headers={k:v for k,v in response.headers.items() if k.lower() in ('date','etag','last-modified','content-type')}
    finally:response.close()
    if not 0<len(raw)<=MAX:raise ValueError('Complete bounded response required')
    return raw,{'source_url':spec['url'],'http_status':status,'headers':headers,'acquired_at':datetime.now(timezone.utc).isoformat(),
        'sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw)}

def inspect_csv(raw,sid):
    if sid not in SERIES:raise ValueError('Unreviewed series identity')
    rows=list(csv.reader(io.StringIO(raw.decode('utf-8-sig'),newline='')))
    if not rows or rows[0] not in (['observation_date',sid],['DATE',sid]):raise ValueError('Exact source series header required')
    if not 1<=len(rows)-1<=50000:raise ValueError('Original row population outside bound')
    seen=set();missing=0;nonpositive=0
    for row in rows[1:]:
        if len(row)!=2:raise ValueError('Exactly two original columns required')
        if date.fromisoformat(row[0]).isoformat()!=row[0] or row[0] in seen:raise ValueError('Unique canonical observation dates required')
        seen.add(row[0])
        if row[1] in ('','.'):missing+=1;continue
        value=Decimal(row[1])
        if not value.is_finite() or len(value.as_tuple().digits)>30:raise ValueError('Finite original decimal required')
        nonpositive+=value<=0
    dates=[r[0] for r in rows[1:]]
    if dates!=sorted(dates):raise ValueError('Monotonic original date order required')
    return {'status':'original_csv_inspected','series_id':sid,'headers':rows[0],'original_rows':len(rows)-1,'missing_values':missing,
        'nonpositive_values':nonpositive,'first_date':dates[0],'last_date':dates[-1],'forecast_qualified':False,'point_in_time_qualified':False}

def inspect_quote(raw,symbol):
    if symbol not in SYMBOLS:raise ValueError('Unreviewed quote request')
    doc=json.loads(raw,parse_constant=lambda _:(_ for _ in ()).throw(ValueError('Nonfinite quote JSON')))
    result=(doc.get('chart') or {}).get('result') if isinstance(doc,dict) else None
    if not isinstance(result,list) or len(result)!=1 or not isinstance(result[0],dict):raise ValueError('One original quote result required')
    row=result[0];meta=row.get('meta');stamps=row.get('timestamp');quote=(row.get('indicators') or {}).get('quote')
    if not isinstance(meta,dict) or not isinstance(stamps,list) or not 1<=len(stamps)<=50000 or not isinstance(quote,list) or len(quote)!=1 or not isinstance(quote[0],dict):raise ValueError('Complete original quote schema required')
    arrays=quote[0]
    if 'close' not in arrays or any(not isinstance(v,list) or len(v)!=len(stamps) for v in arrays.values()):raise ValueError('Aligned original quote arrays required')
    if any(type(t) is not int or not 0<=t<=253402214400 for t in stamps) or len(set(stamps))!=len(stamps):raise ValueError('Unique bounded source timestamps required')
    return {'status':'original_quote_inspected','requested_symbol':symbol,'metadata':{k:meta.get(k) for k in
        ('symbol','shortName','longName','instrumentType','currency','exchangeName','exchangeTimezoneName','dataGranularity')},
        'original_rows':len(stamps),'missing_closes':sum(x is None for x in arrays['close']),'arrays':list(arrays),
        'first_timestamp':min(stamps),'last_timestamp':max(stamps),'instrument_identity_qualified':False,
        'official_feed_parity_verified':False,'forecast_qualified':False,'point_in_time_qualified':False}
