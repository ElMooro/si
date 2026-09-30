"""Exact public FRED API request and complete original JSON protocol."""
from datetime import date,datetime,timezone
from decimal import Decimal
import json,math,re,urllib.parse

START='1988-01-01'
LIMIT=50000
SERIES=('VIXCLS','DGS10','DEXUSEU','DEXJPUS','DEXUSUK','DTWEXBGS')

def day(value):
    if type(value) is not str or date.fromisoformat(value).isoformat()!=value:
        raise ValueError('Exact source date required')
    return value

def parameters(sid,as_of):
    if sid not in SERIES:raise ValueError('Reviewed FRED identity required')
    day(as_of)
    if as_of<START:raise ValueError('Reviewed observation range required')
    return {'series_id':sid,'file_type':'json','realtime_start':as_of,'realtime_end':as_of,
            'observation_start':START,'observation_end':as_of,'units':'lin',
            'sort_order':'asc','limit':str(LIMIT),'offset':'0','output_type':'1'}

def source_url(sid,as_of):
    return 'https://api.stlouisfed.org/fred/series/observations?'+urllib.parse.urlencode(parameters(sid,as_of))

def request_identity(url,sid,as_of):
    parsed=urllib.parse.urlsplit(url)
    if (parsed.scheme!='https' or parsed.netloc!='api.stlouisfed.org' or parsed.path!='/fred/series/observations'
        or parsed.fragment or parsed.username or parsed.password or parsed.port):raise ValueError('Exact FRED API origin required')
    values=urllib.parse.parse_qs(parsed.query,strict_parsing=True,keep_blank_values=True)
    if values!={k:[v] for k,v in parameters(sid,as_of).items()}:raise ValueError('Exact public request controls required')
    return parameters(sid,as_of)

def request_day(url,sid,received_at):
    clock=datetime.fromisoformat(received_at.replace('Z','+00:00'))
    if clock.tzinfo is None:raise ValueError('Explicit original receipt timezone required')
    values=urllib.parse.parse_qs(urllib.parse.urlsplit(url).query,strict_parsing=True,keep_blank_values=True)
    stamps=values.get('realtime_start')
    if type(stamps) is not list or len(stamps)!=1:raise ValueError('One original request day required')
    as_of=day(stamps[0]);request_identity(url,sid,as_of)
    if not 0<=(clock.astimezone(timezone.utc).date()-date.fromisoformat(as_of)).days<=1:
        raise ValueError('Original request day is future or too old')
    return as_of

def unique(items):
    out={}
    for key,value in items:
        if key in out:raise ValueError('Duplicate FRED response member')
        out[key]=value
    return out

def original(raw,sid,url,as_of):
    request_identity(url,sid,as_of)
    if type(raw) is not bytes or not 0<len(raw)<=8*1024*1024:raise ValueError('Whole bounded FRED response required')
    def invalid(value):raise ValueError('Finite FRED JSON required')
    def finite(token):
        value=float(token)
        if not math.isfinite(value):raise ValueError('Finite FRED JSON required')
        return value
    doc=json.loads(raw.decode('utf-8'),object_pairs_hook=unique,parse_constant=invalid,parse_float=finite)
    if type(doc) is not dict:raise ValueError('Original FRED envelope required')
    expected={'realtime_start':as_of,'realtime_end':as_of,'observation_start':START,'observation_end':as_of,
              'units':'lin','output_type':1,'file_type':'json','order_by':'observation_date','sort_order':'asc','offset':0,'limit':LIMIT}
    for key,value in expected.items():
        if type(doc.get(key)) is not type(value) or doc[key]!=value:raise ValueError('Original FRED control differs: '+key)
    count=doc.get('count');observations=doc.get('observations')
    if type(count) is not int or not 0<count<=LIMIT or type(observations) is not list or len(observations)!=count:
        raise ValueError('Complete requested FRED population required')
    rows=[]
    for i,item in enumerate(observations):
        if type(item) is not dict or set(item)!={'realtime_start','realtime_end','date','value'}:
            raise ValueError('Whole original FRED row required')
        if item['realtime_start']!=as_of or item['realtime_end']!=as_of:
            raise ValueError('Original FRED row vintage differs')
        stamp=day(item['date']);value=item['value']
        if not START<=stamp<=as_of or type(value) is not str:raise ValueError('Bound source row and numeric text required')
        if value!='.':
            if not re.fullmatch(r'[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[Ee][+-]?[0-9]+)?',value):
                raise ValueError('Exact finite original numeric token required')
            number=Decimal(value)
            if not number.is_finite() or len(number.as_tuple().digits)>40 or not -40<=number.as_tuple().exponent<=15:
                raise ValueError('Original value outside reviewed precision')
        if rows and stamp<=rows[-1]['date']:raise ValueError('Unique ascending source dates required')
        rows.append({'original_row':i,'date':stamp,'value':value})
    return rows,{'requested_start':START,'requested_end':as_of,'realtime_start':as_of,'realtime_end':as_of,
                 'offset':0,'limit':LIMIT,'returned_rows':count,'reported_rows':count,
                 'complete_requested_window':True,'full_series_history':False,'point_in_time_backtest':False}
