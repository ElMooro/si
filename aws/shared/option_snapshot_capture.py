"""Bounded original option snapshots. Collection is not a positioning inference."""
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal
import hashlib, json, re, time, urllib.request, urllib.error, urllib.parse

VERSION = 'option-snapshot-capture.v1'
MAX_PAGES = 128
MAX_PAGE_BYTES = 4*1024*1024


def sha(raw): return hashlib.sha256(raw).hexdigest()
def now(): return datetime.now(timezone.utc).isoformat()


def decode(raw):
    def reject(_): raise ValueError('Nonfinite source number')
    def unique(pairs):
        result = {}
        for k, v in pairs:
            if k in result: raise ValueError('Duplicate source key')
            result[k] = v
        return result
    return json.loads(raw, parse_float=Decimal, parse_constant=reject, object_pairs_hook=unique)


def ticker(symbol):
    if not isinstance(symbol, str) or not re.fullmatch(r'[A-Z][A-Z0-9.]{0,9}', symbol):
        raise ValueError('Invalid underlying identity')
    return symbol


def initial_url(symbol):
    return 'https://api.polygon.io/v3/snapshot/options/'+ticker(symbol)+'?limit=250&sort=ticker&order=asc'


def next_url(value, symbol):
    ticker(symbol)
    if not isinstance(value, str) or len(value)>12000: raise ValueError('Invalid source cursor')
    p = urllib.parse.urlsplit(value)
    if (p.scheme!='https' or p.netloc not in ('api.polygon.io','api.massive.com')
            or p.path!='/v3/snapshot/options/'+symbol or p.fragment or p.username or p.password):
        raise ValueError('Unreviewed source destination')
    clean, seen = [], set()
    for key, v in urllib.parse.parse_qsl(p.query, keep_blank_values=True):
        if key in seen or not v or len(v)>10000: raise ValueError('Ambiguous source cursor')
        seen.add(key)
        if key=='apiKey': continue
        if key not in ('cursor','limit','sort','order'): raise ValueError('Source scope changed')
        if key=='limit' and (not v.isdigit() or not 1<=int(v)<=250): raise ValueError('Source page bound changed')
        if key=='sort' and v!='ticker': raise ValueError('Source order changed')
        if key=='order' and v!='asc': raise ValueError('Source order changed')
        clean.append((key,v))
    if not clean: raise ValueError('Source cursor required')
    return urllib.parse.urlunsplit(('https',p.netloc,p.path,urllib.parse.urlencode(sorted(clean)),''))


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs): raise ValueError('Provider redirect refused')


def bounded(stream, limit):
    try: raw=stream.read(limit+1)
    finally: stream.close()
    if len(raw)>limit: raise ValueError('Source byte bound exceeded')
    return raw


def request(url, secret, limit, deadline, retain, budget, opener=None):
    """Only the existing option sources; never send credentials to a cursor host."""
    p=urllib.parse.urlsplit(url)
    if secret:
        symbol=p.path.rsplit('/',1)[-1]
        if url!=next_url(url,symbol): raise ValueError('Canonical source destination required')
    elif (p.scheme!='https' or p.netloc!='cdn.cboe.com' or p.query or p.fragment
            or not re.fullmatch(r'/api/global/delayed_quotes/options/[A-Z][A-Z0-9.]{0,9}\.json',p.path)):
        raise ValueError('Unreviewed public option source')
    if time.monotonic()>=deadline: return {'status':'time_budget','acquired_at':now(),'original':None,'http_status':None},None
    if budget['requests']>=budget['max_requests']: raise ValueError('Request budget exhausted')
    budget['requests']+=1
    full=url+('&apiKey='+urllib.parse.quote(secret,safe='') if secret else '')
    req=urllib.request.Request(full,headers={'User-Agent':'JustHodl-Option-Source-Audit/1.0','Accept':'application/json','Accept-Encoding':'identity'})
    raw=None;status=None
    try:
        response=(opener or urllib.request.build_opener(NoRedirect())).open(req,timeout=min(25,max(1,deadline-time.monotonic())))
        status=response.status;raw=bounded(response,limit)
    except urllib.error.HTTPError as exc:
        status=exc.code;raw=bounded(exc,limit)
    except (OSError,ValueError,TimeoutError):
        return {'status':'transport_or_body_failure','acquired_at':now(),'original':None,'http_status':status},None
    if secret and secret.encode() in raw: raise ValueError('Credential reflection rejected before retention')
    budget['bytes']+=len(raw)
    if budget['bytes']>budget['max_bytes']: raise ValueError('Total source byte budget exceeded')
    return {'status':'received','http_status':status,'acquired_at':now(),'original':retain(raw)},raw


def collect(symbol, secret, deadline, retain, budget, checkpoint, opener=None):
    if not secret: raise ValueError('Existing managed source configuration required')
    symbol=ticker(symbol);url=next_url(initial_url(symbol),symbol)
    pages=[];seen=set();stop='page_limit';started=now()
    for index in range(MAX_PAGES):
        identity=sha(url.encode())
        if identity in seen: stop='pagination_cycle';break
        seen.add(identity)
        meta,raw=request(url,secret,MAX_PAGE_BYTES,deadline,retain,budget,opener)
        meta.update(page=index+1,request_url=url,request_sha256=identity)
        pages.append(meta)
        checkpoint({'underlying':symbol,'started_at':started,'pages':pages,'stop':'collecting'})
        if meta['status']!='received': stop=meta['status'];break
        if meta['http_status']!=200: stop='provider_http_failure';break
        try:
            doc=decode(raw)
            if (not isinstance(doc,dict) or doc.get('status') not in ('OK','DELAYED')
                    or not isinstance(doc.get('results'),list) or len(doc['results'])>250):
                raise ValueError('Unexpected snapshot envelope')
        except (ValueError,UnicodeDecodeError): stop='invalid_provider_envelope';break
        if not doc.get('next_url'): stop='complete_returned_pagination';break
        try: url=next_url(doc['next_url'],symbol)
        except ValueError: stop='invalid_pagination_address';break
    result={'underlying':symbol,'started_at':started,'completed_at':now(),'pages':pages,'stop':stop,
        'pagination_complete':stop=='complete_returned_pagination','capture_is_atomic':False,
        'exchange_chain_completeness_verified':False}
    checkpoint(result)
    return result


def numeric_state(row, key):
    if key not in row:return 'missing'
    value=row[key]
    if value is None:return 'null'
    if isinstance(value,bool) or not isinstance(value,(int,Decimal)):return 'invalid_type'
    if not Decimal(value).is_finite():return 'nonfinite'
    return 'zero' if value==0 else 'positive' if value>0 else 'negative'


def summary(rows):
    """Field presence and raw timestamp range; no synthetic session/OI date."""
    valid=[r for r in rows if isinstance(r,dict)]
    out={'returned_rows':len(rows),'object_rows':len(valid),'row_fields':dict(Counter(k for r in valid for k in r))}
    out['numeric_states']={name:dict(Counter(numeric_state((r.get(parent) or {}) if parent else r,key)
        if isinstance((r.get(parent) or {}) if parent else r,dict) else 'invalid_parent'
        for r in valid)) for name,parent,key in (
        ('daily_volume','day','volume'),('open_interest',None,'open_interest'),('vendor_iv',None,'implied_volatility'),
        ('gamma','greeks','gamma'),('delta','greeks','delta'),('shares_per_contract','details','shares_per_contract'))}
    clocks={}
    for name,parent,key in (('daily_bar','day','last_updated'),('underlying','underlying_asset','last_updated'),
            ('quote','last_quote','last_updated'),('trade','last_trade','sip_timestamp')):
        vals=[r.get(parent,{}).get(key) for r in valid if isinstance(r.get(parent),dict)]
        ns=[v for v in vals if type(v) is int and 946684800000000000<=v<=4102444800000000000]
        clocks[name]={'present_non_null':sum(v is not None for v in vals),'plausible_nanosecond_integers':len(ns),
            'min_raw_ns':str(min(ns)) if ns else None,'max_raw_ns':str(max(ns)) if ns else None,
            'distinct_raw_timestamps':len(set(ns))}
    out['reported_clocks']=clocks
    details=[r.get('details') for r in valid if isinstance(r.get('details'),dict)]
    ids=[r.get('ticker') for r in details if isinstance(r.get('ticker'),str)]
    out['duplicate_contract_id_rows']=sum(n-1 for n in Counter(ids).values() if n>1)
    out['contract_types']=dict(Counter(str(r.get('contract_type')) for r in details))
    out['exercise_styles']=dict(Counter(str(r.get('exercise_style')) for r in details))
    out['deliverable_sizes']=dict(Counter(str(r.get('shares_per_contract')) for r in details))
    out['with_additional_underlyings']=sum(bool(r.get('additional_underlyings')) for r in details)
    out['expirations']=sorted({str(r.get('expiration_date')) for r in details})
    out['first_contract_id']=ids[0] if ids else None
    out['last_contract_id']=ids[-1] if ids else None
    return out
