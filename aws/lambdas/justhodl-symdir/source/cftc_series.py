"""Exact CFTC public-report scalar adapter; no proxy, price or trading authority."""
import base64
from collections import Counter
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import math
from pathlib import Path
import re
import time
import urllib.error
import urllib.parse
import urllib.request

CONTRACT = 'cftc-exact-series.v1'
PAGE_SIZE, MAX_PAGES, MAX_BYTES, DEADLINE_S = 1000, 8, 1000000, 25
DATE_FIELD, CODE_FIELD = 'report_date_as_yyyy_mm_dd', 'cftc_contract_market_code'
ROOT = 'https://publicreporting.cftc.gov'
_catalog_raw = (Path(__file__).with_name('cftc-series.json')).read_bytes()
CATALOG_HASH = hashlib.sha256(_catalog_raw).hexdigest()
CATALOG = json.loads(_catalog_raw)


def definition(sid):
    if not isinstance(sid, str) or len(sid) > 240:
        raise ValueError('Invalid CFTC identifier')
    requested = sid
    if sid in CATALOG['aliases']:
        sid = CATALOG['aliases'][sid]['canonical']
    m = re.fullmatch(r'cftc:([a-z0-9]{4}-[a-z0-9]{4})\|([0-9A-Z+]{4,7})\|([a-z][a-z0-9_]{0,79})', sid)
    if not m:
        raise ValueError('Select an exact CFTC report, contract code and field')
    dataset, code, field = m.groups()
    report = CATALOG['reports'].get(dataset)
    if not report or field not in report['fields']:
        raise ValueError('CFTC report or metric has no reviewed schema definition')
    return dict(report['fields'][field], id=sid, requested=requested, dataset=dataset,
                contract_market_code=code, field=field, report_name=report['name'],
                report_family=report['family'], report_basis=report['basis'],
                schema_sha256=report['schema_sha256'])


def directory(q='', limit=50, offset=0):
    if type(limit) is not int or type(offset) is not int or not 1 <= limit <= 500 or offset < 0:
        raise ValueError('Invalid CFTC directory page')
    tokens = str(q).lower().split()
    rows = []
    for requested, row in CATALOG['aliases'].items():
        d = definition(row['canonical'])
        title = d['report_name']+' / '+d['contract_market_code']+' / '+d['label']
        if not all(t in (requested+' '+d['id']+' '+title).lower() for t in tokens):
            continue
        rows.append(dict(id=d['id'], requested_alias=requested, name=title,
                         provider='cftc', kind='series', chartable=True,
                         unit=d['unit'], freq='W', report_family=d['report_family'],
                         report_basis=d['report_basis'], scope=d['scope'],
                         contract_existence_verified=False, history_verified=False))
    rows.sort(key=lambda row:(row['id'],row['requested_alias']))
    return {'provider':'cftc','provider_name':'CFTC','rows':rows[offset:offset+limit],
            'total':len(rows),'offset':offset,'limit':limit,
            'catalogue_scope':'Reviewed watchlist definitions, not every CFTC contract or metric',
            'contract':CONTRACT,'definition_sha256':CATALOG_HASH}


def query_url(d, offset):
    query = {'$select':','.join(['id',DATE_FIELD,CODE_FIELD,'market_and_exchange_names',d['field']]),
             '$where':CODE_FIELD+"='"+d['contract_market_code']+"'",
             '$order':DATE_FIELD+' ASC,id ASC','$limit':str(PAGE_SIZE),'$offset':str(offset)}
    return ROOT+'/resource/'+d['dataset']+'.json?'+urllib.parse.urlencode(query)


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise urllib.error.HTTPError(req.full_url,code,'CFTC redirect not followed',headers,fp)


def read_http(url, timeout, cap):
    req = urllib.request.Request(url, headers={'User-Agent':'JustHodl-CFTC/1.0 (+https://justhodl.ai)',
                                               'Accept':'application/json','Accept-Encoding':'identity'})
    opener = urllib.request.build_opener(NoRedirect())
    with opener.open(req, timeout=timeout) as res:
        if res.status != 200:
            raise ValueError('CFTC HTTP '+str(res.status))
        raw = res.read(cap+1)
        if len(raw) > cap:
            raise ValueError('CFTC response exceeds remaining byte budget')
        return raw, res.status, {k:res.headers[k] for k in ('Last-Modified','ETag','Content-Type') if k in res.headers}


def unique_object(pairs):
    out = {}
    for k,v in pairs:
        if k in out:
            raise ValueError('Duplicate JSON member in CFTC response')
        out[k]=v
    return out


def number(raw, unit):
    if type(raw) not in (str,int,float,Decimal) or isinstance(raw,bool):
        return None,'missing_or_invalid_value'
    if isinstance(raw,str) and (len(raw)>80 or not re.fullmatch(r'[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?',raw)):
        return None,'suppressed_or_invalid_value'
    try:
        value=Decimal(str(raw))
        if not value.is_finite() or value < 0:
            return None,'invalid_range'
        if value>9007199254740991 or unit=='traders' and value!=value.to_integral_value():
            return None,'not_safe_nonnegative_integer'
        if unit=='percent_of_open_interest' and value>100:
            return None,'invalid_percentage'
        projected=float(value)
        if not math.isfinite(projected) or Decimal(str(projected))!=value:
            return None,'lossy_numeric_projection'
        return int(value) if unit in ('traders','contracts') and value==value.to_integral_value() else projected,None
    except (InvalidOperation,ValueError,OverflowError):
        return None,'invalid_numeric_value'


def observation_date(raw):
    if not isinstance(raw,str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}(?:T00:00:00(?:\.000)?)?',raw):
        return None
    try:
        return date.fromisoformat(raw[:10]).isoformat()
    except ValueError:
        return None


def project(rows,d):
    records=[]
    for i,item in enumerate(rows):
        row=item['row'];dt=observation_date(row.get(DATE_FIELD)) if isinstance(row,dict) else None
        value,reason=number(row.get(d['field']),d['unit']) if isinstance(row,dict) else (None,'malformed_row')
        if not dt:reason='invalid_observation_date'
        if not isinstance(row,dict) or row.get(CODE_FIELD)!=d['contract_market_code']:reason='contract_identity_mismatch'
        records.append({'ordinal':i,'page':item['page'],'page_row':item['page_row'],
                        'source_row':row,'observation_date':dt,'value':value if reason is None else None,
                        'accepted':reason is None,'reason':reason,'source_available_at':None})
    dates=Counter(r['observation_date'] for r in records if r['observation_date'])
    groups={}
    for r in records:
        if r['observation_date']:groups.setdefault(r['observation_date'],[]).append(r)
        if r['observation_date'] and dates[r['observation_date']]>1:
            r.update(accepted=False,value=None,reason='duplicate_observation_date')
    obs=[]
    for dt in sorted(dates):
        found=groups[dt]
        obs.append([dt,found[0]['value'] if len(found)==1 and found[0]['accepted'] else None])
    return obs,records


def fetch(sid, reader=None, monotonic=time.monotonic):
    d=definition(sid);reader=reader or read_http;started=monotonic();rows=[];captures=[];used=0;complete=False;failure=None
    for page in range(MAX_PAGES):
        url=query_url(d,page*PAGE_SIZE);remaining=DEADLINE_S-(monotonic()-started)
        if remaining<=0 or used>=MAX_BYTES:
            failure='CFTC history request budget exhausted';break
        capture={'url':url,'page':page,'received_at':None,'http_status':None,'bytes':None,'sha256':None}
        captures.append(capture)
        try:
            raw,status,headers=reader(url,max(.1,min(8,remaining)),MAX_BYTES-used)
            if not isinstance(raw,bytes) or len(raw)>MAX_BYTES-used:
                raise ValueError('CFTC response exceeds remaining byte budget')
            used+=len(raw);capture.update(http_status=status,bytes=len(raw),sha256=hashlib.sha256(raw).hexdigest(),
                received_at=datetime.now(timezone.utc).isoformat(),headers=headers,body_base64=base64.b64encode(raw).decode('ascii'))
            if status!=200:raise ValueError('CFTC HTTP '+str(status))
            parsed=json.loads(raw.decode('utf-8'),parse_float=Decimal,parse_constant=lambda v:(_ for _ in ()).throw(ValueError('Non-finite JSON number')),object_pairs_hook=unique_object)
            if not isinstance(parsed,list) or len(parsed)>PAGE_SIZE:
                raise ValueError('CFTC response is not a bounded row array')
            # Keep decimal lexical values losslessly in source-row evidence too; original bytes remain authoritative.
            parsed=json.loads(json.dumps(parsed,default=str))
            rows.extend({'page':page,'page_row':i,'row':r} for i,r in enumerate(parsed))
            if len(parsed)<PAGE_SIZE:
                complete=True;break
        except urllib.error.HTTPError as e:
            capture['http_status']=e.code;failure='CFTC HTTP '+str(e.code);break
        except (ValueError,UnicodeError,OSError,TimeoutError) as e:
            failure=type(e).__name__+': '+str(e)[:180];break
    if not complete and failure is None:failure='CFTC history exceeds page budget'
    obs,records=project(rows,d)
    if not complete:obs=[]
    n=sum(value is not None for _,value in obs)
    accepted=sum(r['accepted'] for r in records)
    return {'contract':CONTRACT,'definition_sha256':CATALOG_HASH,'id':d['id'],'requested_id':sid,
            'provider':'cftc','provider_name':'CFTC','name':d['report_name']+' / '+d['contract_market_code']+' / '+d['label'],
            'unit':d['unit'],'freq':'W','source':ROOT+'/resource/'+d['dataset']+'.json','definition':d,
            'obs':obs,'n':n,'first':obs[0][0] if obs else None,'last':obs[-1][0] if obs else None,
            'acquired_at':datetime.now(timezone.utc).isoformat(),'source_published_at':None,
            'quality':{'status':'observations' if complete and n and accepted==len(records) else 'partial' if complete and n else 'unavailable',
                       'error':failure,'received_rows':len(records),'accepted_rows':accepted,'rejected_rows':len(records)-accepted},
            'history':{'pagination_complete':complete,'atomic_snapshot_verified':False,'point_in_time_vintages_verified':False,
                       'full_upstream_history_verified':False,'observation_clock':'CFTC report observation date; publication timestamp is not supplied',
                       'missing_periods_filled':False,'max_pages':MAX_PAGES,'page_size':PAGE_SIZE},
            'source_receipts':captures,'measurement_evidence':records,
            'calls_eligible':False,'sizing_eligible':False}


def cache_valid(packet,sid):
    try:
        d=definition(sid)
    except ValueError:
        return False
    return (isinstance(packet,dict) and packet.get('contract')==CONTRACT and packet.get('id')==d['id']
            and packet.get('definition_sha256')==CATALOG_HASH and packet.get('definition')==d
            and isinstance(packet.get('obs'),list) and isinstance(packet.get('history'),dict)
            and packet['history'].get('pagination_complete') is True)
