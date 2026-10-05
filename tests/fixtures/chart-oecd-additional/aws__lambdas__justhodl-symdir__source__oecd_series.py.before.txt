"""Reviewed OECD histories with shared daily source acquisition and exact dimensions.

Original consolidated responses are retained before any measurements are returned.
This adapter does not read the inaccessible warehouse, invoke a producer, or change
its schedule. Unknown datasets/series never fall through to a market provider.
"""
import base64
from collections import Counter
import csv
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
import gzip
import hashlib
import io
import json
from pathlib import Path
import re
import urllib.error
import urllib.request

CONTRACT = 'oecd-reviewed-series.v1'
_catalog_raw = Path(__file__).with_name('oecd-series.json').read_bytes()
CATALOG_HASH = hashlib.sha256(_catalog_raw).hexdigest()
CATALOG = json.loads(_catalog_raw)
FLOW = CATALOG['flow']
SOURCE_URL = 'https://sdmx.oecd.org/public/rest/data/'+FLOW+'/all?format=csvfile'
PREFIX = 'data/series-cache/oecd-source/'+hashlib.sha256(FLOW.encode()).hexdigest()[:20]+'/'
MAX_WIRE = 16000000
MAX_RAW = 128000000
MAX_ROWS = 1000000
MAX_AGE = 86400
_memory_source = None
_memory_index = None


class SourceUnavailable(ValueError):
    pass


def definition(sid):
    if not isinstance(sid, str) or len(sid)>240 or sid.split(':',1)[0].lower()!='oecd':
        raise ValueError('Select an exact reviewed OECD series')
    parts=sid.split(':')
    if len(parts) not in (3,4) or parts[1].upper()!=FLOW.upper() or parts[2].upper() not in CATALOG['series']:
        raise ValueError('OECD dataset/version/dimensions have not been reviewed')
    key=parts[2].upper()
    d=dict(CATALOG['series'][key],id='oecd:'+FLOW+':'+key,flow=FLOW)
    if len(parts)==4:
        transform=parts[3].upper()
        if transform not in ('G1','GY') or d['freq']!='M' or d['unit_code']!='IX':
            raise ValueError('Only exact 1-month or 12-month changes of reviewed monthly indices are available')
        d.update(id=d['id']+':'+transform,source_unit=d['unit'],unit='Percent change ('+('1 month' if transform=='G1' else '12 months')+')',
                 computed_transform=transform,lag_months=1 if transform=='G1' else 12,
                 name=d['name']+' / Computed '+('month-on-month' if transform=='G1' else 'year-on-year')+' change')
    return d


def directory(q='',limit=50,offset=0,dataset=False):
    if type(limit) is not int or not 1<=limit<=500 or type(offset) is not int or offset<0:
        raise ValueError('Invalid OECD directory page')
    tokens=str(q).lower().split();rows=[]
    for key in sorted(CATALOG['series']):
        d=definition('oecd:'+FLOW+':'+key)
        if not all(t in (d['id']+' '+d['name']+' '+d['unit']).lower() for t in tokens):continue
        rows.append(dict(id=d['id'],name=d['name'],provider='oecd',provider_name='OECD',kind='series',
                         chartable=True,freq=d['freq'],unit=d['unit'],geo=d['geo'],
                         reference_last=d['reference_last'],history_verified=False,
                         dimensions=d['dimensions'],labels=d['labels'],base_period=d['base_period']))
    return dict(provider='oecd',rows=rows[offset:offset+limit],total=len(rows),offset=offset,limit=limit,
                definition_sha256=CATALOG_HASH,contract=CONTRACT,
                catalogue_scope='Exact reviewed production, sales, orders and construction definitions; catalogue counts are not live history proof',
                **({'ds':'oecd:'+FLOW} if dataset else {}))


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,req,fp,code,msg,headers,newurl):
        raise urllib.error.HTTPError(req.full_url,code,'OECD redirect not followed',headers,fp)


def read_http(url):
    if url!=SOURCE_URL:raise ValueError('Unreviewed OECD source URL')
    req=urllib.request.Request(url,headers={'Accept':'text/csv','Accept-Encoding':'gzip',
                                          'User-Agent':'JustHodl-OECD/1.0 (+https://justhodl.ai)'})
    with urllib.request.build_opener(NoRedirect()).open(req,timeout=60) as res:
        wire=res.read(MAX_WIRE+1)
        if res.status!=200 or len(wire)>MAX_WIRE:raise ValueError('OECD wire status or size invalid')
        return wire,dict(res.headers)


def unpack(wire):
    if not isinstance(wire,bytes) or len(wire)>MAX_WIRE:raise ValueError('OECD wire bound exceeded')
    if wire[:2]==b'\x1f\x8b':
        with gzip.GzipFile(fileobj=io.BytesIO(wire)) as stream:raw=stream.read(MAX_RAW+1)
    else:raw=wire
    if len(raw)>MAX_RAW:raise ValueError('OECD raw bound exceeded')
    return raw


def _get(store,bucket,key,bound):
    try:
        response=store.get_object(Bucket=bucket,Key=key)
    except Exception as exc:
        code=getattr(exc,'response',{}).get('Error',{}).get('Code')
        if code in ('NoSuchKey','404','NotFound'):return None
        raise SourceUnavailable('OECD shared cache unavailable; source download not attempted') from exc
    body=response['Body']
    try:raw=body.read(bound+1)
    finally:
        if hasattr(body,'close'):body.close()
    if len(raw)>bound:raise SourceUnavailable('OECD cache size bound exceeded')
    return raw


def _put(store,bucket,key,body,**extra):
    try:return store.put_object(Bucket=bucket,Key=key,Body=body,**extra)
    except Exception as exc:raise SourceUnavailable('OECD evidence retention failed') from exc


def _load(store,bucket):
    global _memory_source
    raw=_get(store,bucket,PREFIX+'current.json',32000)
    if raw is None:return None
    try:
        receipt=json.loads(raw)
        assert receipt['contract']==CONTRACT and receipt['url']==SOURCE_URL and receipt['flow']==FLOW
        sha=receipt['sha256'];assert re.fullmatch('[0-9a-f]{64}',sha)
        assert receipt['retained_key']==PREFIX+'responses/'+sha+'.csv.gz'
        acquired=datetime.fromisoformat(receipt['received_at']);assert acquired.tzinfo is not None
        if _memory_source and _memory_source[0]==(sha,receipt['retained_sha256']):
            original=_memory_source[1]
        else:
            wire=_get(store,bucket,receipt['retained_key'],MAX_WIRE)
            if wire is None:raise ValueError('Missing retained response')
            assert hashlib.sha256(wire).hexdigest()==receipt['retained_sha256']
            original=unpack(wire);assert hashlib.sha256(original).hexdigest()==sha
            _memory_source=((sha,receipt['retained_sha256']),original)
        assert len(original)==receipt['bytes']
        return original,receipt
    except (KeyError,ValueError,AssertionError,TypeError,EOFError,OSError) as exc:
        raise SourceUnavailable('OECD retained response failed integrity checks') from exc


def snapshot(store,bucket,reader=None,now=None):
    now=now or datetime.now(timezone.utc)
    blocked=_get(store,bucket,PREFIX+'blocked.json',32000)
    if blocked is not None:raise SourceUnavailable('OECD source stopped after recorded HTTP refusal; access review required')
    current=_load(store,bucket)
    if current:
        age=(now-datetime.fromisoformat(current[1]['received_at'])).total_seconds()
        if 0<=age<MAX_AGE:return current[0],dict(current[1],snapshot_age_s=int(age),stale=False)
    # A shared conditional write is required before any upstream request. Daily
    # claims remain in storage even on failure. nocache cannot bypass this gate.
    claim=PREFIX+'download-claims/'+now.date().isoformat()+'.json'
    body=json.dumps({'contract':CONTRACT,'flow':FLOW,'requested_at':now.isoformat()}).encode()
    try:store.put_object(Bucket=bucket,Key=claim,Body=body,ContentType='application/json',IfNoneMatch='*')
    except Exception as exc:
        code=getattr(exc,'response',{}).get('Error',{}).get('Code')
        if code not in ('PreconditionFailed','412','ConditionalRequestConflict','409'):
            raise SourceUnavailable('OECD shared download gate unavailable; no source request') from exc
        if current:
            return current[0],dict(current[1],snapshot_age_s=max(0,int((now-datetime.fromisoformat(current[1]['received_at'])).total_seconds())),stale=True,
                                   cache_note='Daily source request already claimed; retained historical snapshot only')
        raise SourceUnavailable('OECD daily download already claimed; no retained response is available') from exc
    try:
        wire,headers=(reader or read_http)(SOURCE_URL);raw=unpack(wire)
        validate_schema(raw)
        sha=hashlib.sha256(raw).hexdigest();retained=gzip.compress(raw,mtime=0)
        if len(retained)>MAX_WIRE:raise ValueError('OECD retained response bound exceeded')
        key=PREFIX+'responses/'+sha+'.csv.gz'
        receipt=dict(contract=CONTRACT,flow=FLOW,url=SOURCE_URL,http_status=200,received_at=now.isoformat(),
                     wire_bytes=len(wire),wire_sha256=hashlib.sha256(wire).hexdigest(),bytes=len(raw),sha256=sha,
                     retained_key=key,retained_url='https://justhodl.ai/'+key,retained_sha256=hashlib.sha256(retained).hexdigest(),
                     complete_response_retained=True,headers={k:v for k,v in headers.items() if k.lower() in ('last-modified','etag','content-type','content-encoding')})
        _put(store,bucket,key,retained,ContentType='application/gzip',CacheControl='public, max-age=31536000, immutable')
        _put(store,bucket,PREFIX+'current.json',json.dumps(receipt).encode(),ContentType='application/json',CacheControl='no-cache')
        return raw,dict(receipt,snapshot_age_s=0,stale=False)
    except urllib.error.HTTPError as exc:
        record=json.dumps({'contract':CONTRACT,'url':SOURCE_URL,'status':exc.code,'received_at':now.isoformat(),'retry_allowed':False}).encode()
        _put(store,bucket,PREFIX+'blocked.json',record,ContentType='application/json',CacheControl='no-cache')
        raise SourceUnavailable('OECD HTTP '+str(exc.code)+'; no fallback or retry') from exc
    except (ValueError,OSError,UnicodeError,EOFError,csv.Error) as exc:
        raise SourceUnavailable('OECD acquisition failed; shared daily claim retained') from exc


def validate_schema(raw):
    text=raw.decode('utf-8-sig');reader=csv.DictReader(io.StringIO(text,newline=''),strict=True)
    fields=reader.fieldnames or []
    required=set(CATALOG['dimension_order'])|{'DATAFLOW','TIME_PERIOD','OBS_VALUE','OBS_STATUS','UNIT_MULT','DECIMALS','BASE_PER'}
    if len(fields)!=len(set(fields)) or not required.issubset(fields):raise ValueError('OECD CSV schema mismatch')
    return reader


def period(value,freq):
    try:
        if freq=='M' and re.fullmatch(r'\d{4}-\d{2}',value):return date.fromisoformat(value+'-01').isoformat()
        if freq=='Q' and re.fullmatch(r'\d{4}-Q[1-4]',value):return date(int(value[:4]),int(value[-1])*3-2,1).isoformat()
        if freq=='A' and re.fullmatch(r'\d{4}',value):return date(int(value),1,1).isoformat()
    except (ValueError,TypeError):pass
    return None


def measured(value):
    if not isinstance(value,str) or len(value)>80 or not re.fullmatch(r'[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?',value):return None
    try:
        number=Decimal(value);out=float(number)
        return out if number.is_finite() and Decimal(str(out))==number else None
    except (ValueError,InvalidOperation,OverflowError):return None


def parse(raw,d):
    global _memory_index
    sha=_memory_index[0] if _memory_index and _memory_index[3] is raw else hashlib.sha256(raw).hexdigest()
    if not _memory_index or _memory_index[0]!=sha:
        reader=validate_schema(raw);fields=reader.fieldnames;groups={}
        for ordinal,row in enumerate(reader):
            if ordinal>=MAX_ROWS or None in row or any(v is None for v in row.values()):raise ValueError('OECD CSV row bound or shape mismatch')
            if row['DATAFLOW']!=CATALOG['dataflow']:raise ValueError('OECD dataflow/version mismatch')
            key='.'.join(row[k] for k in CATALOG['dimension_order'])
            groups.setdefault(key,[]).append((ordinal,tuple(row[k] for k in fields)))
        _memory_index=(sha,fields,groups,raw)
    _,fields,groups,_=_memory_index;selected=[];dates=Counter()
    for ordinal,values in groups.get(d['key'],[]):
        row=dict(zip(fields,values))
        if (row['UNIT_MEASURE'],row['UNIT_MULT'],row['BASE_PER'])!=(d['unit_code'],d['unit_mult'],d['base_period']):
            raise ValueError('OECD row unit, multiplier or base period changed; definition review required')
        anchor=period(row['TIME_PERIOD'],d['freq']);value=measured(row['OBS_VALUE']);reason=None
        if anchor is None:reason='invalid_reference_period'
        elif row['OBS_STATUS'] not in ('A','E','P','B'):reason='observation_status_withheld'
        elif value is None:reason='missing_or_invalid_value'
        if anchor:dates[anchor]+=1
        selected.append({'source_row':ordinal,'original':row,'anchor':anchor,'value':value if reason is None else None,'rejection':reason})
    observations={}
    for record in selected:
        anchor=record['anchor']
        if anchor:
            if dates[anchor]>1:record.update(value=None,rejection='duplicate_reference_period')
            observations[anchor]=record['value']
    extracted=io.StringIO(newline='');writer=csv.DictWriter(extracted,fieldnames=fields,lineterminator='\n');writer.writeheader();writer.writerows(r['original'] for r in selected)
    return sorted([k,v] for k,v in observations.items()),selected,extracted.getvalue().encode()


def growth(obs,records,months):
    if months not in (1,12):raise ValueError('Unreviewed monthly lag')
    lookup={t:v for t,v in obs};by_date={r['anchor']:r for r in records if r['anchor']};out=[];evidence=[]
    breaks=[r['anchor'] for r in records if r['anchor'] and r['original']['OBS_STATUS']=='B']
    for anchor,current in obs:
        year,month=int(anchor[:4]),int(anchor[5:7]);serial=year*12+month-1-months
        prior=date(serial//12,serial%12+1,1).isoformat() if serial>=12 else None
        previous=lookup.get(prior);reason=None;value=None
        if current is None or previous is None:reason='missing_exact_calendar_operand'
        elif previous==0:reason='zero_denominator'
        elif any(prior<t<=anchor for t in breaks):reason='source_break_inside_comparison_window'
        if reason is None:
            number=(Decimal(str(current))/Decimal(str(previous))-1)*100;value=float(number)
            if not Decimal(str(value)).is_finite():value=None;reason='nonfinite_growth'
        out.append([anchor,value]);evidence.append([anchor,prior,by_date.get(anchor,{}).get('source_row'),by_date.get(prior,{}).get('source_row'),current,previous,value,reason])
    return out,evidence


def fetch(sid,store,bucket,reader=None,now=None):
    d=definition(sid);obs=[];records=[];receipt=None;error=None;extracted=b'';derived=[]
    try:
        raw,receipt=snapshot(store,bucket,reader,now);candidate,records,extracted=parse(raw,d)
        if d.get('computed_transform'):candidate,derived=growth(candidate,records,d['lag_months'])
        obs=candidate
    except (SourceUnavailable,ValueError,UnicodeError,csv.Error) as exc:error=str(exc)[:200]
    n=sum(v is not None for _,v in obs);rejected=sum(r['rejection'] is not None for r in records)
    if derived:rejected=sum(r[-1] is not None for r in derived)
    stale=bool(receipt and receipt.get('stale'))
    out=dict(contract=CONTRACT,definition_sha256=CATALOG_HASH,id=d['id'],requested_id=sid,provider='oecd',provider_name='OECD',
             name=d['name'],unit=d['unit'],freq=d['freq'],definition=d,source=SOURCE_URL,
             obs=obs,n=n,first=obs[0][0] if obs else None,last=obs[-1][0] if obs else None,
             last_valid=next((t for t,v in reversed(obs) if v is not None),None),acquired_at=receipt.get('received_at') if receipt else None,
             source_published_at=None,quality={'status':'unavailable' if not n else 'stale_source_snapshot' if stale else 'partial' if rejected else 'observations',
                                              'error':error,'received_rows':len(records),'rejected_rows':rejected},
             source_receipts=[receipt] if receipt else [],
             source_extract={'scope':'Only exact matching rows from the retained consolidated response; this is not the complete upstream response',
                             'sha256':hashlib.sha256(extracted).hexdigest(),'bytes':len(extracted),'body_encoding':'gzip+base64',
                             'body_base64':base64.b64encode(gzip.compress(extracted,mtime=0)).decode()},
             measurement_evidence={'columns':['source_row','original_period','chart_anchor','value','status','rejection'],
                                   'rows':[[r['source_row'],r['original']['TIME_PERIOD'],r['anchor'],r['value'],r['original']['OBS_STATUS'],r['rejection']] for r in records]},
             history={'response_complete':error is None,'full_upstream_history_verified':False,'point_in_time_vintages_verified':False,
                      'missing_periods_filled':False,'release_clock_verified':False,
                      'observation_clock':'Original reference period, anchored to its first calendar day for plotting; not a daily observation or release date',
                      'break_status_preserved':True,'source_snapshot_stale':stale},
             equivalence_to_watchlist_provider_verified=False,calls_eligible=False,sizing_eligible=False)
    if d.get('computed_transform'):
        out['transformation']={'id':d['computed_transform'],'formula':'100 * (index(t) / index(t - lag_months) - 1)',
                               'lag_months':d['lag_months'],'source_unit':d['source_unit'],'computed_by':'JustHodl',
                               'provider_published_growth':False,'exact_calendar_matching':True,'annualised':False,
                               'seasonal_adjustment':d['labels']['ADJUSTMENT'],'breaks_inside_window_withheld':True,
                               'columns':['chart_anchor','prior_period','current_source_row','prior_source_row','current_index','prior_index','growth_percent','rejection'],
                               'rows':derived}
        out['measurement_evidence']['scope']='Untransformed source indices; plotted growth is reproduced by the transformation evidence'
    if len(json.dumps(out).encode())>3900000:raise ValueError('OECD evidence exceeds response budget; no truncated history returned')
    return out


def cache_valid(packet,sid):
    try:d=definition(sid)
    except ValueError:return False
    return (isinstance(packet,dict) and packet.get('contract')==CONTRACT and packet.get('id')==d['id']
            and packet.get('definition_sha256')==CATALOG_HASH and packet.get('definition')==d
            and packet.get('history',{}).get('response_complete') is True and not packet.get('history',{}).get('source_snapshot_stale'))
