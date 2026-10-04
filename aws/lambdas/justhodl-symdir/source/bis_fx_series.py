"""Exact BIS bilateral-FX histories. Public BIS API only; no proxy or trading vote."""
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

CONTRACT = 'bis-fx-series.v1'
ROOT = 'https://stats.bis.org/api/v2/data/dataflow/BIS/WS_XRU/1.0/'
MAX_WIRE = 4000000
MAX_RAW = 96000000
MAX_ROWS = 40000
_raw = Path(__file__).with_name('bis-fx-series.json').read_bytes()
CATALOG_HASH = hashlib.sha256(_raw).hexdigest()
CATALOG = json.loads(_raw)


def definition(sid):
    if not isinstance(sid, str) or not re.fullmatch(r'(?i)bis:WS_XRU:[DMQA]\.[A-Z0-9]{2}\.[A-Z]{3}\.[AE]', sid):
        raise ValueError('Select an exact reviewed BIS bilateral-FX series')
    key = sid.split(':')[2].upper()
    if key not in CATALOG['series']:
        raise ValueError('BIS bilateral-FX identity not in reviewed source directory')
    return dict(CATALOG['series'][key], id='bis:WS_XRU:'+key, key=key)


def directory(q='', limit=50, offset=0, dataset=False):
    if type(limit) is not int or not 1 <= limit <= 500 or type(offset) is not int or offset < 0:
        raise ValueError('Invalid BIS directory page')
    tokens = str(q).lower().split()
    rows = []
    for key in sorted(CATALOG['series']):
        d = definition('bis:WS_XRU:'+key)
        if not all(t in (d['id']+' '+d['name']+' '+d['compilation']).lower() for t in tokens):
            continue
        rows.append(dict(id=d['id'], name=d['name'], provider='bis', provider_name='BIS', kind='series',
                         chartable=True, freq=d['freq'], unit=d['unit'], geo=d['geo'],
                         compilation=d['compilation'], source_ref=d['source_ref'],
                         reference_last=d['reference_last'], history_verified=False))
    return dict(provider='bis', provider_name='BIS', rows=rows[offset:offset+limit], total=len(rows),
                offset=offset, limit=limit, definition_sha256=CATALOG_HASH, contract=CONTRACT,
                catalogue_scope='Reviewed WS_XRU bilateral exchange rates; other BIS datasets remain available as catalogue records',
                observation_clock='Source reference period; not a release or common FX fixing timestamp',
                **({'ds':'bis:WS_XRU'} if dataset else {}))


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise urllib.error.HTTPError(req.full_url, code, 'BIS redirect not followed', headers, fp)


def read_http(url):
    req = urllib.request.Request(url, headers={'Accept':'text/csv', 'Accept-Encoding':'gzip',
                                               'User-Agent':'JustHodl-BIS/1.0 (+https://justhodl.ai)'})
    with urllib.request.build_opener(NoRedirect()).open(req, timeout=20) as res:
        wire = res.read(MAX_WIRE+1)
        if res.status != 200 or len(wire) > MAX_WIRE:
            raise ValueError('BIS response status or byte bound failed')
        return wire, dict(res.headers)


def unpack(wire):
    if not isinstance(wire, bytes) or len(wire) > MAX_WIRE:
        raise ValueError('BIS wire byte bound exceeded')
    if wire[:2] == b'\x1f\x8b':
        with gzip.GzipFile(fileobj=io.BytesIO(wire)) as stream:
            raw = stream.read(MAX_RAW+1)
    else:
        raw = wire
    if len(raw) > MAX_RAW:
        raise ValueError('BIS decoded byte bound exceeded')
    return raw


def period(value, freq):
    if not isinstance(value, str):
        return None
    try:
        if freq == 'A' and re.fullmatch(r'\d{4}', value):
            return date(int(value),1,1).isoformat()
        if freq == 'Q' and re.fullmatch(r'\d{4}-Q[1-4]', value):
            return date(int(value[:4]),int(value[-1])*3-2,1).isoformat()
        if freq == 'M' and re.fullmatch(r'\d{4}-\d{2}', value):
            # Chart anchor only: original monthly period remains in row evidence.
            return date.fromisoformat(value+'-01').isoformat()
        if freq == 'D' and re.fullmatch(r'\d{4}-\d{2}-\d{2}', value):
            return date.fromisoformat(value).isoformat()
    except ValueError:
        pass
    return None


def measured(value):
    if not isinstance(value,str) or len(value)>80 or not re.fullmatch(r'[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?',value):
        return None
    try:
        n=Decimal(value);out=float(n)
        if not n.is_finite() or Decimal(str(out))!=n:
            return None
        return out
    except (ValueError,InvalidOperation,OverflowError):
        return None


def parse(raw,d):
    reader=csv.DictReader(io.StringIO(raw.decode('utf-8-sig'),newline=''),strict=True)
    fields=reader.fieldnames or []
    required={'FREQ','REF_AREA','CURRENCY','COLLECTION','UNIT_MULT','TIME_PERIOD','OBS_VALUE','OBS_STATUS','OBS_CONF'}
    if len(fields)!=len(set(fields)) or not required.issubset(fields):
        raise ValueError('BIS CSV schema mismatch or duplicate header')
    records=[];dates=Counter();names=set();units=set()
    for ordinal,row in enumerate(reader):
        if ordinal>=MAX_ROWS or None in row or any(v is None for v in row.values()):
            raise ValueError('BIS CSV row bound or shape mismatch')
        if row['FREQ']!=d['freq'] or row['REF_AREA']!=d['geo'] or row['CURRENCY']!=d['currency'] or row['COLLECTION']!=d['collection']:
            raise ValueError('BIS returned a different series')
        if row['UNIT_MULT']!='0':
            raise ValueError('BIS returned a different unit or multiplier')
        dt=period(row['TIME_PERIOD'],d['freq']);value=measured(row['OBS_VALUE']);reason=None
        if dt is None:reason='invalid_reference_period'
        elif row['OBS_CONF']!='F':reason='not_free_to_publish'
        elif row['OBS_STATUS'] not in ('A','E','P'):reason='observation_status_withheld'
        elif value is None or value <= 0:reason='missing_or_invalid_exchange_rate'
        if dt:dates[dt]+=1
        records.append([ordinal,row['TIME_PERIOD'],dt,value if reason is None else None,
                        row['OBS_STATUS'],row['OBS_CONF'],reason])
        if row.get('TITLE'):names.add(row['TITLE'].strip())
    groups={}
    for r in records:
        if r[2]:
            groups.setdefault(r[2],[]).append(r)
            if dates[r[2]]>1:r[3]=None;r[6]='duplicate_reference_period'
    obs=[[dt,rs[0][3] if len(rs)==1 else None] for dt,rs in sorted(groups.items())]
    return obs,records,sorted(names)


def fetch(sid, reader=None):
    d=definition(sid);url=ROOT+d['key']+'?format=csv';receipt={'url':url,'received_at':None,'http_status':None}
    obs=[];records=[];names=[];error=None
    try:
        wire,headers=(reader or read_http)(url);raw=unpack(wire)
        receipt.update(http_status=200,received_at=datetime.now(timezone.utc).isoformat(),
                       wire_bytes=len(wire),wire_sha256=hashlib.sha256(wire).hexdigest(),
                       bytes=len(raw),sha256=hashlib.sha256(raw).hexdigest(),
                       body_encoding='gzip+base64',body_base64=base64.b64encode(gzip.compress(raw,mtime=0)).decode('ascii'),
                       headers={k:v for k,v in headers.items() if k.lower() in ('last-modified','etag','content-type','content-encoding')})
        obs,records,names=parse(raw,d)
    except urllib.error.HTTPError as exc:
        receipt['http_status']=exc.code;error='BIS HTTP '+str(exc.code)
    except (ValueError,OSError,UnicodeError,csv.Error,EOFError) as exc:
        error=type(exc).__name__+': '+str(exc)[:180]
    n=sum(v is not None for _,v in obs);rejected=sum(r[6] is not None for r in records)
    out=dict(contract=CONTRACT,definition_sha256=CATALOG_HASH,id=d['id'],requested_id=sid,
             provider='bis',provider_name='BIS',name=d['name'],unit=d['unit'],freq=d['freq'],
             source=url,definition=d,obs=obs,n=n,first=obs[0][0] if obs else None,last=obs[-1][0] if obs else None,
             acquired_at=datetime.now(timezone.utc).isoformat(),source_published_at=None,
             quality={'status':'unavailable' if not n else 'partial' if rejected else 'observations',
                      'error':error,'received_rows':len(records),'rejected_rows':rejected},
             source_receipts=[receipt],measurement_evidence={'columns':['source_row','original_period','chart_anchor','value','status','confidentiality','rejection'],'rows':records},
             source_titles=names,history={'response_complete':error is None,'full_upstream_history_verified':False,
                                        'point_in_time_vintages_verified':False,'missing_periods_filled':False,
                                        'observation_clock':'Source period anchored to its first calendar day for plotting; daily observations use their reported date',
                                        'release_clock_verified':False,'same_time_fix_verified':False,'redenomination_continuity_verified':False},
             equivalence_to_watchlist_provider_verified=False,calls_eligible=False,sizing_eligible=False)
    if len(json.dumps(out).encode())>3900000:
        raise ValueError('BIS evidence exceeds response budget; no partial history returned')
    return out


def cache_valid(packet,sid):
    try:d=definition(sid)
    except ValueError:return False
    return (isinstance(packet,dict) and packet.get('contract')==CONTRACT and packet.get('id')==d['id']
            and packet.get('definition_sha256')==CATALOG_HASH and packet.get('definition')==d
            and isinstance(packet.get('history'),dict) and packet['history'].get('response_complete') is True)
