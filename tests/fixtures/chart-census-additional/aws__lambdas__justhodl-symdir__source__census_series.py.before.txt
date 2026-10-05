"""Exact Census bulk-series definitions and source-preserving observation parsing.

Original public archives and dictionary definitions remain inspectable. This
adapter never queries the key-requiring observations API or guesses a series.
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
import zipfile

CONTRACT = 'census-reviewed-series.v1'
_catalogue_raw = Path(__file__).with_name('census-series.json').read_bytes()
CATALOGUE_HASH = hashlib.sha256(_catalogue_raw).hexdigest()
CATALOGUE = json.loads(_catalogue_raw)
assert CATALOGUE['contract'] == 'census-reviewed-catalogue.v1'
_canonical = {key.upper(): key for key in CATALOGUE['series']}
assert len(_canonical) == len(CATALOGUE['series'])
MAX_WIRE, MAX_RAW, MAX_ROWS = 16000000, 50000000, 2000000
HEADINGS = {'CATEGORIES', 'DATA TYPES', 'ERROR TYPES', 'GEO LEVELS', 'TIME PERIODS', 'NOTES', 'DATA UPDATED ON', 'DATA'}
HEADERS = {
    'CATEGORIES': ['cat_idx', 'cat_code', 'cat_desc', 'cat_indent'],
    'DATA TYPES': ['dt_idx', 'dt_code', 'dt_desc', 'dt_unit'],
    'ERROR TYPES': ['et_idx', 'err_code', 'err_desc', 'err_unit'],
    'GEO LEVELS': ['geo_idx', 'geo_code', 'geo_desc'],
    'TIME PERIODS': ['per_idx', 'per_name'],
    'DATA': ['per_idx', 'cat_idx', 'dt_idx', 'et_idx', 'geo_idx', 'is_adj', 'val'],
}
MONTHS = {name: i + 1 for i, name in enumerate(('Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'))}
_memory_index = {}
_memory_source = {}
MAX_AGE = 86400


class SourceUnavailable(ValueError):
    pass


def prefix(dataset):
    if reviewed_dataset(dataset) != dataset:
        raise ValueError('Unreviewed Census dataset')
    return 'data/series-cache/census-source/' + dataset + '/'


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise urllib.error.HTTPError(req.full_url, code, 'Census redirect not followed', headers, fp)


def read_http(url):
    if url not in {d['source_url'] for d in CATALOGUE['dataset_definitions'].values()}:
        raise ValueError('Unreviewed Census bulk URL')
    req = urllib.request.Request(url, headers={'Accept': 'application/zip, application/octet-stream',
        'User-Agent': 'JustHodl-Census/1.0 (+https://justhodl.ai)'})
    with urllib.request.build_opener(NoRedirect()).open(req, timeout=60) as response:
        wire = response.read(MAX_WIRE + 1)
        if response.status != 200 or len(wire) > MAX_WIRE:
            raise ValueError('Census wire status or size invalid')
        return wire, dict(response.headers)


def _get(store, bucket, key, bound):
    try:
        response = store.get_object(Bucket=bucket, Key=key)
    except Exception as exc:
        if getattr(exc, 'response', {}).get('Error', {}).get('Code') in ('NoSuchKey', '404', 'NotFound'):
            return None
        raise SourceUnavailable('Census shared cache unavailable; no source request') from exc
    body = response['Body']
    try:
        raw = body.read(bound + 1)
    finally:
        if hasattr(body, 'close'):
            body.close()
    if len(raw) > bound:
        raise SourceUnavailable('Census cache bounds exceeded')
    return raw


def _put(store, bucket, key, body, **extra):
    try:
        return store.put_object(Bucket=bucket, Key=key, Body=body, **extra)
    except Exception as exc:
        raise SourceUnavailable('Census source evidence retention failed') from exc


def validate_receipt(receipt, dataset, raw):
    cfg = CATALOGUE['dataset_definitions'][dataset]
    if (not isinstance(receipt, dict) or receipt.get('contract') != CONTRACT or receipt.get('dataset') != dataset
        or receipt.get('url') != cfg['source_url'] or receipt.get('http_status') != 200
        or receipt.get('complete_response_retained') is not True
        or receipt.get('csv_sha256') != hashlib.sha256(raw).hexdigest()
        or receipt.get('csv_bytes') != len(raw)
        or receipt.get('readme_sha256') != cfg['reviewed_readme_sha256']):
        raise ValueError('Census receipt, units dictionary or CSV integrity invalid')
    digest = receipt.get('zip_sha256', '')
    key = prefix(dataset) + 'responses/' + digest + '.zip'
    if not re.fullmatch('[0-9a-f]{64}', digest) or receipt.get('retained_key') != key or receipt.get('retained_url') != 'https://justhodl.ai/' + key:
        raise ValueError('Census retained archive identity invalid')
    clock = datetime.fromisoformat(receipt['received_at'])
    if clock.tzinfo is None:
        raise ValueError('Census receipt clock needs a timezone')


def _load(store, bucket, dataset):
    meta = _get(store, bucket, prefix(dataset) + 'current.json', 32000)
    if meta is None:
        return None
    try:
        receipt = json.loads(meta)
        digest = receipt['zip_sha256']
        expected_key = prefix(dataset) + 'responses/' + digest + '.zip'
        if not re.fullmatch('[0-9a-f]{64}', digest) or receipt['retained_key'] != expected_key:
            raise ValueError('Invalid retained archive key')
        memo = _memory_source.get(dataset)
        if memo and memo[0] == digest:
            raw, readme = memo[1:]
        else:
            wire = _get(store, bucket, expected_key, MAX_WIRE)
            if wire is None or hashlib.sha256(wire).hexdigest() != digest or len(wire) != receipt['zip_bytes']:
                raise ValueError('Retained archive failed integrity checks')
            raw, readme = unpack(wire, dataset)
            _memory_source[dataset] = (digest, raw, readme)
        validate_receipt(receipt, dataset, raw)
        if hashlib.sha256(readme).hexdigest() != receipt['readme_sha256']:
            raise ValueError('Census units dictionary integrity invalid')
        return raw, receipt
    except (KeyError, ValueError, TypeError, EOFError, OSError, zipfile.BadZipFile) as exc:
        raise SourceUnavailable('Census retained archive failed integrity checks') from exc


def snapshot(store, bucket, dataset, reader=None, now=None):
    namespace = prefix(dataset)
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        raise ValueError('Census acquisition clock needs a timezone')
    if _get(store, bucket, namespace + 'blocked.json', 32000) is not None:
        raise SourceUnavailable('Census bulk source stopped after HTTP refusal; access review required')
    current = _load(store, bucket, dataset)
    if current:
        age = (now - datetime.fromisoformat(current[1]['received_at'])).total_seconds()
        if age < 0:
            raise SourceUnavailable('Census source receipt clock is in the future')
        if age < MAX_AGE:
            return current[0], dict(current[1], snapshot_age_s=int(age), stale=False)
    claim = namespace + 'download-claims/' + now.date().isoformat() + '.json'
    try:
        store.put_object(Bucket=bucket, Key=claim, Body=json.dumps({'contract': CONTRACT,
            'dataset': dataset, 'requested_at': now.isoformat()}).encode(), ContentType='application/json', IfNoneMatch='*')
    except Exception as exc:
        if getattr(exc, 'response', {}).get('Error', {}).get('Code') not in ('PreconditionFailed', '412', 'ConditionalRequestConflict', '409'):
            raise SourceUnavailable('Census shared download gate unavailable; no source request') from exc
        if current:
            return current[0], dict(current[1], snapshot_age_s=int(age), stale=True,
                cache_note='Daily download already claimed; retained historical snapshot only')
        raise SourceUnavailable('Census daily download already claimed; no retained archive available') from exc
    url = CATALOGUE['dataset_definitions'][dataset]['source_url']
    try:
        wire, headers = (reader or read_http)(url)
        raw, readme = unpack(wire, dataset)
        readme_sha = hashlib.sha256(readme).hexdigest()
        if readme_sha != CATALOGUE['dataset_definitions'][dataset]['reviewed_readme_sha256']:
            raise ValueError('Census definitions README changed; units review required')
        index(raw, dataset)
        digest = hashlib.sha256(wire).hexdigest()
        key = namespace + 'responses/' + digest + '.zip'
        receipt = dict(contract=CONTRACT, dataset=dataset, url=url, http_status=200, received_at=now.isoformat(),
            zip_sha256=digest, zip_bytes=len(wire), csv_sha256=hashlib.sha256(raw).hexdigest(), csv_bytes=len(raw),
            readme_sha256=readme_sha, readme_bytes=len(readme), retained_key=key, retained_url='https://justhodl.ai/' + key,
            complete_response_retained=True, headers={k:v for k,v in headers.items() if k.lower() in ('etag', 'last-modified', 'content-type')})
        _put(store, bucket, key, wire, ContentType='application/zip', CacheControl='public, max-age=31536000, immutable')
        _put(store, bucket, namespace+'current.json', json.dumps(receipt).encode(), ContentType='application/json', CacheControl='no-cache')
        return raw, dict(receipt, snapshot_age_s=0, stale=False)
    except urllib.error.HTTPError as exc:
        _put(store, bucket, namespace+'blocked.json', json.dumps({'contract':CONTRACT,'url':url,'http_status':exc.code,
            'received_at':now.isoformat(),'retry_allowed':False}).encode(), ContentType='application/json', CacheControl='no-cache')
        raise SourceUnavailable('Census HTTP '+str(exc.code)+'; no fallback or retry') from exc
    except (ValueError, OSError, EOFError, csv.Error, zipfile.BadZipFile) as exc:
        raise SourceUnavailable('Census acquisition failed; shared daily claim retained') from exc


def reviewed_dataset(value):
    if not isinstance(value, str):
        return None
    return value.lower() if value.lower() in CATALOGUE['dataset_definitions'] else None


def definition(sid):
    if not isinstance(sid, str) or len(sid) > 200 or sid.split(':', 1)[0].lower() != 'census':
        raise ValueError('Select a reviewed complete Census series identifier')
    key = _canonical.get(sid.partition(':')[2].upper())
    if key is None:
        raise ValueError('Census dataset, measure, category, adjustment or geography is unreviewed')
    return dict(CATALOGUE['series'][key], id='census:' + key, key=key)


def directory(q='', limit=50, offset=0, dataset=None):
    if type(limit) is not int or not 1 <= limit <= 500 or type(offset) is not int or offset < 0:
        raise ValueError('Invalid Census directory page')
    if dataset is not None and reviewed_dataset(dataset) is None:
        raise ValueError('Unreviewed Census dataset')
    terms=str(q).casefold().split();rows=[]
    for key, d in CATALOGUE['series'].items():
        if dataset is not None and d['dataset'] != dataset.lower():
            continue
        sid='census:'+key
        if not all(term in (sid+' '+d['name']).casefold() for term in terms):
            continue
        rows.append(dict(id=sid, symbol=key, provider='census', provider_name='U.S. Census Bureau',
                         kind='series', chartable=True, name=d['name'], unit=d['unit'], freq=d['freq'],
                         contract=CONTRACT, definition_sha256=CATALOGUE_HASH,
                         first=d['snapshot_first'], last=d['snapshot_last'], n=None,
                         sampling_error=d['sampling_error'], live_history_verified=False,
                         snapshot_numeric_rows=d['snapshot_numeric_rows']))
    return dict(provider='census', rows=rows[offset:offset+limit], total=len(rows), offset=offset, limit=limit,
                contract=CONTRACT, catalogue_scope='Reviewed definitions; catalogue membership does not establish current observations or historical completeness')


def unpack(wire, dataset):
    if reviewed_dataset(dataset) != dataset or not isinstance(wire, bytes) or len(wire)>MAX_WIRE:
        raise ValueError('Census archive identity or size invalid')
    member=CATALOGUE['dataset_definitions'][dataset]['program']+'-mf.csv'
    with zipfile.ZipFile(io.BytesIO(wire)) as archive:
        entries=archive.infolist()
        if len(entries)!=2 or {r.filename for r in entries}!={member,'/README'}:
            raise ValueError('Census archive members changed')
        if any(r.flag_bits&1 or r.file_size>MAX_RAW for r in entries) or sum(r.file_size for r in entries)>MAX_RAW+100000:
            raise ValueError('Census archive bounds exceeded')
        # Read fixed member names into memory. Never extract the absolute /README path.
        raw=archive.read(member);readme=archive.read('/README')
    if len(raw)>MAX_RAW or len(readme)>100000:
        raise ValueError('Census decoded archive bounds exceeded')
    return raw, readme


def period(text):
    if not isinstance(text,str):
        return None, None
    try:
        m=re.fullmatch(r'([A-Za-z]{3})-?(\d{4})',text)
        if m and m[1] in MONTHS:return date(int(m[2]),MONTHS[m[1]],1).isoformat(),'M'
        m=re.fullmatch(r'Q([1-4])-?(\d{4})',text)
        if m:return date(int(m[2]),int(m[1])*3-2,1).isoformat(),'Q'
        if re.fullmatch(r'\d{4}',text):return date(int(text),1,1).isoformat(),'A'
    except ValueError:
        pass
    return None,None


def measured(value):
    if not isinstance(value,str) or len(value)>80 or not re.fullmatch(r'[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?',value):
        return None
    try:
        number=Decimal(value);floating=float(number)
        return floating if number.is_finite() and Decimal(str(floating))==number else None
    except (InvalidOperation,ValueError,OverflowError):
        return None


def sections(raw):
    if not isinstance(raw,bytes) or len(raw)>MAX_RAW:
        raise ValueError('Census source bounds exceeded')
    groups={};current=None
    for ordinal,row in enumerate(csv.reader(io.StringIO(raw.decode('utf-8-sig'),newline=''),strict=True)):
        if ordinal>MAX_ROWS+100000:raise ValueError('Census row bounds exceeded')
        if not row or all(v=='' for v in row):continue
        if len(row)==1 and row[0] in HEADINGS:
            current=row[0]
            if current in groups:raise ValueError('Duplicate Census section')
            groups[current]=[];continue
        if current is None:raise ValueError('Unknown Census source preamble')
        groups[current].append((ordinal,row))
    if set(groups)!=HEADINGS:raise ValueError('Census source sections changed')
    tables={}
    for section,header in HEADERS.items():
        if not groups[section] or groups[section][0][1]!=header:raise ValueError('Census '+section+' header changed')
        if any(len(r)!=len(header) for _,r in groups[section][1:]):raise ValueError('Census row width mismatch')
        tables[section]=[(i,dict(zip(header,row))) for i,row in groups[section][1:]]
    if len(tables['DATA'])>MAX_ROWS:raise ValueError('Census data row limit exceeded')
    return groups,tables


def index(raw,dataset):
    memo=_memory_index.get(dataset)
    digest=memo[0] if memo and memo[1] is raw else hashlib.sha256(raw).hexdigest()
    if memo and memo[0]==digest:return memo[2]
    groups,tables=sections(raw);maps={}
    for name in ('CATEGORIES','DATA TYPES','ERROR TYPES','GEO LEVELS','TIME PERIODS'):
        key=HEADERS[name][0];maps[name]={row[key]:row for _,row in tables[name]}
        if len(maps[name])!=len(tables[name]):raise ValueError('Duplicate Census dictionary key')
    series={}
    for ordinal,row in tables['DATA']:
        error=row['et_idx']!='0'
        if (row['dt_idx']=='0')!=error or row['is_adj'] not in ('0','1'):
            raise ValueError('Census estimate/error or adjustment identity invalid')
        try:
            measure=maps['ERROR TYPES' if error else 'DATA TYPES'][row['et_idx'] if error else row['dt_idx']]
            category=maps['CATEGORIES'][row['cat_idx']];geo=maps['GEO LEVELS'][row['geo_idx']];time=maps['TIME PERIODS'][row['per_idx']]
        except KeyError as exc:raise ValueError('Census source dictionary reference absent') from exc
        code=measure['err_code' if error else 'dt_code']
        key=':'.join((dataset,code,category['cat_code'],'yes' if row['is_adj']=='1' else 'no',geo['geo_code']))
        series.setdefault(key,[]).append((ordinal,row,measure,category,geo,time,error))
    result=(groups,series);_memory_index[dataset]=(digest,raw,result)
    return result


def parse(raw,d):
    groups,series=index(raw,d['dataset']);records=[];counts=Counter()
    for ordinal,row,measure,category,geo,time,error in series.get(d['key'],[]):
        if (measure,category,geo,error)!=(d['measure_definition'],d['category_definition'],d['geography_definition'],d['sampling_error']):
            raise ValueError('Census measure, units, category or geography changed; definition review required')
        anchor,freq=period(time['per_name']);value=measured(row['val']);reason=None
        if anchor is None or freq!=d['freq']:reason='invalid_or_changed_reference_frequency'
        elif value is None:reason='estimate_rounds_to_zero' if row['val']=='Z' else 'source_suppressed' if row['val'] in ('S','(S)') else 'source_unavailable_or_unrepresentable_value'
        if anchor:counts[anchor]+=1
        records.append(dict(source_csv_record=ordinal,original=row,original_period=time['per_name'],anchor=anchor,value=value if reason is None else None,rejection=reason))
    observations={}
    for r in records:
        if r['anchor']:
            if counts[r['anchor']]>1:r.update(value=None,rejection='duplicate_reference_period')
            observations[r['anchor']]=r['value']
    output=io.StringIO(newline='');writer=csv.DictWriter(output,fieldnames=HEADERS['DATA'],lineterminator='\n');writer.writeheader();writer.writerows(r['original'] for r in records)
    return sorted([t,v] for t,v in observations.items()),records,output.getvalue().encode(),groups


def packet(sid,raw,receipt):
    d=definition(sid);validate_receipt(receipt,d['dataset'],raw)
    obs,records,extract,groups=parse(raw,d);n=sum(v is not None for _,v in obs)
    return dict(contract=CONTRACT,definition_sha256=CATALOGUE_HASH,id=d['id'],requested_id=sid,provider='census',
                provider_name='U.S. Census Bureau',name=d['name'],unit=d['unit'],freq=d['freq'],definition=d,
                source=CATALOGUE['dataset_definitions'][d['dataset']]['source_url'],source_receipts=[receipt],
                obs=obs,n=n,first=obs[0][0] if obs else None,last=obs[-1][0] if obs else None,
                last_valid=next((t for t,v in reversed(obs) if v is not None),None),
                source_update_text=[row for _,row in groups['DATA UPDATED ON']],source_notes=[row for _,row in groups['NOTES']],
                acquired_at=receipt.get('received_at'),source_published_at=None,
                source_extract={'scope':'Exact selected DATA records from the complete retained bulk CSV; dictionaries, source notes and units remain in the original retained archive',
                                'sha256':hashlib.sha256(extract).hexdigest(),'bytes':len(extract),'body_encoding':'gzip+base64','body_base64':base64.b64encode(gzip.compress(extract,mtime=0)).decode()},
                measurement_evidence={'columns':['source_csv_record','original_period','chart_anchor','value','original_value','rejection'],
                                      'rows':[[r['source_csv_record'],r['original_period'],r['anchor'],r['value'],r['original']['val'],r['rejection']] for r in records]},
                quality={'status':'unavailable' if not n else 'stale_source_snapshot' if receipt.get('stale') else 'partial' if any(r['rejection'] for r in records) else 'observations',
                         'error':None,'received_rows':len(records),'rejected_rows':sum(r['rejection'] is not None for r in records)},
                history={'response_complete':True,'full_upstream_history_verified':False,'point_in_time_vintages_verified':False,
                         'release_clock_verified':False,'missing_periods_filled':False,'source_snapshot_stale':bool(receipt.get('stale')),
                         'observation_clock':'Original reference period anchored to its first calendar day; not a daily observation or release date',
                         'sampling_error_preserved':True,'suppression_and_rounding_markers_preserved':True},
                equivalence_to_watchlist_provider_verified=False,calls_eligible=False,sizing_eligible=False)


def fetch(sid, store, bucket, reader=None, now=None):
    d = definition(sid)
    try:
        raw, receipt = snapshot(store, bucket, d['dataset'], reader, now)
        out = packet(sid, raw, receipt)
    except (SourceUnavailable, ValueError, KeyError, TypeError, UnicodeError, csv.Error) as exc:
        out = dict(contract=CONTRACT, definition_sha256=CATALOGUE_HASH, id=d['id'], requested_id=sid,
            provider='census', provider_name='U.S. Census Bureau', definition=d, name=d['name'], unit=d['unit'], freq=d['freq'],
            source=CATALOGUE['dataset_definitions'][d['dataset']]['source_url'], obs=[], n=0, first=None, last=None, last_valid=None,
            acquired_at=None, source_published_at=None, source_receipts=[],
            quality={'status':'unavailable','error':str(exc)[:200],'received_rows':0,'rejected_rows':0},
            history={'response_complete':False,'full_upstream_history_verified':False,'point_in_time_vintages_verified':False,
                     'release_clock_verified':False,'missing_periods_filled':False},
            equivalence_to_watchlist_provider_verified=False, calls_eligible=False, sizing_eligible=False)
    if len(json.dumps(out).encode()) > 3900000:
        raise ValueError('Census evidence exceeds response budget; no truncated history returned')
    return out


def cache_valid(value, sid):
    try:
        d = definition(sid)
    except ValueError:
        return False
    return (isinstance(value, dict) and value.get('contract') == CONTRACT and value.get('id') == d['id']
        and value.get('definition_sha256') == CATALOGUE_HASH and value.get('definition') == d
        and value.get('history', {}).get('response_complete') is True
        and value.get('history', {}).get('source_snapshot_stale') is False and not value.get('quality', {}).get('error'))
