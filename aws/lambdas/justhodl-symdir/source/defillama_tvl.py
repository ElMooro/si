"""Exact public DefiLlama TVL observations with retained original response evidence."""
import base64
from collections import Counter
from datetime import datetime,timezone
from decimal import Decimal,InvalidOperation
import gzip,hashlib,json,math,re
from pathlib import Path
import urllib.request,urllib.error

CONTRACT='defillama-reviewed-tvl-series.v1'
_catalogue_raw=Path(__file__).with_name('defillama-tvl.json').read_bytes()
CATALOGUE_HASH=hashlib.sha256(_catalogue_raw).hexdigest()
CATALOGUE=json.loads(_catalogue_raw)
assert CATALOGUE['contract']=='defillama-reviewed-tvl-catalogue.v1'
IDENTITIES={d['id'].casefold():d for d in CATALOGUE['series'].values()}
assert len(IDENTITIES)==len(CATALOGUE['series'])
MAX_WIRE=4000000
MAX_ROWS=20000
MAX_AGE=86400
ROOT='data/series-cache/defillama-tvl-source/'
class SourceUnavailable(ValueError):pass
class NumericLexeme(str):pass

def definition(sid):
    if not isinstance(sid,str) or len(sid)>300 or sid.casefold() not in IDENTITIES:
        raise ValueError('Select an exact reviewed DefiLlama TVL identifier; no chain, token or protocol substitution')
    return dict(IDENTITIES[sid.casefold()])

def directory(q='',limit=50,offset=0):
    if type(limit) is not int or not 1<=limit<=500 or type(offset) is not int or offset<0:raise ValueError('Invalid DefiLlama directory page')
    terms=str(q).casefold().split();rows=[]
    for value in CATALOGUE['series'].values():
        d=definition(value['id'])
        if not all(t in (d['id']+' '+d['name']+' '+d['unit']).casefold() for t in terms):continue
        rows.append(dict(id=d['id'],provider='defillama',provider_name='DefiLlama',kind='series',chartable=True,name=d['name'],unit=d['unit'],currency=d['currency'],freq=d['freq'],first=None,last=None,n=None,live_history_verified=False,contract=CONTRACT,definition_sha256=CATALOGUE_HASH,measurement_kind=d['measurement_kind']))
    return dict(provider='defillama',rows=rows[offset:offset+limit],total=len(rows),offset=offset,limit=limit,contract=CONTRACT,catalogue_scope='Exact endpoint definitions, not a current historical-coverage claim')

def measured(text):
    if not isinstance(text,NumericLexeme) or len(text)>80 or not re.fullmatch(r'[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?',text):return None
    try:
        dec=Decimal(text);value=float(dec)
        return value if dec.is_finite() and math.isfinite(value) and (value!=0 or dec==0) else None
    except (InvalidOperation,ValueError,OverflowError):return None

def parsed(raw,d):
    if not isinstance(raw,bytes) or len(raw)>MAX_WIRE:raise ValueError('DefiLlama response exceeds bounds')
    def object_pairs(pairs):
        out={}
        for key,value in pairs:
            if key in out:raise ValueError('Duplicate JSON member in DefiLlama response')
            out[key]=value
        return out
    packet=json.loads(raw.decode('utf-8'),object_pairs_hook=object_pairs,parse_int=NumericLexeme,parse_float=NumericLexeme,parse_constant=lambda _:(_ for _ in ()).throw(ValueError('Nonfinite JSON constant')))
    if not isinstance(packet,list) or len(packet)>MAX_ROWS:raise ValueError('DefiLlama history array invalid')
    records=[];counts=Counter()
    for ordinal,row in enumerate(packet):
        if not isinstance(row,dict) or set(row)!={'date','tvl'}:raise ValueError('DefiLlama historical row schema changed')
        anchor=None;reason=None;flags=[];stamp=row['date'];value=measured(row['tvl'])
        if isinstance(stamp,NumericLexeme) and re.fullmatch(r'[0-9]{1,11}',stamp):
            seconds=int(stamp)
            if seconds%86400==0 and 0<=seconds<7258118400:anchor=datetime.fromtimestamp(seconds,timezone.utc).date().isoformat()
        if anchor is None:reason='invalid_or_non_midnight_reference_time'
        elif value is None:reason='source_tvl_unavailable_or_unrepresentable'
        elif value<0:reason='negative_tvl_requires_source_review'
        if anchor:counts[anchor]+=1
        if value==0:flags.append('source_reported_zero_not_missingness_proof')
        rounded=value is not None and Decimal(str(value))!=Decimal(row['tvl'])
        records.append(dict(ordinal=ordinal,original=dict(row),anchor=anchor,value=value if reason is None else None,rejection=reason,flags=flags,binary64_rounding=rounded))
    obs={}
    for r in records:
        if r['anchor']:
            if counts[r['anchor']]>1:r.update(value=None,rejection='duplicate_reference_date')
            obs[r['anchor']]=r['value']
    return sorted([k,v] for k,v in obs.items()),records,None

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,req,fp,code,msg,headers,newurl):raise urllib.error.HTTPError(req.full_url,code,'DefiLlama redirect not followed',headers,fp)
def read_http(url):
    if not isinstance(url,str) or url not in {d['source_url'] for d in CATALOGUE['series'].values()}:raise ValueError('Unreviewed DefiLlama source URL')
    with urllib.request.build_opener(NoRedirect()).open(urllib.request.Request(url,headers={'User-Agent':'JustHodl-DefiLlama-Index/1.0 (+https://justhodl.ai)'}),timeout=55) as response:
        raw=response.read(MAX_WIRE+1)
        if response.status!=200 or len(raw)>MAX_WIRE:raise ValueError('DefiLlama response status or length invalid')
        return raw,dict(response.headers)


def prefix(d):
    return ROOT + hashlib.sha256(d['id'].encode()).hexdigest() + '/'


def _get(store, bucket, key, bound):
    try:
        response = store.get_object(Bucket=bucket, Key=key)
    except Exception as exc:
        if getattr(exc, 'response', {}).get('Error', {}).get('Code') in ('NoSuchKey', '404', 'NotFound'):
            return None
        raise SourceUnavailable('DefiLlama shared cache unavailable; no source request') from exc
    body = response['Body']
    try:
        raw = body.read(bound + 1)
    finally:
        body.close()
    if len(raw) > bound:
        raise SourceUnavailable('DefiLlama retained object exceeds bound')
    return raw


def _put(store, bucket, key, raw, **kw):
    try:
        store.put_object(Bucket=bucket, Key=key, Body=raw, **kw)
    except Exception as exc:
        raise SourceUnavailable('DefiLlama source retention failed; no unretained chart') from exc


def definition_hash(d):
    return hashlib.sha256(json.dumps(d,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()).hexdigest()


def receipt_definition_matches(receipt,d):
    return (isinstance(receipt.get('definition_sha256'),str)
        and re.fullmatch('[0-9a-f]{64}',receipt['definition_sha256']) is not None
        and receipt.get('series_definition_sha256')==definition_hash(d))


def validate_receipt(receipt, d, raw):
    digest = hashlib.sha256(raw).hexdigest()
    key = prefix(d) + 'responses/' + digest + '.json'
    if (not isinstance(receipt, dict) or receipt.get('contract') != CONTRACT or receipt.get('id') != d['id']
        or not receipt_definition_matches(receipt,d) or receipt.get('url') != d['source_url']
        or receipt.get('http_status') != 200 or receipt.get('sha256') != digest or receipt.get('bytes') != len(raw)
        or receipt.get('retained_key') != key or receipt.get('retained_url') != 'https://justhodl.ai/' + key
        or receipt.get('complete_response_retained') is not True):
        raise ValueError('DefiLlama source receipt failed integrity checks')
    clock = datetime.fromisoformat(receipt['received_at'])
    if clock.tzinfo is None:
        raise ValueError('DefiLlama receipt timezone required')


def snapshot(d, store, bucket, reader=None, now=None):
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        raise ValueError('DefiLlama acquisition timezone required')
    namespace = prefix(d)
    if _get(store, bucket, ROOT + 'blocked.json', 32000) is not None or _get(store, bucket, namespace + 'blocked.json', 32000) is not None:
        raise SourceUnavailable('DefiLlama source stopped after HTTP refusal; access review required')
    current = _get(store, bucket, namespace + 'current.json', 32000)
    if current is not None:
        receipt = json.loads(current)
        digest = receipt.get('sha256', '')
        if not re.fullmatch('[0-9a-f]{64}', digest):
            raise SourceUnavailable('DefiLlama retained identity invalid')
        raw = _get(store, bucket, namespace + 'responses/' + digest + '.json', MAX_WIRE)
        if raw is None:
            raise SourceUnavailable('DefiLlama retained response absent')
        validate_receipt(receipt, d, raw)
        age = (now - datetime.fromisoformat(receipt['received_at'])).total_seconds()
        if age < 0:
            raise SourceUnavailable('DefiLlama receipt is in the future')
        if age < MAX_AGE:
            return raw, dict(receipt, snapshot_age_s=int(age))
    # Shared request admission: at most one upstream request per two-second slot.
    # Busy slots do not consume the per-series daily claim.
    slot = ROOT + 'request-slots/' + str(int(now.timestamp()) // 2) + '.json'
    try:
        store.put_object(Bucket=bucket,Key=slot,Body=json.dumps({'id':d['id'],'requested_at':now.isoformat()}).encode(),ContentType='application/json',IfNoneMatch='*')
    except Exception as exc:
        raise SourceUnavailable('DefiLlama shared source slot unavailable; no upstream request') from exc
    claim = namespace + 'request-claims/' + now.date().isoformat() + '.json'
    try:
        store.put_object(Bucket=bucket, Key=claim, Body=json.dumps({'contract': CONTRACT, 'id': d['id'], 'requested_at': now.isoformat()}).encode(), ContentType='application/json', IfNoneMatch='*')
    except Exception as exc:
        if getattr(exc, 'response', {}).get('Error', {}).get('Code') in ('PreconditionFailed', '412', 'ConditionalRequestConflict', '409'):
            raise SourceUnavailable('DefiLlama request already claimed today; no repeat request or stale substitution') from exc
        raise SourceUnavailable('DefiLlama shared request gate unavailable; no source request') from exc
    try:
        raw, headers = (reader or read_http)(d['source_url'])
        parsed(raw, d)  # Verify the exact response identity and row schema before retaining.
        digest = hashlib.sha256(raw).hexdigest()
        key = namespace + 'responses/' + digest + '.json'
        receipt = dict(contract=CONTRACT, id=d['id'], definition_sha256=CATALOGUE_HASH, series_definition_sha256=definition_hash(d), url=d['source_url'],
            http_status=200, received_at=now.isoformat(), sha256=digest, bytes=len(raw), retained_key=key,
            retained_url='https://justhodl.ai/' + key, complete_response_retained=True,
            headers={k: v for k, v in headers.items() if k.lower() in ('content-type', 'etag', 'last-modified')})
        _put(store, bucket, key, raw, ContentType='application/json', CacheControl='public, max-age=31536000, immutable')
        _put(store, bucket, namespace + 'current.json', json.dumps(receipt).encode(), ContentType='application/json', CacheControl='no-cache')
        return raw, dict(receipt, snapshot_age_s=0)
    except urllib.error.HTTPError as exc:
        blocked = ROOT if exc.code in (401, 403, 429) else namespace
        _put(store, bucket, blocked + 'blocked.json', json.dumps({'contract': CONTRACT, 'id': d['id'], 'http_status': exc.code, 'received_at': now.isoformat(), 'retry_allowed': False}).encode(), ContentType='application/json', CacheControl='no-cache')
        raise SourceUnavailable('DefiLlama HTTP ' + str(exc.code) + '; no fallback or retry') from exc
    except (ValueError, OSError) as exc:
        raise SourceUnavailable('DefiLlama acquisition failed; shared daily claim retained') from exc


def packet(sid,raw,receipt):
    d=definition(sid);validate_receipt(receipt,d,raw);obs,records,_=parsed(raw,d)
    received_day=datetime.fromisoformat(receipt['received_at']).astimezone(timezone.utc).date().isoformat()
    for r in records:
        if r['anchor'] and r['anchor']>received_day:r.update(value=None,rejection='future_reference_date_at_receipt')
    obs=sorted({r['anchor']:r['value'] for r in records if r['anchor']}.items())
    obs=[[date,value] for date,value in obs]
    n=sum(v is not None for _,v in obs);rejected=sum(r['rejection'] is not None for r in records);flags=Counter(flag for r in records for flag in r['flags'])
    extract=json.dumps([r['original'] for r in records],separators=(',',':'),ensure_ascii=False).encode()
    return dict(contract=CONTRACT,id=d['id'],requested_id=sid,provider='defillama',provider_name='DefiLlama',name=d['name'],unit=d['unit'],currency=d['currency'],freq=d['freq'],definition=d,definition_sha256=CATALOGUE_HASH,
        source=d['source_url'],source_receipts=[receipt],definition_source_receipt=d['definition_receipt'],acquired_at=receipt['received_at'],source_published_at=None,
        obs=obs,n=n,first=obs[0][0] if obs else None,last=obs[-1][0] if obs else None,last_valid=next((t for t,v in reversed(obs) if v is not None),None),
        source_extract={'scope':'Every received original row; JSON numeric lexemes retained as strings. Whole unmodified response retained at its content-addressed receipt URL.','sha256':hashlib.sha256(extract).hexdigest(),'bytes':len(extract),'body_encoding':'gzip+base64','body_base64':base64.b64encode(gzip.compress(extract,mtime=0)).decode()},
        measurement_evidence={'columns':['source_row','source_unix_seconds','chart_anchor','tvl_usd','original_tvl_lexeme','rejection','source_flags','binary64_rounding'],'rows':[[r['ordinal'],r['original']['date'],r['anchor'],r['value'],r['original']['tvl'],r['rejection'],r['flags'],r['binary64_rounding']] for r in records]},
        quality={'status':'unavailable' if not n else 'partial' if rejected else 'observations','error':None,'received_rows':len(records),'rejected_rows':rejected,'source_flag_counts':dict(flags),'plot_rounding_rows':sum(r['binary64_rounding'] for r in records)},
        history={'response_complete':True,'full_upstream_history_verified':False,'point_in_time_vintages_verified':False,'latest_reference_date_verified':False,'missing_dates_filled':False,'release_clock_verified':False,'completed_day_verified':False,'source_field':'tvl','market_ohlc_qualified':False,'traded_volume_qualified':False,'intraday_quote':False,'measurement_kind':d['measurement_kind'],'observation_clock':d['calendar'],'identity_binding':'Exact reviewed endpoint URL and source catalogue name; response rows do not contain a series identifier.','numeric_representation':'Original JSON numeric lexemes retained; finite binary64 chart values disclose rounding.','interpretation':d['interpretation'],'exclusions':d['exclusions']},
        equivalence_to_watchlist_provider_verified=False,calls_eligible=False,sizing_eligible=False)

def fetch(sid,store,bucket,reader=None,now=None):
    d=definition(sid)
    try:
        raw,receipt=snapshot(d,store,bucket,reader,now);out=packet(sid,raw,receipt)
    except (SourceUnavailable,ValueError,KeyError,TypeError,UnicodeError) as exc:
        out=dict(contract=CONTRACT,id=d['id'],requested_id=sid,provider='defillama',provider_name='DefiLlama',name=d['name'],unit=d['unit'],currency=d['currency'],freq=d['freq'],definition=d,definition_sha256=CATALOGUE_HASH,source=d['source_url'],source_receipts=[],obs=[],n=0,first=None,last=None,last_valid=None,acquired_at=None,quality={'status':'unavailable','error':str(exc)[:200],'received_rows':0,'rejected_rows':0},history={'response_complete':False,'full_upstream_history_verified':False,'point_in_time_vintages_verified':False,'intraday_quote':False,'market_ohlc_qualified':False,'traded_volume_qualified':False},equivalence_to_watchlist_provider_verified=False,calls_eligible=False,sizing_eligible=False)
    if len(json.dumps(out).encode())>3900000:raise ValueError('DefiLlama evidence exceeds response budget; no truncated history returned')
    return out

def cache_valid(value,sid):
    try:d=definition(sid)
    except ValueError:return False
    return isinstance(value,dict) and value.get('contract')==CONTRACT and value.get('id')==d['id'] and value.get('definition_sha256')==CATALOGUE_HASH and value.get('definition')==d and value.get('history',{}).get('response_complete') is True and not value.get('quality',{}).get('error')
