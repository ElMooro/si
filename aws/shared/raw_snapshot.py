"""Complete supplied-body archive with conditional writes and source identity.

snapshot() retains the existing key-or-None interface. It never truncates input,
never preserves credentials in source URLs, and returns no key when capture fails.
The contract is application-level conditional retention, not S3 Object Lock.
Legacy 12-character hashes remain stored but are not upgraded into v2 proof.
Collectors may decode HTTP compression before calling. Archive verification is
not independent proof of original wire bytes or provider response completeness.
"""
import gzip
import hashlib
import io
import re
from datetime import datetime, timezone
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
import boto3

_s3=boto3.client('s3',region_name='us-east-1')
BUCKET='justhodl-dashboard-live'
CONTRACT='raw-snapshot.v2'
MAX_BYTES=64*1024*1024
MAX_COMPRESSED_BYTES=MAX_BYTES+1024*1024
SAFE_QUERY_KEYS={
    'series_id','file_type','sort_order','limit','offset','units','frequency',
    'observation_start','observation_end','realtime_start','realtime_end',
    'symbol','symbols','range','interval','id','datasetname','tablename','year',
    'filter','fields','sort','page[number]','page[size]','format','output_type',
    'mnemonic','dataset','dataflow','startperiod','endperiod','lastnobservations',
    'startdate','enddate','start','end','tag_names','order_by','method','resultformat',
    '$where','$select','$limit','$offset','$order',
}
KEY=re.compile(r'data/raw/v2/([a-z0-9_-]{1,40})/([a-f0-9]{64})/([a-f0-9]{64})\.bin\.gz')


def source_url(url):
    parsed=urlsplit(url)
    if parsed.scheme!='https' or not parsed.hostname:raise ValueError('HTTPS provider URL required')
    authority=parsed.hostname+(':'+str(parsed.port) if parsed.port else '')
    query=urlencode([(key,value) for key,value in parse_qsl(parsed.query,keep_blank_values=True)
                     if key.lower() in SAFE_QUERY_KEYS])
    safe=urlunsplit(('https',authority,parsed.path,query,''))
    if len(safe.encode('ascii'))>1400:raise ValueError('source identity exceeds metadata bound')
    return safe


def _read(key,bucket):
    match=KEY.fullmatch(str(key))
    if not match:raise ValueError('full v2 source identity required')
    provider,request_sha,sha=match.groups()
    obj=_s3.get_object(Bucket=bucket,Key=key);meta=obj.get('Metadata') or {}
    if meta.get('contract')!=CONTRACT or meta.get('provider')!=provider or meta.get('sha256')!=sha:
        raise ValueError('source identity metadata differs')
    url=meta.get('source_url')
    if url!=source_url(url) or hashlib.sha256(url.encode()).hexdigest()!=request_sha:
        raise ValueError('source request identity differs')
    size=int(meta.get('bytes','0'))
    if not 0<size<=MAX_BYTES:raise ValueError('invalid source byte bound')
    stamp=datetime.fromisoformat(meta.get('captured_at','').replace('Z','+00:00'))
    if stamp.tzinfo is None:raise ValueError('capture clock lacks timezone')
    stored=obj.get('LastModified')
    if not isinstance(stored,datetime) or stored.tzinfo is None or abs((stored-stamp).total_seconds())>300:
        raise ValueError('capture clock differs from actual storage clock')
    compressed=obj['Body'].read(MAX_COMPRESSED_BYTES+1)
    if len(compressed)>MAX_COMPRESSED_BYTES:raise ValueError('archive exceeds byte bound')
    with gzip.GzipFile(fileobj=io.BytesIO(compressed)) as stream:raw=stream.read(MAX_BYTES+1)
    if len(raw)!=size or hashlib.sha256(raw).hexdigest()!=sha:raise ValueError('source bytes differ')
    return raw,meta


def snapshot(provider,url,raw_bytes,bucket=None,meta=None):
    """Capture exact supplied bytes; no caller metadata may override identity.

    Inputs above the explicit bound return None, never a partial archive. The
    deprecated meta argument is accepted for compatibility but not published.
    Only allowlisted public query parameters are retained. A capture clock is
    not the provider's original publication time.
    """
    try:
        if not isinstance(provider,str) or not re.fullmatch(r'[a-z0-9_-]{1,40}',provider):
            raise ValueError('invalid provider')
        if not isinstance(raw_bytes,bytes) or not 0<len(raw_bytes)<=MAX_BYTES:
            raise ValueError('nonempty bounded original bytes required')
        safe=source_url(url);sha=hashlib.sha256(raw_bytes).hexdigest()
        request_sha=hashlib.sha256(safe.encode()).hexdigest()
        key=f'data/raw/v2/{provider}/{request_sha}/{sha}.bin.gz';target=bucket or BUCKET
        metadata={'contract':CONTRACT,'provider':provider,'source_url':safe,'sha256':sha,
                  'bytes':str(len(raw_bytes)),'captured_at':datetime.now(timezone.utc).isoformat()}
        try:
            _s3.put_object(Bucket=target,Key=key,Body=gzip.compress(raw_bytes,mtime=0),
                           ContentType='application/gzip',Metadata=metadata,IfNoneMatch='*')
        except Exception as exc:
            code=str(getattr(exc,'response',{}).get('Error',{}).get('Code',''))
            if code not in ('409','412','ConditionalRequestConflict','PreconditionFailed'):raise
            existing,_=_read(key,target)
            if existing!=raw_bytes:raise ValueError('conditional archive conflict') from exc
        return key
    except Exception as exc:
        # URLs, credentials and provider error bodies never enter logs.
        print('[raw_snapshot] capture unavailable: '+type(exc).__name__)
        return None


def read_snapshot(key,bucket=None):
    """Return only bytes whose full content and request identity verify.

    Existing legacy objects are preserved. A short-hash legacy key, arbitrary
    object key or unverifiable object returns None rather than v2 evidence.
    """
    try:return _read(key,bucket or BUCKET)[0]
    except Exception:return None


def snapshot_receipt(key,bucket=None):
    """Inspectable proof after an actual archive read; no forecast authority."""
    raw,meta=_read(key,bucket or BUCKET)
    return {'contract':CONTRACT,'key':key,'provider':meta['provider'],'source_url':meta['source_url'],
            'sha256':meta['sha256'],'bytes':len(raw),'captured_at':meta['captured_at'],
            'captured_bytes_verified':True,'publication_time_verified':False,
            'provider_response_completeness':'caller_responsibility',
            'representation':'complete_bytes_supplied_by_collector','sizing_eligible':False}
