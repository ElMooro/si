"""Whole public Backlog snapshots and conditional head publication; no acquisition."""
from pathlib import Path
import hashlib
import json
from backlog_measurements import decode

HEAD='data/backlog.json'
PREFIX='data/backlog/history/'
CONTRACT='backlog-publication.v1'
BOUND=64*1024*1024


def reference(raw):
    return {'key':PREFIX+hashlib.sha256(raw).hexdigest()+'.json',
            'sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw)}


def whole(obj):
    raw=obj['Body'].read(BOUND+1)
    if not isinstance(raw,bytes) or len(raw)>BOUND or type(obj.get('ContentLength')) is not int or obj['ContentLength']!=len(raw):
        raise ValueError('Whole bounded public Backlog object required')
    return raw


def load_head(s3,bucket):
    try:obj=s3.get_object(Bucket=bucket,Key=HEAD)
    except Exception as exc:
        if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) in ('404','NoSuchKey'):
            return None,None,None
        raise
    raw=whole(obj);packet=decode(raw)
    if not isinstance(packet,dict) or not isinstance(packet.get('by_ticker'),dict):
        raise ValueError('Complete prior Backlog ledger required')
    etag=obj.get('ETag')
    if not isinstance(etag,str) or not etag:raise ValueError('Prior publication version required')
    return packet,raw,etag


def archive(s3,bucket,raw):
    if not isinstance(raw,bytes) or len(raw)>BOUND:raise ValueError('Complete bounded archive required')
    ref=reference(raw)
    try:
        s3.put_object(Bucket=bucket,Key=ref['key'],Body=raw,ContentType='application/json',
                      CacheControl='public, max-age=31536000, immutable',IfNoneMatch='*')
    except Exception as exc:
        if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) not in ('412','PreconditionFailed'):
            raise
    actual=whole(s3.get_object(Bucket=bucket,Key=ref['key']))
    if actual!=raw:raise ValueError('Whole immutable Backlog archive differs')
    return ref


def source_identity():
    parent=Path(__file__).parent
    return {name:{'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()}
            for name in ('lambda_function.py','backlog_measurements.py','backlog_store.py','backlog_sources.py')
            for raw in [(parent/name).read_bytes()]}


def publish(s3,bucket,out,prior_raw,etag):
    if (prior_raw is None)!=(etag is None) or etag is not None and (not isinstance(etag,str) or not etag):
        raise ValueError('Publication precondition and prior bytes must agree')
    packet=dict(out)
    packet.update(publication_contract=CONTRACT,source_files=source_identity(),
                  previous_publication=archive(s3,bucket,prior_raw) if prior_raw is not None else None)
    raw=json.dumps(packet,allow_nan=False,separators=(',',':'),ensure_ascii=False).encode('utf-8')
    current=archive(s3,bucket,raw)
    precondition={'IfMatch':etag} if etag is not None else {'IfNoneMatch':'*'}
    s3.put_object(Bucket=bucket,Key=HEAD,Body=raw,ContentType='application/json',
                  CacheControl='public, max-age=3600',**precondition)
    return current
