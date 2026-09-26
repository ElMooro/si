"""Whole bounded public research objects, including stored gzip representations.

The hash identifies complete stored bytes. Transport decoding is not source,
observation-time, forecasting or portfolio qualification.
"""
from datetime import datetime,timezone
from pathlib import Path
import hashlib,json,math,re,zlib
from private_artifact import public_source_allowed

MAX_BYTES=32*1024*1024
CHUNK=64*1024


def policy():
    return {'contract':'research-source-read.v1','stored_byte_limit':MAX_BYTES,
        'decoded_byte_limit':MAX_BYTES,'source_hash_basis':'complete_stored_representation',
        'compression':'identity or one complete gzip member; declared encoding and magic must agree when declared',
        'reader_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}


def read(client,bucket,key):
    if not isinstance(key,str) or not re.fullmatch(r'data/[A-Za-z0-9_.-]+\.json',key) or not public_source_allowed(key):raise ValueError('private research source rejected')
    obj=client.get_object(Bucket=bucket,Key=key);stream=obj['Body'];chunks=[];length=0
    try:
        while True:
            chunk=stream.read(min(CHUNK,MAX_BYTES+1-length))
            if not chunk:break
            length+=len(chunk)
            if length>MAX_BYTES:raise ValueError('SOURCE_EXCEEDS_CAPTURE_BOUND')
            chunks.append(chunk)
    finally:stream.close()
    raw=b''.join(chunks);chunks.clear();declared=obj.get('ContentLength')
    if declared is not None and (type(declared) is not int or declared!=len(raw)):raise ValueError('SOURCE_STORED_LENGTH_MISMATCH')
    encoding=str(obj.get('ContentEncoding') or '').strip().lower();compressed=raw.startswith(b'\x1f\x8b')
    if encoding not in ('','identity','gzip') or encoding=='gzip' and not compressed or encoding=='identity' and compressed:
        raise ValueError('SOURCE_ENCODING_MISMATCH')
    if compressed:
        try:
            decoder=zlib.decompressobj(16+zlib.MAX_WBITS);decoded=decoder.decompress(raw,MAX_BYTES+1)
            if len(decoded)>MAX_BYTES or decoder.unconsumed_tail:raise ValueError('SOURCE_EXCEEDS_DECODED_BOUND')
            if not decoder.eof or decoder.unused_data:raise ValueError('SOURCE_COMPRESSED_BODY_INCOMPLETE')
        except zlib.error as exc:raise ValueError('SOURCE_COMPRESSED_BODY_INVALID') from exc
    else:decoded=raw
    def pairs(items):
        out={}
        for k,v in items:
            if k in out:raise ValueError('Duplicate source JSON key')
            out[k]=v
        return out
    def number(value):
        n=float(value)
        if not math.isfinite(n):raise ValueError('Nonfinite source JSON')
        return n
    def constant(value):raise ValueError('Nonfinite source JSON')
    try:doc=json.loads(decoded.decode('utf-8'),object_pairs_hook=pairs,parse_float=number,parse_constant=constant)
    except (ValueError,UnicodeError,RecursionError) as exc:raise ValueError('SOURCE_INVALID_JSON') from exc
    if not isinstance(doc,dict):raise ValueError('UNSUPPORTED_SOURCE_SHAPE')
    return doc,hashlib.sha256(raw).hexdigest(),datetime.now(timezone.utc).isoformat()
