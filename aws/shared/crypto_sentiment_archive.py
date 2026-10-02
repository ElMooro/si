"""Retain public sentiment originals and reviewed compilers, then replay before use.

Only content-addressed data/crypto-sentiment-research artifacts may be read.
No mutable pointer, account input, downloaded-code execution or decision authority.
"""
from base64 import b64decode
from datetime import datetime, timezone
from pathlib import Path
import hashlib, json, math, re
import crypto_sentiment_observations as model
import crypto_sentiment_transport as transport

PREFIX = 'data/crypto-sentiment-research/'
CONTRACT = 'crypto-sentiment-original-replay.v1'
MAX_BYTES = 64 * 1024 * 1024


def encode(doc):
    return (json.dumps(doc, sort_keys=True, ensure_ascii=True, allow_nan=False, separators=(',', ':')) + '\n').encode()


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def clock(value):
    if not isinstance(value, str):
        raise ValueError('Exact acquisition timestamp required')
    try:
        stamp = datetime.fromisoformat(value.replace('Z', '+00:00'))
    except (ValueError, OverflowError):
        raise ValueError('Invalid acquisition timestamp') from None
    if stamp.tzinfo is None or stamp.utcoffset().total_seconds() != 0:
        raise ValueError('UTC acquisition clock required')
    return stamp


def strict(raw):
    if not isinstance(raw, bytes) or not 0 < len(raw) <= MAX_BYTES:
        raise ValueError('Whole bounded archive required')
    def pairs(items):
        out = {}
        for key, value in items:
            if key in out:
                raise ValueError('Duplicate archive member')
            out[key] = value
        return out
    def finite(value):
        number = float(value)
        if not math.isfinite(number):
            raise ValueError('Nonfinite archive number')
        return number
    return json.loads(raw.decode('utf-8'), object_pairs_hook=pairs, parse_float=finite,
                      parse_constant=lambda _: (_ for _ in ()).throw(ValueError('Nonfinite archive')))


def reference(kind, raw):
    if kind not in ('captures', 'runs', 'compilers') or not isinstance(raw, bytes) or not 0 < len(raw) <= MAX_BYTES:
        raise ValueError('Named bounded archive artifact required')
    suffix = '.py' if kind == 'compilers' else '.json'
    return {'key': PREFIX + kind + '/' + sha(raw) + suffix, 'bytes': len(raw), 'sha256': sha(raw)}


def validate_ref(ref, kind):
    if not isinstance(ref, dict) or set(ref) != {'key', 'bytes', 'sha256'}:
        raise ValueError('Exact archive reference required')
    if kind not in ('captures', 'runs', 'compilers') or not isinstance(ref['sha256'], str) or not re.fullmatch('[a-f0-9]{64}', ref['sha256']):
        raise ValueError('Exact archive identity required')
    suffix = '.py' if kind == 'compilers' else '.json'
    if type(ref['bytes']) is not int or not 0 < ref['bytes'] <= MAX_BYTES or ref['key'] != PREFIX + kind + '/' + ref['sha256'] + suffix:
        raise ValueError('Archive namespace or byte count differs')
    return ref


def complete_read(client, bucket, ref, kind):
    validate_ref(ref, kind)
    obj = client.get_object(Bucket=bucket, Key=ref['key'])
    stream, chunks, count = obj['Body'], [], 0
    try:
        if type(obj.get('ContentLength')) is not int or obj['ContentLength'] != ref['bytes']:
            raise ValueError('Declared stored length differs')
        while count <= ref['bytes']:
            chunk = stream.read(min(65536, ref['bytes'] + 1 - count))
            if not isinstance(chunk, bytes):
                raise ValueError('Binary archive body required')
            if not chunk:
                break
            chunks.append(chunk); count += len(chunk)
    finally:
        stream.close()
    raw = b''.join(chunks)
    if len(raw) != ref['bytes'] or sha(raw) != ref['sha256']:
        raise ValueError('Complete archive bytes differ')
    return raw


def retain_bytes(client, bucket, kind, raw):
    ref = reference(kind, raw)
    try:
        client.put_object(Bucket=bucket, Key=ref['key'], Body=raw, IfNoneMatch='*',
                          ContentType='text/plain; charset=utf-8' if kind == 'compilers' else 'application/json',
                          CacheControl='public, max-age=31536000, immutable')
    except Exception as exc:
        code = str(getattr(exc, 'response', {}).get('Error', {}).get('Code', ''))
        if code not in ('PreconditionFailed', 'ConditionalRequestConflict', '409', '412'):
            raise
    if complete_read(client, bucket, ref, kind) != raw:
        raise ValueError('Immutable archive readback differs')
    return ref


def replay_capture(packet):
    if not isinstance(packet,dict) or 'source_attempts' not in packet:
        raise ValueError('Complete sentiment source ledger required')
    recomputed=transport.project(packet['source_attempts'],model)
    if encode(packet)!=encode(recomputed):raise ValueError('Whole sentiment source computation differs')
    return recomputed


def compiler_bytes():
    return {'crypto_sentiment_observations.py':Path(model.__file__).read_bytes(),
            'crypto_sentiment_transport.py':Path(transport.__file__).read_bytes(),
            'crypto_sentiment_archive.py':Path(__file__).read_bytes()}


def replay(client,bucket,ref):
    manifest=strict(complete_read(client,bucket,ref,'runs'))
    keys={'contract','capture','compilers','acquisition_started_at','acquisition_completed_at',
          'calls_eligible','sizing_eligible','execution_eligible','source_qualified','point_in_time_qualified','retention_mode'}
    if not isinstance(manifest,dict) or set(manifest)!=keys or manifest['contract']!=CONTRACT:
        raise ValueError('Exact sentiment run manifest required')
    for key in ('calls_eligible','sizing_eligible','execution_eligible','source_qualified','point_in_time_qualified'):
        if manifest[key] is not False:raise ValueError('Original archive cannot grant decision authority')
    if manifest['retention_mode']!='content_addressed_conditional_write_not_object_lock':raise ValueError('Exact retention assurance required')
    compilers=compiler_bytes()
    if not isinstance(manifest['compilers'],dict) or set(manifest['compilers'])!=set(compilers):raise ValueError('Complete local compiler set required')
    for name,raw in compilers.items():
        if manifest['compilers'][name]!=reference('compilers',raw) or complete_read(client,bucket,manifest['compilers'][name],'compilers')!=raw:
            raise ValueError('Compiler differs; use the matching reviewed checkout')
    packet=replay_capture(strict(complete_read(client,bucket,manifest['capture'],'captures')))
    for key in ('acquisition_started_at','acquisition_completed_at'):
        if manifest[key]!=packet['source_attempts'][0 if key=='acquisition_started_at' else -1][key]:raise ValueError('Manifest clock differs from original acquisition')
    return packet


def retain(client,bucket,packet):
    replay_capture(packet)
    compilers={name:retain_bytes(client,bucket,'compilers',raw) for name,raw in compiler_bytes().items()}
    capture=retain_bytes(client,bucket,'captures',encode(packet))
    manifest={'contract':CONTRACT,'capture':capture,'compilers':compilers,
              'acquisition_started_at':packet['source_attempts'][0]['acquisition_started_at'],
              'acquisition_completed_at':packet['source_attempts'][-1]['acquisition_completed_at'],
              'calls_eligible':False,'sizing_eligible':False,'execution_eligible':False,
              'source_qualified':False,'point_in_time_qualified':False,
              'retention_mode':'content_addressed_conditional_write_not_object_lock'}
    ref=retain_bytes(client,bucket,'runs',encode(manifest))
    if encode(replay(client,bucket,ref))!=encode(packet):raise ValueError('Retained sentiment original differs')
    return {'contract':CONTRACT,'manifest':ref,'capture':capture,'complete_capture_replayed':True,
            'source_qualified':False,'point_in_time_qualified':False,'investment_authority':False}


class BudgetStore:
    """Reserve final publication time; no SDK retries or new provider requests.

    Check before every storage call and body chunk. Network timeouts remain
    per operation, not a claim of an end-to-end Lambda execution guarantee.
    """
    def __init__(self, client, remaining_ms):
        self.client, self.remaining_ms = client, remaining_ms

    def check(self):
        remaining = self.remaining_ms()
        if type(remaining) is not int or remaining < 30000:
            raise ValueError('Sentiment archive publication budget unavailable')

    def put_object(self, **kw):
        self.check()
        return self.client.put_object(**kw)

    def get_object(self, **kw):
        self.check()
        value = self.client.get_object(**kw)
        stream, check = value['Body'], self.check
        class CheckedBody:
            def read(self, count):
                check()
                return stream.read(count)
            def close(self):
                stream.close()
        return {**value, 'Body': CheckedBody()}


def storage_client():
    import boto3
    from botocore.config import Config
    return boto3.client('s3', config=Config(connect_timeout=2, read_timeout=3,
                                          retries={'total_max_attempts': 1}))


def collect_retained(opener,client_factory,bucket,remaining_ms):
    packet=transport.collect(opener,model,remaining_ms)
    try:
        remaining=remaining_ms()
        if type(remaining) is not int or remaining<30000:raise ValueError('Sentiment archive budget unavailable')
        client=client_factory()
        try:proof=retain(BudgetStore(client,remaining_ms),bucket,packet)
        finally:client.close()
    except Exception:
        raise ValueError('Sentiment originals unavailable; descriptive publication withheld') from None
    return {**packet,'original_capture':proof}
