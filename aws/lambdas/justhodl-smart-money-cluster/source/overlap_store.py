"""Immutable overlap runs and conditional publication of the existing public key."""
from datetime import datetime, timezone
import hashlib
from pathlib import Path
import re

import holdings_overlap as model

PRIVATE = 'audit-private/20260909-originals/holdings-overlap/'
COMPILERS = ('holdings_overlap.py', 'overlap_store.py')


def missing(exc):
    return getattr(exc, 'response', {}).get('Error', {}).get('Code') in ('NoSuchKey', '404', 'NotFound')


def bounded(body):
    try: raw = body.read(model.MAX_BYTES+1)
    finally: body.close()
    if len(raw) > model.MAX_BYTES: raise ValueError('Complete artifact exceeds bound')
    return raw


def reader(client, bucket):
    cache, used = {}, 0
    def read(key):
        nonlocal used
        if key in cache: return cache[key]
        raw = bounded(client.get_object(Bucket=bucket, Key=key)['Body'])
        if re.search(r'/[a-f0-9]{64}\.(json|py)$', key) and used+len(raw) <= 48*1024*1024:
            cache[key] = raw; used += len(raw)
        return raw
    return read


def immutable(client, bucket, key, raw, content_type='application/json'):
    try: client.put_object(Bucket=bucket, Key=key, Body=raw, IfNoneMatch='*',
                          ContentType=content_type, CacheControl='public, max-age=31536000, immutable' if key.startswith(model.PREFIX) else 'no-store')
    except Exception as exc:
        if getattr(exc, 'response', {}).get('Error', {}).get('Code') not in ('PreconditionFailed', 'ConditionalRequestConflict'): raise
    if bounded(client.get_object(Bucket=bucket, Key=key)['Body']) != raw:
        raise ValueError('Immutable write readback differs')


def compilers():
    result, artifacts = {}, {}
    for name in COMPILERS:
        raw = (Path(__file__).parent/name).read_bytes(); sha = hashlib.sha256(raw).hexdigest()
        key = model.PREFIX+'compilers/'+sha+'.py'
        result[name] = {'key': key, 'sha256': sha, 'bytes': len(raw)}; artifacts[key] = raw
    return result, artifacts


def retain(kind, value, artifacts):
    raw = model.encoded(value)
    if len(raw) > model.MAX_BYTES: raise ValueError('Complete overlap product exceeds bound')
    key = model.PREFIX+kind+'/'+hashlib.sha256(raw).hexdigest()+'.json'
    artifacts[key] = raw
    return model.reference(key, raw)


def replay(manifest, read):
    code, artifacts = compilers()
    if manifest.get('contract') != 'holdings-overlap-replay.v1' or manifest.get('compilers') != code:
        raise ValueError('Reviewed overlap compiler differs')
    for key, raw in artifacts.items():
        if read(key) != raw: raise ValueError('Retained compiler bytes differ')
    source, binding = model.source(read, manifest['source']['canonical_replay'])
    if binding != manifest['source']: raise ValueError('Overlap source binding differs')
    output, artifacts = model.build(source, binding, read, manifest['generated_at'])
    if output != model.verified(manifest['output'], read, model.PREFIX, 'outputs'):
        raise ValueError('Complete overlap replay differs')
    for key, raw in artifacts.items():
        if read(key) != raw: raise ValueError('Complete overlap scope replay differs')
    return output


def run(client, bucket, generated_at=None):
    read = reader(client, bucket)
    source, binding = model.source(read)
    at = generated_at or datetime.now(timezone.utc).isoformat()
    if (model.clock(at)-model.clock(source['source_generated_at'])).total_seconds() < -60 or model.clock(source['generated_at']) > model.clock(at):
        raise ValueError('Future source cannot be published or reused')
    code, code_artifacts = compilers()
    try:
        before = client.get_object(Bucket=bucket, Key=model.CURRENT)
        old_raw = bounded(before['Body']); condition = {'IfMatch': before['ETag']}
    except Exception as exc:
        if not missing(exc): raise
        old_raw = None; condition = {'IfNoneMatch': '*'}
    try: old = model.decode(old_raw) if old_raw else {}
    except (ValueError, TypeError): old = {}
    if not isinstance(old, dict): old = {}
    if old.get('contract') == model.CONTRACT:
        if model.clock(old['source_generated_at']) > model.clock(source['source_generated_at']):
            return {'published': False, 'reason': 'newer_source_already_published'}
        ref = old.get('replay', {})
        previous = model.verified(ref, read, model.PREFIX, 'runs')
        source_stale = (model.clock(at)-model.clock(source['source_generated_at'])).total_seconds() > 48*3600
        if (previous['source'] == binding and previous['compilers'] == code
                and (old['quality']['status'] == 'stale') == source_stale):
            retained = model.verified(previous['output'], read, model.PREFIX, 'outputs')
            if retained != {k: v for k, v in old.items() if k != 'replay'}:
                raise ValueError('Current overlap differs from immutable output')
            return {'published': False, 'reason': 'source_and_compiler_unchanged', 'replay': ref}
    output, artifacts = model.build(source, binding, read, at)
    output_ref = retain('outputs', output, artifacts)
    # All calculation/serialization validation precedes any publication.
    previous = None
    if old_raw is not None:
        sha = hashlib.sha256(old_raw).hexdigest(); private = PRIVATE+sha+'.bin'
        immutable(client, bucket, private, old_raw, 'application/octet-stream')
        previous = {'source_key': model.CURRENT, 'sha256': sha, 'bytes': len(old_raw), 'private_key': private}
    for key, raw in {**code_artifacts, **artifacts}.items():
        immutable(client, bucket, key, raw, 'text/x-python' if key.endswith('.py') else 'application/json')
    manifest = {'contract': 'holdings-overlap-replay.v1', 'generated_at': at, 'source': binding,
                'compilers': code, 'output': output_ref, 'whole_preceding_product': previous}
    if replay(manifest, read) != output: raise ValueError('Prepublication replay differs')
    run_ref = retain('runs', manifest, {})
    immutable(client, bucket, run_ref['key'], model.encoded(manifest))
    body = model.encoded({**output, 'replay': run_ref})
    client.put_object(Bucket=bucket, Key=model.CURRENT, Body=body, ContentType='application/json',
                      CacheControl='no-store', **condition)
    if read(model.CURRENT) != body: raise ValueError('Current overlap readback differs')
    return {'published': True, 'replay': run_ref, 'source': binding, 'generated_at': at,
            'scope_count': len(artifacts)-1, 'whole_preceding_product': previous,
            'paid_ai_calls': 0, 'notifications_sent': 0, 'private_account_reads': 0, 'portfolio_writes': 0}
