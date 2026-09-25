"""Immutable FINRA settlement research retention and matching-compiler source replay.

Only content-addressed research artifacts may be written here. This module
cannot publish a current packet, invoke a producer or access account data.
"""
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import re, sys
import offexchange_measurements as source
import short_interest_measurements as measurements
import short_interest_research_model as model

COMPILERS = (source, measurements, model, sys.modules[__name__])
MAX = model.MAX


def conflict(exc):
    return str(getattr(exc, 'response', {}).get('Error', {}).get('Code')) in (
        '409', '412', 'ConditionalRequestConflict', 'PreconditionFailed')


def bounded(stream):
    try:
        raw = stream.read(MAX + 1)
    finally:
        stream.close()
    if len(raw) > MAX:
        raise ValueError('Whole artifact byte bound')
    return raw


def artifact_key(key):
    return isinstance(key, str) and bool(
        re.fullmatch(re.escape(model.PRIVATE) + r'[a-f0-9]{64}\.bin', key)
        or re.fullmatch(re.escape(model.PREFIX) + r'(?:(?:inputs|outputs|runs|records)/[a-f0-9]{64}\.json|compilers/[a-f0-9]{64}\.py)', key))


def reader(client, bucket):
    cache = OrderedDict()
    used = 0
    def read(key):
        nonlocal used
        if not artifact_key(key):
            raise ValueError('Reviewed immutable research path required')
        if key in cache:
            cache.move_to_end(key)
            return cache[key]
        raw = bounded(client.get_object(Bucket=bucket, Key=key)['Body'])
        while cache and used + len(raw) > 16 * 1024 * 1024:
            _, removed = cache.popitem(last=False)
            used -= len(removed)
        cache[key] = raw
        used += len(raw)
        return raw
    return read


def identity(raw, kind):
    if kind not in ('inputs', 'outputs', 'runs', 'records', 'compilers'):
        raise ValueError('Reviewed artifact class required')
    return {'key': model.PREFIX + kind + '/' + model.sha(raw) + ('.py' if kind == 'compilers' else '.json'),
            'sha256': model.sha(raw), 'bytes': len(raw)}


def put_immutable(client, bucket, key, raw):
    if (not isinstance(raw, bytes) or not 0 < len(raw) <= MAX or not artifact_key(key)
            or not key.startswith(model.PREFIX) or key.rsplit('/', 1)[-1].split('.')[0] != model.sha(raw)):
        raise ValueError('Exact immutable public research artifact required')
    try:
        client.put_object(Bucket=bucket, Key=key, Body=raw,
                          ContentType='text/x-python' if key.endswith('.py') else 'application/json',
                          CacheControl='public, max-age=31536000, immutable', IfNoneMatch='*')
    except Exception as exc:
        if not conflict(exc):
            raise
    if bounded(client.get_object(Bucket=bucket, Key=key)['Body']) != raw:
        raise ValueError('Immutable artifact readback differs')


def checked(ref, kind, read):
    if not isinstance(ref, dict) or not artifact_key(ref.get('key')):
        raise ValueError('Artifact identity required')
    raw = read(ref['key'])
    if identity(raw, kind) != ref:
        raise ValueError('Recorded ' + kind + ' differs')
    return model.strict(raw)


def retain(client, bucket, inputs, compiled):
    packet, shards = compiled['packet'], compiled['shards']
    if set(packet['record_shards']) != set(shards):
        raise ValueError('Exact record-shard index required')
    # Validate every shard before the first write; serialize again in a bounded
    # worker pool rather than holding a second whole-history byte copy in RAM.
    for key, shard in sorted(shards.items()):
        raw = model.encoded(shard)
        if identity(raw, 'records') != packet['record_shards'][key] or len(raw) > MAX:
            raise ValueError('Record shard differs')
    for doc in (inputs, packet):
        if len(model.encoded(doc)) > MAX:
            raise ValueError('Manifest bound exceeded')
    def write(item):
        key, shard = item
        put_immutable(client, bucket, packet['record_shards'][key]['key'], model.encoded(shard))
    with ThreadPoolExecutor(max_workers=4) as pool:
        for _ in pool.map(write, sorted(shards.items())):
            pass
    refs = {}
    for kind, doc in (('inputs', inputs), ('outputs', packet)):
        raw = model.encoded(doc)
        ref = identity(raw, kind)
        put_immutable(client, bucket, ref['key'], raw)
        refs[kind[:-1]] = ref
    compilers = {}
    for module in COMPILERS:
        raw = Path(module.__file__).read_bytes()
        ref = identity(raw, 'compilers')
        put_immutable(client, bucket, ref['key'], raw)
        compilers[module.__name__] = ref
    run = {'contract': 'short-interest-original-replay.v1', 'generated_at': packet['generated_at'],
           **refs, 'compilers': compilers, 'output_sha256': refs['output']['sha256']}
    raw = model.encoded(run)
    ref = identity(raw, 'runs')
    put_immutable(client, bucket, ref['key'], raw)
    return {'manifest_key': ref['key'], 'output_sha256': refs['output']['sha256']}


def verified_run(ref, read):
    if (not isinstance(ref, dict) or set(ref) != {'manifest_key', 'output_sha256'}
            or not isinstance(ref['manifest_key'], str)
            or not re.fullmatch(re.escape(model.PREFIX) + r'runs/[a-f0-9]{64}\.json', ref['manifest_key'])):
        raise ValueError('Immutable short-interest run required')
    raw = read(ref['manifest_key'])
    run = model.strict(raw)
    if (identity(raw, 'runs')['key'] != ref['manifest_key'] or run.get('contract') != 'short-interest-original-replay.v1'
            or run.get('output_sha256') != ref['output_sha256']):
        raise ValueError('Recorded run differs')
    if set(run.get('compilers', {})) != {m.__name__ for m in COMPILERS}:
        raise ValueError('Exact compiler set required')
    for module in COMPILERS:
        raw = Path(module.__file__).read_bytes()
        expected = identity(raw, 'compilers')
        if run['compilers'][module.__name__] != expected or read(expected['key']) != raw:
            raise ValueError('Matching frozen compiler checkout required')
    return run


def replay(ref, read):
    run = verified_run(ref, read)
    inputs = checked(run['input'], 'inputs', read)
    expected = checked(run['output'], 'outputs', read)
    compiled = model.compile_output(inputs, read)
    if (compiled['packet'] != expected or model.digest(expected) != run['output_sha256']
            or expected['generated_at'] != run['generated_at']):
        raise ValueError('Original-source reconstruction differs')
    for key, shard in compiled['shards'].items():
        if checked(expected['record_shards'][key], 'records', read) != shard:
            raise ValueError('Recorded shard differs')
    return compiled
