"""Immutable accounting research storage and exact source/compiler replay.

Cannot change a current packet, fetch provider data, or invoke any consumer.
"""
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import re, sys
import statement_research_source as source
import statement_measurements as measurements
import statement_research_model as model

COMPILERS = (source, measurements, model, sys.modules[__name__])
MAX = source.MAX


def bounded(stream):
    try:
        raw = stream.read(MAX + 1)
    finally:
        stream.close()
    if len(raw) > MAX:
        raise ValueError('Whole accounting artifact byte bound')
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
            raise ValueError('Reviewed immutable accounting path required')
        if key in cache:
            cache.move_to_end(key)
            return cache[key]
        raw = bounded(client.get_object(Bucket=bucket, Key=key)['Body'])
        while cache and used + len(raw) > 96 * 1024 * 1024:
            _, removed = cache.popitem(last=False)
            used -= len(removed)
        cache[key] = raw
        used += len(raw)
        return raw
    return read


def identity(raw, kind):
    if kind not in ('inputs', 'outputs', 'runs', 'records', 'compilers') or not isinstance(raw, bytes) or not 0 < len(raw) <= MAX:
        raise ValueError('Complete reviewed artifact class required')
    return {'key': model.PREFIX + kind + '/' + source.sha(raw) + ('.py' if kind == 'compilers' else '.json'),
            'sha256': source.sha(raw), 'bytes': len(raw)}


def put_immutable(client, bucket, ref, raw):
    if (not artifact_key(ref.get('key')) or not ref['key'].startswith(model.PREFIX)
            or ref['sha256'] != source.sha(raw) or ref['bytes'] != len(raw)
            or ref['key'].rsplit('/', 1)[-1].split('.')[0] != source.sha(raw) or not 0 < len(raw) <= MAX):
        raise ValueError('Exact immutable public accounting artifact required')
    try:
        client.put_object(Bucket=bucket, Key=ref['key'], Body=raw,
            ContentType='text/x-python' if ref['key'].endswith('.py') else 'application/json',
            CacheControl='public, max-age=31536000, immutable', IfNoneMatch='*')
    except Exception as exc:
        if str(getattr(exc, 'response', {}).get('Error', {}).get('Code')) not in ('409', '412', 'ConditionalRequestConflict', 'PreconditionFailed'):
            raise
    if bounded(client.get_object(Bucket=bucket, Key=ref['key'])['Body']) != raw:
        raise ValueError('Immutable artifact readback differs')


def checked(ref, kind, read):
    if not isinstance(ref, dict) or not artifact_key(ref.get('key')):
        raise ValueError('Artifact identity required')
    raw = read(ref['key'])
    if identity(raw, kind) != ref:
        raise ValueError('Recorded ' + kind + ' differs')
    return source.strict(raw)


def retain(client, bucket, manifest_ref, compiled):
    packet, shards = compiled['packet'], compiled['shards']
    refs = {row['symbol']: row['record'] for row in packet['issuers']}
    if set(refs) != set(shards) or len(refs) != len(packet['issuers']):
        raise ValueError('Exact complete issuer index required')
    # Prepare/validate everything before writing. Originals stay private.
    for symbol, shard in shards.items():
        if identity(source.encoded(shard), 'records') != refs[symbol]:
            raise ValueError('Accounting record identity differs')
    inputs = {'contract': 'financial-statement-research-inputs.v1', 'source_manifest': manifest_ref}
    for doc in (inputs, packet):
        identity(source.encoded(doc), 'inputs')
    def write(symbol):
        put_immutable(client, bucket, refs[symbol], source.encoded(shards[symbol]))
    with ThreadPoolExecutor(max_workers=4) as pool:
        for _ in pool.map(write, sorted(shards)):
            pass
    retained = {}
    for kind, doc in (('inputs', inputs), ('outputs', packet)):
        raw = source.encoded(doc); ref = identity(raw, kind)
        put_immutable(client, bucket, ref, raw)
        retained[kind[:-1]] = ref
    compilers = {}
    for module in COMPILERS:
        raw = Path(module.__file__).read_bytes(); ref = identity(raw, 'compilers')
        put_immutable(client, bucket, ref, raw)
        compilers[module.__name__] = ref
    run = {'contract': 'financial-statement-original-replay.v1', 'generated_at': packet['generated_at'],
        **retained, 'compilers': compilers, 'output_sha256': retained['output']['sha256']}
    raw = source.encoded(run); ref = identity(raw, 'runs')
    put_immutable(client, bucket, ref, raw)
    return {'manifest_key': ref['key'], 'output_sha256': retained['output']['sha256']}


def verified_run(ref, read):
    if (not isinstance(ref, dict) or set(ref) != {'manifest_key', 'output_sha256'}
            or not isinstance(ref.get('manifest_key'), str)
            or not re.fullmatch(re.escape(model.PREFIX) + r'runs/[a-f0-9]{64}\.json', ref['manifest_key'])):
        raise ValueError('Immutable accounting replay reference required')
    raw = read(ref['manifest_key']); run = source.strict(raw)
    if (identity(raw, 'runs')['key'] != ref['manifest_key'] or run.get('contract') != 'financial-statement-original-replay.v1'
            or run.get('output_sha256') != ref['output_sha256']):
        raise ValueError('Recorded accounting run differs')
    if set(run.get('compilers', {})) != {m.__name__ for m in COMPILERS}:
        raise ValueError('Exact accounting compiler set required')
    for module in COMPILERS:
        raw = Path(module.__file__).read_bytes(); expected = identity(raw, 'compilers')
        if run['compilers'][module.__name__] != expected or read(expected['key']) != raw:
            raise ValueError('Matching frozen accounting compiler checkout required')
    return run


def replay(ref, read):
    run = verified_run(ref, read)
    inputs, expected = (checked(run[name], name + 's', read) for name in ('input', 'output'))
    if inputs.get('contract') != 'financial-statement-research-inputs.v1':
        raise ValueError('Reviewed accounting source manifest required')
    compiled = model.compile_output(inputs['source_manifest'], read)
    if (compiled['packet'] != expected or source.sha(source.encoded(expected)) != run['output_sha256']
            or expected['generated_at'] != run['generated_at']):
        raise ValueError('Complete original-source reconstruction differs')
    for summary in expected['issuers']:
        if checked(summary['record'], 'records', read) != compiled['shards'][summary['symbol']]:
            raise ValueError('Recorded complete issuer history differs')
    return compiled
