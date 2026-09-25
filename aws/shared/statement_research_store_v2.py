"""Immutable v2 accounting replay, with bounded parallel original acquisition.

No provider request, current-head write, account read or consumer invocation.
Frozen v1 dependencies are explicitly included in the compiler closure.
"""
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import RLock
import re, sys
import statement_research_source as source
import statement_research_store as previous
import statement_research_identity as identity_source
import statement_research_v2 as model

COMPILERS = (*previous.COMPILERS, identity_source, model, sys.modules[__name__])
MAX = previous.MAX
bounded, artifact_key, identity, put_immutable, checked = (previous.bounded, previous.artifact_key,
    previous.identity, previous.put_immutable, previous.checked)


def reader(client, bucket, before_read=None):
    cache, lock = OrderedDict(), RLock()
    used = 0
    def read(key):
        nonlocal used
        if before_read is not None: before_read()
        if not artifact_key(key):
            raise ValueError('Reviewed immutable accounting path required')
        with lock:
            if key in cache:
                cache.move_to_end(key)
                return cache[key]
        raw = bounded(client.get_object(Bucket=bucket, Key=key)['Body'])
        with lock:
            if key in cache:
                return cache[key]
            while cache and used + len(raw) > 96 * 1024 * 1024:
                _, removed = cache.popitem(last=False)
                used -= len(removed)
            cache[key] = raw
            used += len(raw)
        return raw
    def prefetch(keys):
        keys = list(dict.fromkeys(keys))
        if len(keys) > 8000 or any(not artifact_key(key) for key in keys):
            raise ValueError('Bounded reviewed source acquisition required')
        with ThreadPoolExecutor(max_workers=8) as pool:
            for _ in pool.map(read, keys): pass
    read.prefetch = prefetch
    return read


def retain(client, bucket, manifest_ref, identity_ref, compiled):
    packet, shards = compiled['packet'], compiled['shards']
    if packet.get('contract') != model.CONTRACT:
        raise ValueError('V2 accounting qualification required')
    refs = {row['symbol']: row['record'] for row in packet['issuers']}
    if set(refs) != set(shards) or len(refs) != len(packet['issuers']):
        raise ValueError('Exact complete issuer index required')
    for symbol, shard in shards.items():
        if identity(source.encoded(shard), 'records') != refs[symbol]:
            raise ValueError('Accounting record identity differs')
    read = reader(client, bucket)
    cap, raw_index, _ = identity_source.capture(identity_ref, read)
    index_ref = identity(raw_index, 'inputs')
    if packet['identity_index']['original'] != index_ref or packet['identity_index']['received_at'] != cap['received_at']:
        raise ValueError('Retained SEC index differs')
    inputs = {'contract': 'financial-statement-research-inputs.v2',
        'source_manifest': manifest_ref, 'identity_capture': identity_ref}
    for doc in (inputs, packet): identity(source.encoded(doc), 'inputs')
    # Complete original SEC index is already public at its primary source.
    # FMP responses and capture journals remain protected.
    put_immutable(client, bucket, index_ref, raw_index)
    def write(symbol):
        put_immutable(client, bucket, refs[symbol], source.encoded(shards[symbol]))
    with ThreadPoolExecutor(max_workers=4) as pool:
        for _ in pool.map(write, sorted(shards)): pass
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
    run = {'contract': 'financial-statement-original-replay.v2', 'generated_at': packet['generated_at'],
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
    if (identity(raw, 'runs')['key'] != ref['manifest_key'] or run.get('contract') != 'financial-statement-original-replay.v2'
            or run.get('output_sha256') != ref['output_sha256']):
        raise ValueError('Recorded accounting run differs')
    if set(run.get('compilers', {})) != {m.__name__ for m in COMPILERS}:
        raise ValueError('Exact accounting compiler closure required')
    for module in COMPILERS:
        raw = Path(module.__file__).read_bytes(); expected = identity(raw, 'compilers')
        if run['compilers'][module.__name__] != expected or read(expected['key']) != raw:
            raise ValueError('Matching frozen accounting compiler checkout required')
    return run


def replay(ref, read):
    run = verified_run(ref, read)
    inputs, expected = (checked(run[name], name + 's', read) for name in ('input', 'output'))
    if inputs.get('contract') != 'financial-statement-research-inputs.v2':
        raise ValueError('Reviewed accounting and identity source inputs required')
    compiled = model.compile_output(inputs['source_manifest'], inputs['identity_capture'], read)
    if (compiled['packet'] != expected or source.sha(source.encoded(expected)) != run['output_sha256']
            or expected['generated_at'] != run['generated_at']):
        raise ValueError('Complete original-source reconstruction differs')
    checked(expected['identity_index']['original'], 'inputs', read)
    prefetch = getattr(read, 'prefetch', None)
    if callable(prefetch): prefetch([v['record']['key'] for v in expected['issuers']])
    for summary in expected['issuers']:
        if checked(summary['record'], 'records', read) != compiled['shards'][summary['symbol']]:
            raise ValueError('Recorded complete issuer history differs')
    return compiled
