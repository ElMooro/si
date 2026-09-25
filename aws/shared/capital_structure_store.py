"""Immutable capital-structure artifacts and exact compiler/source replay.

This storage boundary cannot publish a current head, acquire vendor data,
read accounts, invoke consumers or emit signals. Vendor originals stay private.
"""
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import RLock
import re, sys
import capital_structure_source as source
import capital_structure_measurements as measurements
import capital_structure_research as model
import statement_research_source as identity_common
import statement_research_identity as identity_source
import statement_measurements as identity_measurements

COMPILERS = (source, measurements, model, identity_common, identity_source, identity_measurements, sys.modules[__name__])
MAX = source.MAX


def bounded(stream):
    try:
        raw = stream.read(MAX+1)
    finally:
        stream.close()
    if len(raw) > MAX:
        raise ValueError('Whole capital-structure artifact byte bound exceeded')
    return raw


def artifact_key(key):
    return isinstance(key, str) and bool(
        re.fullmatch(re.escape(source.PRIVATE)+r'[a-f0-9]{64}\.bin', key)
        or re.fullmatch(re.escape(identity_common.PRIVATE)+r'[a-f0-9]{64}\.bin', key)
        or re.fullmatch(re.escape(source.PREFIX)+r'(?:(?:inputs|outputs|runs|records)/[a-f0-9]{64}\.json|compilers/[a-f0-9]{64}\.py)', key))


def reader(client, bucket, before_read=None):
    cache, lock, used = OrderedDict(), RLock(), 0
    def read(key):
        nonlocal used
        if before_read is not None:
            before_read()
        if not artifact_key(key):
            raise ValueError('Reviewed immutable capital-structure source required')
        with lock:
            if key in cache:
                cache.move_to_end(key); return cache[key]
        raw = bounded(client.get_object(Bucket=bucket, Key=key)['Body'])
        with lock:
            if key in cache:
                return cache[key]
            while cache and used+len(raw) > 384*1024*1024:
                _, removed = cache.popitem(last=False); used -= len(removed)
            cache[key] = raw; used += len(raw)
        return raw
    def prefetch(keys):
        keys = list(dict.fromkeys(keys))
        if len(keys) > 35000 or any(not artifact_key(key) for key in keys):
            raise ValueError('Bounded complete immutable source list required')
        with ThreadPoolExecutor(max_workers=8) as pool:
            for _ in pool.map(read, keys): pass
    read.prefetch = prefetch
    return read


def identity(raw, kind):
    if kind not in ('inputs','outputs','runs','records','compilers') or not isinstance(raw, bytes) or not 0 < len(raw) <= MAX:
        raise ValueError('Complete reviewed immutable artifact required')
    return {'key': source.PREFIX+kind+'/'+source.sha(raw)+('.py' if kind=='compilers' else '.json'),
        'sha256': source.sha(raw), 'bytes': len(raw)}


def put_immutable(client, bucket, ref, raw):
    if (not isinstance(ref, dict) or not artifact_key(ref.get('key')) or not ref['key'].startswith(source.PREFIX)
            or ref.get('sha256') != source.sha(raw) or ref.get('bytes') != len(raw)
            or ref['key'].rsplit('/',1)[-1].split('.')[0] != source.sha(raw) or not 0 < len(raw) <= MAX):
        raise ValueError('Exact immutable public capital-structure artifact required')
    try:
        client.put_object(Bucket=bucket, Key=ref['key'], Body=raw,
            ContentType='text/x-python' if ref['key'].endswith('.py') else 'application/json',
            CacheControl='public, max-age=31536000, immutable', IfNoneMatch='*')
    except Exception as exc:
        if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) not in ('409','412','ConditionalRequestConflict','PreconditionFailed'):
            raise
    if bounded(client.get_object(Bucket=bucket, Key=ref['key'])['Body']) != raw:
        raise ValueError('Immutable capital-structure readback differs')


def checked(ref, kind, read):
    if not isinstance(ref, dict) or not artifact_key(ref.get('key')):
        raise ValueError('Recorded artifact identity required')
    raw = read(ref['key'])
    if identity(raw, kind) != ref:
        raise ValueError('Recorded capital-structure '+kind+' bytes differ')
    return source.strict(raw)


def retain(client, bucket, manifest_ref, identity_ref, compiled):
    packet, shards = compiled['packet'], compiled['shards']
    if packet.get('contract') != model.CONTRACT:
        raise ValueError('Qualified capital-structure compiler output required')
    refs = {row['symbol']: row['record'] for row in packet['issuers']}
    if set(refs) != set(shards) or len(refs) != len(packet['issuers']):
        raise ValueError('Every reported issuer shard is required')
    for symbol, shard in shards.items():
        if identity(source.encoded(shard), 'records') != refs[symbol]:
            raise ValueError('Recorded issuer history identity differs')
    # Re-read the retained identity original; caller-supplied bytes cannot
    # substitute for the dated SEC source in the compiled packet.
    cap, raw_index, _ = identity_source.capture(identity_ref, reader(client, bucket))
    index_ref = identity(raw_index, 'inputs')
    if packet['identity_index']['original'] != index_ref or packet['identity_index']['received_at'] != cap['received_at']:
        raise ValueError('Original SEC identity index differs')
    inputs = {'contract':'capital-structure-research-inputs.v1','source_manifest':manifest_ref,'identity_capture':identity_ref}
    for doc in (inputs, packet): identity(source.encoded(doc), 'inputs')
    compilers = {module.__name__: Path(module.__file__).read_bytes() for module in COMPILERS}
    for raw in compilers.values(): identity(raw, 'compilers')
    # All validation above precedes every public write. Originals and journals
    # stay protected; only derived research and already-public SEC data publish.
    put_immutable(client, bucket, index_ref, raw_index)
    def write(symbol): put_immutable(client, bucket, refs[symbol], source.encoded(shards[symbol]))
    with ThreadPoolExecutor(max_workers=4) as pool:
        for _ in pool.map(write, sorted(shards)): pass
    retained = {}
    for kind, doc in (('inputs',inputs),('outputs',packet)):
        raw = source.encoded(doc); ref = identity(raw, kind)
        put_immutable(client, bucket, ref, raw); retained[kind[:-1]] = ref
    compiler_refs = {}
    for name, raw in compilers.items():
        ref = identity(raw, 'compilers'); put_immutable(client, bucket, ref, raw); compiler_refs[name] = ref
    run = {'contract':'capital-structure-original-replay.v1','generated_at':packet['generated_at'],
        **retained,'compilers':compiler_refs,'output_sha256':retained['output']['sha256']}
    raw = source.encoded(run); ref = identity(raw, 'runs'); put_immutable(client, bucket, ref, raw)
    return {'manifest_key':ref['key'],'output_sha256':retained['output']['sha256']}


def verified_run(ref, read):
    if (not isinstance(ref,dict) or set(ref) != {'manifest_key','output_sha256'}
            or not isinstance(ref.get('manifest_key'),str)
            or not re.fullmatch(re.escape(source.PREFIX)+r'runs/[a-f0-9]{64}\.json',ref['manifest_key'])):
        raise ValueError('Immutable capital-structure replay reference required')
    raw = read(ref['manifest_key']); run = source.strict(raw)
    if (identity(raw,'runs')['key'] != ref['manifest_key'] or run.get('contract') != 'capital-structure-original-replay.v1'
            or run.get('output_sha256') != ref['output_sha256']):
        raise ValueError('Recorded capital-structure run differs')
    if set(run.get('compilers',{})) != {m.__name__ for m in COMPILERS}:
        raise ValueError('Exact original compiler closure required')
    for module in COMPILERS:
        raw = Path(module.__file__).read_bytes(); expected = identity(raw,'compilers')
        if run['compilers'][module.__name__] != expected or read(expected['key']) != raw:
            raise ValueError('Matching frozen capital-structure compiler required')
    return run


def replay(ref, read):
    run = verified_run(ref,read)
    inputs, expected = (checked(run[name], name+'s', read) for name in ('input','output'))
    if inputs.get('contract') != 'capital-structure-research-inputs.v1':
        raise ValueError('Original capital-structure and identity inputs required')
    compiled = model.compile_output(inputs['source_manifest'],inputs['identity_capture'],read)
    if (compiled['packet'] != expected or source.sha(source.encoded(expected)) != run['output_sha256']
            or expected['generated_at'] != run['generated_at']):
        raise ValueError('Complete capital-structure original-source replay differs')
    checked(expected['identity_index']['original'],'inputs',read)
    prefetch = getattr(read,'prefetch',None)
    if callable(prefetch): prefetch([v['record']['key'] for v in expected['issuers']])
    for row in expected['issuers']:
        if checked(row['record'],'records',read) != compiled['shards'][row['symbol']]:
            raise ValueError('Recorded whole issuer history differs')
    return compiled
