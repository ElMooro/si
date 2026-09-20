"""Retained inputs, complete replay and conditional CapitalFlow publication."""
from datetime import datetime, timezone
from pathlib import Path
import hashlib
import re

import capital_research as model

COMPILERS = ('capital_bridge.py', 'capital_research.py', 'capital_store.py')
PRIVATE = 'audit-private/20260909-originals/capital-research/'
HISTORY = 'data/capital-flow-history.json'


def bounded(body):
    try: raw = body.read(model.MAX_BYTES+1)
    finally: body.close()
    if len(raw) > model.MAX_BYTES: raise ValueError('Complete artifact exceeds bound')
    return raw


def reader(client, bucket):
    cache, size = {}, 0
    def read(key):
        nonlocal size
        if not isinstance(key, str) or not re.fullmatch(r'data/[A-Za-z0-9_./-]+', key) or '..' in key:
            raise ValueError('Public research path required')
        if key in cache: return cache[key]
        raw = bounded(client.get_object(Bucket=bucket, Key=key)['Body'])
        if re.search(r'/[a-f0-9]{64}\.(json|py)$', key) and size+len(raw) <= 48*1024*1024:
            cache[key] = raw; size += len(raw)
        return raw
    return read


def immutable(client, bucket, key, raw):
    if not key.startswith((model.PREFIX, PRIVATE)):
        raise ValueError('Capital research retention prefix differs')
    try:
        client.put_object(Bucket=bucket, Key=key, Body=raw, IfNoneMatch='*',
            ContentType='text/x-python' if key.endswith('.py') else 'application/octet-stream' if key.endswith('.bin') else 'application/json',
            CacheControl='public, max-age=31536000, immutable' if key.startswith(model.PREFIX) else 'no-store')
    except Exception as exc:
        if getattr(exc, 'response', {}).get('Error', {}).get('Code') not in ('PreconditionFailed', 'ConditionalRequestConflict'): raise
    if bounded(client.get_object(Bucket=bucket, Key=key)['Body']) != raw:
        raise ValueError('Immutable write readback differs')


def compilers():
    refs, artifacts = {}, {}
    for name in COMPILERS:
        raw = (Path(__file__).parent/name).read_bytes(); sha = hashlib.sha256(raw).hexdigest()
        key = model.PREFIX+'compilers/'+sha+'.py'
        refs[name] = model.reference(key, raw); artifacts[key] = raw
    return refs, artifacts


def retain(kind, value, artifacts):
    raw = model.encoded(value)
    if len(raw) > model.MAX_BYTES: raise ValueError('Complete research product exceeds bound')
    key = model.PREFIX+kind+'/'+hashlib.sha256(raw).hexdigest()+'.json'; artifacts[key] = raw
    return model.reference(key, raw)


def legacy_contexts(old, old_raw, read):
    if old.get('contract') == model.CONTRACT: return old['legacy_contexts'], {}, {}
    if old.get('engine') != 'capital-flow' or old.get('version') != '2.0':
        raise ValueError('Unrecognized preceding producer; review before cutover')
    history = read(HISTORY)
    if not isinstance(model.decode(history).get('entries'), list): raise ValueError('Whole preceding history required')
    refs, artifacts = {}, {}
    for key, raw in ((model.CURRENT, old_raw), (HISTORY, history)):
        sha = hashlib.sha256(raw).hexdigest(); destination = model.PREFIX+'legacy/'+sha+'.json'
        artifacts[destination] = raw
        refs[key] = {'artifact': model.reference(destination, raw), 'qualification': 'unqualified_legacy_calculation',
                     'interpretation': 'Complete earlier public calculation retained for audit. These scores and dollar-flow labels are not qualified transactions or forecasting evidence.'}
    return refs, artifacts, {model.CURRENT: old_raw, HISTORY: history}


def replay(manifest, read):
    code, code_bytes = compilers()
    if manifest.get('contract') != 'capital-evidence-replay.v1' or manifest.get('compilers') != code:
        raise ValueError('Reviewed CapitalFlow compiler differs')
    for key, raw in code_bytes.items():
        if read(key) != raw: raise ValueError('Retained compiler bytes differ')
    holdings, flows, bindings = model.source(read, manifest['source'])
    output, artifacts = model.build(holdings, flows, bindings, read, manifest['generated_at'], manifest['legacy_contexts'])
    if output != model.verified(manifest['output'], read, model.PREFIX, 'outputs'):
        raise ValueError('Complete CapitalFlow replay differs')
    for key, raw in artifacts.items():
        if read(key) != raw: raise ValueError('Complete manager bridge replay differs')
    return output


def run(client, bucket, generated_at=None):
    read = reader(client, bucket)
    holdings, flows, bindings = model.source(read)
    at = generated_at or datetime.now(timezone.utc).isoformat()
    clocks = model.freshness(holdings, flows, at)
    code, code_bytes = compilers()
    before = client.get_object(Bucket=bucket, Key=model.CURRENT)
    old_raw = bounded(before['Body']); old = model.decode(old_raw)
    if not isinstance(old, dict): raise ValueError('Whole preceding CapitalFlow packet required')
    if old.get('contract') == model.CONTRACT:
        if model.clock(old['generated_at']) > model.clock(at):
            return {'published': False, 'reason': 'newer_publication_already_exists'}
        if any(model.clock(old['source_clocks'][k]['source_generated_at']) > model.clock(clocks[k]['source_generated_at']) for k in clocks):
            return {'published': False, 'reason': 'newer_source_already_published'}
        previous = model.verified(old['replay'], read, model.PREFIX, 'runs')
        stale = any(v['collection_stale'] for v in clocks.values())
        if previous['source'] == bindings and previous['compilers'] == code and (old['quality']['status'] == 'stale') == stale:
            retained = model.verified(previous['output'], read, model.PREFIX, 'outputs')
            if retained != {k: v for k, v in old.items() if k != 'replay'}: raise ValueError('Current output differs from retained evidence')
            return {'published': False, 'reason': 'source_and_compiler_unchanged', 'replay': old['replay']}
    legacy, context_artifacts, initial_previous = legacy_contexts(old, old_raw, read)
    def prepared(key): return context_artifacts[key] if key in context_artifacts else read(key)
    output, artifacts = model.build(holdings, flows, bindings, prepared, at, legacy)
    output_ref = retain('outputs', output, artifacts)
    # No write occurs before full source validation and complete calculation.
    previous_refs = {}
    for key, raw in {model.CURRENT: old_raw, **initial_previous}.items():
        sha = hashlib.sha256(raw).hexdigest(); destination = PRIVATE+sha+'.bin'
        immutable(client, bucket, destination, raw)
        previous_refs[key] = {'sha256': sha, 'bytes': len(raw), 'private_key': destination}
    for key, raw in {**code_bytes, **context_artifacts, **artifacts}.items(): immutable(client, bucket, key, raw)
    manifest = {'contract': 'capital-evidence-replay.v1', 'generated_at': at, 'source': bindings,
        'legacy_contexts': legacy, 'compilers': code, 'output': output_ref, 'whole_preceding_products': previous_refs}
    if replay(manifest, read) != output: raise ValueError('Prepublication replay differs')
    run_ref = retain('runs', manifest, {})
    immutable(client, bucket, run_ref['key'], model.encoded(manifest))
    body = model.encoded({**output, 'replay': run_ref})
    client.put_object(Bucket=bucket, Key=model.CURRENT, Body=body, ContentType='application/json',
                      CacheControl='no-store', IfMatch=before['ETag'])
    if read(model.CURRENT) != body: raise ValueError('Current publication readback differs')
    return {'published': True, 'replay': run_ref, 'generated_at': at, 'source': bindings,
            'counts': output['counts'], 'whole_preceding_products': previous_refs,
            'paid_ai_calls': 0, 'notifications_sent': 0, 'private_account_reads': 0, 'portfolio_writes': 0}
