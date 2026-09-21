"""Retained parent composition, bounded replay and conditional publication.

No source-provider calls, account reads, AI, notifications or engine invocation.
"""
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path
import re, sys
import massive_research_model as model

COMPILERS = (model, sys.modules[__name__])
MAX_CAPTURE_BYTES = 128 * 1024 * 1024


def now(): return datetime.now(timezone.utc).isoformat()
def code(exc): return str(getattr(exc, 'response', {}).get('Error', {}).get('Code', ''))
def missing(exc): return code(exc) in ('404', 'NoSuchKey')
def conflict(exc): return code(exc) in ('409', '412', 'PreconditionFailed', 'ConditionalRequestConflict')


def bounded(stream):
    try: raw = stream.read(model.MAX + 1)
    finally: stream.close()
    if not 0 < len(raw) <= model.MAX: raise ValueError('Composite evidence byte bound')
    return raw


def own_artifact(key):
    return isinstance(key, str) and bool(re.fullmatch(re.escape(model.PRIVATE) + r'[a-f0-9]{64}\.bin|'
        + re.escape(model.PREFIX) + r'(?:inputs|outputs|runs)/[a-f0-9]{64}\.json|'
        + re.escape(model.PREFIX) + r'compilers/[a-f0-9]{64}\.py', key))


def parent_artifact(key):
    return isinstance(key, str) and any(re.fullmatch(re.escape(v[2]) + r'(?:runs|outputs)/[a-f0-9]{64}\.json', key)
        for v in model.SOURCES.values())


def reader(client, bucket, capacity=64 * 1024 * 1024):
    cache = OrderedDict(); size = 0
    def remember(key, raw):
        nonlocal size
        if (not (own_artifact(key) or parent_artifact(key)) or not isinstance(raw, bytes)
                or not 0 < len(raw) <= model.MAX or key.rsplit('/', 1)[-1].split('.')[0] != model.sha(raw)):
            raise ValueError('Verified content-addressed evidence required')
        if len(raw) > capacity: return
        if key in cache: size -= len(cache.pop(key))
        while cache and size + len(raw) > capacity:
            _, old = cache.popitem(last=False); size -= len(old)
        cache[key] = raw; size += len(raw)
    def read(key):
        if not (own_artifact(key) or parent_artifact(key)): raise ValueError('Unreviewed composite evidence read')
        if key in cache: cache.move_to_end(key); return cache[key]
        raw = bounded(client.get_object(Bucket=bucket, Key=key)['Body']); remember(key, raw); return raw
    read.remember = remember
    return read


def immutable(client, bucket, identity, raw, read):
    key = identity.get('key')
    if (not own_artifact(key) or identity.get('sha256') != model.sha(raw)
            or identity.get('bytes') != len(raw) or not 0 < len(raw) <= model.MAX
            or key.rsplit('/', 1)[-1].split('.')[0] != model.sha(raw)):
        raise ValueError('Exact bounded composite write required')
    kind = 'application/octet-stream' if key.startswith(model.PRIVATE) else 'text/x-python' if key.endswith('.py') else 'application/json'
    try: client.put_object(Bucket=bucket, Key=key, Body=raw, IfNoneMatch='*', ContentType=kind,
        CacheControl='no-store' if key.startswith(model.PRIVATE) else 'public, max-age=31536000, immutable')
    except Exception as exc:
        if not conflict(exc): raise
    if bounded(client.get_object(Bucket=bucket, Key=key)['Body']) != raw: raise ValueError('Composite immutable readback differs')
    read.remember(key, raw)


def protect(client, bucket, raw, read):
    identity = model.ref(raw, 'originals'); immutable(client, bucket, identity, raw, read); return identity


def capture(client, bucket, key, read):
    if key not in (*model.CAPTURE_KEYS, *model.PREDECESSORS): raise ValueError('Reviewed public research key required')
    stamp = now()
    try: obj = client.get_object(Bucket=bucket, Key=key)
    except Exception as exc:
        state = 'missing' if missing(exc) else 'read_failed'
        return {'source_key': key, 'acquired_at': stamp, 'status': state, 'original': None}
    raw = bounded(obj['Body'])
    return {'source_key': key, 'acquired_at': now(), 'status': 'retained', 'original': protect(client, bucket, raw, read)}


def capture_inputs(client, bucket, read, checkpoint):
    snapshots = {}; size = 0
    for key in (*model.CAPTURE_KEYS, *model.PREDECESSORS):
        snapshots[key] = capture(client, bucket, key, read)
        size += (snapshots[key].get('original') or {}).get('bytes', 0)
        checkpoint(captured=snapshots, captured_bytes=size)
        if size > MAX_CAPTURE_BYTES: raise ValueError('Whole composite capture bound exceeded')
    # The first retirement must preserve the exact whole predecessor packets.
    if any(snapshots[key]['status'] != 'retained' for key in model.PREDECESSORS):
        raise ValueError('Whole predecessor preservation required before composition')
    return {'contract': 'massive-composite-inputs.v1', 'generated_at': now(),
        'sources': {key: snapshots[key] for key in model.CAPTURE_KEYS},
        'predecessors': {key: snapshots[key] for key in model.PREDECESSORS}}


def checked(identity, kind, read):
    return model.strict(model.exact(identity, read, model.PREFIX, kind))


def verified_run(identity, read):
    if (not isinstance(identity, dict) or set(identity) != {'manifest_key', 'output_sha256'}
            or not model.digest(identity.get('output_sha256')) or not isinstance(identity.get('manifest_key'), str)
            or not re.fullmatch(re.escape(model.PREFIX) + r'runs/[a-f0-9]{64}\.json', identity['manifest_key'])):
        raise ValueError('Reviewed composite run identity required')
    raw = read(identity['manifest_key']); run = model.strict(raw)
    if (model.ref(raw, 'runs')['key'] != identity['manifest_key'] or run.get('contract') != 'massive-composite-replay.v1'
            or run.get('output_sha256') != identity['output_sha256']): raise ValueError('Composite run differs')
    if set(run['compilers']) != {m.__name__ for m in COMPILERS}: raise ValueError('Complete composite compiler inventory required')
    for module in COMPILERS:
        raw = Path(module.__file__).read_bytes(); expected = model.ref(raw, 'compilers')
        if run['compilers'][module.__name__] != expected or read(expected['key']) != raw:
            raise ValueError('Reviewed composite compiler differs')
    return run


def replay(identity, read):
    run = verified_run(identity, read); inputs = checked(run['input'], 'inputs', read)
    output = model.build(inputs, read)
    if (output != checked(run['output'], 'outputs', read) or model.sha(model.encoded(output)) != identity['output_sha256']
            or output['generated_at'] != run['generated_at']): raise ValueError('Composite replay differs')
    return output


def retain(client, bucket, inputs, output, read, checkpoint):
    refs = {}
    for name, doc in (('input', inputs), ('output', output)):
        raw = model.encoded(doc); refs[name] = model.ref(raw, name + 's'); immutable(client, bucket, refs[name], raw, read)
    compilers = {}
    for module in COMPILERS:
        raw = Path(module.__file__).read_bytes(); identity = model.ref(raw, 'compilers')
        immutable(client, bucket, identity, raw, read); compilers[module.__name__] = identity
    run = {'contract': 'massive-composite-replay.v1', 'generated_at': output['generated_at'], **refs,
        'compilers': compilers, 'output_sha256': refs['output']['sha256']}
    raw = model.encoded(run); identity = model.ref(raw, 'runs'); immutable(client, bucket, identity, raw, read)
    replay_ref = {'manifest_key': identity['key'], 'output_sha256': refs['output']['sha256']}
    checkpoint(candidate_replay=replay_ref)
    if replay(replay_ref, read) != output: raise ValueError('Retained composite reconstruction differs')
    return replay_ref


def compatibility(packet, key):
    if packet.get('contract') != model.CONTRACT or key not in model.PREDECESSORS: raise ValueError('Reviewed compatibility head required')
    model.permissions(packet)
    out = {'contract': 'massive-research-compatibility.v1', 'generated_at': packet['generated_at'],
        'canonical': {'key': model.CURRENT, 'replay': packet['replay']},
        'status': 'superseded_by_recorded_research', 'retained_predecessor': packet['predecessors'][key],
        'source_key': key, 'call': None, 'score': None, 'portfolio_action': 'WAIT',
        'independent_investment_votes': 0, **model.PERMISSIONS,
        'meaning': 'Recorded source evidence replaces unsupported ranks and inferred entitlements. Empty legacy collections are not zero market activity.'}
    if key == 'data/massive-signals.json': out.update(tickers={}, top_prepump=[], market={}, sources={}, n_tickers=0)
    elif key == 'data/massive-capability.json': out.update(products={}, entitlement_verified=False)
    elif key == 'data/polygon-options.json': out.update(contracts=[], n=0, entitled=None)
    else: out.update(tickers={}, n=0, n_ok=0, entitled=None)
    return out


def native_clocks(packet):
    return {kind: model.clock(row['source_generated_at']) for kind, row in packet.get('sources', {}).items()
        if isinstance(row, dict) and row.get('source_generated_at')}


def conditional(client, bucket, key, packet, read, publish=None):
    if key not in (model.CURRENT, *model.PREDECESSORS): raise ValueError('Reviewed composite public head required')
    stamp = model.clock(packet['generated_at'])
    if stamp > model.clock(now()): raise ValueError('Future composite publication')
    expected = model.CONTRACT if key == model.CURRENT else 'massive-research-compatibility.v1'
    if packet.get('contract') != expected: raise ValueError('Native public head contract differs')
    model.permissions(packet)
    for _ in range(4):
        try:
            obj = client.get_object(Bucket=bucket, Key=key); raw = bounded(obj['Body']); old = model.strict(raw)
            if old.get('contract') == expected:
                old_at = model.clock(old['generated_at'])
                if old_at > stamp: return False
                if old_at == stamp and old != packet: raise ValueError('Conflicting same-clock composite')
                prior, current = native_clocks(old), native_clocks(packet)
                if any(current[kind] < value for kind, value in prior.items() if kind in current): return False
            elif key == model.CURRENT:
                raise ValueError('Unreviewed canonical predecessor')
            else:
                # Retirement cannot overwrite a concurrently changed predecessor.
                previous = packet['retained_predecessor']
                if previous['status'] != 'retained' or model.original(previous['original'], read) != raw:
                    raise ValueError('Compatibility predecessor changed after capture')
            if old == packet: return True
            protect(client, bucket, raw, read); condition = {'IfMatch': obj['ETag']}
        except Exception as exc:
            if not missing(exc): raise
            condition = {'IfNoneMatch': '*'}
        try:
            body = model.encoded(packet)
            if publish is not None and key == model.CURRENT: publish(client, bucket, key, body, condition)
            else: client.put_object(Bucket=bucket, Key=key, Body=body, ContentType='application/json', CacheControl='no-store', **condition)
            live = model.strict(bounded(client.get_object(Bucket=bucket, Key=key)['Body']))
            if live != packet and model.clock(live['generated_at']) <= stamp: raise ValueError('Composite public readback differs')
            return True
        except Exception as exc:
            if not conflict(exc): raise
    raise RuntimeError('Composite publication contention; immutable evidence retained')


def request_key(request_id):
    if not isinstance(request_id, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,120}', request_id): raise ValueError('Bounded request identity required')
    return model.PRIVATE + 'requests/' + model.sha(request_id.encode()) + '.json'


def status_write(client, bucket, key, packet, **condition):
    if not re.fullmatch(re.escape(model.PRIVATE) + r'requests/[a-f0-9]{64}\.json', key): raise ValueError('Private composite status path required')
    client.put_object(Bucket=bucket, Key=key, Body=model.encoded(packet), ContentType='application/json', CacheControl='no-store', **condition)


def run(client, bucket, request_id, execution_id, recover_run=None, publish=None, publish_current=True):
    if not isinstance(execution_id, str) or not execution_id: raise ValueError('Durable execution identity required')
    key = request_key(request_id)
    status = {'contract': 'massive-composite-request.v1', 'request_id': request_id, 'execution_id': execution_id,
        'started_at': now(), 'status': 'running', 'phase': 'capture', 'publish_current': publish_current}
    try: status_write(client, bucket, key, status, IfNoneMatch='*')
    except Exception as exc:
        if not conflict(exc): raise
        return model.strict(bounded(client.get_object(Bucket=bucket, Key=key)['Body']))
    def checkpoint(**fields): status.update(fields); status_write(client, bucket, key, status)
    try:
        read = reader(client, bucket); expected = None
        if recover_run is None: inputs = capture_inputs(client, bucket, read, checkpoint)
        else:
            old = verified_run(recover_run, read); inputs = checked(old['input'], 'inputs', read)
            expected = checked(old['output'], 'outputs', read); checkpoint(recovered_from=recover_run)
        raw = model.encoded(inputs); identity = model.ref(raw, 'inputs'); immutable(client, bucket, identity, raw, read)
        checkpoint(phase='composition', retained_input=identity); output = model.build(inputs, read)
        if expected is not None and expected != output: raise ValueError('Recovered composite differs')
        identity = retain(client, bucket, inputs, output, read, checkpoint)
        checkpoint(phase='publish'); published = False; aliases = {}
        if publish_current:
            packet = {**output, 'replay': identity}; published = conditional(client, bucket, model.CURRENT, packet, read, publish)
            if published:
                for alias in model.PREDECESSORS:
                    aliases[alias] = conditional(client, bucket, alias, compatibility(packet, alias), read)
                    checkpoint(aliases=aliases)
        checkpoint(status='complete', phase='complete', completed_at=now(), generated_at=output['generated_at'],
            replay=identity, published=published, aliases=aliases, quality=output['quality'], provider_requests=0,
            engine_invocations=0, private_account_reads=0, paid_ai_calls=0, notifications_sent=0, portfolio_writes=0)
        return status
    except Exception as exc:
        checkpoint(status='failed', completed_at=now(), error='composite_capture_replay_or_publication_failed', failure_class=type(exc).__name__)
        raise RuntimeError('Native composite failed; inspect retained request evidence') from None
