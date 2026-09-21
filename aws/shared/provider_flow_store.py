"""Retain whole originals, replay exact calculations and publish reviewed flow research."""
from datetime import datetime, timezone
from pathlib import Path
import json, re, sys, time
import provider_flow_native as native
import provider_flow_model as model
import provider_flow_catalog as catalog
import provider_flow_collect as collector

MAX = 64 * 1024 * 1024
PRIVATE = native.PRIVATE
COMPILERS = (native, model, catalog, collector, sys.modules[__name__])
KINDS = {'flow': (model.PREFIX, model.CURRENT, model.CONTRACT),
         'radar': (model.RADAR_PREFIX, model.RADAR_CURRENT, model.RADAR_CONTRACT)}
CONTEXTS = ('etf-flows/measurements.json', 'etf-flows/daily.json', 'etf-flows/composite.json',
    'etf-flows/event-study.json', 'etf-flows/rotation.json', 'etf-flows/per-ticker-context.json',
    'etf-flows/ai-analysis.json', 'etf-flows/constituent-pressure.json', 'data/capital-flow-radar.json',
    'data/capital-flow-radar-state.json', 'data/etf-true-flows.json', 'data/sector-rotation.json')
ALIASES = CONTEXTS[:7] + tuple('data/' + key for key in CONTEXTS[:7])
CONTEXTS = CONTEXTS + ALIASES[7:]
MIGRATION = model.PREFIX + 'migration.json'


def now(): return datetime.now(timezone.utc).isoformat()
def code(exc): return str(getattr(exc, 'response', {}).get('Error', {}).get('Code', ''))
def missing(exc): return code(exc) in ('404', 'NoSuchKey')
def conflict(exc): return code(exc) in ('409', '412', 'PreconditionFailed', 'ConditionalRequestConflict')


def bounded(stream):
    try: raw = stream.read(MAX + 1)
    finally: stream.close()
    if len(raw) > MAX: raise ValueError('Reviewed research byte bound')
    return raw


def artifact(key):
    return isinstance(key, str) and bool(re.fullmatch(re.escape(PRIVATE) + r'[a-f0-9]{64}\.bin|'
        r'data/(?:provider-flow|capital-radar)-research/(?:inputs|outputs|runs|compilers|histories)/[a-f0-9]{64}\.(?:json|py)', key))


def source_key(key):
    return (isinstance(key, str) and (key in CONTEXTS or key == model.CURRENT or
            bool(re.fullmatch(r'etf-flows/history/\d{4}-\d{2}-\d{2}\.json', key))))


def reader(client, bucket):
    cache = {}
    def read(key):
        if not (artifact(key) or source_key(key) or key == MIGRATION): raise ValueError('Unreviewed research read')
        if artifact(key) and key in cache: return cache[key]
        raw = bounded(client.get_object(Bucket=bucket, Key=key)['Body'])
        if artifact(key): cache[key] = raw
        return raw
    return read


def immutable(client, bucket, key, raw, kind='application/json'):
    if (not artifact(key) or not isinstance(raw, bytes) or len(raw) > MAX
            or key.rsplit('/', 1)[-1].split('.')[0] != model.sha(raw)):
        raise ValueError('Exact bounded content-addressed artifact required')
    try:
        client.put_object(Bucket=bucket, Key=key, Body=raw, ContentType=kind, IfNoneMatch='*',
            CacheControl='no-store' if key.startswith(PRIVATE) else 'public, max-age=31536000, immutable')
    except Exception as exc:
        if not conflict(exc): raise
    if bounded(client.get_object(Bucket=bucket, Key=key)['Body']) != raw: raise ValueError('Immutable readback differs')


def protect(client, bucket, raw):
    digest = model.sha(raw); key = PRIVATE + digest + '.bin'
    immutable(client, bucket, key, raw, 'application/octet-stream')
    return {'key': key, 'sha256': digest, 'bytes': len(raw)}


def snapshot(client, bucket, key):
    if not source_key(key): raise ValueError('Unreviewed source snapshot')
    raw = bounded(client.get_object(Bucket=bucket, Key=key)['Body']); doc = json.loads(raw)
    if not isinstance(doc, (dict, list)): raise ValueError('Whole structured predecessor required')
    return {'source_key': key, **protect(client, bucket, raw)}


def protected(ref, read):
    digest = ref.get('sha256', '')
    if (not re.fullmatch('[a-f0-9]{64}', digest) or ref.get('key') != PRIVATE + digest + '.bin'
            or type(ref.get('bytes')) is not int or not 0 < ref['bytes'] <= MAX):
        raise ValueError('Protected artifact identity required')
    raw = read(ref['key'])
    if len(raw) != ref['bytes'] or model.sha(raw) != digest: raise ValueError('Protected original bytes differ')
    return raw


def source(ref, key, read):
    if not source_key(key) or ref.get('source_key') != key: raise ValueError('Retained source identity differs')
    return json.loads(protected(ref, read))


def validate_contexts(contexts, read):
    extra = set(contexts) - set(CONTEXTS)
    if not set(CONTEXTS) <= set(contexts) or len(extra) > 1 or any(not source_key(key) for key in extra):
        raise ValueError('Whole predecessor inventory required')
    for key, ref in contexts.items():
        if ref is not None: source(ref, key, read)
    daily_ref = contexts['etf-flows/daily.json']
    if not daily_ref: raise ValueError('Whole previous flow dataset required')
    stamp = source(daily_ref, 'etf-flows/daily.json', read).get('generated_at', '')[:10]
    if extra and extra != {'etf-flows/history/' + stamp + '.json'}: raise ValueError('Predecessor archive date differs')


def preserve(client, bucket):
    read = reader(client, bucket)
    try: marker = json.loads(read(MIGRATION))
    except Exception as exc:
        if not missing(exc): raise
        refs = {}
        for key in CONTEXTS:
            try: refs[key] = snapshot(client, bucket, key)
            except Exception as error:
                if not missing(error): raise
                refs[key] = None
        if not refs['etf-flows/daily.json']: raise ValueError('Predecessor fund-flow snapshot absent')
        prior = source(refs['etf-flows/daily.json'], 'etf-flows/daily.json', read)
        if prior.get('contract') == 'provider-flow-compatibility.v1': raise ValueError('First migration marker missing')
        stamp = prior.get('generated_at', '')[:10]
        if re.fullmatch(r'\d{4}-\d{2}-\d{2}', stamp):
            key = 'etf-flows/history/' + stamp + '.json'
            try: refs[key] = snapshot(client, bucket, key)
            except Exception as error:
                if not missing(error): raise
                refs[key] = None
        body = {'contract': 'provider-flow-preservation.v1', 'preserved_at': now(), 'contexts': refs}
        marker = protect(client, bucket, model.encoded(body))
        try: status_write(client, bucket, MIGRATION, marker, IfNoneMatch='*')
        except Exception as error:
            if not conflict(error): raise
            marker = json.loads(read(MIGRATION))
    body = json.loads(protected(marker, read))
    if body.get('contract') != 'provider-flow-preservation.v1': raise ValueError('Predecessor preservation contract differs')
    validate_contexts(body['contexts'], read)
    return body['contexts']


def compile_output(inputs, read):
    kind = inputs.get('kind')
    if kind == 'flow':
        validate_contexts(inputs['contexts'], read)
        if inputs.get('previous'): source(inputs['previous'], model.CURRENT, read)
        if type(inputs.get('provider_requests')) is not int or not 0 <= inputs['provider_requests'] <= len(catalog.ETF_UNIVERSE) * native.MAX_PAGES * 2:
            raise ValueError('Bounded provider request count required')
        for collection in inputs['collections'].values():
            if collection.get('rejected_original'): protected(collection['rejected_original'], read)
        return model.build(inputs, read)
    if kind != 'radar' or inputs.get('contract') != 'capital-radar-inputs.v1':
        raise ValueError('Reviewed flow input kind required')
    packet = source(inputs['canonical_source'], model.CURRENT, read)
    if replay(packet['replay'], read) != {k: v for k, v in packet.items() if k != 'replay'}:
        raise ValueError('Canonical provider body differs from original replay')
    if inputs['previous']: source(inputs['previous'], model.RADAR_CURRENT, read)
    return model.radar(packet, inputs['generated_at'], inputs['canonical_source'], inputs['previous']), {}


def checked(ref, prefix, kind, read):
    digest = ref.get('sha256', '')
    if not re.fullmatch('[a-f0-9]{64}', digest) or ref.get('key') != prefix + kind + '/' + digest + '.json':
        raise ValueError('Research artifact identity differs')
    raw = read(ref['key'])
    if type(ref.get('bytes')) is not int or len(raw) != ref['bytes'] or model.sha(raw) != digest:
        raise ValueError('Research artifact bytes differ')
    return json.loads(raw)


def replay(ref, read):
    key = ref.get('manifest_key', '')
    matches = [kind for kind, (prefix, _, _) in KINDS.items()
               if re.fullmatch(re.escape(prefix) + r'runs/[a-f0-9]{64}\.json', key)]
    if len(matches) != 1: raise ValueError('Research run path required')
    kind = matches[0]; prefix, _, contract = KINDS[kind]; raw = read(key); run = json.loads(raw)
    if key != prefix + 'runs/' + model.sha(raw) + '.json' or run.get('contract') != 'provider-flow-replay.v1' or run.get('kind') != kind:
        raise ValueError('Research run identity differs')
    if set(run['compilers']) != {m.__name__ for m in COMPILERS}: raise ValueError('Research compiler inventory differs')
    for module in COMPILERS:
        body = Path(module.__file__).read_bytes(); digest = model.sha(body)
        expected = {'key': prefix + 'compilers/' + digest + '.py', 'sha256': digest}
        if run['compilers'][module.__name__] != expected or read(expected['key']) != body:
            raise ValueError('Matching reviewed compiler required')
    inputs = checked(run['input'], prefix, 'inputs', read)
    if inputs.get('kind') != kind: raise ValueError('Run/input kind differs')
    output, histories = compile_output(inputs, read)
    for key, body in histories.items():
        if read(key) != body: raise ValueError('Source history reconstruction differs')
    if (output != checked(run['output'], prefix, 'outputs', read) or output.get('contract') != contract
            or model.sha(model.encoded(output)) != ref.get('output_sha256') or run['output_sha256'] != ref['output_sha256']
            or output['generated_at'] != run['generated_at']):
        raise ValueError('Original provider replay differs')
    return output


def retain(client, bucket, inputs, output, histories):
    kind = inputs['kind']; prefix = KINDS[kind][0]; refs = {}
    if len(model.encoded(output)) > 12 * 1024 * 1024:
        raise ValueError('Public research output exceeds the reviewed browser bound')
    for key, raw in histories.items(): immutable(client, bucket, key, raw)
    for name, doc in (('input', inputs), ('output', output)):
        raw = model.encoded(doc); digest = model.sha(raw); key = prefix + name + 's/' + digest + '.json'
        immutable(client, bucket, key, raw); refs[name] = {'key': key, 'sha256': digest, 'bytes': len(raw)}
    compilers = {}
    for module in COMPILERS:
        raw = Path(module.__file__).read_bytes(); digest = model.sha(raw); key = prefix + 'compilers/' + digest + '.py'
        immutable(client, bucket, key, raw, 'text/x-python'); compilers[module.__name__] = {'key': key, 'sha256': digest}
    run = {'contract': 'provider-flow-replay.v1', 'kind': kind, 'generated_at': output['generated_at'],
           **refs, 'compilers': compilers, 'output_sha256': refs['output']['sha256']}
    raw = model.encoded(run); key = prefix + 'runs/' + model.sha(raw) + '.json'; immutable(client, bucket, key, raw)
    ref = {'manifest_key': key, 'output_sha256': refs['output']['sha256']}
    if replay(ref, reader(client, bucket)) != output: raise ValueError('Retained original replay differs')
    return ref


def conditional(client, bucket, current, packet, publish=None):
    stamp = model.clock(packet['generated_at'])
    for _ in range(4):
        try:
            obj = client.get_object(Bucket=bucket, Key=current); raw = bounded(obj['Body']); old = json.loads(raw)
            old_at = old.get('generated_at')
            if old_at and model.clock(old_at) > stamp: return False
            if old_at and model.clock(old_at) == stamp and old != packet: raise ValueError('Conflicting same-clock publication')
            if old.get('contract') == packet.get('contract'):
                if old.get('source_generated_at') and model.clock(old['source_generated_at']) > model.clock(packet['source_generated_at']): return False
                for ticker, row in old.get('funds', {}).items():
                    previous = row.get('latest_effective_date'); new = packet.get('funds', {}).get(ticker, {}).get('latest_effective_date')
                    if previous and new and previous > new: return False
            protect(client, bucket, raw); condition = {'IfMatch': obj['ETag']}
        except Exception as exc:
            if not missing(exc): raise
            condition = {'IfNoneMatch': '*'}
        try:
            if publish is None:
                client.put_object(Bucket=bucket, Key=current, Body=model.encoded(packet), ContentType='application/json', CacheControl='no-store', **condition)
            else:
                publish(client, bucket, current, model.encoded(packet), condition)
            live = json.loads(bounded(client.get_object(Bucket=bucket, Key=current)['Body']))
            if live != packet and model.clock(live['generated_at']) <= stamp: raise ValueError('Publication readback differs')
            return True
        except Exception as exc:
            if not conflict(exc): raise
    raise RuntimeError('Publication conflict limit; immutable evidence retained')


def request_key(kind, request_id):
    if kind not in KINDS or not isinstance(request_id, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,100}', request_id):
        raise ValueError('Canonical idempotent request identity required')
    return KINDS[kind][0] + 'requests/' + model.sha(request_id.encode()) + '.json'


def status_write(client, bucket, key, doc, **condition):
    client.put_object(Bucket=bucket, Key=key, Body=model.encoded(doc), ContentType='application/json', CacheControl='no-store', **condition)


def publish_aliases(client, bucket, packet):
    """Every alias is conservative even if a write fails or a newer run wins."""
    result = {}
    for target in ALIASES:
        result[target] = conditional(client, bucket, target, model.compatibility(packet, target))
    return result


def run(client, bucket, kind, request_id, execution_id, credential='', remaining_seconds=300, publish=None):
    budget_end = time.monotonic() + max(1, min(remaining_seconds, 300))
    key = request_key(kind, request_id)
    status = {'contract': 'provider-flow-request.v1', 'kind': kind, 'request_id': request_id,
              'execution_id': execution_id, 'started_at': now(), 'status': 'running', 'phase': 'snapshot'}
    try: status_write(client, bucket, key, status, IfNoneMatch='*')
    except Exception as exc:
        if not conflict(exc): raise
        return json.loads(bounded(client.get_object(Bucket=bucket, Key=key)['Body']))
    try:
        if kind == 'flow':
            contexts = preserve(client, bucket)
            try: previous = snapshot(client, bucket, model.CURRENT)
            except Exception as exc:
                if not missing(exc): raise
                previous = None
            acquisition = collector.Collector(credential, lambda raw: protect(client, bucket, raw),
                budget_end - 90)
            status['phase'] = 'collect_originals'; status_write(client, bucket, key, status)
            collections, requests = acquisition.collect()
            inputs = {'contract': 'provider-flow-inputs.v1', 'kind': kind, 'generated_at': now(),
                      'contexts': contexts, 'collections': collections, 'provider_requests': requests, 'previous': previous}
        else:
            canonical = snapshot(client, bucket, model.CURRENT)
            try: previous = snapshot(client, bucket, model.RADAR_CURRENT)
            except Exception as exc:
                if not missing(exc): raise
                previous = None
            inputs = {'contract': 'capital-radar-inputs.v1', 'kind': kind, 'generated_at': now(),
                      'canonical_source': canonical, 'previous': previous}
        status['phase'] = 'compile'; status_write(client, bucket, key, status)
        output, histories = compile_output(inputs, reader(client, bucket))
        status['phase'] = 'retained_replay'; status_write(client, bucket, key, status)
        ref = retain(client, bucket, inputs, output, histories)
        status['phase'] = 'publish'; status_write(client, bucket, key, status)
        published = False; aliases = {}
        if output['quality']['status'] != 'unavailable':
            published = conditional(client, bucket, KINDS[kind][1], {**output, 'replay': ref}, publish)
            if kind == 'flow' and published:
                aliases = publish_aliases(client, bucket, {**output, 'replay': ref})
        result = {**status, 'status': 'complete', 'phase': 'complete', 'completed_at': now(),
            'published': published, 'compatibility_publications': aliases,
            'generated_at': output['generated_at'], 'quality': output['quality'], 'replay': ref,
            'provider_requests': inputs.get('provider_requests', 0), 'private_account_reads': 0,
            'paid_ai_calls': 0, 'signals_emitted': 0, 'notifications_sent': 0, 'portfolio_writes': 0}
        status_write(client, bucket, key, result); return result
    except Exception:
        status_write(client, bucket, key, {**status, 'status': 'failed', 'completed_at': now(),
                                           'error': 'original_collection_replay_or_publication_failed'})
        raise RuntimeError('Native provider flow research failed; inspect retained request evidence') from None
