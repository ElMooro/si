"""Original-bound liquidity replay and conditional publication, without acquisition."""
from datetime import datetime, timezone
from pathlib import Path
import gzip, hashlib, io, json, re, sys
import canonical_fred_replay as canonical
import evidence_store
import report_observations
import research_brief_model
import liquidity_flow_arithmetic as arithmetic
import liquidity_flow_model as model

MAX = 32*1024*1024
PRIVATE = 'audit-private/20260909-originals/liquidity-flow-research/'
SOURCE = 'data/report-measurements.json'
SETTLEMENT = 'data/settlement-fails.json'
COMPILERS = (model, arithmetic, canonical, report_observations, research_brief_model, evidence_store, sys.modules[__name__])
sha = lambda raw: hashlib.sha256(raw).hexdigest()


def bounded(stream):
    try: raw = stream.read(MAX+1)
    finally: stream.close()
    if len(raw) > MAX: raise ValueError('Complete artifact exceeds bound')
    return raw


def error(exc): return str(getattr(exc, 'response', {}).get('Error', {}).get('Code', ''))
def missing(exc): return error(exc) in ('404', 'NoSuchKey')
def conflict(exc): return error(exc) in ('409', '412', 'ConditionalRequestConflict', 'PreconditionFailed')


def allowed(key):
    return isinstance(key, str) and (key in (SOURCE, SETTLEMENT, model.CURRENT) or bool(re.fullmatch(
        r'data/(?:evidence/fred/[a-f0-9]{64}/[a-f0-9]{64}\.bin\.gz|'
        r'(?:report-research|liquidity-flow-research)/(?:runs|inputs|outputs|compilers|snapshots)/[a-f0-9]{64}\.(?:json|py))', key)))


def reader(client, bucket):
    def read(key):
        if not allowed(key): raise ValueError('Unapproved public liquidity source')
        raw = bounded(client.get_object(Bucket=bucket, Key=key)['Body'])
        if key.endswith('.gz'): raw = bounded(gzip.GzipFile(fileobj=io.BytesIO(raw)))
        return raw
    return read


def immutable(client, bucket, key, raw, private=False):
    if not isinstance(raw, bytes) or not 0 < len(raw) <= MAX: raise ValueError('Complete bounded bytes required')
    if private:
        if key != PRIVATE+sha(raw)+'.bin': raise ValueError('Exact private archive identity required')
    elif not re.fullmatch(re.escape(model.PREFIX)+r'(?:runs|inputs|outputs|compilers|snapshots)/'+sha(raw)+r'\.(?:json|py)', key):
        raise ValueError('Exact immutable artifact identity required')
    kind = 'application/octet-stream' if private else 'text/plain' if key.endswith('.py') else 'application/json'
    try:
        client.put_object(Bucket=bucket, Key=key, Body=raw, ContentType=kind, IfNoneMatch='*',
            CacheControl='no-store' if private else 'public, max-age=31536000, immutable')
    except Exception as exc:
        if not conflict(exc): raise
    if bounded(client.get_object(Bucket=bucket, Key=key)['Body']) != raw: raise ValueError('Immutable artifact differs')


def retain_bytes(client, bucket, raw, category):
    key = model.PREFIX+category+'/'+sha(raw)+'.json'
    immutable(client, bucket, key, raw)
    return {'key': key, 'sha256': sha(raw), 'bytes': len(raw)}


def checked(ref, category, read):
    if not isinstance(ref, dict) or not re.fullmatch('[a-f0-9]{64}', ref.get('sha256', '')): raise ValueError('Exact artifact reference required')
    if ref.get('key') != model.PREFIX+category+'/'+ref['sha256']+'.json': raise ValueError('Artifact namespace differs')
    raw = read(ref['key'])
    if sha(raw) != ref['sha256'] or len(raw) != ref.get('bytes'): raise ValueError('Artifact content differs')
    return json.loads(raw)


def compile_output(inputs, read):
    if inputs.get('contract') != 'liquidity-flow-inputs.v1': raise ValueError('Liquidity inputs contract required')
    macro = checked(inputs['macro'], 'snapshots', read)
    originals = canonical.restore(macro, tuple(arithmetic.SPECS), read)
    settlement = checked(inputs['settlement'], 'snapshots', read) if inputs['settlement'] else None
    return model.build(macro, originals, settlement, inputs['generated_at'], inputs['legacy_context'], inputs['settlement'])


def replay(ref, read):
    key = ref.get('manifest_key', '')
    if not re.fullmatch(re.escape(model.PREFIX)+r'runs/[a-f0-9]{64}\.json', key): raise ValueError('Liquidity run identity required')
    raw = read(key); manifest = json.loads(raw)
    if (key != model.PREFIX+'runs/'+sha(raw)+'.json' or manifest.get('contract') != 'liquidity-flow-replay.v1'
        or manifest.get('output_sha256') != ref.get('output_sha256')): raise ValueError('Liquidity run binding differs')
    if set(manifest['compilers']) != {module.__name__ for module in COMPILERS}: raise ValueError('Compiler set differs')
    for module in COMPILERS:
        body = Path(module.__file__).read_bytes(); expected = {'key': model.PREFIX+'compilers/'+sha(body)+'.py', 'sha256': sha(body)}
        if manifest['compilers'][module.__name__] != expected or read(expected['key']) != body: raise ValueError('Matching reviewed compiler required')
    inputs = checked(manifest['input'], 'inputs', read)
    output = compile_output(inputs, read)
    if (output != checked(manifest['output'], 'outputs', read) or model.digest(output) != manifest['output_sha256']
        or output['generated_at'] != manifest['generated_at']): raise ValueError('Original-source liquidity replay differs')
    return output


def retain(client, bucket, inputs, output):
    refs = {name: retain_bytes(client, bucket, model.encoded(value), name+'s') for name, value in (('input', inputs), ('output', output))}
    compilers = {}
    for module in COMPILERS:
        raw = Path(module.__file__).read_bytes(); key = model.PREFIX+'compilers/'+sha(raw)+'.py'
        immutable(client, bucket, key, raw); compilers[module.__name__] = {'key': key, 'sha256': sha(raw)}
    manifest = {'contract': 'liquidity-flow-replay.v1', 'generated_at': inputs['generated_at'], **refs,
        'compilers': compilers, 'output_sha256': model.digest(output),
        'scope': 'Canonical FRED originals replayed; separate FR2004 scoped snapshot is context only.'}
    raw = model.encoded(manifest); key = model.PREFIX+'runs/'+sha(raw)+'.json'; immutable(client, bucket, key, raw)
    ref = {'manifest_key': key, 'output_sha256': manifest['output_sha256']}
    if replay(ref, reader(client, bucket)) != output: raise ValueError('Retained replay differs')
    return ref


def previous_context(client, bucket):
    try: raw = bounded(client.get_object(Bucket=bucket, Key=model.CURRENT)['Body'])
    except Exception as exc:
        if missing(exc): return {}
        raise
    try: previous = json.loads(raw)
    except (ValueError, UnicodeDecodeError): previous = None
    immutable(client, bucket, PRIVATE+sha(raw)+'.bin', raw, True)
    if isinstance(previous, dict) and previous.get('contract') == model.CONTRACT: return previous.get('legacy_context') or {}
    return {'whole_predecessor': {'key': PRIVATE+sha(raw)+'.bin', 'sha256': sha(raw), 'bytes': len(raw), 'status': 'UNQUALIFIED_LEGACY'}}


def publish(client, bucket, packet):
    stamp = model.clock(packet['generated_at'])
    for _ in range(4):
        try:
            obj = client.get_object(Bucket=bucket, Key=model.CURRENT); raw = bounded(obj['Body'])
            try: old = json.loads(raw)
            except (ValueError, UnicodeDecodeError): old = {}
            if not isinstance(old, dict): old = {}
            if old.get('generated_at'):
                previous_stamp = model.clock(old['generated_at'])
                if previous_stamp > stamp: return False
                if previous_stamp == stamp:
                    if old != packet: raise ValueError('Conflicting same-clock publication')
                    return True
            if old.get('contract') == model.CONTRACT:
                if model.clock(old['source_generated_at']) > model.clock(packet['source_generated_at']): return False
                # Protect each original's acquisition clock, not just the wrapper.
                for sid in arithmetic.SPECS:
                    if model.clock(old['series'][sid]['acquired_at']) > model.clock(packet['series'][sid]['acquired_at']): return False
            immutable(client, bucket, PRIVATE+sha(raw)+'.bin', raw, True)
            condition = {'IfMatch': obj['ETag']}
        except Exception as exc:
            if not missing(exc): raise
            condition = {'IfNoneMatch': '*'}
        try:
            client.put_object(Bucket=bucket, Key=model.CURRENT, Body=model.encoded(packet), ContentType='application/json', CacheControl='no-store', **condition)
            live = json.loads(reader(client, bucket)(model.CURRENT))
            if live != packet and model.clock(live['generated_at']) <= stamp: raise ValueError('Publication readback differs')
            return True
        except Exception as exc:
            if not conflict(exc): raise
    raise RuntimeError('Publication conflict limit; immutable run retained')


def run(client, bucket):
    read = reader(client, bucket)
    raw = read(SOURCE); macro = json.loads(raw)
    # Authenticate originals before any publication artifact is written.
    canonical.restore(macro, tuple(arithmetic.SPECS), read)
    macro_ref = retain_bytes(client, bucket, raw, 'snapshots')
    try:
        raw = read(SETTLEMENT); json.loads(raw)
        settlement_ref = retain_bytes(client, bucket, raw, 'snapshots')
    except Exception as exc:
        if not missing(exc): raise
        settlement_ref = None
    inputs = {'contract': 'liquidity-flow-inputs.v1', 'generated_at': datetime.now(timezone.utc).isoformat(),
        'macro': macro_ref, 'settlement': settlement_ref, 'legacy_context': previous_context(client, bucket)}
    output = compile_output(inputs, read)
    ref = retain(client, bucket, inputs, output)
    published = publish(client, bucket, {**output, 'replay': ref})
    return {'published': published, 'generated_at': output['generated_at'], 'quality': output['quality'],
        'replay': ref, 'provider_requests': 0, 'paid_ai_calls': 0, 'notifications_sent': 0,
        'private_account_reads': 0, 'portfolio_writes': 0}
