"""Immutable original-bound Pulse runs and conditional current publication."""
from datetime import datetime, timezone
from pathlib import Path
import gzip, hashlib, io, json, re, sys
import canonical_fred_replay as canonical
import evidence_store
import report_observations
import research_brief_model
import liquidity_pulse_arithmetic as arithmetic
import liquidity_pulse_model as model

MAX = 32*1024*1024
PRIVATE = 'audit-private/20260909-originals/liquidity-pulse-research/'
SOURCE = 'data/report-measurements.json'
COMPILERS = (model, arithmetic, canonical, report_observations, research_brief_model, evidence_store, sys.modules[__name__])
sha = lambda raw: hashlib.sha256(raw).hexdigest()


def strict(raw):
    def pairs(items):
        result={}
        for key,value in items:
            if key in result:raise ValueError('Duplicate research JSON key')
            result[key]=value
        return result
    def invalid(value):raise ValueError('Nonfinite research JSON number')
    value=json.loads(raw,object_pairs_hook=pairs,parse_constant=invalid)
    model.encoded(value)
    return value


def same_json(left,right):
    # Keep boolean/integer/float types distinct throughout evidence checks.
    return model.encoded(left)==model.encoded(right)


def bounded(stream):
    try: raw = stream.read(MAX+1)
    finally: stream.close()
    if len(raw) > MAX: raise ValueError('Complete artifact exceeds bound')
    return raw


def code(exc): return str(getattr(exc, 'response', {}).get('Error', {}).get('Code', ''))
def missing(exc): return code(exc) in ('404', 'NoSuchKey')
def conflict(exc): return code(exc) in ('409', '412', 'ConditionalRequestConflict', 'PreconditionFailed')


def allowed(key):
    return isinstance(key, str) and (key in (SOURCE, model.CURRENT) or bool(re.fullmatch(
        r'data/(?:evidence/fred/[a-f0-9]{64}/[a-f0-9]{64}\.bin\.gz|'
        r'(?:report-research|liquidity-pulse-research)/(?:runs|inputs|outputs|compilers|snapshots)/[a-f0-9]{64}\.(?:json|py))', key)))


def reader(client, bucket):
    def read(key):
        if not allowed(key): raise ValueError('Unapproved public Pulse source')
        raw = bounded(client.get_object(Bucket=bucket, Key=key)['Body'])
        return bounded(gzip.GzipFile(fileobj=io.BytesIO(raw))) if key.endswith('.gz') else raw
    return read


def immutable(client, bucket, key, raw, private=False):
    if not isinstance(raw, bytes) or len(raw) > MAX or (not private and not raw): raise ValueError('Complete bounded bytes required')
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
    key = model.PREFIX+category+'/'+sha(raw)+'.json'; immutable(client, bucket, key, raw)
    return {'key': key, 'sha256': sha(raw), 'bytes': len(raw)}


def checked(ref, category, read):
    if not isinstance(ref, dict) or not re.fullmatch('[a-f0-9]{64}', ref.get('sha256', '')): raise ValueError('Exact artifact reference required')
    if type(ref.get('bytes')) is not int or not 0 < ref['bytes'] <= MAX: raise ValueError('Exact artifact byte count required')
    if ref.get('key') != model.PREFIX+category+'/'+ref['sha256']+'.json': raise ValueError('Artifact namespace differs')
    raw = read(ref['key'])
    if sha(raw) != ref['sha256'] or len(raw) != ref.get('bytes'): raise ValueError('Artifact content differs')
    return strict(raw)


def compile_output(inputs, read):
    if inputs.get('contract') != 'liquidity-pulse-inputs.v1': raise ValueError('Pulse inputs contract required')
    macro = checked(inputs['macro'], 'snapshots', read)
    originals = canonical.restore(macro, tuple(arithmetic.SPECS), read)
    return model.build(macro, originals, inputs['generated_at'], inputs['legacy_context'], inputs['previous_watermarks'])


def replay(ref, read):
    key = ref.get('manifest_key', '')
    if not re.fullmatch(re.escape(model.PREFIX)+r'runs/[a-f0-9]{64}\.json', key): raise ValueError('Pulse run identity required')
    raw = read(key); manifest = strict(raw)
    if (key != model.PREFIX+'runs/'+sha(raw)+'.json' or manifest.get('contract') != 'liquidity-pulse-replay.v1'
        or manifest.get('output_sha256') != ref.get('output_sha256')): raise ValueError('Pulse run binding differs')
    if set(manifest['compilers']) != {module.__name__ for module in COMPILERS}: raise ValueError('Compiler set differs')
    for module in COMPILERS:
        body = Path(module.__file__).read_bytes(); expected = {'key': model.PREFIX+'compilers/'+sha(body)+'.py', 'sha256': sha(body)}
        if manifest['compilers'][module.__name__] != expected or read(expected['key']) != body: raise ValueError('Matching reviewed compiler required')
    inputs = checked(manifest['input'], 'inputs', read); output = compile_output(inputs, read)
    if (not same_json(output, checked(manifest['output'], 'outputs', read)) or model.digest(output) != manifest['output_sha256']
        or output['generated_at'] != manifest['generated_at']): raise ValueError('Original-source Pulse replay differs')
    return output


def retain(client, bucket, inputs, output):
    refs = {name: retain_bytes(client, bucket, model.encoded(value), name+'s') for name, value in (('input', inputs), ('output', output))}
    compilers = {}
    for module in COMPILERS:
        raw = Path(module.__file__).read_bytes(); key = model.PREFIX+'compilers/'+sha(raw)+'.py'
        immutable(client, bucket, key, raw); compilers[module.__name__] = {'key': key, 'sha256': sha(raw)}
    manifest = {'contract': 'liquidity-pulse-replay.v1', 'generated_at': inputs['generated_at'], **refs,
        'compilers': compilers, 'output_sha256': model.digest(output), 'scope': 'Eleven canonical FRED originals; research measurements only.'}
    raw = model.encoded(manifest); key = model.PREFIX+'runs/'+sha(raw)+'.json'; immutable(client, bucket, key, raw)
    ref = {'manifest_key': key, 'output_sha256': manifest['output_sha256']}
    if not same_json(replay(ref, reader(client, bucket)), output): raise ValueError('Retained replay differs')
    return ref


def previous_state(client, bucket):
    empty = {sid: None for sid in arithmetic.SPECS}
    try: raw = bounded(client.get_object(Bucket=bucket, Key=model.CURRENT)['Body'])
    except Exception as exc:
        if missing(exc): return {}, empty
        raise
    try: previous = strict(raw)
    except (ValueError, UnicodeDecodeError): previous = None
    immutable(client, bucket, PRIVATE+sha(raw)+'.bin', raw, True)
    if isinstance(previous, dict) and previous.get('contract') == model.CONTRACT:
        return previous.get('legacy_context') or {}, model.watermarks(previous)
    return {'whole_predecessor': {'key': PRIVATE+sha(raw)+'.bin', 'sha256': sha(raw), 'bytes': len(raw), 'status': 'UNQUALIFIED_LEGACY'}}, empty


def binding(packet, read):
    flags = ('calls_eligible', 'sizing_eligible', 'execution_eligible', 'forecast_qualified', 'point_in_time_backtest_qualified')
    if (packet.get('contract') != model.CONTRACT or any(packet.get(k) is not False for k in flags)
        or packet.get('decision') != {'verb': 'WAIT', 'meaning': 'abstain'} or packet.get('call') is not None
        or packet.get('transitions') != [] or set(packet.get('series', {})) != set(arithmetic.SPECS)
        or any(row.get(k) is not False for row in packet['series'].values() for k in ('calls_eligible', 'sizing_eligible'))):
        raise ValueError('Complete research-only Pulse publication required')
    ref = packet.get('replay') or {}; key = ref.get('manifest_key', '')
    if not re.fullmatch(re.escape(model.PREFIX)+r'runs/[a-f0-9]{64}\.json', key): raise ValueError('Pulse replay reference required')
    raw = read(key); manifest = strict(raw)
    if (key != model.PREFIX+'runs/'+sha(raw)+'.json' or manifest.get('contract') != 'liquidity-pulse-replay.v1'
        or manifest.get('output_sha256') != ref.get('output_sha256') or manifest.get('generated_at') != packet['generated_at']):
        raise ValueError('Pulse run binding differs')
    output = {k: v for k, v in packet.items() if k != 'replay'}
    if model.digest(output) != manifest['output_sha256'] or not same_json(output, checked(manifest['output'], 'outputs', read)):
        raise ValueError('Pulse publication differs from retained original replay')
    return manifest


def publish(client, bucket, packet):
    # Final callers cannot borrow an earlier run's evidence or grant authority.
    binding(packet, reader(client, bucket))
    stamp = model.clock(packet['generated_at']); watermarks = model.watermarks(packet)
    for _ in range(4):
        try:
            obj = client.get_object(Bucket=bucket, Key=model.CURRENT); raw = bounded(obj['Body'])
            try: old = strict(raw)
            except (ValueError, UnicodeDecodeError): old = {}
            if not isinstance(old, dict): old = {}
            if old.get('generated_at'):
                previous_stamp = model.clock(old['generated_at'])
                if previous_stamp > stamp: return False
                if previous_stamp == stamp:
                    if not same_json(old, packet): raise ValueError('Conflicting same-clock publication')
                    return True
            if old.get('contract') == model.CONTRACT:
                if model.clock(old['source_generated_at']) > model.clock(packet['source_generated_at']): return False
                for sid, before in model.watermarks(old).items():
                    # Missing current rows cannot erase an accepted acquisition
                    # watermark. A later recovery must pass the same boundary.
                    if before and (not watermarks[sid] or model.clock(watermarks[sid]) < model.clock(before)): return False
                    row = packet['series'][sid]
                    if before and row['latest_value'] is not None and (not row['acquired_at'] or model.clock(row['acquired_at']) < model.clock(before)): return False
            immutable(client, bucket, PRIVATE+sha(raw)+'.bin', raw, True); condition = {'IfMatch': obj['ETag']}
        except Exception as exc:
            if not missing(exc): raise
            condition = {'IfNoneMatch': '*'}
        try:
            client.put_object(Bucket=bucket, Key=model.CURRENT, Body=model.encoded(packet), ContentType='application/json', CacheControl='no-store', **condition)
            live = strict(reader(client, bucket)(model.CURRENT))
            if not same_json(live, packet) and model.clock(live['generated_at']) <= stamp: raise ValueError('Publication readback differs')
            return True
        except Exception as exc:
            if not conflict(exc): raise
    raise RuntimeError('Publication conflict limit; immutable run retained')


def run(client, bucket):
    read = reader(client, bucket); raw = read(SOURCE); macro = strict(raw)
    canonical.restore(macro, tuple(arithmetic.SPECS), read)
    source_ref = retain_bytes(client, bucket, raw, 'snapshots')
    legacy, previous = previous_state(client, bucket)
    inputs = {'contract': 'liquidity-pulse-inputs.v1', 'generated_at': datetime.now(timezone.utc).isoformat(),
        'macro': source_ref, 'legacy_context': legacy, 'previous_watermarks': previous}
    output = compile_output(inputs, read); ref = retain(client, bucket, inputs, output)
    published = publish(client, bucket, {**output, 'replay': ref})
    return {'published': published, 'generated_at': output['generated_at'], 'quality': output['quality'],
        'replay': ref, 'provider_requests': 0, 'paid_ai_calls': 0, 'notifications_sent': 0,
        'private_account_reads': 0, 'portfolio_writes': 0}
