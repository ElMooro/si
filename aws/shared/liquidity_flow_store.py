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


# Reviewed storage-only predecessor in release 80fecaa5; never execute archived code.
# Stage 428 also reviews the preceding complete source: only bounded transport
# and this compatibility list changed; arithmetic/output compilers remain exact.
REVIEWED_STORAGE_REVISIONS=frozenset({'2897e2d75ec708ac49188d37af645f5ae920ac5de0608db2bbc3dd5afbecc3a6',
    '4e7c99fe5eabdae3380cbdbbb7ecf6d65d25928c21b64c36b373402924336533'})


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
    return model.encoded(left)==model.encoded(right)


def bounded(stream):
    chunks=[];size=0
    try:
        while True:
            chunk=stream.read(min(64*1024,MAX+1-size))
            if not isinstance(chunk,bytes):raise ValueError('Complete artifact byte stream required')
            if not chunk:break  # Reach EOF so the SDK validates ContentLength.
            size+=len(chunk)
            if size>MAX:raise ValueError('Complete artifact exceeds bound')
            chunks.append(chunk)
        return b''.join(chunks)
    finally:stream.close()



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
    key = model.PREFIX+category+'/'+sha(raw)+'.json'
    immutable(client, bucket, key, raw)
    return {'key': key, 'sha256': sha(raw), 'bytes': len(raw)}


def checked(ref, category, read):
    if not isinstance(ref, dict) or not re.fullmatch('[a-f0-9]{64}', ref.get('sha256', '')): raise ValueError('Exact artifact reference required')
    if type(ref.get('bytes')) is not int or not 0 < ref['bytes'] <= MAX: raise ValueError('Exact artifact byte count required')
    if ref.get('key') != model.PREFIX+category+'/'+ref['sha256']+'.json': raise ValueError('Artifact namespace differs')
    raw = read(ref['key'])
    if sha(raw) != ref['sha256'] or len(raw) != ref.get('bytes'): raise ValueError('Artifact content differs')
    return strict(raw)


def compile_output(inputs, read):
    if inputs.get('contract') != 'liquidity-flow-inputs.v1': raise ValueError('Liquidity inputs contract required')
    macro = checked(inputs['macro'], 'snapshots', read)
    originals = canonical.restore(macro, tuple(arithmetic.SPECS), read)
    settlement = checked(inputs['settlement'], 'snapshots', read) if inputs['settlement'] else None
    output = model.build(macro, originals, settlement, inputs['generated_at'], inputs['legacy_context'], inputs['settlement'])
    output['pd_settlement_fails']['source_read_status'] = inputs.get('settlement_read_status', {'status': 'retained' if inputs['settlement'] else 'unavailable'})
    return output


def settlement_input(client, bucket, read):
    try: raw = read(SETTLEMENT)
    except Exception:
        # Optional FR2004 context cannot invalidate independently verified FRED
        # roots. Do not copy exception text, URLs or transport diagnostics.
        return None, {'status': 'unavailable', 'reason': 'SOURCE_READ_FAILED'}
    try: strict(raw)
    except (ValueError, UnicodeDecodeError):
        key = PRIVATE+sha(raw)+'.bin'
        immutable(client, bucket, key, raw, True)
        return None, {'status': 'unavailable', 'reason': 'SOURCE_INVALID_JSON',
                      'whole_original': {'key': key, 'sha256': sha(raw), 'bytes': len(raw)}}
    return retain_bytes(client, bucket, raw, 'snapshots'), {'status': 'retained'}


def replay(ref, read):
    key = ref.get('manifest_key', '')
    if not re.fullmatch(re.escape(model.PREFIX)+r'runs/[a-f0-9]{64}\.json', key): raise ValueError('Liquidity run identity required')
    raw = read(key); manifest = strict(raw)
    if (key != model.PREFIX+'runs/'+sha(raw)+'.json' or manifest.get('contract') != 'liquidity-flow-replay.v1'
        or manifest.get('output_sha256') != ref.get('output_sha256')): raise ValueError('Liquidity run binding differs')
    if set(manifest['compilers']) != {module.__name__ for module in COMPILERS}: raise ValueError('Compiler set differs')
    for module in COMPILERS:
        body = Path(module.__file__).read_bytes(); expected = {'key': model.PREFIX+'compilers/'+sha(body)+'.py', 'sha256': sha(body)}
        ref = manifest['compilers'][module.__name__]
        if module is sys.modules[__name__] and ref.get('sha256') in REVIEWED_STORAGE_REVISIONS:
            digest = ref['sha256']; expected = {'key': model.PREFIX+'compilers/'+digest+'.py', 'sha256': digest}
            if ref != expected or sha(read(expected['key'])) != digest: raise ValueError('Reviewed predecessor storage bytes differ')
        elif ref != expected or read(expected['key']) != body: raise ValueError('Matching reviewed compiler required')
    inputs = checked(manifest['input'], 'inputs', read)
    output = compile_output(inputs, read)
    if (not same_json(output, checked(manifest['output'], 'outputs', read)) or model.digest(output) != manifest['output_sha256']
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
    if not same_json(replay(ref, reader(client, bucket)), output): raise ValueError('Retained replay differs')
    return ref


def previous_context(client, bucket):
    try: raw = bounded(client.get_object(Bucket=bucket, Key=model.CURRENT)['Body'])
    except Exception as exc:
        if missing(exc): return {}
        raise
    try: previous = strict(raw)
    except (ValueError, UnicodeDecodeError): previous = None
    immutable(client, bucket, PRIVATE+sha(raw)+'.bin', raw, True)
    if isinstance(previous, dict) and previous.get('contract') == model.CONTRACT:
        binding(previous, reader(client, bucket))
        return previous.get('legacy_context') or {}
    return {'whole_predecessor': {'key': PRIVATE+sha(raw)+'.bin', 'sha256': sha(raw), 'bytes': len(raw), 'status': 'UNQUALIFIED_LEGACY'}}


def binding(packet, read):
    ref = packet.get('replay') or {}; key = ref.get('manifest_key', '')
    if not re.fullmatch(re.escape(model.PREFIX)+r'runs/[a-f0-9]{64}\.json', key): raise ValueError('Native immutable run required')
    raw = read(key); manifest = strict(raw)
    output = {k:v for k,v in packet.items() if k != 'replay'}
    if (key != model.PREFIX+'runs/'+sha(raw)+'.json' or manifest.get('contract') != 'liquidity-flow-replay.v1'
        or manifest.get('output_sha256') != ref.get('output_sha256') or model.digest(output) != ref.get('output_sha256')
        or manifest.get('generated_at') != packet.get('generated_at')
        or not same_json(checked(manifest['output'], 'outputs', read), output)):
        raise ValueError('Publication differs from retained original-source output')
    return manifest


def publish(client, bucket, packet):
    if (packet.get('contract') != model.CONTRACT or any(packet.get(k) is not False for k in ('calls_eligible','sizing_eligible','execution_eligible'))
        or packet.get('call') is not None or packet.get('decision') != {'verb':'WAIT','meaning':'abstain'}
        or packet.get('portfolio_consequences') != {'status':'UNAVAILABLE','target_weights':None,'forced_liquidation':False}
        or set(packet.get('series',{})) != set(arithmetic.SPECS)):
        raise ValueError('Complete research-only publication required')
    binding(packet, reader(client, bucket))
    stamp = model.clock(packet['generated_at'])
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
            live = strict(reader(client, bucket)(model.CURRENT))
            if not same_json(live, packet) and model.clock(live['generated_at']) <= stamp: raise ValueError('Publication readback differs')
            return True
        except Exception as exc:
            if not conflict(exc): raise
    raise RuntimeError('Publication conflict limit; immutable run retained')


def run(client, bucket):
    read = reader(client, bucket)
    raw = read(SOURCE); macro = strict(raw)
    # Authenticate originals before any publication artifact is written.
    canonical.restore(macro, tuple(arithmetic.SPECS), read)
    macro_ref = retain_bytes(client, bucket, raw, 'snapshots')
    settlement_ref, settlement_status = settlement_input(client, bucket, read)
    inputs = {'contract': 'liquidity-flow-inputs.v1', 'generated_at': datetime.now(timezone.utc).isoformat(),
        'macro': macro_ref, 'settlement': settlement_ref, 'settlement_read_status': settlement_status,
        'legacy_context': previous_context(client, bucket)}
    output = compile_output(inputs, read)
    ref = retain(client, bucket, inputs, output)
    published = publish(client, bucket, {**output, 'replay': ref})
    return {'published': published, 'generated_at': output['generated_at'], 'quality': output['quality'],
        'replay': ref, 'provider_requests': 0, 'paid_ai_calls': 0, 'notifications_sent': 0,
        'private_account_reads': 0, 'portfolio_writes': 0}
