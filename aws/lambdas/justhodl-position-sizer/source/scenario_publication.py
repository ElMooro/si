"""Publish the reviewed scenario model; never read accounts or infer weights."""
from datetime import datetime, timezone
from pathlib import Path
import hashlib
import json
import math
import re

CONTRACT = 'portfolio-scenario-availability.v1'
CURRENT = 'data/position-sizing.json'
PREFIX = 'data/scenario-model/'
PRIVATE = 'audit-private/20260909-originals/legacy-public-sizer/'
FILES = ('scenario_model.js', 'scenario_publication.py', 'lambda_function.py')
LIMIT = 4 * 1024 * 1024


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def reference(key, raw):
    return {'key': key, 'sha256': digest(raw), 'bytes': len(raw)}


def decoded(raw):
    if not isinstance(raw, bytes) or len(raw) > LIMIT: raise ValueError('Complete JSON bytes required')
    def pairs(rows):
        result = {}
        for key, value in rows:
            if key in result: raise ValueError('Duplicate JSON key')
            result[key] = value
        return result
    def number(text):
        value = float(text)
        if not math.isfinite(value): raise ValueError('Nonfinite JSON number')
        return value
    def constant(text): raise ValueError('Nonfinite JSON constant')
    def unicode(value, depth=0):
        if depth > 128: raise ValueError('JSON nesting exceeds bound')
        if isinstance(value, str): value.encode('utf-8')
        elif isinstance(value, dict):
            for key, child in value.items(): unicode(key, depth+1); unicode(child, depth+1)
        elif isinstance(value, list):
            for child in value: unicode(child, depth+1)
    try:
        result = json.loads(raw.decode('utf-8'), object_pairs_hook=pairs, parse_float=number, parse_constant=constant)
        unicode(result)
        return result
    except (UnicodeError, RecursionError) as exc: raise ValueError('Invalid JSON encoding or nesting') from exc


def clock(value):
    if not isinstance(value, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})', value):
        raise ValueError('Exact timezone-aware publication clock required')
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if not value.endswith('Z') and (int(value[-5:-3]) > 23 or int(value[-2:]) > 59):
        raise ValueError('Invalid publication timezone offset')
    if parsed.utcoffset() is None: raise ValueError('Publication timezone required')
    return parsed.astimezone(timezone.utc)


def read(client, bucket, key):
    obj = client.get_object(Bucket=bucket, Key=key); body = obj['Body']
    try:
        length = obj.get('ContentLength')
        if length is not None and (type(length) is not int or length < 0 or length > LIMIT):
            raise ValueError('Invalid or oversized declared artifact length')
        chunks, total = [], 0
        while True:
            chunk = body.read(min(65536, LIMIT+1-total))
            if not isinstance(chunk, bytes): raise ValueError('Artifact stream did not return bytes')
            if not chunk: break
            total += len(chunk)
            if total > LIMIT: raise ValueError('Complete artifact exceeds bound')
            chunks.append(chunk)
        if length is not None and total != length: raise ValueError('Incomplete declared artifact transfer')
        return b''.join(chunks), obj.get('ETag')
    finally: body.close()


def immutable(client, bucket, key, raw):
    if not key.startswith((PREFIX, PRIVATE)): raise ValueError('Retention path differs')
    try:
        client.put_object(Bucket=bucket, Key=key, Body=raw, IfNoneMatch='*',
            ContentType='application/javascript' if key.endswith('.js') else 'application/octet-stream',
            CacheControl='public, max-age=31536000, immutable' if key.startswith(PREFIX) else 'no-store')
    except Exception as exc:
        if getattr(exc, 'response', {}).get('Error', {}).get('Code') not in ('PreconditionFailed', 'ConditionalRequestConflict'): raise
    if read(client, bucket, key)[0] != raw: raise ValueError('Immutable artifact readback differs')


def sources():
    refs, artifacts = {}, {}
    for name in FILES:
        raw = (Path(__file__).parent/name).read_bytes()
        key = PREFIX+'models/'+digest(raw)+Path(name).suffix
        refs[name] = reference(key, raw); artifacts[key] = raw
    return refs, artifacts


def build(at, refs, preceding):
    return {'engine': 'position-sizer', 'version': '2.0', 'contract': CONTRACT,
        'generated_at': at, 'timestamp_meaning': 'Model publication time; not a market observation or portfolio valuation.',
        'status': 'SCENARIO_MODEL_AVAILABLE', 'call': 'WAIT',
        'quality': {'status': 'model_only', 'market_data_freshness': 'not_applicable'},
        'permissions': {'calls_eligible': False, 'sizing_eligible': False, 'execution_eligible': False, 'may_recommend_trades': False},
        'scenario_model': refs['scenario_model.js'], 'compilers': refs,
        'scenario_page': '/position-sizer.html', 'model_contract': 'linear-portfolio-scenario.v1',
        'input_contract': 'linear-portfolio-scenario-input.v1',
        'sized_positions': [], 'suggested_gross_top15_pct': None, 'risk_posture': None, 'posture_mult': None,
        'regime': {'bond_vol': None, 'plumbing': None, 'gamma_regime': None, 'vol_surface_regime': None,
                   'term_inverted': None, 'gamma_vol_mult': None, 'combined_mult': None},
        'note': 'Calculate consequences of explicit hypothetical weights and shocks. Research scores do not determine allocations.',
        'caveat': 'No expected returns, Kelly fraction, recommended weights, trade authority, market prices or account balances are inferred.',
        'required_inputs': ['signed weights', 'local price shocks', 'FX shocks', 'horizon', 'cash or financing rate for that horizon',
                            'aggregate costs as percent of initial NAV', 'explicit NAV or unavailable'],
        'whole_preceding_product': preceding,
        'private_account_reads': 0, 'paid_ai_calls': 0, 'notifications_sent': 0, 'portfolio_writes': 0}


# Exact inert predecessor compiler identities; replay never executes stored code.
APPROVED_PREDECESSOR = {
    "lambda_function.py": {
        "bytes": 639,
        "key": "data/scenario-model/models/5ffa4e991572f0eb49cd326312b7dd17ca6b2bcebe75dab50dee369c669f67f9.py",
        "sha256": "5ffa4e991572f0eb49cd326312b7dd17ca6b2bcebe75dab50dee369c669f67f9"
    },
    "scenario_model.js": {
        "bytes": 7387,
        "key": "data/scenario-model/models/b57f2ae734e5361da388e5821e0ebe0a6e4dc03fad0ed3e23db94c651d879512.js",
        "sha256": "b57f2ae734e5361da388e5821e0ebe0a6e4dc03fad0ed3e23db94c651d879512"
    },
    "scenario_publication.py": {
        "bytes": 7171,
        "key": "data/scenario-model/models/09221024588b75b8c48bc4d5774b817600171803aceb49aea38d6f6420a8b324.py",
        "sha256": "09221024588b75b8c48bc4d5774b817600171803aceb49aea38d6f6420a8b324"
    }
}


def checked_reference(ref, reader, folder, suffix='.json'):
    if not isinstance(ref, dict) or set(ref) != {'key', 'sha256', 'bytes'}: raise ValueError('Artifact reference differs')
    if type(ref['bytes']) is not int or not 0 < ref['bytes'] <= LIMIT: raise ValueError('Exact artifact byte count required')
    if not isinstance(ref['sha256'], str) or not re.fullmatch(r'[a-f0-9]{64}', ref['sha256']): raise ValueError('Artifact hash differs')
    if ref['key'] != PREFIX+folder+'/'+ref['sha256']+suffix: raise ValueError('Artifact path differs')
    raw = reader(ref['key'])
    if not isinstance(raw, bytes) or len(raw) != ref['bytes'] or digest(raw) != ref['sha256']: raise ValueError('Artifact bytes differ')
    return raw


def verified(ref, reader, folder, suffix='.json'):
    return decoded(checked_reference(ref, reader, folder, suffix))


def preceding_identity(value):
    if not isinstance(value, dict) or set(value) != {'sha256','bytes','generated_at','qualification','retention'}:
        raise ValueError('Whole predecessor identity differs')
    if type(value['bytes']) is not int or not 0 < value['bytes'] <= LIMIT or not isinstance(value['sha256'], str) or not re.fullmatch(r'[a-f0-9]{64}',value['sha256']):
        raise ValueError('Whole predecessor byte identity differs')
    if value['qualification'] != 'unqualified_legacy_allocation' or value['retention'] != 'private_audit_copy':
        raise ValueError('Unqualified predecessor scope differs')
    if value['generated_at'] is not None: clock(value['generated_at'])


def replay(manifest, reader):
    if not isinstance(manifest, dict) or set(manifest) != {'contract','generated_at','compilers','whole_preceding_product','output'} or manifest['contract'] != 'portfolio-scenario-publication-replay.v1':
        raise ValueError('Publication replay contract differs')
    clock(manifest['generated_at']); preceding_identity(manifest['whole_preceding_product'])
    current, artifacts = sources(); refs = manifest['compilers']
    # Current reviewed Python rebuilds both reviewed source generations. The
    # arithmetic and output contract are unchanged; stored Python is only bytes.
    if refs not in (current, APPROVED_PREDECESSOR): raise ValueError('Reviewed model or compiler differs')
    for name in FILES:
        raw = checked_reference(refs[name], reader, 'models', Path(name).suffix)
        if refs == current and artifacts[refs[name]['key']] != raw: raise ValueError('Retained model source differs')
    output = build(manifest['generated_at'], refs, manifest['whole_preceding_product'])
    stored = verified(manifest['output'], reader, 'outputs')
    if encoded(stored) != encoded(output): raise ValueError('Complete publication replay differs')
    return output


def run(client, bucket, at=None):
    at = datetime.now(timezone.utc).isoformat() if at is None else at
    candidate_clock = clock(at)
    before, etag = read(client,bucket,CURRENT); old = decoded(before)
    if not isinstance(old, dict): raise ValueError('Current publication must be an object')
    if not isinstance(etag, str) or not etag: raise ValueError('Conditional publication requires preceding ETag')
    refs, artifacts = sources()
    reader = lambda key: read(client,bucket,key)[0]
    if old.get('contract') == CONTRACT:
        previous = verified(old['replay'],reader,'runs')
        if encoded(replay(previous,reader)) != encoded({k:v for k,v in old.items() if k != 'replay'}): raise ValueError('Current packet differs from retained output')
        if old.get('compilers') == refs:
            return {'published': False, 'reason': 'reviewed_model_unchanged', 'replay': old['replay']}
        if candidate_clock < clock(old['generated_at']): raise ValueError('Cannot replace a newer publication with an earlier clock')
        preceding = old['whole_preceding_product']
    else:
        if old.get('engine') != 'position-sizer' or old.get('version') != '1.1': raise ValueError('Review unrecognized predecessor before cutover')
        preceding = {'sha256': digest(before), 'bytes': len(before), 'generated_at': old.get('generated_at'),
                     'qualification': 'unqualified_legacy_allocation', 'retention': 'private_audit_copy'}
    preceding_identity(preceding)
    # Preserve the complete preceding product before replacing the mutable pointer.
    immutable(client,bucket,PRIVATE+digest(before)+'.bin',before)
    output = build(at,refs,preceding); output_raw = encoded(output)
    output_key = PREFIX+'outputs/'+digest(output_raw)+'.json'; artifacts[output_key] = output_raw
    manifest = {'contract': 'portfolio-scenario-publication-replay.v1', 'generated_at': at,
                'compilers': refs, 'whole_preceding_product': preceding, 'output': reference(output_key,output_raw)}
    for key, raw in artifacts.items(): immutable(client,bucket,key,raw)
    replay(manifest,reader)
    manifest_raw = encoded(manifest); key = PREFIX+'runs/'+digest(manifest_raw)+'.json'
    immutable(client,bucket,key,manifest_raw)
    body = encoded({**output, 'replay': reference(key,manifest_raw)})
    client.put_object(Bucket=bucket,Key=CURRENT,Body=body,ContentType='application/json',CacheControl='no-store',IfMatch=etag)
    if reader(CURRENT) != body: raise ValueError('Current publication readback differs')
    return {'published': True, 'generated_at': at, 'replay': reference(key,manifest_raw),
            'private_account_reads': 0, 'paid_ai_calls': 0, 'notifications_sent': 0, 'portfolio_writes': 0}
