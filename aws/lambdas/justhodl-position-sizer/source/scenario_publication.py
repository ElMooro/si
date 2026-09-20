"""Publish the reviewed scenario model; never read accounts or infer weights."""
from datetime import datetime, timezone
from pathlib import Path
import hashlib
import json
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


def read(client, bucket, key):
    obj = client.get_object(Bucket=bucket, Key=key)
    try: raw = obj['Body'].read(LIMIT+1)
    finally: obj['Body'].close()
    if len(raw) > LIMIT: raise ValueError('Complete artifact exceeds bound')
    return raw, obj.get('ETag')


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


def verified(ref, reader, folder, suffix='.json'):
    if not isinstance(ref, dict) or set(ref) != {'key','sha256','bytes'}: raise ValueError('Artifact reference differs')
    if not re.fullmatch(re.escape(PREFIX+folder+'/')+r'[a-f0-9]{64}'+re.escape(suffix), ref['key']): raise ValueError('Artifact path differs')
    raw = reader(ref['key'])
    if reference(ref['key'],raw) != ref: raise ValueError('Artifact bytes differ')
    return json.loads(raw)


def replay(manifest, reader):
    refs, artifacts = sources()
    if manifest.get('contract') != 'portfolio-scenario-publication-replay.v1' or manifest.get('compilers') != refs:
        raise ValueError('Reviewed model or compiler differs')
    for key, raw in artifacts.items():
        if reader(key) != raw: raise ValueError('Retained model source differs')
    output = build(manifest['generated_at'], refs, manifest['whole_preceding_product'])
    if verified(manifest['output'],reader,'outputs') != output: raise ValueError('Complete publication replay differs')
    return output


def run(client, bucket, at=None):
    at = at or datetime.now(timezone.utc).isoformat()
    before, etag = read(client,bucket,CURRENT); old = json.loads(before)
    if not etag: raise ValueError('Conditional publication requires preceding ETag')
    refs, artifacts = sources()
    reader = lambda key: read(client,bucket,key)[0]
    if old.get('contract') == CONTRACT:
        previous = verified(old['replay'],reader,'runs')
        if old.get('compilers') == refs:
            if replay(previous,reader) != {k:v for k,v in old.items() if k != 'replay'}: raise ValueError('Current packet differs from retained output')
            return {'published': False, 'reason': 'reviewed_model_unchanged', 'replay': old['replay']}
        preceding = old['whole_preceding_product']
    else:
        if old.get('engine') != 'position-sizer' or old.get('version') != '1.1': raise ValueError('Review unrecognized predecessor before cutover')
        preceding = {'sha256': digest(before), 'bytes': len(before), 'generated_at': old.get('generated_at'),
                     'qualification': 'unqualified_legacy_allocation', 'retention': 'private_audit_copy'}
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
