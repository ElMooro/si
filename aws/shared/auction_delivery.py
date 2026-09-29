"""Compact Treasury desk projections with complete content-addressed evidence.

This is delivery integrity for an already-computed packet. It neither certifies
provider originals nor grants historical, forecast, Calls or sizing authority.
"""
from datetime import datetime
from pathlib import Path
import gzip
import hashlib
import json
import re

CONTRACT = 'auction-desk-delivery.v1'
PREFIX = 'data/auction-desk-delivery/'
CURRENT = 'data/auction-desk-view.json'
MAX_PACKET = 128 * 1024 * 1024
MAX_VIEW = 4 * 1024 * 1024
MAX_ROWS = 8 * 1024 * 1024
CHUNK_TARGET = 2 * 1024 * 1024
FLAGS = {key: False for key in ('original_source_verified', 'historical_point_in_time_verified',
                               'forecast_eligible', 'calls_eligible', 'sizing_eligible', 'execution_eligible')}


def encoded(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode('utf-8')


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def clock(value):
    if type(value) is not str or not re.fullmatch(r'\d{4}-\d{2}-\d{2}T.*(?:Z|[+-]\d{2}:\d{2})', value):
        raise ValueError('Dated timezone-aware publication required')
    result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if result.tzinfo is None:
        raise ValueError('Publication timezone required')
    return result


def reference(raw, category, compressed=False):
    if category not in ('snapshots', 'rows', 'sections', 'views', 'manifests') or not 0 < len(raw) <= MAX_PACKET:
        raise ValueError('Complete bounded delivery artifact required')
    sha = digest(raw)
    return {'key': PREFIX+category+'/'+sha+('.json.gz' if compressed else '.json'),
            'sha256': sha, 'bytes': len(raw), 'encoding': 'gzip' if compressed else 'json'}


def validate_reference(ref, category=None):
    if not isinstance(ref, dict) or set(ref) != {'key', 'sha256', 'bytes', 'encoding'}:
        raise ValueError('Exact delivery reference fields required')
    if type(ref['bytes']) is not int or not 0 < ref['bytes'] <= MAX_PACKET or not re.fullmatch('[a-f0-9]{64}', str(ref['sha256'])):
        raise ValueError('Exact delivery size and digest required')
    categories = (category,) if category else ('snapshots', 'rows', 'sections', 'views', 'manifests')
    suffix = '.json.gz' if ref['encoding'] == 'gzip' else '.json' if ref['encoding'] == 'json' else None
    if suffix is None or ref['key'] not in {PREFIX+kind+'/'+ref['sha256']+suffix for kind in categories}:
        raise ValueError('Delivery path is outside the bound artifact namespace')
    if ref['encoding'] == 'gzip' and ref['key'] != PREFIX+'snapshots/'+ref['sha256']+'.json.gz':
        raise ValueError('Only complete snapshots use gzip storage')
    if category in ('views', 'manifests') and ref['bytes'] > MAX_VIEW:
        raise ValueError('Initial delivery artifact exceeds its bound')
    if '/rows/' in ref['key'] and ref['bytes'] > MAX_ROWS:
        raise ValueError('Complete row artifact exceeds its bound; no inputs truncated')
    return ref


def strict(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError('Duplicate delivery JSON field')
            result[key] = value
        return result
    def invalid(value):
        raise ValueError('Nonfinite delivery JSON number')
    result = json.loads(raw, object_pairs_hook=pairs, parse_constant=invalid)
    encoded(result)
    return result


def verified_bytes(ref, read, category=None):
    validate_reference(ref, category)
    raw = read(ref['key'])
    if not isinstance(raw, bytes) or len(raw) > MAX_PACKET:
        raise ValueError('Bounded complete bytes required')
    if ref['encoding'] == 'gzip':
        import io
        with gzip.GzipFile(fileobj=io.BytesIO(raw)) as stream:
            raw = stream.read(ref['bytes']+1)
    if len(raw) != ref['bytes'] or digest(raw) != ref['sha256']:
        raise ValueError('Complete delivery bytes differ from their reference')
    return raw


def checked(ref, read, category=None):
    return strict(verified_bytes(ref, read, category))


def build(packet):
    if not isinstance(packet, dict) or packet.get('engine') != 'justhodl-auction-desk':
        raise ValueError('Complete Treasury desk packet required')
    clock(packet.get('generated_at'))
    for name in ('today', 'buybacks', 'calendar', 'reactions', 'freshness', 'decision'):
        if not isinstance(packet.get(name), dict):
            raise ValueError('Complete desk section required: '+name)
    for value in (packet.get('auctions'), packet.get('recent_days'), packet['today'].get('auctions'),
                  packet['today'].get('buybacks'), packet['buybacks'].get('operations'),
                  packet['calendar'].get('auctions'), packet['calendar'].get('buybacks')):
        if not isinstance(value, list) or any(not isinstance(row, dict) for row in value):
            raise ValueError('Complete desk operation arrays required')
    if packet.get('decision', {}).get('call') is not None or packet.get('decision', {}).get('sizing_eligible') is not False:
        raise ValueError('Delivery may not grant a portfolio action')
    artifacts = {}
    def retain(value, category, compressed=False):
        raw = encoded(value)
        ref = reference(raw, category, compressed)
        validate_reference(ref, category)
        artifacts[ref['key']] = gzip.compress(raw, mtime=0) if compressed else raw
        return ref
    source = retain(packet, 'snapshots', True)
    today = packet.get('today') or {}
    buybacks = packet.get('buybacks') or {}
    auctions = list(packet.get('auctions') or []) + list(today.get('auctions') or [])
    buys = list(buybacks.get('operations') or []) + list(today.get('buybacks') or [])
    row_refs = {}
    chunks = []
    for kind, rows in (('auction', auctions), ('buyback', buys)):
        unique = {}
        for row in rows:
            if not isinstance(row, dict):
                raise ValueError('Every operation must be a complete object')
            unique.setdefault(digest(encoded(row)), row)
        pending, size = [], 0
        groups = []
        for sha, row in unique.items():
            length = len(encoded(row))
            if pending and (len(pending) >= 25 or size+length > CHUNK_TARGET):
                groups.append(pending); pending, size = [], 0
            pending.append((sha, row)); size += length
        if pending:
            groups.append(pending)
        for group in groups:
            ref = retain({'contract': CONTRACT, 'kind': kind, 'rows': [row for _, row in group]}, 'rows')
            chunks.append(ref)
            for index, (sha, _) in enumerate(group):
                row_refs[(kind, sha)] = {'artifact': ref, 'index': index, 'kind': kind}
    def row_view(row, kind):
        excluded = ('grading_inputs', 'bidder_share_inputs') if kind == 'auction' else ('measurement_inputs', 'normalization_inputs')
        result = {key: value for key, value in row.items() if key not in excluded}
        result['delivery_detail'] = row_refs[(kind, digest(encoded(row)))]
        result['delivery_contract'] = CONTRACT
        if kind == 'auction':
            trace = row.get('grading_inputs') or {}
            result.update(grading_contract=trace.get('contract'), grading_status=trace.get('status'), grading_score=trace.get('score'))
        else:
            fields = (row.get('normalization_inputs') or {}).get('fields') or {}
            result['normalized_metric_summary'] = {key: {name: value.get(name) for name in ('status', 'value', 'unit')} for key, value in fields.items()}
            trace = row.get('measurement_inputs') or {}
            result['measurement_summary'] = {key: value for key, value in trace.items() if key not in ('input', 'size_comparison')}
            result['measurement_summary']['size_comparison'] = {key: value for key, value in (trace.get('size_comparison') or {}).items() if key != 'prior'}
        return result
    sections = {}
    for name, value in (('buyback_program', buybacks.get('program') or {}),
                        ('reactions', packet.get('reactions') or {}), ('composite_history', packet.get('composite_history'))):
        sections[name] = retain({'contract': CONTRACT, 'section': name, 'value': value}, 'sections')
    program = {key: value for key, value in (buybacks.get('program') or {}).items() if key != 'aggregate_inputs'}
    aggregate = (buybacks.get('program') or {}).get('aggregate_inputs') or {}
    program['aggregate_summary'] = {key: value for key, value in aggregate.items() if key not in ('windows', 'by_bucket')}
    program['aggregate_summary']['windows'] = {name: {key: value for key, value in row.items() if key != 'inputs'} for name, row in (aggregate.get('windows') or {}).items()}
    program['delivery_detail'] = sections['buyback_program']
    view = {key: value for key, value in packet.items() if key not in ('auctions', 'today', 'buybacks', 'reactions', 'composite_history')}
    view['auctions'] = [row_view(row, 'auction') for row in packet.get('auctions') or []]
    view['today'] = {**today, 'auctions': [row_view(row, 'auction') for row in today.get('auctions') or []],
                     'buybacks': [row_view(row, 'buyback') for row in today.get('buybacks') or []]}
    view['buybacks'] = {**buybacks, 'program': program, 'operations': [row_view(row, 'buyback') for row in buybacks.get('operations') or []]}
    view['reactions'] = {key: value for key, value in (packet.get('reactions') or {}).items() if key not in ('comparison_inputs', 'classification_inputs')}
    view['reactions'].update(delivery_detail=sections['reactions'], supplied_input_replay_available=False,
                             replay_note='Load the complete retained packet for supplied-input replay')
    view['composite_history'] = {'delivery_detail': sections['composite_history'], 'status': 'deferred_complete_evidence'}
    view['delivery'] = {'contract': CONTRACT, 'source_packet': source, 'row_chunks': len(chunks),
                        'complete_input_access': 'Referenced artifacts preserve complete inputs; this initial view is a projection', **FLAGS}
    view_ref = retain(view, 'views')
    manifest = {'contract': CONTRACT, 'generated_at': packet['generated_at'], 'engine_version': packet.get('version'),
                'projection_compiler_sha256': digest(Path(__file__).read_bytes()),
                'source_packet': source, 'view': view_ref, 'row_chunks': chunks, 'sections': sections, **FLAGS}
    manifest_ref = retain(manifest, 'manifests')
    locator = {'contract': CONTRACT, 'generated_at': packet['generated_at'], 'manifest': manifest_ref, 'view': view_ref, **FLAGS}
    return locator, artifacts


def replay(locator, read):
    if locator.get('contract') != CONTRACT or any(locator.get(key) is not False for key in FLAGS):
        raise ValueError('Delivery contract and research-only authority required')
    manifest = checked(locator['manifest'], read, 'manifests')
    source = checked(manifest['source_packet'], read, 'snapshots')
    expected, artifacts = build(source)
    if encoded(expected) != encoded(locator):
        raise ValueError('Delivery projection, compiler, dates or artifact inventory differ')
    for key, value in artifacts.items():
        if key.endswith('.json.gz'):
            # gzip header/deflate variants are storage encoding, not source
            # identity. The retained uncompressed bytes must still be exact.
            verified_bytes(manifest['source_packet'], read, 'snapshots')
        elif read(key) != value:
            raise ValueError('Retained delivery artifact differs: '+key)
    return source, checked(locator['view'], read, 'views')
