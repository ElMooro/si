"""Complete derived-source inventory; no score normalization or investment vote.

The retained sidecars are evidence of what an engine reported. They are not
original-provider records. Replaying this inventory cannot qualify a forecast.
"""
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import re

CONTRACT = 'signal-board-research.v1'
PRIVATE = frozenset(('data/pm-decision.json', 'data/sizing.json'))
PERMISSIONS = ('calls_eligible', 'sizing_eligible', 'execution_eligible', 'forecast_qualified')
encoded = lambda value: json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()
sha = lambda raw: hashlib.sha256(raw).hexdigest()


def stamp(value):
    if not isinstance(value, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})', value):
        return None
    try:
        result = datetime.fromisoformat(value.replace('Z', '+00:00'))
        return result.astimezone(timezone.utc)
    except ValueError:
        return None


def strict_json(raw):
    def pairs(items):
        out = {}
        for key, value in items:
            if key in out:
                raise ValueError('Duplicate JSON member')
            out[key] = value
        return out
    def invalid(_):
        raise ValueError('Non-finite JSON number')
    return json.loads(raw, object_pairs_hook=pairs, parse_float=Decimal, parse_constant=invalid)


def registry(rows):
    if not isinstance(rows, list) or len(rows) != 99:
        raise ValueError('Every reviewed registry row is required')
    names = set()
    for row in rows:
        if (not isinstance(row, dict) or set(row) != {'engine', 'category', 'source_key', 'normalizer', 'capture_scope'} or
                not all(isinstance(row[k], str) and row[k] for k in row) or row['engine'] in names or
                not re.fullmatch(r'(?:data|screener)/[A-Za-z0-9_-]+\.json', row['source_key'])):
            raise ValueError('Unambiguous reviewed feed identity required')
        expected = 'excluded_private_account_input' if row['source_key'] in PRIVATE else 'anonymous_public_sidecar_only'
        if row['capture_scope'] != expected:
            raise ValueError('Capture scope changed')
        names.add(row['engine'])
    if len({r['source_key'] for r in rows}) != 98 or {r['source_key'] for r in rows} & PRIVATE != PRIVATE:
        raise ValueError('Complete source population required')
    return rows


def reference(ref):
    if (not isinstance(ref, dict) or set(ref) != {'key', 'sha256', 'bytes'} or
            not isinstance(ref['key'], str) or not re.fullmatch(r'[A-Za-z0-9_./-]+', ref['key']) or
            any(p in ('', '.', '..') for p in ref['key'].split('/')) or
            not isinstance(ref['sha256'], str) or not re.fullmatch(r'[0-9a-f]{64}', ref['sha256']) or
            type(ref['bytes']) is not int or not 0 <= ref['bytes'] <= 64 * 1024 * 1024):
        raise ValueError('Whole bounded artifact reference required')
    return ref


def source(key, entry, read, generated_at):
    if not isinstance(entry, dict) or entry.get('source_key') != key:
        raise ValueError('Complete matching capture receipt required')
    base = {'source_key': key, 'capture_receipt_sha256': sha(encoded(entry)), 'original': None,
            'original_provider_verified': False, 'observation_freshness': 'unverified',
            'publication': {'reported_at': None, 'parsed_at': None, 'status': 'not_reported'},
            'contract_declared': None, 'replay_declared': False,
            'permission_declarations': {k: 'not_reported' for k in PERMISSIONS},
            **{k: False for k in PERMISSIONS}}
    if key in PRIVATE:
        if entry != {'source_key': key, 'status': 'excluded_private_account_input', 'requested': False}:
            raise ValueError('Private inputs must be excluded before any read')
        return {**base, 'status': 'private_input_excluded', 'http_status': None}
    state = entry.get('status')
    if state == 'transport_or_size_unavailable':
        if 'original' in entry:
            raise ValueError('Unavailable capture cannot claim a whole original')
        return {**base, 'status': 'transport_unavailable', 'http_status': None}
    status = entry.get('http_status')
    if state not in ('public_sidecar_retained', 'whole_http_error_retained') or type(status) is not int or not 100 <= status <= 599:
        raise ValueError('Completed capture status required')
    if (status == 200) != (state == 'public_sidecar_retained'):
        raise ValueError('Capture state contradicts HTTP result')
    ref = reference(entry.get('original'))
    raw = read(ref)
    if not isinstance(raw, bytes) or len(raw) != ref['bytes'] or sha(raw) != ref['sha256']:
        raise ValueError('Whole original bytes differ')
    base.update(original=ref, http_status=status)
    if status != 200:
        return {**base, 'status': 'http_error'}
    try:
        packet = strict_json(raw)
    except (ValueError, UnicodeError, RecursionError):
        return {**base, 'status': 'invalid_json'}
    if not isinstance(packet, dict):
        return {**base, 'status': 'non_object_json'}
    declared = packet.get('generated_at')
    parsed = stamp(declared)
    publication = {'reported_at': declared if isinstance(declared, str) else None,
                   'parsed_at': parsed.isoformat() if parsed else None,
                   'status': 'future' if parsed and parsed > stamp(generated_at) else 'reported_past' if parsed else
                             'not_reported' if 'generated_at' not in packet or declared is None else 'invalid'}
    base.update(status='derived_packet_retained', publication=publication,
                contract_declared=packet.get('contract') if isinstance(packet.get('contract'), str) else None,
                replay_declared=isinstance(packet.get('replay'), dict),
                permission_declarations={k: ('declared_true' if packet[k] is True else 'declared_false' if packet[k] is False else 'invalid')
                                         if k in packet else 'not_reported' for k in PERMISSIONS})
    return base


def build(rows, captures, read, generated_at):
    registry(rows)
    if stamp(generated_at) is None:
        raise ValueError('Explicit timezone-qualified compilation time required')
    keys = {row['source_key'] for row in rows}
    if not isinstance(captures, dict) or set(captures) != keys:
        raise ValueError('Every registered source needs exactly one capture outcome')
    sources = {key: source(key, captures[key], read, generated_at) for key in sorted(keys)}
    groups = {key: [r['engine'] for r in rows if r['source_key'] == key] for key in sorted(keys)}
    engines = []
    for i, row in enumerate(rows):
        item = sources[row['source_key']]
        engines.append({'row_id': i + 1, 'engine': row['engine'], 'category': row['category'], 'source_key': row['source_key'],
                        'source_views': len(groups[row['source_key']]), 'source_status': item['status'],
                        'signal': None, 'signal_label': 'ABSTAIN', 'read': 'Derived-source inventory; no qualified investment vote',
                        'as_of': None, 'stale': None, 'stale_basis': 'observation freshness unverified',
                        'reported_generated_at': item['publication']['reported_at'], **{k: False for k in PERMISSIONS}})
    categories = {cat: {'signal': None, 'n': 0, 'registered_rows': sum(r['category'] == cat for r in rows)}
                  for cat in sorted({r['category'] for r in rows})}
    return {'contract': CONTRACT, 'schema_version': '2.0', 'method': 'complete_derived_source_inventory',
            'generated_at': generated_at, 'registry_sha256': sha(encoded(rows)), 'capture_receipts_sha256': sha(encoded(captures)),
            'composite_signal': None, 'composite_posture': 'WAIT', 'deep_read': None,
            'n_engines': len(engines), 'n_live': 0, 'n_stale': None, 'elapsed_s': None, 'categories': categories, 'engines': engines,
            'sources': sources, 'source_status_counts': dict(sorted(Counter(v['status'] for v in sources.values()).items())),
            'dependency_graph': {'basis': 'shared derived-packet keys only; original-provider ancestry unverified',
                                 'source_groups': groups, 'registered_views': len(rows), 'distinct_derived_sources': len(keys),
                                 'independent_original_roots': None, 'independent_votes_qualified': 0},
            'snapshot_atomic': False, 'original_provider_verified': False, 'point_in_time_qualified': False,
            'decision': {'verb': 'WAIT', 'meaning': 'abstain', 'qualified_votes': 0,
                         'portfolio_consequences': {'status': 'unavailable', 'target_weights': None,
                                                    'reason': 'No qualified forecast, holdings, covariance or cost model bound to this inventory'}},
            'note': 'All registered inputs are conserved as whole derived packets or explicit unavailable/excluded outcomes. No normalization, composite vote, LLM lean or posture event is justified by this inventory.',
            **{k: False for k in PERMISSIONS}}
