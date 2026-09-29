"""Calls CISS original-source qualification.

Whole native replay and a separate rational checker precede a small, strictly
typed ancestry view. No current/private reads, publication or native invocation.
"""
from concurrent.futures import Future
from datetime import date, timedelta
import hashlib
import json
from pathlib import Path
import re
import sys
from threading import Lock

import ciss_original_replay as replay
import verify_ciss_arithmetic as arithmetic

CONTRACT = 'calls-ciss-originals.v1'
IMMUTABLE = (r'data/ciss-research/runs/[a-f0-9]{64}\.json',
             r'data/ciss-research/compilers/[a-f0-9]{64}\.py',
             r'data/evidence/ecb/[a-f0-9]{64}/[a-f0-9]{64}\.bin\.gz')
MAX_ARTIFACTS = 256
MAX_TOTAL = 256*1024*1024
MAX_OBJECT = 64*1024*1024
encoded = replay.model.encoded
digest = replay.model.digest


def strict_json(raw):
    def pairs(items):
        out = {}
        for key, value in items:
            if key in out: raise ValueError('Duplicate JSON key')
            out[key] = value
        return out
    def invalid(value): raise ValueError('Nonfinite JSON value')
    return json.loads(raw, object_pairs_hook=pairs, parse_constant=invalid)


class ImmutableReader:
    """Thread-safe whole-body cache; reject mutable paths before transport."""
    def __init__(self, read):
        self.read = read; self.cache = {}; self.pending = {}; self.bytes = 0; self.lock = Lock()

    def __call__(self, key):
        if not isinstance(key, str) or not any(re.fullmatch(pattern, key) for pattern in IMMUTABLE):
            raise ValueError('Only reviewed immutable CISS public artifacts are readable')
        with self.lock:
            if key in self.cache: return self.cache[key]
            owner = key not in self.pending
            if owner:
                if len(self.cache)+len(self.pending) >= MAX_ARTIFACTS: raise ValueError('Complete archive count exceeds bound')
                self.pending[key] = Future()
            future = self.pending[key]
        if not owner: return future.result()
        try:
            raw = self.read(key)
            if not isinstance(raw, bytes) or not 0 < len(raw) <= MAX_OBJECT: raise ValueError('Complete bounded artifact required')
            sha = re.search(r'/([a-f0-9]{64})\.(?:json|py|bin\.gz)$', key)[1]
            if hashlib.sha256(raw).hexdigest() != sha: raise ValueError('Immutable content hash differs')
            with self.lock:
                if self.bytes+len(raw) > MAX_TOTAL: raise ValueError('Complete archive bytes exceed bound')
                self.cache[key] = raw; self.bytes += len(raw)
            future.set_result(raw)
            return raw
        except BaseException as exc:
            future.set_exception(exc)
            raise
        finally:
            with self.lock: del self.pending[key]


def inspect(raw_packet, read, as_of):
    if not isinstance(raw_packet, bytes) or not 0 < len(raw_packet) <= MAX_OBJECT:
        raise ValueError('Complete bounded public packet required')
    packet = strict_json(raw_packet); now = arithmetic.clock(as_of)
    if not isinstance(packet, dict) or packet.get('contract') != replay.model.CONTRACT:
        raise ValueError('Reviewed CISS packet required')
    ref = packet.get('replay')
    if (not isinstance(ref, dict) or set(ref) != {'manifest_key', 'output_sha256', 'compiler'} or
        not isinstance(ref['manifest_key'], str) or not re.fullmatch(IMMUTABLE[0], ref['manifest_key']) or
        not isinstance(ref['output_sha256'], str) or not re.fullmatch('[a-f0-9]{64}', ref['output_sha256'])):
        raise ValueError('Exact immutable CISS run identity required')
    generated = arithmetic.clock(packet['generated_at'])
    if generated > now: raise ValueError('Future source packet')
    retained = ImmutableReader(read); manifest = strict_json(retained(ref['manifest_key']))
    if manifest.get('compiler') != ref['compiler'] or manifest.get('output_sha256') != ref['output_sha256']:
        raise ValueError('Referenced compiler or output differs')
    output = replay.replay(manifest, retained)
    if encoded(output) != encoded({k: v for k, v in packet.items() if k != 'replay'}):
        raise ValueError('Whole supplied packet differs from retained replay')
    histories = {key: retained(item['evidence']['key']) for key, item in manifest['histories'].items()}
    discoveries = {key: retained(item['evidence']['key']) for key, item in manifest['discoveries'].items()}
    proof = arithmetic.verify(output, histories, discoveries)
    series = {row['key']: row for row in output['series']}; observations = {}; views = {}; sources = {}
    for key, verified in proof['selected_series'].items():
        row = series[key]; item = manifest['histories'][key]; evidence = item['evidence']
        # Native replay authenticates every descriptor. Only these fields may
        # leave this candidate; upstream titles/comments/arbitrary metadata do not.
        sources[key] = {'key': evidence['key'], 'sha256': evidence['sha256'], 'bytes': evidence['bytes'],
                        'provider': 'ecb', 'acquired_at': item['acquired_at'], 'first_received_at': evidence['first_received_at']}
        parsed, _ = arithmetic.parse(histories[key], key)
        by_row = {point['source_row']: point for point in parsed[key]['rows']}
        coordinates = {verified['original_row']} | {v['baseline_source_row'] for v in verified['comparisons'].values() if v['baseline_source_row'] is not None}
        identities = {}
        for index in sorted(coordinates):
            point = by_row[index]
            period = {'provider': 'ecb', 'series_id': key, 'observation_period': point['period'],
                      'period_end': point['end'].isoformat(), 'native_unit': 'PURE_NUMB', 'unit_scale': 0}
            period_id = 'ecb-period-'+digest(period)
            occurrence = {**period, 'period_id': period_id, 'response_sha256': evidence['sha256'],
                          'original_row': index, 'reported_decimal': point['text'], 'observation_status': point['status']}
            identity = 'ecb-occurrence-'+digest(occurrence)
            observations[identity] = occurrence; identities[index] = identity
        issues = []
        if now-generated > timedelta(hours=72): issues.append('native_packet:publication_age')
        if now-arithmetic.clock(item['acquired_at']) > timedelta(hours=72): issues.append('original:acquisition_age')
        if (now.date()-date.fromisoformat(row['observation_period_end'])).days > 14: issues.append('original:observation_age')
        if row['quality']['status'] != 'fresh': issues.append('native_series:not_fresh')
        if proof['latest_reconciliation'] != 'matched': issues.append('headline:same_date_reconciliation_unavailable')
        views[key] = {'reported': {'value': row['latest'], 'decimal': row['latest_decimal'], 'period': row['latest_date'], 'unit': row['unit']},
                      'latest_occurrence': identities[verified['original_row']],
                      'comparisons': {name: {**entry, 'baseline_occurrence': identities.get(entry['baseline_source_row'])}
                                      for name, entry in verified['comparisons'].items()},
                      'current_use': {'eligible': not issues, 'issues': issues},
                      'current_research': row['latest_decimal'] if not issues else None}
    # A component's own clock cannot revive an expired/missing headline leg.
    joint = len(views) == 7 and all(view['current_use']['eligible'] for view in views.values())
    for view in views.values():
        if not joint and view['current_use']['eligible']:
            view['current_use'] = {'eligible': False, 'issues': ['headline:another_required_leg_unavailable']}
            view['current_research'] = None
    return {'contract': CONTRACT, 'candidate_only': False, 'as_of': now.isoformat(),
            'packet_sha256': hashlib.sha256(raw_packet).hexdigest(), 'source_generated_at': output['generated_at'],
            'source_replay': ref, 'original_sources': sources, 'observations': observations, 'series': views,
            'current_headline_research_eligible': joint, 'independent_arithmetic': proof,
            'coverage': {'original_series': len(histories), 'original_rows': proof['original_history_rows_checked'],
                         'retained_artifacts': len(retained.cache), 'retained_uncompressed_bytes': retained.bytes,
                         'selected_original_occurrences': len(observations)},
            'retained_artifact_inventory': [{'key': key, 'sha256': hashlib.sha256(raw).hexdigest(), 'bytes': len(raw)} for key, raw in sorted(retained.cache.items())],
            'calls_eligible': False, 'sizing_eligible': False, 'execution_eligible': False, 'publication_eligible': False,
            'limitations': ['All retrieved current-vintage histories are replayed; historical publication availability is not established.',
                           'The seven headline/component series share one ECB system. Seven series are not seven independent votes.',
                           'Independent checks cover rows, calendars, chart coordinates and reported contribution sums, not distribution statistics or forecasts.',
                           'Current-use ceilings are 72 hours acquisition/publication and 14 days daily observation age; exact release calendars remain unverified.',
                           'Every original history is retained whole. This compact view references current and comparison rows and does not replace the full histories.',
                           'Original-source lineage is descriptive; native runtime capacity and new scheduled publication require separate acceptance.']}
