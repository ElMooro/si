"""Unpublished Calls ancestry candidate using complete FR2004 originals.

No acquisition, native invocation, current/private reads or publication. This
replays the existing reviewed native compiler and separately checks all integer
sums. Retained Python bytes are compared with the checkout, never executed.
"""
from copy import deepcopy
from pathlib import Path
import hashlib
import re
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT/'aws/lambdas/justhodl-settlement-fails/source'), str(ROOT/'aws/shared'), str(ROOT/'scripts')]
import fails_store as store
import verify_fails_arithmetic as arithmetic

CONTRACT = 'calls-fails-lineage-candidate.v1'
MAX_ARTIFACTS = 128
MAX_TOTAL = 256*1024*1024
encoded = store.native.encoded
digest = store.native.digest
IMMUTABLE = (r'data/fails-research/(?:runs|inputs|outputs)/[a-f0-9]{64}\.json',
             r'data/fails-research/compilers/[a-f0-9]{64}\.py',
             r'data/evidence/fr2004/[a-f0-9]{64}/[a-f0-9]{64}\.bin\.gz')


class ImmutableReader:
    def __init__(self, read): self.read = read; self.cache = {}; self.bytes = 0

    def __call__(self, key):
        if not isinstance(key, str) or not any(re.fullmatch(pattern, key) for pattern in IMMUTABLE):
            raise ValueError('Only reviewed immutable FR2004 public artifacts are readable')
        if key in self.cache: return self.cache[key]
        if len(self.cache) >= MAX_ARTIFACTS: raise ValueError('Complete archive count exceeds bound')
        raw = self.read(key)
        if not isinstance(raw, bytes) or not 0 < len(raw) <= store.MAX_BYTES: raise ValueError('Complete bounded artifact required')
        if len(raw)+self.bytes > MAX_TOTAL: raise ValueError('Complete archive bytes exceed bound')
        sha = re.search(r'/([a-f0-9]{64})\.(?:json|py|bin\.gz)$', key)[1]
        if hashlib.sha256(raw).hexdigest() != sha: raise ValueError('Immutable content hash differs')
        self.cache[key] = raw; self.bytes += len(raw)
        return raw


def inspect(raw_packet, read, as_of):
    if not isinstance(raw_packet, bytes) or not 0 < len(raw_packet) <= store.MAX_BYTES:
        raise ValueError('Complete bounded public packet required')
    now = store.native.clock(as_of)
    packet = store.native.strict_json(raw_packet)
    if not isinstance(packet, dict) or packet.get('contract') != store.model.CONTRACT:
        raise ValueError('Reviewed FR2004 packet required')
    ref = packet.get('replay')
    if (not isinstance(ref, dict) or set(ref) != {'manifest_key', 'output_sha256'} or
        not isinstance(ref['manifest_key'], str) or not re.fullmatch(IMMUTABLE[0].replace('(?:runs|inputs|outputs)', 'runs'), ref['manifest_key']) or
        not isinstance(ref['output_sha256'], str) or not re.fullmatch('[a-f0-9]{64}', ref['output_sha256'])):
        raise ValueError('Exact immutable fails run identity required')
    retained = ImmutableReader(read)
    manifest = store.native.strict_json(retained(ref['manifest_key']))
    if manifest.get('output_sha256') != ref['output_sha256']: raise ValueError('Referenced output differs')
    output = store.replay(manifest, retained)
    if encoded(output) != encoded({k: v for k, v in packet.items() if k != 'replay'}):
        raise ValueError('Whole supplied packet differs from retained replay')
    generated = store.native.clock(output['generated_at'])
    if generated > now: raise ValueError('Future source packet')
    inputs = store.native.strict_json(retained(manifest['input']['key']))
    originals = {name: store.native.strict_json(retained(item['evidence']['key']))
                 for name, item in inputs['sources'].items() if name == 'observations'}
    proof = arithmetic.verify(output, originals['observations'])
    # Native replay authenticates every source definition and original response.
    # The full original observation population, including suppressed/missing
    # values, receives both a period identity and an exact response occurrence.
    descriptor = inputs['sources']['observations']; source_ref = descriptor['evidence']
    observations = {}; coordinate = {}
    for index, row in enumerate(originals['observations']['pd']['timeseries']):
        period = {'provider': 'nyfed_fr2004c', 'series_id': row['keyid'], 'observation_date': row['asofdate'],
                  'reporting_definition': store.native.period(row['asofdate']), 'native_unit': 'usd_mn',
                  'period_measure': 'cumulative_reported_fails'}
        period_id = 'fr2004-period-'+digest(period)
        item = {**period, 'period_id': period_id, 'response_sha256': source_ref['sha256'],
                'original_row': index, 'reported_native_value': row.get('value')}
        identity = 'fr2004-occurrence-'+digest(item)
        if identity in observations: raise ValueError('Duplicate original occurrence')
        observations[identity] = item; coordinate[(row['keyid'], row['asofdate'])] = identity

    scopes = {}; publication_issues = []
    if (now-generated).total_seconds() > 36*3600: publication_issues.append('native_packet:publication_age')
    acquired = store.native.clock(descriptor['acquired_at'])
    if acquired > now: raise ValueError('Future original acquisition')
    all_scopes = [(r['scope_id'], r) for r in output['classes']]+[
        ('treasury_incl_tips', output['treasury']), ('all_asset', output['totals'])]
    for identity, scope in all_scopes:
        history = []
        for row in scope['history']:
            legs = {key: coordinate.get((key, row['date'])) for key in scope['source_series']}
            calculation = {'scope_id': identity, 'date': row['date'], 'reporting_definition': row['seriesbreak'],
                'original_observations': legs, 'ftd_usd_mn': row['ftd_usd_mn'], 'ftr_usd_mn': row['ftr_usd_mn'],
                'gross_usd_mn': row['gross_usd_mn'], 'complete': row['complete']}
            history.append({'calculation_id': 'fr2004-calculation-'+digest(calculation), **calculation})
        current_quality = store.model.quality(scope['as_of'], descriptor['acquired_at'], as_of, scope['complete'])
        issues = list(publication_issues)+list(current_quality['missing'])
        if scope['quality']['status'] != 'fresh': issues.append('native_scope:not_fresh')
        usable = not issues and current_quality['status'] == 'fresh'
        scopes[identity] = {'label': scope['label'], 'source_series': list(scope['source_series']),
            'reported': {k: scope[k] for k in ('as_of', 'ftd_bn', 'ftr_bn', 'gross_bn', 'unit', 'native_unit', 'exact_usd_bn')},
            'current_research': deepcopy(scope['exact_usd_bn']) if usable else None,
            'current_use': {'eligible': usable, 'issues': issues, 'quality': current_quality},
            'latest_calculation': history[-1], 'history': history}
    headline = scopes['ust_ex_tips']; treasury = scopes['treasury_incl_tips']
    overlap = sorted(set(headline['source_series']) & set(treasury['source_series']))
    return {'contract': CONTRACT, 'candidate_only': True, 'as_of': now.isoformat(),
        'packet_sha256': hashlib.sha256(raw_packet).hexdigest(), 'source_generated_at': output['generated_at'],
        'source_acquired_at': descriptor['acquired_at'], 'source_replay': deepcopy(ref),
        'original_sources': deepcopy(inputs['sources']), 'observations': observations, 'scopes': scopes,
        'overlap': {'headline_scope': 'ust_ex_tips', 'treasury_scope': 'treasury_incl_tips', 'shared_series': overlap,
            'additional_treasury_series': sorted(set(treasury['source_series'])-set(headline['source_series'])),
            'independent_evidence_count': None,
            'note': 'Headline is contained in Treasury including TIPS. FTD and FTR are two-sided gross; repeated claims may occur on both sides and across reporting days.'},
        'coverage': {'original_series': len(output['series_coverage']), 'original_rows': len(observations),
            'scopes': len(scopes), 'scope_history_rows': sum(len(s['history']) for s in scopes.values()),
            'retained_artifacts': len(retained.cache), 'retained_uncompressed_bytes': retained.bytes},
        'retained_artifact_inventory': [{'key': key, 'sha256': hashlib.sha256(raw).hexdigest(), 'bytes': len(raw)}
            for key, raw in sorted(retained.cache.items())], 'independent_arithmetic': proof,
        'calls_eligible': False, 'sizing_eligible': False, 'execution_eligible': False, 'publication_eligible': False,
        'limitations': ['Complete current-vintage originals do not establish historical information availability or predictive edge.',
            'Stable period identifiers locate reported periods across response vintages; they are not independent economic events.',
            'The separate integer check covers every scope sum and unit conversion; descriptive z statistics are replayed, not independently qualified.',
            'The normal weekly publication rule is a policy ceiling. Actual release times and holiday exceptions remain unverified.']}
