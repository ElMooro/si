"""Native Pulse measurements and original-acquisition continuity; no forecast."""
from copy import deepcopy
import liquidity_pulse_arithmetic as arithmetic
from report_observations import encoded, digest
from research_brief_model import clock

CONTRACT = 'liquidity-pulse-research.v1'
PREFIX = 'data/liquidity-pulse-research/'
CURRENT = 'data/liquidity-pulse.json'


def watermarks(packet):
    values = packet.get('source_acquisition_watermarks') or {}
    if set(values) != set(arithmetic.SPECS): raise ValueError('Complete acquisition watermarks required')
    for value in values.values():
        if value is not None: clock(value)
    return deepcopy(values)


def build(source, originals, generated_at, legacy_context, previous_watermarks):
    if set(previous_watermarks) != set(arithmetic.SPECS): raise ValueError('Complete previous watermarks required')
    out = arithmetic.build(source, originals, generated_at)
    out.pop('candidate_only'); out.pop('publication_eligible'); out['contract'] = CONTRACT
    out['source_acquisition_watermarks'] = {}
    for sid, row in out['series'].items():
        before = previous_watermarks[sid]; current = row['acquired_at']
        if before and clock(before) > clock(generated_at): raise ValueError('Future previous watermark')
        if before and current and clock(current) < clock(before):
            row['quality']['status'] = 'source_regression'
            row['latest_value'] = row['latest_value_decimal'] = None
            row['calendar_comparisons'] = {}; row['deltas'] = {key: None for key in row['deltas']}
            out['fetch_errors'][sid] = 'source_regression'
        out['source_acquisition_watermarks'][sid] = max(
            (value for value in (before, current) if value is not None), key=clock, default=None)
    fresh = sum(row['quality']['status'] == 'fresh' for row in out['series'].values())
    out['n_series_ok'] = fresh
    out['quality'].update(status='fresh' if fresh == len(arithmetic.SPECS) else 'degraded' if fresh else 'unavailable', fresh_series=fresh)
    out['legacy_context'] = deepcopy(legacy_context)
    out['migration'] = {'previous_measurements_preserved': True, 'legacy_scores_qualified': False,
        'basis': 'Complete original rows, official definitions and calendar endpoints; no investment score.'}
    return out
