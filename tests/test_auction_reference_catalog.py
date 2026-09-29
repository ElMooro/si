"""Legacy declarations never become comparable historical measurements."""
from pathlib import Path
from contextlib import redirect_stdout
from copy import deepcopy
from datetime import date
import ast, io, runpy, sys
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'aws/shared'))
from auction_reference_catalog import build, PERMISSIONS, REASONS
TODAY = date(2026, 9, 29)


def constants():
    values = {}
    paths = ['lambda_function.py', 'auction_crisis_v2.py']
    for path in paths:
        tree = ast.parse((ROOT / 'aws/lambdas/justhodl-auction-crisis-detector/source' / path).read_text(encoding='utf-8'))
        for node in tree.body:
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id in ('CRISIS_REFERENCE', 'ANALOG_OUTCOMES'):
                        values[target.id] = ast.literal_eval(node.value)
    return values['CRISIS_REFERENCE'], values['ANALOG_OUTCOMES']


def native():
    support = runpy.run_path(str(ROOT / 'tests/deployment/test_auction_output_ownership.py'))
    with redirect_stdout(io.StringIO()): store, scope = support['detector_fixture']()
    return store.docs['data/auction-crisis.json'], scope


def catalog(vector=None):
    refs, notes = constants()
    return build(refs, notes, vector if vector is not None else [0] * 6, TODAY)


def test_every_real_legacy_constant_and_narrative_survives_without_ranking():
    refs, notes = constants(); out = catalog()
    assert len(refs) == out['reference_count'] == len(out['all_matches']) == 10
    assert [row['legacy_reference'] for row in out['all_matches']] == refs
    assert out['legacy_narratives'] == notes
    assert [row['legacy_narrative'] for row in out['all_matches']] == [notes[r['date']] for r in refs]
    assert out['top_matches'] == [] and out['ranking_eligible'] is False


def test_complete_zero_vector_cannot_rank_first_gfc_catalog_entry():
    out = catalog([0] * 6)
    assert out['current_vector'] == [0] * 6 and out['current_vector_status'] == 'complete_unqualified'
    assert out['top_matches'] == []
    assert all(row['rank'] is row['similarity'] is None for row in out['all_matches'])


def test_complete_nonzero_vector_cannot_repair_missing_original_anchor_inputs():
    for vector in ([100] * 6, [100, 70, 75, 100, 100, 90]):
        out = catalog(vector)
        assert out['current_vector'] == vector and out['similarity_metric'] is None
        assert out['missing_verification'] == REASONS and not out['top_matches']


def test_missing_current_values_never_become_measured_zero():
    for invalid in (None, True, '0', float('nan'), float('inf'), -1, 101, 10**1000):
        out = catalog([invalid, 0, 20, 30, 40, 50])
        assert out['current_vector'][0] is None and out['current_vector_status'] == 'incomplete'
        assert not out['top_matches'] and out['current_vector'][1] == 0


def test_no_current_or_reference_permission_can_be_inferred_from_complete_arithmetic():
    out = catalog()
    for row in [out] + out['all_matches']:
        assert all(row[key] is False for key in PERMISSIONS)
        for key in ('source_capture_verified', 'instrument_comparability_verified', 'outcome_measurement_verified'):
            assert row[key] is False


def test_duplicate_dates_are_retained_as_distinct_occurrences_never_deduplicated():
    refs, notes = constants(); refs.insert(1, deepcopy(refs[0]))
    out = build(refs, notes, [0] * 6, TODAY)
    assert out['reference_count'] == 11
    assert out['all_matches'][0]['date'] == out['all_matches'][1]['date']
    assert out['all_matches'][0]['entry_id'] != out['all_matches'][1]['entry_id']
    assert [r['legacy_reference'] for r in out['all_matches']] == refs


def test_extra_reference_fields_and_unmatched_narratives_are_preserved():
    refs, notes = constants(); refs[0]['additional_declared_input'] = {'unknown': [None, 0, False]}
    notes['unmatched_source_note'] = {'unverified_text': 'Retain, do not use as measured outcome'}
    out = build(refs, notes, [0] * 6, TODAY)
    assert out['all_matches'][0]['legacy_reference'] == refs[0] and out['legacy_narratives'] == notes


def test_declared_dates_are_validated_without_assuming_security_identity():
    for day in (None, 123, [], '2026-02-31', '2999-01-01', '2020-01-01T00:00:00Z'):
        out = build([{'date': day}], {}, [None] * 6, TODAY)
        assert out['all_matches'][0]['legacy_reference']['date'] == day
        assert out['all_matches'][0]['date_status'] == 'invalid_declared_date'
        assert out['all_matches'][0]['status'] == 'unverified_legacy_entry'


def test_self_declared_provenance_fields_cannot_promote_a_reference():
    refs, notes = constants()
    refs[0].update(cusip='CLAIM0001', quote_basis='nominal_yield_pct', source_ref='unverified claim',
                   source_capture_verified=True, calls_eligible=True)
    out = build(refs, notes, [0] * 6, TODAY)
    assert out['all_matches'][0]['legacy_reference'] == refs[0]
    assert out['all_matches'][0]['calls_eligible'] is False and out['top_matches'] == []


def test_catalog_is_independent_copy_with_no_external_mutation():
    refs, notes = constants(); saved = deepcopy((refs, notes))
    out = build(refs, notes, [0] * 6, TODAY)
    out['all_matches'][0]['legacy_reference']['date'] = 'changed'
    out['legacy_narratives'].clear()
    assert (refs, notes) == saved


def test_malformed_catalogs_fail_whole_without_returning_partial_entries():
    for refs, notes, vector, today in (({}, {}, [0]*6, TODAY), ([{}, None], {}, [0]*6, TODAY),
                                      ([], [], [0]*6, TODAY), ([], {}, [0]*5, TODAY),
                                      ([], {}, [0]*6, '2026-09-29')):
        try: build(refs, notes, vector, today)
        except ValueError: pass
        else: raise AssertionError('Malformed catalog accepted')
    out = build([], {}, [None]*6, TODAY)
    assert out['status'] == 'empty' and out['reference_count'] == 0 and out['all_matches'] == []


def test_actual_native_adapter_retains_all_inputs_and_withholds_claimed_outcomes():
    out, scope = native(); refs, notes = constants(); result = out['historical_analog']
    assert out['historical_reference'] == refs
    assert result['contract'] == 'auction-reference-catalog.v1' and result['legacy_narratives'] == notes
    assert result['top_matches'] == [] and len(result['all_matches']) == 10
    for row in result['all_matches']:
        assert row['anchor_vec'] == [None]*6
        assert row['context'] is row['what_happened_next'] is row['duration'] is None
    adapter = scope['lambda_handler'].__globals__['find_historical_analogs']
    result = adapter(out['auction_observations'], refs, issuance_score=100, top_n=100)
    assert result['legacy_requested_top_n'] == 100 and result['top_matches'] == []


if __name__ == '__main__':
    tests = [value for key, value in list(globals().items()) if key.startswith('test_')]
    for test in tests: test()
    print('Auction reference catalog regressions passed:', len(tests))
