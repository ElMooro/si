"""Exercise alert construction only; no Lambda initialization or message transport."""
import ast
import copy
from datetime import datetime, timezone, timedelta
import importlib.util
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / 'aws/lambdas/justhodl-alert-router/source'
sys.path.insert(0, str(ROOT / 'aws/shared'))
from bottom_context import context_rows
from donor_contract import numeric

spec = importlib.util.spec_from_file_location('audited_cot_context', SOURCE / 'cot_context.py')
cot = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cot)
NOW = datetime(2026, 9, 9, 18, tzinfo=timezone.utc)


def cot_document():
    return {'engine': 'justhodl-cot-extremes-scanner', 'schema_version': 'cot-extremes.v2',
            'generated_at': NOW.isoformat(), 'execution_eligible': False,
            'contracts': [{'contract': '6B', 'status': 'ok', 'percentile': 0,
                           'n_prior_observations': 26, 'report_date': '2026-09-01',
                           'report_type': 'tff', 'extreme': 'low', 'execution_eligible': False}]}


def run_check(name, document):
    reads = []
    def load(key):
        reads.append(key)
        return document
    tree = ast.parse((SOURCE / 'lambda_function.py').read_text())
    function = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == name)
    namespace = {'load_json': load, 'numeric': numeric,
                 'context_rows': lambda doc: context_rows(doc, now=NOW),
                 'extreme_rows': lambda doc: cot.extreme_rows(doc, now=NOW)}
    exec(compile(ast.Module(body=[function], type_ignores=[]), str(SOURCE / 'lambda_function.py'), 'exec'), namespace)
    alerts = []
    namespace[name](alerts)
    return alerts, reads


def test_cot_consumer_reads_dedicated_output_preserves_zero_and_actual_sample_size():
    alerts, reads = run_check('check_cot_extremes', cot_document())
    assert reads == ['cot/extremes/current.json'] and len(alerts) == 1
    assert 'Percentile 0 against 26 prior observations' in alerts[0]['detail']
    assert '2026-09-01' in alerts[0]['id'] and '5y' not in alerts[0]['detail']
    assert alerts[0]['execution_eligible'] is False


def test_cot_consumer_withholds_wrong_identity_old_schema_and_stale_or_future_vintages():
    for update in ({'engine': 'other'}, {'schema_version': 'v1'}, {'execution_eligible': True},
                   {'generated_at': (NOW - timedelta(days=9)).isoformat()},
                   {'generated_at': (NOW + timedelta(seconds=1)).isoformat()},
                   {'generated_at': '2026-09-09T18:00:00'}, {'generated_at': None}):
        assert not cot.extreme_rows({**cot_document(), **update}, now=NOW)


def test_cot_consumer_requires_typed_valid_rank_dates_history_and_consistent_direction():
    for update in ({'percentile': True}, {'percentile': '0'}, {'percentile': float('nan')},
                   {'percentile': -1}, {'status': 'stale_report'}, {'n_prior_observations': 25},
                   {'report_date': '2026-08-01'}, {'report_date': '2026-09-10'},
                   {'report_type': 'unknown'}, {'extreme': 'high'}):
        doc = cot_document(); doc['contracts'][0].update(update)
        assert not cot.extreme_rows(doc, now=NOW)
    doc = cot_document(); doc['contracts'].append(copy.deepcopy(doc['contracts'][0]))
    assert not cot.extreme_rows(doc, now=NOW)


def bottom_document(ratio=0):
    row = {'ticker': 'SPY', 'state': 'ST_CONFIRMED', 'frame': 'D', 'grade': 'A',
           'score': 80, 'bars_in_state': 0, 'st_vol_ratio_sc': ratio}
    return {'engine': 'justhodl-bottom', 'generated_at': NOW.isoformat(), 'session': '2026-09-09',
            'board_all': [row], 'board': [], 'changes': {'new_test_confirmed': ['SPY']}}


def test_bottom_consumer_reads_complete_validated_board_and_preserves_zero_volume_ratio():
    alerts, reads = run_check('check_bottom', bottom_document())
    assert reads == ['data/bottom.json'] and len(alerts) == 1
    assert alerts[0]['id'] == 'bottom_test_SPY_2026-09-09'


def test_bottom_consumer_withholds_missing_invalid_large_ratios_and_inconsistent_states():
    for ratio in (None, True, float('nan'), -0.1, 0.51, 'unknown'):
        assert not run_check('check_bottom', bottom_document(ratio))[0]
    doc = bottom_document(); doc['board_all'][0]['state'] = 'FAILED'
    assert not run_check('check_bottom', doc)[0]
    doc = bottom_document(); doc['generated_at'] = (NOW - timedelta(hours=31)).isoformat()
    assert not run_check('check_bottom', doc)[0]


def test_bottom_consumer_ignores_unreviewed_subset_and_malformed_change_metadata():
    doc = bottom_document(); doc['board'] = [{'ticker': 'EVIL', 'grade': 'A'}]
    doc['changes'] = {'new_test_confirmed': ['EVIL', {}, None]}
    assert not run_check('check_bottom', doc)[0]
    doc['changes'] = []
    assert not run_check('check_bottom', doc)[0]
