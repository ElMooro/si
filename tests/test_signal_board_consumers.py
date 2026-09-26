"""Exercise actual consumer functions without invoking providers or portfolios."""
from copy import deepcopy
from pathlib import Path
import ast
from datetime import datetime, timezone
import json
import sys
import time
from types import SimpleNamespace
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'aws/shared'), str(ROOT/'aws/ops/checks')]
import signal_board_authority as gate
from release_package_evidence import shared_imports

LEGACY = {'composite_signal': 2, 'composite': 100, 'composite_score': 100, 'composite_posture': 'STRONG RISK-ON',
          'engines': [{'engine': 'A', 'signal': 2, 'state': 'LONG', 'score': 99, 'calls_eligible': True}],
          'calls_eligible': True, 'sizing_eligible': True, 'generated_at': '2026-09-26T06:15:00Z'}
CHANGED = ('desk-allocator', 'master-allocator', 'cross-asset-confirm')


def functions(name, names, scope=None):
    scope = {} if scope is None else scope
    text = (ROOT/f'aws/lambdas/justhodl-{name}/source/lambda_function.py').read_text(encoding='utf-8')
    nodes = [node for node in ast.parse(text).body if isinstance(node, ast.FunctionDef) and node.name in names]
    assert len(nodes) == len(names)
    exec(compile(ast.Module(body=nodes, type_ignores=[]), name, 'exec'), scope)
    return scope


class Tests(unittest.TestCase):
    def packets(self):
        return (None, {}, [], LEGACY, {**LEGACY, 'contract': 'signal-board-research.v1', 'replay': {'verified': True}},
                {**LEGACY, 'contract': 'invented-qualified-model.v9', 'decision_qualification': {'status': 'qualified'}})

    def test_packet_claims_cannot_grant_forecast_or_sizing_authority(self):
        for packet in self.packets():
            before = deepcopy(packet); view = gate.decision_view(packet)
            for key in ('composite_signal', 'composite_score', 'composite', 'score', 'deep_read'):
                self.assertIsNone(view[key])
            self.assertEqual(view['composite_posture'], 'WAIT')
            self.assertEqual(view['engines'], [])
            self.assertTrue(all(view[k] is False for k in gate.FLAGS))
            self.assertEqual(gate.qualified_signals(packet), [])
            self.assertEqual(packet, before)

    def test_context_names_its_limits_and_keeps_reported_source_clock(self):
        result = gate.context(LEGACY)
        self.assertEqual(result['reported_engine_rows'], 1)
        self.assertEqual(result['reported_generated_at'], LEGACY['generated_at'])
        self.assertEqual(result['independent_investment_votes'], 0)
        self.assertFalse(result['inventory_replay_performed_by_consumer'])
        self.assertFalse(result['current_observation_freshness_verified_by_consumer'])
        self.assertIsNone(gate.context({'generated_at': 7})['reported_generated_at'])

    def test_actual_desk_allocator_abstains_without_inventing_neutral(self):
        for packet in self.packets():
            scope = functions('desk-allocator', {'read_regime', 'clamp'},
                              {'get_json': lambda key: packet if key == gate.CURRENT else {}})
            result = scope['read_regime']()
            self.assertIsNone(result['signal_board_composite'])
            self.assertIsNone(result['blended_risk_axis'])
            self.assertEqual(result['inputs_available'], 0)
            self.assertEqual(result['label'], 'UNAVAILABLE')
            self.assertFalse(result['signal_board_permission']['sizing_eligible'])

    def test_actual_master_allocator_cannot_reinterpret_legacy_scales(self):
        for packet in self.packets():
            scope = functions('master-allocator', {'gather_signals', 'clamp'},
                              {'read_json': lambda key: packet if key == gate.CURRENT else {}})
            self.assertNotIn('signal_board', scope['gather_signals']())

    def test_actual_cross_asset_overlay_does_not_confirm_or_veto_unqualified_rows(self):
        scope = functions('cross-asset-confirm', {'extract_signals_from_board'})
        for packet in (*self.packets(), {'state': {'score': 99}}, {'signals': {'a': 'LONG'}}):
            before = deepcopy(packet)
            self.assertEqual(scope['extract_signals_from_board'](packet), [])
            self.assertEqual(packet, before)

    def test_actual_handler_cannot_alert_from_legacy_board_rows(self):
        writes = []
        def forbidden(*args, **kwargs): raise AssertionError('Unqualified signal reached an overlay or notification')
        scope = functions('cross-asset-confirm', {'lambda_handler', 'extract_signals_from_board'}, {
            'time': time, 'datetime': datetime, 'timezone': timezone, 'json': json, 'VERSION': 'test',
            'compute_bonds_state': lambda: {}, 'compute_fx_state': lambda: {},
            'compute_credit_state': lambda: {}, 'compute_vol_state': lambda: {},
            'synthesize_regime': lambda *args: {'regime': 'TEST_ONLY', 'composite_score': 0},
            'load_signal_board': lambda: LEGACY, 'confirm_signal': forbidden, 'send_telegram': forbidden,
            's3': SimpleNamespace(put_object=lambda **kwargs: writes.append(kwargs)), 'BUCKET': 'fixture', 'OUT_KEY': 'fixture.json'})
        response = scope['lambda_handler']()
        self.assertEqual(response['statusCode'], 200)
        self.assertEqual(len(writes), 1)
        payload = json.loads(writes[0]['Body'])
        self.assertEqual(payload['signal_overlays'], [])
        self.assertEqual(payload['overlay_summary'], dict.fromkeys(('n_signals', 'n_confirmed', 'n_vetoed', 'n_neutral'), 0))
        self.assertFalse(payload['signal_board_context']['calls_eligible'])

    def test_other_sources_unchanged_and_three_packages_bundle_boundary(self):
        self.assertIs(gate.guard('data/unrelated.json', LEGACY), LEGACY)
        self.assertEqual(gate.guard(gate.CURRENT, LEGACY)['n_live'], 0)
        for name in CHANGED:
            source = ROOT/f'aws/lambdas/justhodl-{name}/source'
            self.assertIn('signal_board_authority.py', [p.name for p in shared_imports(ROOT, list(source.glob('*.py')))])

    def test_code_deployment_preserves_all_existing_schedule_bindings(self):
        sys.path.insert(0, str(ROOT/'scripts'))
        from normalize_lambda_config import normalize_config
        for name in CHANGED:
            config = json.loads((ROOT/f'aws/lambdas/justhodl-{name}/config.json').read_bytes())
            normalized = normalize_config(config)
            self.assertNotIn('schedule', normalized)
            self.assertNotIn('eventbridge_scheduler', normalized)
            self.assertEqual(normalized['release_schedule_note']['binding_action'], 'PRESERVE_EXISTING')
            ref = config.get('preserved_schedule_reference') or config['preserved_eventbridge_scheduler_reference']
            self.assertEqual(normalized['release_schedule_note']['configured_expression'], ref['cron'])


if __name__ == '__main__': unittest.main(verbosity=2)
