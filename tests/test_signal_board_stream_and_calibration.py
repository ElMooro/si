"""Legacy board data cannot acquire authority through alerts or historical fits."""
from copy import deepcopy
from datetime import datetime, timezone, timedelta
from pathlib import Path
from types import SimpleNamespace
import ast, json, sys, time, unittest
ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'tests'), str(ROOT/'aws/shared'), str(ROOT/'aws/ops/checks')]
from test_signal_board_consumers import functions, LEGACY
import signal_board_authority as gate
from release_package_evidence import shared_imports


def registry(name, variable):
    tree = ast.parse((ROOT/f'aws/lambdas/justhodl-{name}/source/lambda_function.py').read_bytes())
    return next(ast.literal_eval(n.value) for n in tree.body if isinstance(n, ast.Assign)
                and any(isinstance(t, ast.Name) and t.id == variable for t in n.targets))


class Tests(unittest.TestCase):
    def test_no_first_broadcast_or_flip_can_restore_board_authority(self):
        engine = next(e for e in registry('streaming-fanout', 'TRACKED') if e['name'] == 'signal_board')
        scope = functions('streaming-fanout', {'_extract_summary', '_is_meaningful_delta'})
        for packet in (None, {}, LEGACY, {**LEGACY, 'contract': 'signal-board-research.v1', 'forecast_qualified': True}):
            result = scope['_extract_summary'](engine, packet)
            self.assertEqual(result['research_context']['status'], 'ABSTAIN')
            self.assertNotIn('composite_score', result)
            for previous in (None, {}, {'composite_score': -100, 'posture': 'RISK_OFF'}):
                self.assertEqual(scope['_is_meaningful_delta'](engine, previous, result), (False, 'unqualified_signal_board'))

    def test_complete_mock_handler_updates_audit_without_notification(self):
        engine = next(e for e in registry('streaming-fanout', 'TRACKED') if e['name'] == 'signal_board')
        for packet in (LEGACY, {**LEGACY, 'posture': 'RISK_OFF'}):
            writes = {}; reads = []
            def read(key):
                reads.append(key)
                if key == gate.CURRENT: return packet
                if key == 'side/signal_board_last.json': return {'last_broadcast_at': '2026-09-01T12:00:00Z', 'summary': {'posture': 'RISK_ON'}}
                self.fail('Unexpected source read: '+key)
            scope = functions('streaming-fanout', {'lambda_handler', '_extract_summary', '_is_meaningful_delta'}, {
                '_now': lambda: '2026-09-26T12:00:00Z', '_write_streaming_config': lambda: None,
                'TRACKED': [engine], '_read_json': read, 'SIDECAR_PREFIX': 'side/', 'FANOUT_LOG_KEY': 'audit.json',
                '_write_json': lambda key, body: writes.update({key: deepcopy(body)}), 'json': json,
                '_broadcast': lambda *a: self.fail('Board must not broadcast')})
            out = json.loads(scope['lambda_handler']({}, None)['body'])
            self.assertEqual(out['broadcasts'], 0); self.assertEqual(out['no_ops'], 1)
            self.assertEqual(out['actions'][0]['reason'], 'unqualified_signal_board')
            sidecar = writes['side/signal_board_last.json']
            self.assertFalse(sidecar['broadcast']); self.assertIsNone(out['actions'][0]['headline'])
            self.assertEqual(sidecar['last_broadcast_at'], '2026-09-01T12:00:00Z')
            self.assertFalse(sidecar['summary']['research_context']['calls_eligible'])

    def test_historical_scores_cannot_restore_a_calibration_weight(self):
        board = next(e for e in registry('calibration-fleet', 'REGISTRY') if e['name'] == 'signal_board')
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        history = [{'date': (start+timedelta(days=i)).date().isoformat(), 'spy_close': 600-i,
                    'scores': {'signal_board': i}} for i in range(150)]
        original = deepcopy(history); writes = {}; params = []
        def read(key):
            if key == 'gsi': return {'snapshots': history}
            if key == 'hist': return {'snapshots': deepcopy(history)}
            if key == gate.CURRENT: return LEGACY
            self.fail('Unexpected source read: '+key)
        def forbidden(*a, **kw): self.fail('No history query or statistical qualification for board')
        scope = functions('calibration-fleet', {'lambda_handler', 'deep_get'}, {
            'time': time, 'datetime': datetime, 'timezone': timezone, 'json': json, 'REGISTRY': [board],
            'GSI_HIST_KEY': 'gsi', 'HIST_KEY': 'hist', 'REPORT_KEY': 'report', 'WEIGHTS_PARAM': 'weights',
            'FORWARD_DAYS': 21, 'MIN_N': 30, 'MIN_N_STABLE': 60, 'IC_FLOOR': .05, 'HIST_BARS': 400,
            'read_json': read, 'write_json': lambda key, data, **kw: writes.update({key: deepcopy(data)}),
            'ssm': SimpleNamespace(put_parameter=lambda **kw: params.append(json.loads(kw['Value']))),
            'ddb_history_snapshots': forbidden, 'spearman': forbidden, 'quantile': forbidden})
        self.assertEqual(scope['lambda_handler']({}, None)['statusCode'], 200)
        row = writes['report']['engines'][0]
        self.assertEqual(row['quality_rating'], 'UNQUALIFIED'); self.assertIsNone(row['current_score'])
        self.assertIsNone(row['ic_spearman']); self.assertEqual(row['n_paired'], 0)
        self.assertEqual(params[0]['weights']['signal_board'], 0)
        self.assertEqual(writes['hist']['snapshots'][:-1], original)
        self.assertNotIn('signal_board', writes['hist']['snapshots'][-1]['scores'])

    def test_both_packages_include_boundary_without_trigger_mutation(self):
        sys.path.insert(0, str(ROOT/'scripts')); from normalize_lambda_config import normalize_config
        for name in ('streaming-fanout', 'calibration-fleet'):
            root = ROOT/f'aws/lambdas/justhodl-{name}'
            self.assertIn('signal_board_authority.py', [p.name for p in shared_imports(ROOT, list((root/'source').glob('*.py')))])
            config = normalize_config(json.loads((root/'config.json').read_bytes()))
            self.assertNotIn('schedule', config); self.assertNotIn('eventbridge_scheduler', config)
            if name == 'calibration-fleet':
                self.assertEqual(config['release_schedule_note']['binding_action'], 'PRESERVE_EXISTING')


if __name__ == '__main__': unittest.main(verbosity=2)
