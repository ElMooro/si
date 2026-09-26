"""The actual interpretation functions cannot restore a withheld board vote."""
from pathlib import Path
from datetime import datetime, timezone
from copy import deepcopy
from types import SimpleNamespace
import io, json, sys, unittest
ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'tests'), str(ROOT/'aws/shared'), str(ROOT/'aws/ops/checks')]
from test_signal_board_consumers import functions, LEGACY
import signal_board_authority as gate
from release_package_evidence import shared_imports

CHANGED = ('ai-website-synthesis', 'ask-desk', 'page-ai-commentary', 'strategist', 'regime-conditional-router')

class Tests(unittest.TestCase):
    def packets(self):
        return (None, {}, LEGACY, {**LEGACY, 'contract': 'signal-board-research.v1', 'replay': {'verified': True}})
    def storage(self, packet):
        class Storage:
            exceptions = SimpleNamespace(NoSuchKey=KeyError)
            def get_object(self, **kw):
                if kw['Key'] != gate.CURRENT: raise AssertionError('Unexpected source read')
                return {'Body': io.BytesIO(json.dumps(packet).encode()), 'LastModified': datetime.now(timezone.utc)}
        return Storage()
    def test_synthesis_receives_qualification_context_without_an_s3_freshness_claim(self):
        for packet in self.packets():
            scope = functions('ai-website-synthesis', {'fetch_engine'}, {'s3': self.storage(packet), 'S3_BUCKET': 'test', 'json': json,
                              'datetime': datetime, 'timezone': timezone})
            name, data = scope['fetch_engine']('signal_board', {'key': gate.CURRENT, 'fields': ['composite_signal', 'n_live']})
            self.assertEqual(name, 'signal_board'); self.assertEqual(data['status'], 'ABSTAIN')
            self.assertNotIn('composite_signal', data); self.assertNotIn('_stale', data)
            self.assertFalse(data['current_observation_freshness_verified_by_consumer'])
    def test_ask_desk_never_serializes_the_legacy_scores_to_its_model(self):
        for packet in self.packets():
            scope = functions('ask-desk', {'fetch_slim'}, {'S3': self.storage(packet), 'BUCKET': 'test', 'json': json,
                              'slim': lambda p: self.fail('Do not pass the source through the generic score slimmer')})
            data = json.loads(scope['fetch_slim'](gate.CURRENT))
            self.assertEqual(data['status'], 'ABSTAIN'); self.assertNotIn('engines', data); self.assertFalse(data['calls_eligible'])
    def test_page_commentary_context_is_guarded_before_any_prompt(self):
        for packet in self.packets():
            scope = functions('page-ai-commentary', {'gather_page_context'}, {
                'PAGE_CONFIGS': {'signal-board': {'data_files': [gate.CURRENT]}}, 'is_private_source': lambda p: False,
                '_read_json': lambda p: packet})
            data = scope['gather_page_context']('signal-board')['signal_board']
            self.assertEqual(data['contract'], 'signal-board-decision-view.v1'); self.assertIsNone(data['composite_signal'])
    def test_signal_board_commentary_is_deterministic_and_never_calls_an_ai_provider(self):
        def forbidden(*a, **k): self.fail('No model API for unqualified Signal Board commentary')
        scope = functions('page-ai-commentary', {'generate_commentary', 'usable_previous'}, {
            'PAGE_CONFIGS': {'signal-board': {'system': 'legacy directional prompt'}}, 'call_claude': forbidden,
            'datetime': datetime, 'timezone': timezone, 'json': json, 'truncate_for_prompt': forbidden})
        for packet in self.packets():
            out = scope['generate_commentary']('signal-board', {'signal_board': gate.decision_view(packet)})
            self.assertEqual(out['posture'], 'WAIT'); self.assertEqual(out['model_api_calls'], 0)
            self.assertIsNone(out['confidence_score']); self.assertTrue(all(out[k] is False for k in gate.FLAGS))
            self.assertEqual(scope['usable_previous']('signal-board', {'commentary': out}), out)
        self.assertEqual(scope['usable_previous']('signal-board', {'commentary': {'regime': 'BUY'}}), {})
    def test_strategist_omits_abstention_instead_of_counting_it_as_neutral(self):
        for packet in self.packets():
            scope = functions('strategist', {'load', 'extract'}, {'S3': self.storage(packet), 'BUCKET': 'test', 'json': json,
                              'datetime': datetime, 'timezone': timezone})
            key, data, age = scope['load'](gate.CURRENT)
            self.assertEqual(key, gate.CURRENT); self.assertFalse(data['calls_eligible'])
            self.assertIsNone(scope['extract'](data))
    def test_board_posture_cannot_activate_or_boost_portfolio_frameworks(self):
        scope = functions('regime-conditional-router', {'detect_plant_and_harvest', 'detect_us10y_5pct', 'safe_get'})
        for posture in ('BULLISH', 'RISK_ON', 'RISK_OFF', 'DEFENSIVE'):
            packet = {**LEGACY, 'posture': posture}; before = deepcopy(packet)
            value, evidence = scope['detect_plant_and_harvest'](packet, {'score': 0}, {'spike_risk_score': 0})
            self.assertIsNone(value); self.assertEqual(evidence['status'], 'ABSTAIN')
            value, evidence = scope['detect_us10y_5pct']({'rates_signal': 'SHOCK'}, packet, {'spike_risk_score': 70})
            self.assertEqual(value, 80); self.assertIsNone(evidence['posture']); self.assertEqual(packet, before)
    def test_packages_include_boundary_and_no_schedules_are_rewritten(self):
        sys.path.insert(0, str(ROOT/'scripts')); from normalize_lambda_config import normalize_config
        for name in CHANGED:
            root = ROOT/f'aws/lambdas/justhodl-{name}'
            self.assertIn('signal_board_authority.py', [p.name for p in shared_imports(ROOT, list((root/'source').glob('*.py')))])
            if (root/'config.json').exists():
                cfg = normalize_config(json.loads((root/'config.json').read_bytes()))
                self.assertNotIn('schedule', cfg); self.assertNotIn('eventbridge_scheduler', cfg)
                self.assertEqual(cfg['release_schedule_note']['binding_action'], 'PRESERVE_EXISTING')

if __name__ == '__main__': unittest.main(verbosity=2)
