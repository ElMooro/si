"""Exercise the actual deploy entrypoint and complete predecessor/capacity proof."""
from pathlib import Path
import ast, hashlib, importlib.util, json, sys, unittest
from unittest.mock import Mock
ROOT=Path(__file__).resolve().parents[4]
HERE=Path(__file__).resolve().parent
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/ops/checks'),str(ROOT/'tests'),str(ROOT/'scripts')]
import test_capital_structure_native_candidate as behavior
from validate_lambda_configs import validate_configs
from normalize_lambda_config import normalize_config
from lambda_config_environment import config_environment
spec=importlib.util.spec_from_file_location('share_flows_native_under_test',HERE.parent/'source/lambda_function.py')
native=importlib.util.module_from_spec(spec);spec.loader.exec_module(native)


class NativeBehavior(behavior.Tests):
    def setUp(self):
        self.prepared=behavior.native;behavior.native=native
    def tearDown(self):
        behavior.native=self.prepared


class Migration(unittest.TestCase):
    def test_complete_predecessor_and_actual_qualified_compiler_closure(self):
        proof=json.loads((ROOT/'tests/fixtures/capital-structure-native-migration.json').read_bytes())
        for item in proof['complete_predecessors']:
            raw=(ROOT/item['predecessor']).read_bytes()
            self.assertEqual((len(raw),hashlib.sha256(raw).hexdigest()),(item['bytes'],item['sha256']))
        accepted=ROOT/'aws/ops/reports/latest/ops_6123_capital_structure_private_readiness.md'
        self.assertIn('**Status:** success',accepted.read_text(encoding='utf-8'))
        self.assertTrue(proof['private_readiness']['result']['ready_advanced'])
        self.assertEqual(proof['candidate']['replay'],proof['private_readiness']['result']['replay'])
        # Candidate and actual entrypoint must have the identical executable AST.
        def code(path):
            tree=ast.parse(path.read_text(encoding='utf-8'))
            tree.body=tree.body[1:]
            return ast.dump(tree,include_attributes=False)
        self.assertEqual(code(Path(native.__file__)),code(Path(behavior.native.__file__)))

    def test_capacity_uses_measured_population_and_preserves_native_schedule(self):
        config=json.loads((HERE.parent/'config.json').read_bytes())
        proof=json.loads((ROOT/'tests/fixtures/capital-structure-native-migration.json').read_bytes())
        old=json.loads((ROOT/proof['complete_predecessors'][1]['predecessor']).read_bytes())
        self.assertEqual(validate_configs(ROOT,['justhodl-share-flows']),[])
        for key in ('runtime','handler','timeout','role'):self.assertEqual(config[key],old[key])
        self.assertEqual(config['memory'],4096)
        self.assertGreaterEqual(config['memory']*1024,2*proof['private_readiness']['max_rss_kib'])
        self.assertGreater(config['timeout'],2*proof['candidate']['profile']['replay_seconds']+60)
        normalized=normalize_config(config)
        self.assertNotIn('eventbridge_scheduler',normalized);self.assertNotIn('schedule',normalized)
        self.assertEqual(config['release_schedule_note']['configured_expression'],'cron(35 13 * * ? *)')
        fetch=Mock(side_effect=AssertionError('No other function secrets are required'))
        self.assertEqual(config_environment(config,fetch),{});fetch.assert_not_called()


if __name__=='__main__':unittest.main(verbosity=2)
