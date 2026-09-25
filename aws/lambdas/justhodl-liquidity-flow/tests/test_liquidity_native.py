from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch
import ast, hashlib, importlib.util, json, sys, unittest
HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[3]
sys.path[:0] = [str(HERE.parent/'source'), str(ROOT/'aws/shared'), str(ROOT/'scripts'),
               str(ROOT/'aws/lambdas/justhodl-crisis-composite/tests')]
import liquidity_flow_model as model
import liquidity_flow_store as store
import replay_liquidity_flow_research as cli
import verify_liquidity_arithmetic as independent
from test_native_research import fixtures, store_fixture, Storage, STAMP


def setup():
    objects, _ = store_fixture()
    source = json.loads(objects[store.SOURCE])
    raw = objects[store.SOURCE]; digest = hashlib.sha256(raw).hexdigest()
    ref = {'key': model.PREFIX+'snapshots/'+digest+'.json', 'sha256': digest, 'bytes': len(raw)}
    objects[ref['key']] = raw
    return objects, {'contract': 'liquidity-flow-inputs.v1', 'generated_at': STAMP,
        'macro': ref, 'settlement': None, 'legacy_context': {}}, source


class Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.objects, cls.inputs, cls.source = setup()
        cls.output = store.compile_output(cls.inputs, store.reader(Storage(cls.objects), 'b'))
    def setUp(self): self.client = Storage(self.objects); self.read = store.reader(self.client, 'b')

    def test_native_arithmetic_is_exactly_the_qualified_candidate(self):
        accepted = (ROOT/'aws/ops/checks/liquidity_flow_candidate.py').read_bytes()
        self.assertEqual(Path(store.arithmetic.__file__).read_bytes(), accepted)
        source, originals = fixtures()
        candidate = store.arithmetic.build(source, originals, STAMP)
        proof = independent.verify(candidate, source, originals)
        self.assertEqual(proof['calendar_dates_checked'], 180)
        self.assertEqual(self.output['series'], candidate['series'])
        self.assertEqual(self.output['comparisons'], candidate['comparisons'])
        self.assertEqual(self.output['last_reconstructed_snapshot'], candidate['last_reconstructed_snapshot'])
        self.assertFalse(self.output['calls_eligible']); self.assertFalse(self.output['sizing_eligible'])
        self.assertEqual(self.output['regime'], 'descriptive_proxy')
        self.assertEqual(self.output['decision']['verb'], 'WAIT')
        self.assertIsNone(self.output['quality']['publication_date'])

    def test_whole_predecessors_and_actual_production_qualification_remain_bound(self):
        migration = json.loads((ROOT/'tests/fixtures/liquidity-flow-native-migration.json').read_bytes())
        self.assertEqual(migration['qualification_run'], 36186015541)
        self.assertEqual(migration['proof']['original_rows_checked'], 7366)
        self.assertEqual(migration['proof']['exact_rational_comparisons'], 776)
        self.assertEqual(hashlib.sha256(Path(store.arithmetic.__file__).read_bytes()).hexdigest(), migration['qualified_arithmetic']['sha256'])
        for ref in migration['predecessors']:
            raw = (ROOT/ref['path']).read_bytes()
            self.assertEqual(len(raw), ref['bytes']); self.assertEqual(hashlib.sha256(raw).hexdigest(), ref['sha256'])
        self.assertEqual((HERE.parent/'config.json').read_bytes(), (ROOT/'tests/fixtures/legacy-liquidity-flow-config.txt').read_bytes())

    def test_immutable_originals_code_inputs_output_and_cli_replay_exactly(self):
        ref = store.retain(self.client, 'b', self.inputs, self.output)
        packet = {**deepcopy(self.output), 'replay': ref}
        self.assertEqual(store.replay(ref, self.read), self.output)
        self.assertTrue(cli.verify(packet, self.read)['replayed'])
        packet['current']['net_liquidity_b'] = 0
        with self.assertRaises(ValueError): cli.verify(packet, self.read)

    def test_tampered_source_code_and_output_never_publish(self):
        ref = store.retain(self.client, 'b', self.inputs, self.output)
        manifest = json.loads(self.read(ref['manifest_key']))
        for key in (manifest['compilers']['liquidity_flow_store']['key'], manifest['output']['key'], self.inputs['macro']['key']):
            raw = self.client.objects[key]; self.client.objects[key] = b'{}'
            with self.assertRaises(ValueError): store.replay(ref, self.read)
            self.client.objects[key] = raw
        self.assertNotIn(model.CURRENT, self.client.objects)

    def test_private_and_unrelated_paths_are_rejected_before_io(self):
        for key in ('data/portfolio.json', 'data/brain.json', 'audit-private/x', 'data/liquidity-flow-research/inputs/../x.json'):
            with self.assertRaises(ValueError): self.read(key)
        self.assertEqual(self.client.reads, [])

    def test_complete_predecessor_preserved_and_newer_source_cannot_be_replaced(self):
        prior = b'{"generated_at":"2026-09-18T12:00:00Z","legacy":"all bytes retained"}'
        self.client.objects[model.CURRENT] = prior
        context = store.previous_context(self.client, 'b')
        self.assertEqual(self.client.objects[context['whole_predecessor']['key']], prior)
        packet = {**self.output, 'replay': {'fixture': True}}
        self.assertTrue(store.publish(self.client, 'b', packet))
        newer = deepcopy(packet); newer['generated_at'] = '2026-09-21T00:00:00Z'; newer['source_generated_at'] = '2026-09-21T00:00:00Z'
        self.client.objects[model.CURRENT] = model.encoded(newer)
        candidate = deepcopy(packet); candidate['generated_at'] = '2026-09-22T00:00:00Z'
        self.assertFalse(store.publish(self.client, 'b', candidate))
        newer['source_generated_at'] = candidate['source_generated_at']; newer['series']['WALCL']['acquired_at'] = '2026-09-21T00:00:00Z'
        self.client.objects[model.CURRENT] = model.encoded(newer)
        self.assertFalse(store.publish(self.client, 'b', candidate))

    def test_same_clock_conflict_and_racing_newer_write_are_rejected(self):
        packet = deepcopy(self.output); self.client.objects[model.CURRENT] = model.encoded(packet)
        self.assertTrue(store.publish(self.client, 'b', packet))
        packet['interpretation'] = 'conflicting bytes'
        with self.assertRaisesRegex(ValueError, 'same-clock'): store.publish(self.client, 'b', packet)
        self.client.objects[model.CURRENT] = b'{"generated_at":"2026-09-18T00:00:00Z"}'
        newer = deepcopy(packet); newer['generated_at'] = '2026-09-23T00:00:00Z'
        original = self.client.put_object
        def racing(**kw):
            if kw['Key'] == model.CURRENT: self.client.objects[model.CURRENT] = model.encoded(newer)
            return original(**kw)
        with patch.object(self.client, 'put_object', side_effect=racing):
            self.assertFalse(store.publish(self.client, 'b', packet))
        self.assertEqual(json.loads(self.client.objects[model.CURRENT]), newer)

    def test_unavailable_source_keeps_dates_null_without_invented_direction(self):
        inputs = deepcopy(self.inputs); inputs['generated_at'] = '2026-09-25T00:00:00Z'
        out = store.compile_output(inputs, self.read)
        self.assertIsNone(out['current']['net_liquidity_b']); self.assertIsNone(out['as_of'])
        self.assertEqual(out['regime'], 'unavailable'); self.assertTrue(all(row is None for row in out['deltas'].values()))
        self.assertEqual(out['series']['WALCL']['history'], self.output['series']['WALCL']['history'])

    def test_scopes_units_numbers_dates_and_reconciliation_are_required(self):
        row = {'scope_id': 'treasury_incl_tips', 'as_of': '2026-09-16', 'complete': True,
            'ftd_bn': 0, 'ftr_bn': 2.015, 'gross_bn': 2.015,
            'field_units': {key: 'usd_bn' for key in ('ftd_bn', 'ftr_bn', 'gross_bn')}}
        packet = {'contract': 'fr2004-fails-research.v1', 'treasury': row}
        out = model.settlement_context(packet, STAMP, None)
        self.assertEqual(out['ftd_bn'], 0); self.assertEqual(out['combined_bn'], 2.015)
        self.assertIsNone(out['ust_ex_tips']['combined_bn']); self.assertEqual(out['quality']['status'], 'unverified')
        for field, value in (('ftd_bn', '0'), ('gross_bn', 3), ('scope_id', 'ust_ex_tips'), ('as_of', '2099-09-16')):
            bad = deepcopy(packet); bad['treasury'][field] = value
            self.assertIsNone(model.settlement_context(bad, STAMP, None)['combined_bn'])

    def test_scheduled_store_and_handler_cannot_invoke_models_or_accounts(self):
        class Clock:
            @staticmethod
            def now(tz): return datetime.fromisoformat(STAMP)
        with patch.object(store, 'datetime', Clock): result = store.run(self.client, 'b')
        self.assertTrue(result['published'])
        self.assertEqual(result['provider_requests'], 0); self.assertEqual(result['private_account_reads'], 0)
        path = HERE.parent/'source/lambda_function.py'
        spec = importlib.util.spec_from_file_location('liquidity_native_handler_test', path)
        module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
        with patch.object(module, 'run', return_value={'published': True}) as runner, patch.object(module.boto3, 'client', return_value=self.client):
            self.assertEqual(module.lambda_handler({'send_telegram': True, 'portfolio': True, 'invoke': True}, None)['statusCode'], 200)
        runner.assert_called_once_with(self.client, module.S3_BUCKET)


if __name__ == '__main__': unittest.main(verbosity=2)
