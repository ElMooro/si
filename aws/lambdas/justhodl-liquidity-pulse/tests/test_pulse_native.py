from copy import deepcopy
from datetime import datetime
from pathlib import Path
from unittest.mock import patch
import hashlib, json, sys, unittest
HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[3]
SOURCE = HERE.parent/'source'
sys.path[:0] = [str(SOURCE), str(ROOT/'aws/shared'), str(ROOT/'scripts'),
    str(ROOT/'aws/lambdas/justhodl-crisis-composite/tests')]
import liquidity_pulse_model as model
import liquidity_pulse_store as store
from test_native_research import fixtures, store_fixture, Storage, STAMP


def setup():
    objects, _ = store_fixture(); raw = objects[store.SOURCE]; digest = hashlib.sha256(raw).hexdigest()
    ref = {'key': model.PREFIX+'snapshots/'+digest+'.json', 'sha256': digest, 'bytes': len(raw)}
    objects[ref['key']] = raw
    return objects, {'contract':'liquidity-pulse-inputs.v1', 'generated_at': STAMP, 'macro': ref,
        'legacy_context':{}, 'previous_watermarks':{sid: None for sid in store.arithmetic.SPECS}}


class Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.objects, cls.inputs = setup()
        cls.output = store.compile_output(cls.inputs, store.reader(Storage(cls.objects), 'b'))
    def setUp(self): self.client = Storage(self.objects); self.read = store.reader(self.client, 'b')

    def test_native_projection_preserves_candidate_measurements_without_authority(self):
        source, originals = fixtures(); candidate = store.arithmetic.build(source, originals, STAMP)
        self.assertEqual(self.output['series'], candidate['series'])
        self.assertEqual(self.output['composites'], candidate['composites'])
        self.assertFalse(self.output['calls_eligible']); self.assertFalse(self.output['sizing_eligible'])
        self.assertEqual(self.output['decision']['verb'], 'WAIT')
        self.assertEqual(self.output['source_acquisition_watermarks']['WALCL'], originals['WALCL']['acquired_at'])

    def test_immutable_originals_code_inputs_and_full_output_replay(self):
        ref = store.retain(self.client, 'b', self.inputs, self.output)
        self.assertEqual(store.replay(ref, self.read), self.output)
        manifest = json.loads(self.read(ref['manifest_key']))
        for key in (manifest['compilers']['liquidity_pulse_store']['key'], manifest['output']['key'], self.inputs['macro']['key']):
            raw = self.client.objects[key]; self.client.objects[key] = b'{}'
            with self.assertRaises(ValueError): store.replay(ref, self.read)
            self.client.objects[key] = raw
        self.assertNotIn(model.CURRENT, self.client.objects)

    def test_private_and_unrelated_sources_are_rejected_before_io(self):
        for key in ('data/portfolio.json', 'data/brain.json', 'audit-private/x', 'data/liquidity-pulse-research/inputs/../x.json'):
            with self.assertRaises(ValueError): self.read(key)
        self.assertEqual(self.client.reads, [])

    def test_whole_predecessor_preserved_and_latest_source_publication_wins(self):
        prior = b'{"generated_at":"2026-09-18T12:00:00Z","legacy":"entire predecessor"}'
        self.client.objects[model.CURRENT] = prior
        context, watermarks = store.previous_state(self.client, 'b')
        self.assertEqual(self.client.objects[context['whole_predecessor']['key']], prior)
        self.assertTrue(all(v is None for v in watermarks.values()))
        packet = deepcopy(self.output); self.assertTrue(store.publish(self.client, 'b', packet))
        newer = deepcopy(packet); newer['generated_at'] = newer['source_generated_at'] = '2026-09-21T00:00:00Z'
        self.client.objects[model.CURRENT] = model.encoded(newer)
        candidate = deepcopy(packet); candidate['generated_at'] = '2026-09-22T00:00:00Z'
        self.assertFalse(store.publish(self.client, 'b', candidate))

    def test_missing_series_cannot_erase_watermark_and_old_recovery_has_no_current_value(self):
        source, originals = fixtures(); old = deepcopy(self.output)
        high = '2026-09-20T10:00:00Z'; old['source_acquisition_watermarks']['WALCL'] = high
        self.client.objects[model.CURRENT] = model.encoded(old)
        previous = model.watermarks(old); missing = deepcopy(source)
        missing['measurements'].pop('WALCL')
        missing['replay']['output_sha256'] = model.digest({k:v for k,v in missing.items() if k!='replay'})
        subset = {k:v for k,v in originals.items() if k!='WALCL'}
        packet = model.build(missing, subset, '2026-09-20T11:00:00Z', {}, previous)
        self.assertIsNone(packet['series']['WALCL']['latest_value'])
        self.assertEqual(packet['source_acquisition_watermarks']['WALCL'], high)
        self.assertTrue(store.publish(self.client, 'b', packet))
        context, kept = store.previous_state(self.client, 'b')
        recovery = model.build(source, originals, '2026-09-20T12:00:00Z', context, kept)
        self.assertEqual(recovery['series']['WALCL']['quality']['status'], 'source_regression')
        self.assertIsNone(recovery['series']['WALCL']['latest_value'])
        self.assertTrue(recovery['series']['WALCL']['history'])
        self.assertTrue(store.publish(self.client, 'b', recovery))
        # A competing compiler which did not see the accepted watermark loses.
        late = model.build(source, originals, '2026-09-20T13:00:00Z', {}, self.inputs['previous_watermarks'])
        self.assertFalse(store.publish(self.client, 'b', late))

    def test_same_clock_conflict_and_cas_race_never_overwrite_newer_run(self):
        packet = deepcopy(self.output); self.client.objects[model.CURRENT] = model.encoded(packet)
        self.assertTrue(store.publish(self.client, 'b', packet))
        packet['summary'] = 'conflicting bytes'
        with self.assertRaisesRegex(ValueError, 'same-clock'): store.publish(self.client, 'b', packet)
        self.client.objects[model.CURRENT] = b'{"generated_at":"2026-09-18T00:00:00Z"}'
        newer = deepcopy(packet); newer['generated_at'] = '2026-09-23T00:00:00Z'; original = self.client.put_object
        def racing(**request):
            if request['Key'] == model.CURRENT: self.client.objects[model.CURRENT] = model.encoded(newer)
            return original(**request)
        with patch.object(self.client, 'put_object', side_effect=racing):
            self.assertFalse(store.publish(self.client, 'b', packet))
        self.assertEqual(json.loads(self.client.objects[model.CURRENT]), newer)

    def test_scheduled_path_replays_before_publication_without_source_acquisition(self):
        class Clock:
            @staticmethod
            def now(tz): return datetime.fromisoformat(STAMP)
        with patch.object(store, 'datetime', Clock): result = store.run(self.client, 'b')
        self.assertTrue(result['published']); self.assertEqual(result['provider_requests'], 0)
        packet = json.loads(self.client.objects[model.CURRENT])
        self.assertEqual(store.replay(packet['replay'], self.read), {k:v for k,v in packet.items() if k!='replay'})
        self.assertEqual(result['private_account_reads'], 0); self.assertEqual(result['notifications_sent'], 0)


    def test_handler_returns_publication_metadata_and_does_not_forward_event_payload(self):
        import importlib.util
        spec = importlib.util.spec_from_file_location('pulse_native_handler_test', SOURCE/'lambda_function.py')
        module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
        client = object()
        with patch.object(module.boto3, 'client', return_value=client) as factory, patch.object(module, 'run', return_value={'published': True}) as run:
            response = module.lambda_handler({'portfolio': 'private sentinel', 'invoke': 'consumer'}, None)
        run.assert_called_once_with(client, module.S3_BUCKET); factory.assert_called_once_with('s3')
        self.assertEqual(response['statusCode'], 200)
        self.assertEqual(json.loads(response['body'])['engine_contract'], model.CONTRACT)
        self.assertNotIn('private sentinel', response['body'])

    def test_existing_alert_consumer_does_not_turn_measurements_into_alerts(self):
        # Execute only the existing pure consumer function, never import or run
        # the native router, its AWS clients, notifier or handler.
        import ast
        path = ROOT/'aws/lambdas/justhodl-alert-router/source/lambda_function.py'
        tree = ast.parse(path.read_text(encoding='utf-8'))
        method = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == 'check_liquidity_pulse')
        namespace = {'load_json': lambda key: deepcopy(self.output)}
        exec(compile(ast.Module(body=[method], type_ignores=[]), str(path), 'exec'), namespace)
        alerts = []; namespace['check_liquidity_pulse'](alerts)
        self.assertEqual(alerts, [])


    def test_migration_preserves_predecessor_bytes_qualified_arithmetic_and_actual_runtime(self):
        migration = json.loads((ROOT/'tests/fixtures/liquidity-pulse-native-migration.json').read_bytes())
        for item in migration['complete_predecessors']:
            raw = (ROOT/item['predecessor']).read_bytes()
            self.assertEqual(len(raw), item['bytes']); self.assertEqual(hashlib.sha256(raw).hexdigest(), item['sha256'])
        arithmetic = (SOURCE/'liquidity_pulse_arithmetic.py').read_bytes()
        self.assertEqual(hashlib.sha256(arithmetic).hexdigest(), migration['qualified_arithmetic']['sha256'])
        self.assertEqual(arithmetic, (ROOT/'aws/ops/checks/liquidity_pulse_candidate.py').read_bytes())
        config = json.loads((SOURCE.parent/'config.json').read_bytes())
        runtime = migration['native_predecessor']['runtime']
        self.assertEqual(config['memory'], runtime['memory_mb']); self.assertEqual(config['timeout'], runtime['timeout'])
        self.assertEqual(config['runtime'], runtime['runtime']); self.assertEqual(config['architectures'], runtime['architectures'])
        self.assertNotIn('eventbridge_scheduler', config); self.assertNotIn('environment', config)
        self.assertFalse(migration['prior_tracked_config_present'])
        self.assertEqual(migration['proof']['requested_series'], 11); self.assertEqual(migration['proof']['rational_checks'], 253)
        self.assertIn('cron(3 16 * * ? *)', [v['expression'] for v in runtime['schedules']])


if __name__ == '__main__': unittest.main(verbosity=2)
