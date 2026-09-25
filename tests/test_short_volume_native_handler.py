from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch
from io import BytesIO
import importlib.util, json, sys, unittest
ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'aws/shared'), str(ROOT / 'tests'), str(ROOT / 'scripts')]
from test_short_volume_context import packet, model
from gen_engine_manifest import scan_code
from normalize_lambda_config import normalize_config


def load(name):
    path = ROOT / f'aws/lambdas/justhodl-{name}/source/lambda_function.py'
    spec = importlib.util.spec_from_file_location('native_' + name.replace('-', '_'), path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


HANDLERS = {name: load(name) for name in ('finra-short', 'short-pressure')}


class Tests(unittest.TestCase):
    def test_validation_never_constructs_aws_and_http_only_reads_native_current(self):
        for name, h in HANDLERS.items():
            with patch.object(h.boto3, 'client', side_effect=AssertionError('No AWS')):
                self.assertEqual(h.lambda_handler({'validate_only': True})['statusCode'], 200)
            client = Mock()
            value = packet()
            client.get_object.return_value = {'Body': BytesIO(model.encoded(value))}
            producer = h.producer if name == 'finra-short' else h.reference
            with patch.object(h.boto3, 'client', return_value=client), patch.object(producer, 'run', side_effect=AssertionError('No acquisition')):
                self.assertEqual(json.loads(h.lambda_handler({'httpMethod': 'GET'})['body']), value)
            client.get_object.assert_called_once_with(Bucket=h.BUCKET, Key=h.PUBLISHED_KEY)
            client.put_object.assert_not_called()

    def test_legacy_heads_are_unavailable_and_no_aws_execution_is_fabricated(self):
        for h in HANDLERS.values():
            client = Mock()
            client.get_object.return_value = {'Body': BytesIO(b'{"squeeze_candidates":[{"score":99}]}')}
            with patch.object(h.boto3, 'client', return_value=client):
                self.assertEqual(h.lambda_handler({'action': 'current_state'})['statusCode'], 503)
                with self.assertRaises(ValueError):
                    h.lambda_handler({})

    def test_real_identity_and_budget_reach_scoped_producer(self):
        for name, h in HANDLERS.items():
            client = Mock()
            def run(storage, bucket, request, execution, **kwargs):
                self.assertEqual(request, 'once')
                self.assertEqual(execution, 'aws-execution')
                self.assertIs(storage.publisher, h.publish_current)
                if name == 'finra-short':
                    self.assertEqual(kwargs['remaining_seconds'], 299)
                else:
                    self.assertEqual(kwargs, {})
                storage.put_object(Bucket=bucket, Key=h.PUBLISHED_KEY, Body=b'{}', IfMatch='observed',
                                   CacheControl='no-store', ContentType='application/json')
                return {'published': True}
            producer = h.producer if name == 'finra-short' else h.reference
            with patch.object(h.boto3, 'client', return_value=client), patch.object(producer, 'run', side_effect=run):
                result = h.lambda_handler({'request_id': 'once'}, SimpleNamespace(aws_request_id='aws-execution', get_remaining_time_in_millis=lambda: 299000))
                self.assertEqual(result['statusCode'], 200)
            self.assertEqual(client.put_object.call_count, 1)
            path = ROOT / f'aws/lambdas/justhodl-{name}/source/lambda_function.py'
            scan = scan_code(path.read_text(encoding='utf-8'), entrypoint='lambda_handler')
            self.assertEqual(scan.writes, {h.PUBLISHED_KEY})
            self.assertTrue(scan.proofs[h.PUBLISHED_KEY])

    def test_empty_scheduled_events_reuse_daily_request_identity_across_aws_retries(self):
        for name, h in HANDLERS.items():
            producer = h.producer if name == 'finra-short' else h.reference
            with patch.object(h.boto3, 'client', return_value=Mock()), patch.object(producer, 'run', return_value={'published': False}) as fn:
                for execution in ('first', 'retry'):
                    h.lambda_handler({}, SimpleNamespace(aws_request_id=execution, get_remaining_time_in_millis=lambda: 299000))
                self.assertEqual(fn.call_args_list[0].args[2], fn.call_args_list[1].args[2])
                self.assertTrue(fn.call_args.args[2].startswith('scheduled-short-volume'))

    def test_storage_blocks_accounts_unreviewed_outputs_and_unconditional_publication(self):
        for h in HANDLERS.values():
            client = Mock()
            storage = h.EvidenceStorage(client, h.publish_current)
            for key in ('data/trade-tickets.json', 'data/portfolio.json', 'data/unreviewed.json'):
                with self.assertRaises(ValueError):
                    storage.get_object(Bucket=h.BUCKET, Key=key)
                with self.assertRaises(ValueError):
                    storage.put_object(Bucket=h.BUCKET, Key=key, Body=b'{}')
            with self.assertRaises(ValueError):
                storage.put_object(Bucket=h.BUCKET, Key=h.PUBLISHED_KEY, Body=b'{}')
            with self.assertRaises(ValueError):
                storage.get_object(Bucket='unreviewed', Key=h.PUBLISHED_KEY)
            client.get_object.assert_not_called()
            client.put_object.assert_not_called()

    def test_config_preserves_native_allocation_and_requests_no_schedule_or_secret_copy(self):
        for name in HANDLERS:
            config = json.loads((ROOT / f'aws/lambdas/justhodl-{name}/config.json').read_bytes())
            normalized = normalize_config(config)
            self.assertEqual(config['memory'], 1024 if name == 'finra-short' else 512)
            self.assertEqual(config['timeout'], 300)
            self.assertNotIn('schedule', normalized)
            self.assertNotIn('eventbridge_scheduler', normalized)
            self.assertNotIn('inherit_env', normalized)
            self.assertIn('retained_legacy_schedule', config)
            self.assertLessEqual(len(config['description']), 256)


if __name__ == '__main__':
    unittest.main(verbosity=2)
