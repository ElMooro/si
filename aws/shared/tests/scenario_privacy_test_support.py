"""Real calculator handlers with in-memory S3; never contacts cloud/providers."""
import contextlib
import importlib.util
import io
import json
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[3]
MARKER = {'schema_version': 'public-default-scenario.v1', 'scope': 'PUBLIC_DEFAULT_MODEL', 'contains_caller_inputs': False}


class MemoryS3:
    def __init__(self):
        self.reads, self.writes = [], []

    def get_object(self, **kwargs):
        self.reads.append(kwargs)
        key = kwargs['Key']
        if key == 'data/forward-returns.json':
            doc = {'generated_at': '2026-09-09T00:00:00Z', 'assets': {'SPY': {'forward_er_10y_pct': 6, 'risk': {'vol_pct_annualized': 15}}}}
        else:
            doc = {'open_positions': []}
        return {'Body': io.BytesIO(json.dumps(doc).encode())}

    def put_object(self, **kwargs):
        self.writes.append(kwargs)
        return {}


def load_engine(name, memory):
    fake = types.ModuleType('boto3')
    fake.client = lambda *args, **kwargs: memory
    path = ROOT / 'aws/lambdas' / name / 'source/lambda_function.py'
    spec = importlib.util.spec_from_file_location(name.replace('-', '_') + '_privacy_test', path)
    mod = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, {'boto3': fake}):
        spec.loader.exec_module(mod)
    if hasattr(mod, 'monte_carlo'):
        # Exercise the real numeric path at a small sample size; this test's
        # target is handler publication/auth transport, not MC convergence.
        original = mod.monte_carlo
        def bounded(*args, **kwargs):
            kwargs['n_sims'] = 8
            return original(*args, **kwargs)
        mod.monte_carlo = bounded
    return mod


def run(name):
    class HandlerPrivacy(unittest.TestCase):
        def setUp(self):
            self.memory = MemoryS3()
            self.engine = load_engine(name, self.memory)
            self.personal = {'current_nav': 7654321, 'annual_savings': 67891, 'annual_spending': 54321,
                             'age': 44, 'agi': 876543, 'state_rate': 9, 'federal_bracket': 32}
            self.field = 'current_nav' if name.endswith('wealth-plan') else 'agi'

        def invoke(self, event):
            logs = io.StringIO()
            with contextlib.redirect_stdout(logs), contextlib.redirect_stderr(logs):
                result = self.engine.lambda_handler(event, None)
            self.assertEqual(logs.getvalue(), '', 'handler must not log inputs or results')
            self.assertEqual(result['statusCode'], 200)
            self.assertEqual(result['headers']['Cache-Control'], 'private, no-store')
            return result, json.loads(result['body'])

        def check_personal(self, event):
            _, doc = self.invoke(event)
            self.assertEqual(doc['inputs'][self.field], self.personal[self.field])
            self.assertNotIn('publication', doc)
            self.assertEqual(self.memory.writes, [])
            self.assertTrue(self.memory.reads, 'actual handler must read model inputs')

        def test_get_personal_response_is_stateless(self):
            self.check_personal({'requestContext': {'http': {'method': 'GET'}}, 'queryStringParameters': self.personal})

        def test_post_personal_and_scheduled_spoof_cannot_publish(self):
            body = {**self.personal, 'source': 'aws.events', 'scheduled': True, 'publication': MARKER}
            self.check_personal({'requestContext': {'http': {'method': 'POST'}}, 'source': 'aws.events',
                                 'scheduled': True, 'body': json.dumps(body)})

        def test_v1_headers_http_cannot_become_scheduled(self):
            self.check_personal({'headers': {}, 'httpMethod': 'POST', 'source': 'aws.events',
                                 'body': json.dumps(self.personal)})

        def test_options_returns_before_any_model_read(self):
            for event in [{'httpMethod': 'OPTIONS'}, {'requestContext': {'http': {'method': 'OPTIONS'}}}]:
                response, doc = self.invoke(event)
                self.assertTrue(doc['ok'])
                self.assertIn('POST', response['headers']['Access-Control-Allow-Methods'])
                self.assertEqual(response['headers']['Access-Control-Allow-Headers'], 'Content-Type')
            self.assertEqual(self.memory.reads, [])
            self.assertEqual(self.memory.writes, [])

        def test_trusted_default_ignores_every_supplied_scenario(self):
            _, doc = self.invoke({'source': 'aws.events', 'body': json.dumps(self.personal),
                                  'queryStringParameters': self.personal})
            expected = self.engine.parse_inputs({}) if hasattr(self.engine, 'parse_inputs') else self.engine.parse_event({})
            self.assertEqual(doc['inputs'], expected)
            self.assertEqual(doc['publication'], MARKER)
            self.assertEqual(len(self.memory.writes), 1)
            write = self.memory.writes[0]
            self.assertEqual(write['Key'], self.engine.OUT_KEY)
            self.assertEqual(json.loads(write['Body']), doc)
            self.assertNotEqual(doc['inputs'][self.field], self.personal[self.field])

        def test_http_defaults_still_never_publish(self):
            _, doc = self.invoke({'rawPath': '/', 'body': '{}', 'scheduled': True})
            self.assertNotIn('publication', doc)
            self.assertEqual(self.memory.writes, [])

    suite = unittest.defaultTestLoader.loadTestsFromTestCase(HandlerPrivacy)
    return unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful()
