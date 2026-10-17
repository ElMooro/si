"""ops 6520 regressions for justhodl-event-coordinator: self-loop guard + audit storm guard.

Executes the real coordinator functions extracted by AST with fake clients; never contacts AWS.
"""
import ast
from datetime import datetime, timezone
import hashlib
import io
import json
from pathlib import Path
from types import SimpleNamespace
import unittest

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / 'aws/lambdas/justhodl-event-coordinator/source/lambda_function.py'
NAMES = {'_payload_hash', 'delivery_state', 'invoke_target', 'handler', '_engine_id', 'is_self_loop', 'write_audit'}


class NoSuchKey(Exception):
    pass


class FakeS3:
    def __init__(self, existing=None):
        self.existing = existing
        self.puts = []
        self.exceptions = SimpleNamespace(NoSuchKey=NoSuchKey)

    def get_object(self, Bucket, Key):
        if self.existing is None:
            raise NoSuchKey(Key)
        return {'Body': io.BytesIO(self.existing)}

    def put_object(self, **kw):
        self.puts.append(kw)


def load(scope):
    tree = ast.parse(SOURCE.read_text(encoding='utf-8'))
    nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in NAMES]
    assigns = [n for n in tree.body if isinstance(n, ast.Assign)
               and any(isinstance(t, ast.Name) and t.id == 'AUDIT_MAX_BYTES' for t in n.targets)]
    assert {n.name for n in nodes} == NAMES, {n.name for n in nodes}
    for node in nodes:
        node.decorator_list = []
    exec(compile(ast.Module(body=assigns + nodes, type_ignores=[]), str(SOURCE), 'exec'), scope)
    return scope


class LoopGuardTests(unittest.TestCase):
    def setUp(self):
        self.calls = []
        self.audits = []

        def invoke(**kw):
            self.calls.append(kw['FunctionName'])
            return {'StatusCode': 202}

        self.scope = load({'hashlib': hashlib, 'json': json, 'datetime': datetime, 'timezone': timezone,
                           'time': SimpleNamespace(time=lambda: 100.0), '_dedupe_cache': {}, 'DEDUPE_WINDOW_SEC': 60,
                           'governed_target': lambda fn: fn, 'lam': SimpleNamespace(invoke=invoke),
                           'BUCKET': 'b', 's3': FakeS3(),
                           'ROUTES': {'near_miss.extreme': {'invoke': ['justhodl-miss-calibrator', 'justhodl-other'],
                                                            'notify': False, 'audit': False}},
                           'send_telegram_alert': lambda *a: True,
                           'breaker_allow': lambda fn, ev, now=None: {'allow': True}})

    def event(self, producer='miss-calibrator', source='justhodl.miss-calibrator'):
        return {'id': 'e-1', 'detail-type': 'near_miss.extreme', 'source': source,
                'detail': {'signal_type': 'x', 'count': 99, '_source_engine': producer}}

    def test_engine_id_normalisation(self):
        eid = self.scope['_engine_id']
        self.assertEqual(eid('justhodl-miss-calibrator'), 'miss-calibrator')
        self.assertEqual(eid('justhodl.miss-calibrator'), 'miss-calibrator')
        self.assertEqual(eid('miss-calibrator'), 'miss-calibrator')
        self.assertEqual(eid(None), '')

    def test_self_loop_truth_table(self):
        loop = self.scope['is_self_loop']
        self.assertTrue(loop('justhodl-miss-calibrator', {'_source_engine': 'miss-calibrator'}))
        self.assertTrue(loop('justhodl-miss-calibrator', {}, 'justhodl.miss-calibrator'))
        self.assertFalse(loop('justhodl-other', {'_source_engine': 'miss-calibrator'}))
        self.assertFalse(loop('justhodl-miss-calibrator', {'_source_engine': '?'}))
        self.assertFalse(loop('justhodl-miss-calibrator', {}, None))

    def test_router_refuses_to_send_event_back_to_its_producer(self):
        res = self.scope['handler'](self.event(), None)
        body = json.loads(res['body'])
        self.assertEqual(self.calls, ['justhodl-other'])
        skipped = [i for i in body['invokes'] if i.get('skipped') == 'self_loop_guard']
        self.assertEqual([i['fn'] for i in skipped], ['justhodl-miss-calibrator'])
        self.assertTrue(body['ok'])

    def test_other_producers_still_route(self):
        self.scope['handler'](self.event(producer='miss-detector', source='justhodl.miss-detector'), None)
        self.assertEqual(self.calls, ['justhodl-miss-calibrator', 'justhodl-other'])

    def test_audit_appends_below_cap(self):
        self.scope['s3'] = FakeS3(existing=b'{"x":1}\n')
        self.scope['write_audit']('e', {'a': 1}, {'ok': True})
        self.assertEqual(len(self.scope['s3'].puts), 1)
        self.assertTrue(self.scope['s3'].puts[0]['Body'].startswith(b'{"x":1}\n'))

    def test_audit_stops_growing_at_cap(self):
        cap = self.scope['AUDIT_MAX_BYTES']
        self.scope['s3'] = FakeS3(existing=b'x' * cap)
        self.scope['write_audit']('e', {'a': 1}, {'ok': True})
        self.assertEqual(self.scope['s3'].puts, [])

    def test_audit_starts_a_new_day_log(self):
        self.scope['s3'] = FakeS3(existing=None)
        self.scope['write_audit']('e', {'a': 1}, {'ok': True})
        self.assertEqual(len(self.scope['s3'].puts), 1)


if __name__ == '__main__':
    unittest.main()
