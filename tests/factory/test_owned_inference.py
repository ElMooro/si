"""Owned-model chat route: Qwen chat template, async submit/resolve, routing and origin labels."""
import importlib.util
import json
import sys
import types
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'aws/shared'))
sys.path.insert(0, str(ROOT / 'aws/lambdas/justhodl-ai/source'))
sys.path.insert(0, str(ROOT / 'tests/factory'))
from test_factory import MemoryS3  # noqa: E402
from factory_store import Store  # noqa: E402
import factory_inference as fi  # noqa: E402


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, str(ROOT / path))
    loaded = importlib.util.module_from_spec(spec); spec.loader.exec_module(loaded)
    return loaded


class OwnedInferenceTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 9, 14, 12, tzinfo=timezone.utc)
        self.cloud = MemoryS3()
        self.store = Store(self.cloud, 'private', 'public', lambda: self.now)
        self.control = {'enabled': True, 'endpoint_name': 'jh-owned-coder-async', 'model_id': 'qwen2-5-coder-7b-instruct', 'revision': 'c03e6d358207e414', 'adapter_generation': 'base', 'max_new_tokens': 300}
        self.cloud.rows[('private', fi.CONTROL_KEY)] = json.dumps(self.control).encode()

    def test_prompt_is_chatml_with_owner_turns_only(self):
        p = fi.qwen_chat_prompt('SYS', [('owner', 'hi'), ('agent', 'ignored?')], 'can you code?')
        self.assertTrue(p.startswith('<|im_start|>system\nSYS<|im_end|>'))
        self.assertIn('<|im_start|>user\nhi<|im_end|>', p)
        self.assertTrue(p.endswith('<|im_start|>user\ncan you code?<|im_end|>\n<|im_start|>assistant\n'))

    def test_submit_and_resolve(self):
        calls = []
        rt = types.SimpleNamespace(invoke_endpoint_async=lambda **kw: calls.append(kw) or {'OutputLocation': 's3://private/factory/inference/outputs/%s.out' % kw['InferenceId'],
                                                                                             'FailureLocation': 's3://private/factory/inference/outputs/%s.failure' % kw['InferenceId']})
        history = [{'role': 'owner', 'text': 'earlier'}, {'role': 'agent', 'text': 'INSIDE — Brain note', 'model': 'brain+look'},
                   {'role': 'agent', 'text': 'prior model answer', 'model': 'owned:qwen2-5-coder-7b-instruct@c03e6d358207'}]
        pending = fi.submit(self.store, rt, self.control, 'khalid', 'can you code?', history)
        self.assertEqual(pending['state'], 'queued')
        self.assertEqual(pending['origin'], 'owned:qwen2-5-coder-7b-instruct@c03e6d358207')
        req = json.loads(self.cloud.rows[('private', fi.REQ_PREFIX + pending['id'] + '.json')])
        self.assertIn('<|im_start|>assistant\nprior model answer<|im_end|>', req['body']['inputs'])
        self.assertNotIn('Brain note', req['body']['inputs'])                       # F05 discipline holds on this route too
        self.assertEqual(calls[0]['EndpointName'], 'jh-owned-coder-async')
        self.assertEqual(fi.resolve(self.store, pending)[0], 'queued')
        self.cloud.rows[('private', 'factory/inference/outputs/%s.out' % pending['id'])] = json.dumps({'generated_text': 'Yes. def add(a, b):\n    return a + b<|im_end|>trailing'}).encode()
        state, text = fi.resolve(self.store, pending)
        self.assertEqual((state, text), ('done', 'Yes. def add(a, b):\n    return a + b'))

    def test_chat_routes_to_owned_model_and_settles_on_poll(self):
        gw = module('gateway_owned', 'aws/lambdas/justhodl-ai/source/factory_gateway.py')
        calls = []
        gw.boto3 = types.SimpleNamespace(client=lambda *a, **k: types.SimpleNamespace(invoke_endpoint_async=lambda **kw: calls.append(kw) or {
            'OutputLocation': 's3://private/factory/inference/outputs/%s.out' % kw['InferenceId'], 'FailureLocation': 's3://private/factory/inference/outputs/%s.failure' % kw['InferenceId']}))
        gw.drain_fleet = lambda store: {}
        gw.ranks_view = lambda store: {}
        self.cloud.rows[('public', 'data/student-state.json')] = b'{}'
        out = gw.chat_post(self.store, 'khalid', True, {'text': 'can you code? write a fizzbuzz'}, {})
        self.assertEqual(out['model'], 'owned:queued')
        self.assertEqual(len(calls), 1)
        rid = out['messages'][-2]['pending']['id']
        # status stays behind an explicit request
        out2 = gw.chat_post(self.store, 'khalid', True, {'text': 'status'}, {})
        self.assertEqual(out2['model'], 'factory-status:objects')
        # the answer lands on the next poll, labelled with its origin
        self.cloud.rows[('private', 'factory/inference/outputs/%s.out' % rid)] = json.dumps({'generated_text': 'for i in range(1, 101): ...'}).encode()
        snap = gw.chat_snapshot(self.store, 'khalid', True)
        last = snap['messages'][-1]
        self.assertEqual((last['model'], last['request']), ('owned:qwen2-5-coder-7b-instruct@c03e6d358207', rid))
        self.assertIn('range(1, 101)', last['text'])
        # disabled control -> honest not-connected, no canned answer, no call
        self.cloud.rows[('private', fi.CONTROL_KEY)] = json.dumps({**self.control, 'enabled': False}).encode()
        out3 = gw.chat_post(self.store, 'khalid', True, {'text': 'explain generators'}, {})
        self.assertEqual(out3['model'], 'owned:not-connected')
        self.assertEqual(len(calls), 1)


if __name__ == '__main__':
    unittest.main(verbosity=2)
