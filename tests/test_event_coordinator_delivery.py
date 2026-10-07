"""Execute actual coordinator functions with fake targets; never contact AWS."""
import ast
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
import unittest

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / 'aws/lambdas/justhodl-event-coordinator/source/lambda_function.py'


class DeliveryTests(unittest.TestCase):
    def setUp(self):
        self.now = 100.0
        self.calls, self.notifications, self.audits = [], [], []
        self.failures = {'target-b': 1}
        self.status = 202
        def invoke(**kwargs):
            fn = kwargs['FunctionName']
            self.calls.append(fn)
            if self.failures.get(fn, 0):
                self.failures[fn] -= 1
                raise RuntimeError('simulated temporary failure')
            return {'StatusCode':self.status} if self.status is not None else {}
        self.scope = {'hashlib':hashlib, 'json':json, 'datetime':datetime, 'timezone':timezone,
            'time':SimpleNamespace(time=lambda:self.now), '_dedupe_cache':{}, 'DEDUPE_WINDOW_SEC':60,
            'governed_target':lambda fn:fn, 'lam':SimpleNamespace(invoke=invoke),
            'ROUTES':{'regime.changed':{'invoke':['target-a','target-b'],'notify':True,'audit':True}},
            'send_telegram_alert':lambda *args:self.notifications.append(args) or True,
            'write_audit':lambda *args:self.audits.append(args)}
        names={'_payload_hash','delivery_state','invoke_target','handler'}
        tree=ast.parse(SOURCE.read_text(encoding='utf-8'))
        nodes=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in names]
        self.assertEqual({n.name for n in nodes}, names)
        for node in nodes:node.decorator_list=[]
        exec(compile(ast.Module(body=nodes,type_ignores=[]),str(SOURCE),'exec'),self.scope)
        self.event={'id':'broker-1','detail-type':'regime.changed','source':'justhodl.example',
                    'detail':{'previous':'A','current':'B','_event_id':'producer-event-1'}}

    def handle(self, event=None):return json.loads(self.scope['handler'](event or self.event,None)['body'])

    def test_failed_target_raises_instead_of_acknowledging_event(self):
        with self.assertRaisesRegex(RuntimeError,'target-b'):self.handle()
        self.assertEqual(self.notifications, [])
        self.assertFalse(self.audits[-1][2]['ok'])

    def test_retry_only_failed_target_then_marks_complete(self):
        with self.assertRaises(RuntimeError):self.handle()
        self.assertTrue(self.handle()['ok'])
        self.assertTrue(self.handle()['deduped'])
        self.assertEqual(Counter(self.calls), {'target-a':1,'target-b':2})
        self.assertEqual(len(self.notifications), 1)

    def test_republished_stable_id_deduplicates_across_broker_ids(self):
        self.failures={};self.handle()
        replay={**self.event,'id':'broker-2','detail':{**self.event['detail'],'_emitted_at':'later'}}
        self.assertTrue(self.handle(replay)['deduped'])
        self.assertEqual(len(self.calls), 2)

    def test_different_producers_or_stable_ids_are_not_coalesced(self):
        self.failures={};self.handle()
        self.handle({**self.event,'source':'justhodl.other'})
        self.handle({**self.event,'detail':{**self.event['detail'],'_event_id':'producer-event-2'}})
        self.assertEqual(len(self.calls), 6)

    def test_legacy_events_use_broker_identity(self):
        self.failures={}
        event={**self.event,'detail':{'value':1}}
        self.handle(event)
        self.assertTrue(self.handle(event)['deduped'])
        self.handle({**event,'id':'broker-2'})
        self.assertEqual(len(self.calls), 4)

    def test_missing_or_non_202_acknowledgement_is_failure(self):
        self.failures={}
        for value in (None,200,204,429,500):
            self.status=value;self.scope['_dedupe_cache'].clear()
            with self.subTest(status=value), self.assertRaises(RuntimeError):self.handle()
        self.assertEqual(self.notifications, [])

    def test_cold_retry_can_duplicate_acceptance_but_never_drop_failed_target(self):
        with self.assertRaises(RuntimeError):self.handle()
        self.scope['_dedupe_cache'].clear()
        self.assertTrue(self.handle()['ok'])
        self.assertEqual(Counter(self.calls), {'target-a':2,'target-b':2})

    def test_expired_state_allows_retry_and_does_not_claim_exactly_once(self):
        with self.assertRaises(RuntimeError):self.handle()
        self.now+=61
        result=self.handle()
        self.assertEqual(result['delivery_basis'],'async_queue_acceptance_only')
        self.assertEqual(Counter(self.calls), {'target-a':2,'target-b':2})

    def test_unrouted_event_has_no_side_effect_or_cache_entry(self):
        self.assertTrue(self.handle({**self.event,'detail-type':'unknown'})['unrouted'])
        self.assertEqual(self.calls, [])
        self.assertEqual(self.scope['_dedupe_cache'], {})

    def test_existing_no_notify_route_remains_silent(self):
        self.failures={};self.scope['ROUTES']['regime.changed']['notify']=False
        self.handle();self.assertEqual(self.notifications, [])

    def test_cache_has_hard_capacity_bound(self):
        self.scope['_dedupe_cache'].update({str(i):{'at':self.now,'accepted':set(),'complete':False} for i in range(4096)})
        self.scope['delivery_state']('new',{},'source','new')
        self.assertEqual(len(self.scope['_dedupe_cache']),4096)


if __name__ == '__main__':unittest.main()
