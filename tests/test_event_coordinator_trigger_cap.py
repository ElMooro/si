"""The coordinator under the fleet-wide trigger cap (chain_guard v2), offline: the real chain_guard with a fake ledger,
the real coordinator handler / lineage_of / trigger_admit / invoke_target extracted from the source.

A routed invoke continues the publisher's lineage onto the target's payload; a chain at the depth cap is refused and
acknowledged (never retried); a loop back to an engine in the chain is refused and the governor is asked once to hold
both ends; a publisher without a lineage (not yet redeployed) gets a root lineage at itself; an unavailable ledger
refuses without holding."""
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
import ast
import hashlib
import io
import json
import os
import sys
import types
import unittest

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / 'aws/lambdas/justhodl-event-coordinator/source/lambda_function.py'
sys.path.insert(0, str(ROOT / 'tests'))
sys.path.insert(0, str(ROOT / 'aws' / 'shared'))
from test_chain_guard_trigger_cap import FakeDDB, FakeLambda, load_guard  # noqa: E402

FUNCS = {'_payload_hash', 'delivery_state', 'invoke_target', 'handler', '_engine_id', 'is_self_loop', 'lineage_of', 'trigger_admit'}


def load(scope):
    tree = ast.parse(SOURCE.read_text(encoding='utf-8'))
    nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in FUNCS]
    assert {n.name for n in nodes} == FUNCS, {n.name for n in nodes}
    for node in nodes:
        node.decorator_list = []
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(SOURCE), 'exec'), scope)
    return scope


class CoordinatorTriggerCapTests(unittest.TestCase):
    def setUp(self):
        os.environ['AWS_LAMBDA_FUNCTION_NAME'] = 'justhodl-event-coordinator'
        self.ddb, self.lam, self.gov = FakeDDB(), FakeLambda(), FakeLambda()
        self.cg = load_guard(self.ddb, self.lam, self.gov)
        self.audits = []
        self.scope = load({'hashlib': hashlib, 'json': json, 'datetime': datetime, 'timezone': timezone,
                           'time': SimpleNamespace(time=lambda: 100.0), '_dedupe_cache': {}, 'DEDUPE_WINDOW_SEC': 60,
                           'governed_target': lambda fn: fn, 'lam': self.lam, 'chain_guard': self.cg,
                           'ROUTES': {'outcome.resolved': {'invoke': ['justhodl-calibrator', 'justhodl-alpha-calibrator'], 'notify': False, 'audit': True},
                                      'calibrator.weights_updated': {'invoke': ['justhodl-alpha-compass'], 'notify': False, 'audit': False},
                                      'compass.read': {'invoke': ['justhodl-outcome-checker'], 'notify': False, 'audit': False}},
                           'send_telegram_alert': lambda *a: True, 'write_audit': lambda e, d, r: self.audits.append(r),
                           'breaker_allow': lambda fn, ev, now=None: {'allow': True}, 'print': lambda *a: None})

    def event(self, name, publisher, lineage=None, eid='e-1'):
        detail = {'x': 1, '_source_engine': publisher.replace('justhodl-', '')}
        if lineage:
            detail[self.cg.LINEAGE_KEY] = lineage
        return {'id': eid, 'detail-type': name, 'source': 'justhodl.' + publisher.replace('justhodl-', ''), 'detail': detail}

    def sent(self, i=-1):
        return json.loads(self.lam.invokes[i]['Payload'])

    def test_routed_invoke_continues_the_publisher_lineage(self):
        lin = self.cg.fresh('justhodl-outcome-checker')
        body = json.loads(self.scope['handler'](self.event('outcome.resolved', 'justhodl-outcome-checker', lin), None)['body'])
        self.assertTrue(body['ok'])
        self.assertEqual([k['FunctionName'] for k in self.lam.invokes], ['justhodl-calibrator', 'justhodl-alpha-calibrator'])
        p = self.sent(0)
        self.assertEqual((p['trigger_event'], p[self.cg.LINEAGE_KEY]['hop'], p[self.cg.LINEAGE_KEY]['path']),
                         ('outcome.resolved', 1, ['justhodl-outcome-checker', 'justhodl-calibrator']))
        self.assertEqual(body['chain']['id'], lin['id'])

    def test_publisher_without_lineage_gets_a_root_at_itself(self):
        body = json.loads(self.scope['handler'](self.event('outcome.resolved', 'justhodl-outcome-checker'), None)['body'])
        self.assertEqual(body['chain']['path'], ['justhodl-outcome-checker'])
        self.assertEqual(self.sent(0)[self.cg.LINEAGE_KEY]['path'], ['justhodl-outcome-checker', 'justhodl-calibrator'])

    def test_depth_cap_refuses_and_acknowledges(self):
        deep = {'id': 'd', 'root': 'justhodl-a', 'hop': 3, 'path': ['justhodl-a', 'justhodl-b', 'justhodl-c', 'justhodl-outcome-checker'], 'self_hop': 0, 'n': 3}
        res = self.scope['handler'](self.event('outcome.resolved', 'justhodl-outcome-checker', deep), None)
        body = json.loads(res['body'])
        self.assertEqual(res['statusCode'], 200)
        self.assertTrue(body['ok'])
        self.assertEqual(self.lam.invokes, [])
        self.assertEqual([i['skipped'] for i in body['invokes']], ['trigger_cap:max_hops', 'trigger_cap:max_hops'])
        self.assertEqual(body['invokes'][0]['trigger_cap']['hold'], ['justhodl-outcome-checker'])
        self.assertEqual(len(self.gov.invokes), 2)          # one breach record per (day, source, target, rule): two targets -> two records
        self.assertEqual(len([k for k in self.ddb.items if k.startswith('breach#')]), 2)

    def test_loop_back_into_the_chain_holds_both_ends(self):
        lin = {'id': 'L', 'root': 'justhodl-outcome-checker', 'hop': 2,
               'path': ['justhodl-outcome-checker', 'justhodl-calibrator', 'justhodl-alpha-compass'], 'self_hop': 0, 'n': 2}
        body = json.loads(self.scope['handler'](self.event('compass.read', 'justhodl-alpha-compass', lin), None)['body'])
        self.assertEqual(body['invokes'][0]['skipped'], 'trigger_cap:loop')
        self.assertEqual(body['invokes'][0]['trigger_cap']['hold'], ['justhodl-alpha-compass', 'justhodl-outcome-checker'])
        self.assertEqual(json.loads(self.gov.invokes[0]['Payload'])['action'], 'trigger_breach')

    def test_ledger_down_refuses_without_holding(self):
        self.ddb.down = True
        body = json.loads(self.scope['handler'](self.event('outcome.resolved', 'justhodl-outcome-checker'), None)['body'])
        self.assertEqual([i['skipped'] for i in body['invokes']], ['trigger_cap:ledger_unavailable'] * 2)
        self.assertEqual((self.lam.invokes, self.gov.invokes), ([], []))


if __name__ == '__main__':
    result = unittest.main(exit=False, verbosity=2).result
    sys.exit(0 if result.wasSuccessful() else 1)
