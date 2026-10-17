"""Circuit-breaker regressions for justhodl-event-coordinator (2026-10-17, after the Oct 12-16 loop).

Executes the real coordinator functions extracted by AST against an in-memory DynamoDB that honours the
conditional ADD the breaker relies on; never contacts AWS.
"""
import ast
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
import unittest

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / 'aws/lambdas/justhodl-event-coordinator/source/lambda_function.py'
FUNCS = {'_payload_hash', 'delivery_state', 'invoke_target', 'handler', '_engine_id', 'is_self_loop',
         '_breaker_take', 'breaker_allow'}
CONSTS = {'BREAKER_DEFAULT_PER_HOUR', 'BREAKER_PER_HOUR', 'BREAKER_GLOBAL_PER_HOUR', 'BREAKER_TTL_S'}


class ClientError(Exception):
    def __init__(self, code):
        super().__init__(code)
        self.response = {'Error': {'Code': code}}


class FakeDDB:
    """Conditional ADD semantics of the breaker's update_item, per partition key."""

    def __init__(self, fail=None):
        self.items, self.calls, self.fail = {}, [], fail

    def update_item(self, TableName, Key, UpdateExpression, ConditionExpression, ExpressionAttributeValues):
        self.calls.append(Key['pk']['S'])
        if self.fail:
            raise ClientError(self.fail)
        assert UpdateExpression.startswith('SET expires_at') and 'ADD n :one' in UpdateExpression
        n = self.items.get(Key['pk']['S'], {}).get('n')
        if n is not None and not n < int(ExpressionAttributeValues[':limit']['N']):
            raise ClientError('ConditionalCheckFailedException')
        item = self.items.setdefault(Key['pk']['S'], {'expires_at': int(ExpressionAttributeValues[':exp']['N'])})
        item['n'] = (n or 0) + 1


def load(scope):
    tree = ast.parse(SOURCE.read_text(encoding='utf-8'))
    nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in FUNCS]
    consts = [n for n in tree.body if isinstance(n, ast.Assign)
              and any(isinstance(t, ast.Name) and t.id in CONSTS for t in n.targets)]
    assert {n.name for n in nodes} == FUNCS, {n.name for n in nodes}
    assert len(consts) == len(CONSTS), [ast.dump(c.targets[0]) for c in consts]
    for node in nodes:
        node.decorator_list = []
    exec(compile(ast.Module(body=consts + nodes, type_ignores=[]), str(SOURCE), 'exec'), scope)
    return scope


class BreakerTests(unittest.TestCase):
    NOW = datetime(2026, 11, 2, 9, 15, tzinfo=timezone.utc)

    def setUp(self):
        self.calls, self.printed = [], []

        def invoke(**kw):
            self.calls.append(kw['FunctionName'])
            return {'StatusCode': 202}

        self.ddb = FakeDDB()
        self.scope = load({
            'hashlib': hashlib, 'json': json, 'datetime': datetime, 'timezone': timezone,
            'time': SimpleNamespace(time=lambda: 100.0), '_dedupe_cache': {}, 'DEDUPE_WINDOW_SEC': 60,
            'governed_target': lambda fn: fn, 'lam': SimpleNamespace(invoke=invoke), 'ddb': self.ddb,
            'BREAKER_TABLE': 'justhodl-event-breaker', '_breaker_logged': set(), 'ClientError': ClientError,
            'print': lambda *a: self.printed.append(' '.join(str(x) for x in a)),
            'write_audit': lambda *a: None, 'send_telegram_alert': lambda *a: True,
            'ROUTES': {'near_miss.extreme': {'invoke': ['justhodl-miss-calibrator'], 'notify': False, 'audit': True},
                       'regime.changed': {'invoke': ['justhodl-alpha-compass'], 'notify': False, 'audit': False}}})

    def allow(self, target='justhodl-x', now=None):
        return self.scope['breaker_allow'](target, 'regime.changed', now or self.NOW)

    def test_budget_per_target_then_open_for_the_rest_of_the_hour(self):
        limit = self.scope['BREAKER_DEFAULT_PER_HOUR']
        verdicts = [self.allow()['allow'] for _ in range(limit + 5)]
        self.assertEqual(verdicts.count(True), limit)
        self.assertFalse(verdicts[limit])
        self.assertEqual(self.ddb.items['justhodl-x#2026110209']['n'], limit)

    def test_new_hour_new_budget(self):
        for _ in range(self.scope['BREAKER_DEFAULT_PER_HOUR']):
            self.allow()
        self.assertFalse(self.allow()['allow'])
        self.assertTrue(self.allow(now=self.NOW.replace(hour=10))['allow'])

    def test_override_budgets(self):
        per = self.scope['BREAKER_PER_HOUR']
        self.assertEqual(per['justhodl-miss-calibrator'], 6)
        ok = [self.allow('justhodl-miss-calibrator')['allow'] for _ in range(10)]
        self.assertEqual(ok.count(True), 6)

    def test_global_budget_caps_many_targets(self):
        g = self.scope['BREAKER_GLOBAL_PER_HOUR']
        allowed = sum(self.allow('justhodl-t%d' % (i % 40))['allow'] for i in range(g + 50))
        self.assertEqual(allowed, g)

    def test_fail_closed_when_the_table_is_unavailable(self):
        self.ddb.fail = 'ResourceNotFoundException'
        v = self.allow()
        self.assertFalse(v['allow'])
        self.assertEqual(v['reason'], 'breaker_unavailable')
        self.assertTrue(any('BREAKER_UNAVAILABLE' in p for p in self.printed))

    def test_open_breaker_logs_once_per_target_hour(self):
        for _ in range(self.scope['BREAKER_DEFAULT_PER_HOUR'] + 20):
            self.allow()
        self.assertEqual(sum('BREAKER_OPEN' in p for p in self.printed), 1)

    def test_handler_skips_and_acknowledges_when_open(self):
        """The Oct-12 shape: an event whose route target is over budget is acknowledged, never invoked or retried."""
        event = {'id': 'e', 'detail-type': 'regime.changed', 'source': 'justhodl.market-regime',
                 'detail': {'current': 'B', '_source_engine': 'market-regime'}}
        budget = self.scope['BREAKER_PER_HOUR']['justhodl-alpha-compass']
        for i in range(budget + 3):
            ev = dict(event, id='e%d' % i)
            ev['detail'] = dict(event['detail'], n=i)
            body = json.loads(self.scope['handler'](ev, None)['body'])
            self.assertTrue(body['ok'])
        self.assertEqual(len(self.calls), budget)
        self.assertEqual(body['invokes'][0]['skipped'], 'breaker_open')

    def test_storm_is_bounded(self):
        """1,000 deliveries of a looping event in one hour start at most the target's budget of runs."""
        for i in range(1000):
            self.scope['handler']({'id': 'n%d' % i, 'detail-type': 'near_miss.extreme', 'source': 'justhodl.x',
                                   'detail': {'signal_type': 's', 'count': i, '_source_engine': 'x'}}, None)
        self.assertEqual(len(self.calls), self.scope['BREAKER_PER_HOUR']['justhodl-miss-calibrator'])


if __name__ == '__main__':
    unittest.main()
