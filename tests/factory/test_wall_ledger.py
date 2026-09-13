"""Wall ledger + grader quota: costs scale with unfinalized entries, polls never burn verdict quota.

Reproduces the 2026-09-13 audit findings before the fix: one wall pass at full season load
(10 agents x 6 symbols x 13 weeks = 780 entries) made ~3,100 S3 requests and 780 grader calls
every five minutes (>40 s, ~900k S3 requests/day, the 50-verdict quota burned by pending polls).
"""
import io
import json
import sys
import time
import unittest
from datetime import timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'tests/factory'))
sys.path.insert(0, str(ROOT / 'aws/shared'))
sys.path.insert(0, str(ROOT / 'aws/lambdas/justhodl-student-rsi/source'))

import test_factory as base  # noqa: E402  (MemoryS3, module loader, fixtures)
from factory_core import digest, iso, labels_from_prices, make_season, score_prediction, timestamp, validate_prediction, week_window  # noqa: E402
from factory_store import Store  # noqa: E402
import student_lambda as student  # noqa: E402

SYMBOLS = ('SPY', 'QQQ', 'IWM', 'TLT', 'GLD', 'BTC')


class CountingS3(base.MemoryS3):
    def __init__(self):
        super().__init__()
        self.calls = {'get': 0, 'put': 0, 'list': 0}

    def get_object(self, *a, **k):
        self.calls['get'] += 1
        return super().get_object(*a, **k)

    def put_object(self, *a, **k):
        self.calls['put'] += 1
        return super().put_object(*a, **k)

    def list_objects_v2(self, *a, **k):
        self.calls['list'] += 1
        prefix = k.get('Prefix') or (a[1] if len(a) > 1 else '')
        if any(prefix.startswith(d) for d in getattr(self, 'denied_list', ())):
            raise base.CloudError('AccessDenied')
        return super().list_objects_v2(*a, **k)

    def reset(self):
        self.calls = {'get': 0, 'put': 0, 'list': 0}


class WallFixture(unittest.TestCase):
    agents = ('student',) + tuple('agent%d' % i for i in range(9))

    def setUp(self):
        self.now = base.datetime(2026, 9, 13, 15, tzinfo=base.timezone.utc)
        self.cloud = CountingS3()
        self.store = Store(self.cloud, 'private', 'public', lambda: self.now)
        self.season = make_season(self.now)
        self.season.update(calendar_review_required=False, price_sources={s: 'official-consolidated:' + s for s in SYMBOLS})
        self.season.pop('policy_hash')
        self.season['policy_hash'] = digest(self.season)
        self.policy = {'enabled': True, 'max_grades_per_day': 50, 'max_experiments_per_day': 1, 'max_tick_seconds': 40,
                       'max_guest_traces_per_day': 10, 'sense_interval_seconds': 900, 'objective': 'x', 'generative_status': 'blocked'}
        self.store.immutable('private', 'factory/control/policy.json', self.policy)
        self.store.immutable('private', 'factory/control/season.json', self.season)
        self.store.immutable('private', 'factory/control/invites.json',
                             {'allowlist': [{'uid': 'u%d' % i, 'agent': a, 'enabled': True} for i, a in enumerate(self.agents)], 'capacity': 10})
        self.first = base.datetime.fromisoformat(self.season['starts_on']).date()
        self.grader_calls = []
        self.state = student.initial_state(self.now, self.season, self.policy)
        self.store.acquire()

    def week(self, i):
        return (self.first + timedelta(weeks=i)).isoformat()

    def fill(self, weeks):
        n = 0
        for w in range(weeks):
            monday = self.week(w)
            at = timestamp(week_window(monday, self.season)['opens_at']) + timedelta(minutes=1)
            for a in self.agents:
                for s in SYMBOLS:
                    p = {'id': '%s-%s-%s' % (monday, a, s), 'week': monday, 'symbol': s, 'direction': 'UP', 'regime': 'TREND',
                         'crisis_probability': .1, 'direction_probabilities': {'DOWN': .25, 'FLAT': .25, 'UP': .5},
                         'regime_probabilities': {'RANGE': .25, 'TRANSITION': .25, 'TREND': .5},
                         'data_cutoff': iso(at - timedelta(hours=20)), 'model_revision': 'r', 'price_source': 'official-consolidated:' + s}
                    self.store.immutable('private', 'factory/salon/accepted/' + p['id'] + '.json', validate_prediction(p, self.season, at, a))
                    n += 1
        return n

    def after_close(self, week_i):
        self.now = timestamp(week_window(self.week(week_i), self.season)['grade_after']) + timedelta(hours=1)
        self.store.lease = None          # the clock jumped weeks; take a fresh 150 s lease at the new time
        self.store.acquire()

    def pending_lam(self):
        calls = self.grader_calls

        class Lam:
            def invoke(self, FunctionName, InvocationType, Payload):
                calls.append(json.loads(Payload)['id'])
                return {'Payload': io.BytesIO(json.dumps({'ok': False, 'status': 'pending', 'reason': 'official_prints_unavailable'}).encode())}
        return Lam()

    def grading_lam(self):
        fixture = self

        class Lam:
            def invoke(self, FunctionName, InvocationType, Payload):
                eid = json.loads(Payload)['id']
                fixture.grader_calls.append(eid)
                pred = json.loads(fixture.cloud.rows[('private', 'factory/salon/accepted/' + eid + '.json')])
                labels = labels_from_prices(pred['symbol'], 100.0, [101, 102, 103, 104, 105], fixture.season)
                res = {'status': 'graded', 'ok': True, 'labels': labels, 'metrics': score_prediction(pred, labels, fixture.season), 'id': eid,
                       'agent': pred['agent'], 'week': pred['week'], 'symbol': pred['symbol'], 'graded_at': iso(fixture.now)}
                fixture.store.immutable('private', 'factory/salon/results/' + eid + '.json', res)
                return {'Payload': io.BytesIO(json.dumps(res).encode())}
        return Lam()

    def prints(self, weeks):
        for w in range(weeks):
            for s in SYMBOLS:
                self.store.immutable('private', 'factory/official-prints/%s/%s.json' % (self.week(w), s), {'verified_by': 'owner_runner'})


class WallLedgerTests(WallFixture):
    def test_steady_state_pass_is_bounded_and_never_polls_the_grader_before_prints(self):
        n = self.fill(13)
        self.after_close(12)
        student.market_wall(self.store, self.pending_lam(), self.state, self.now, time.monotonic() + 40)
        self.assertEqual(self.state['wall']['entries'], n)
        self.assertEqual(self.grader_calls, [])
        self.cloud.reset()
        student.market_wall(self.store, self.pending_lam(), self.state, self.now, time.monotonic() + 40)
        total = sum(self.cloud.calls.values())
        self.assertLess(total, 120, self.cloud.calls)          # was ~3,100 per pass before the ledger
        self.assertEqual(self.cloud.calls['put'], 0)
        self.assertEqual(self.grader_calls, [])
        self.assertTrue(self.state['wall']['pass_complete'])
        self.assertEqual(self.state['wall']['pending'], n)

    def test_prints_gate_grading_once_and_finalized_weeks_are_never_listed_again(self):
        n = self.fill(2)
        self.after_close(1)
        student.market_wall(self.store, self.pending_lam(), self.state, self.now, time.monotonic() + 40)
        self.prints(2)
        self.grader_calls.clear()
        student.market_wall(self.store, self.grading_lam(), self.state, self.now, time.monotonic() + 40)
        self.assertEqual(len(self.grader_calls), n)
        self.assertEqual(len(set(self.grader_calls)), n)
        self.assertEqual(self.state['wall']['final_weeks'], 2)
        self.assertEqual(self.state['wall']['pending'], 0)
        ledger = json.loads(self.cloud.rows[('private', student.LEDGER_KEY)])
        self.assertEqual(sorted(ledger['final_weeks']), [self.week(0), self.week(1)])
        self.cloud.reset()
        self.grader_calls.clear()
        student.market_wall(self.store, self.grading_lam(), self.state, self.now, time.monotonic() + 40)
        self.assertEqual(self.cloud.calls['list'], 0)
        self.assertEqual(self.grader_calls, [])
        scoreboard = json.loads(self.cloud.rows[('public', 'factory/scoreboard.json')])
        self.assertEqual(scoreboard['independent_weeks'], 2)
        self.assertEqual(len(scoreboard['rows']), len(self.agents))
        wall = [json.loads(line) for line in self.cloud.rows[('public', 'factory/salon/wall.jsonl')].splitlines()]
        self.assertEqual(len(wall), 2 * n)
        self.assertEqual(len({e['id'] for e in wall}), 2 * n)

    def test_grader_is_polled_at_most_hourly_when_prints_cannot_be_read(self):
        self.fill(1)
        self.after_close(0)
        # The student role has neither GetObject nor a prefix-scoped ListBucket on official prints
        # (ops 5507): both come back AccessDenied, which the store surfaces as an exception.
        for s in SYMBOLS:
            self.cloud.denied_get.add(('private', 'factory/official-prints/%s/%s.json' % (self.week(0), s)))
        self.cloud.denied_list = ('factory/official-prints/',)
        student.market_wall(self.store, self.pending_lam(), self.state, self.now, time.monotonic() + 40)
        first = len(self.grader_calls)
        self.assertEqual(first, len(SYMBOLS))                      # one probe per week/symbol, not one per entry
        student.market_wall(self.store, self.pending_lam(), self.state, self.now + timedelta(minutes=5), time.monotonic() + 40)
        self.assertEqual(len(self.grader_calls), first)             # backoff: no second poll five minutes later
        student.market_wall(self.store, self.pending_lam(), self.state, self.now + timedelta(hours=1, minutes=1), time.monotonic() + 40)
        self.assertEqual(len(self.grader_calls), 2 * first)

    def test_deadline_stops_the_pass_and_the_next_pass_resumes(self):
        n = self.fill(3)
        self.after_close(2)
        student.market_wall(self.store, self.pending_lam(), self.state, self.now, time.monotonic() - 1)
        self.assertFalse(self.state['wall']['pass_complete'])
        self.assertLess(self.state['wall']['entries'], n)
        student.market_wall(self.store, self.pending_lam(), self.state, self.now, time.monotonic() + 40)
        self.assertTrue(self.state['wall']['pass_complete'])
        self.assertEqual(self.state['wall']['entries'], n)

    def test_lost_ledger_costs_one_rescan_not_history(self):
        n = self.fill(1)
        self.after_close(0)
        self.prints(1)
        student.market_wall(self.store, self.grading_lam(), self.state, self.now, time.monotonic() + 40)
        events_before = {k for b, k in self.cloud.rows if k.startswith('factory/salon/events/')}
        del self.cloud.rows[('private', student.LEDGER_KEY)]
        self.grader_calls.clear()
        self.cloud.reset()
        student.market_wall(self.store, self.grading_lam(), self.state, self.now, time.monotonic() + 40)
        self.assertEqual(self.grader_calls, [])                    # results exist; the grader is not asked again
        self.assertEqual({k for b, k in self.cloud.rows if k.startswith('factory/salon/events/')}, events_before)
        self.assertEqual(self.state['wall']['graded'], n)

    def test_denied_prints_read_still_grades_the_whole_week_once_the_probe_returns_a_verdict(self):
        n = self.fill(1)
        self.after_close(0)
        for s in SYMBOLS:
            self.cloud.denied_get.add(('private', 'factory/official-prints/%s/%s.json' % (self.week(0), s)))
        self.cloud.denied_list = ('factory/official-prints/',)
        self.prints(1)
        student.market_wall(self.store, self.grading_lam(), self.state, self.now, time.monotonic() + 40)
        self.assertEqual(len(self.grader_calls), n)
        self.assertEqual(self.state['wall']['pending'], 0)
        self.assertEqual(self.state['wall']['final_weeks'], 1)


class GraderQuotaTests(WallFixture):
    def setUp(self):
        super().setUp()
        self.grader = base.module('factory_quota_grader', 'aws/lambdas/justhodl-factory-grader/source/lambda_function.py')

    def count(self):
        row, _ = self.store.read('private', self.grader.quota_key(self.store))
        return (row or {}).get('count', 0)

    def test_pending_market_polls_are_free_and_terminal_verdicts_count(self):
        self.fill(1)
        eid = '%s-student-SPY' % self.week(0)
        self.assertEqual(self.grader.handle({'kind': 'market', 'id': eid}, self.store)['status'], 'pending')   # week open
        self.after_close(0)
        for _ in range(60):
            self.assertEqual(self.grader.handle({'kind': 'market', 'id': eid}, self.store)['reason'], 'official_prints_unavailable')
        self.assertEqual(self.count(), 0)
        prints = {'source': 'official-consolidated:SPY', 'source_url': 'https://example.test/x', 'raw_sha256': 'a' * 64, 'verified_by': 'owner_runner',
                  'window': None, 'opening': 100.0, 'closes': [101, 102, 103, 104, 105], 'sessions': None, 'corporate_action': False,
                  'available_at': iso(self.now - timedelta(minutes=1))}
        accepted, _ = self.store.read('private', 'factory/salon/accepted/' + eid + '.json')
        prints.update(window=accepted['window'], sessions=accepted['window']['sessions'], closes=[101, 102, 103, 104, 105][:len(accepted['window']['sessions'])])
        self.store.immutable('private', 'factory/official-prints/%s/SPY.json' % self.week(0), prints)
        verdict = self.grader.handle({'kind': 'market', 'id': eid}, self.store)
        self.assertEqual(verdict['status'], 'graded')
        self.assertEqual(self.count(), 1)
        self.assertEqual(self.grader.handle({'kind': 'market', 'id': eid}, self.store)['status'], 'graded')   # immutable repeat
        self.assertEqual(self.count(), 2)

    def test_limit_still_refuses_before_work(self):
        self.fill(1)
        self.store.put('private', self.grader.quota_key(self.store), {'count': 50, 'day': 'x'}, absent=True)
        with self.assertRaises(self.grader.Invalid):
            self.grader.handle({'kind': 'market', 'id': '%s-student-SPY' % self.week(0)}, self.store)


if __name__ == '__main__':
    unittest.main(verbosity=2)
