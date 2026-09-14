"""Chain of command in the tick, the evidence contract, and runner-owned official prints."""
import copy
import gzip
import hashlib
import importlib.util
import io
import json
import sys
import unittest
from datetime import datetime, time, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'aws/shared'))
sys.path.insert(0, str(ROOT / 'aws/lambdas/justhodl-student-rsi/source'))
sys.path.insert(0, str(ROOT / 'aws/lambdas/justhodl-ai/source'))
sys.path.insert(0, str(ROOT / 'scripts'))
sys.path.insert(0, str(ROOT / 'tests/factory'))

from test_factory import MemoryS3  # noqa: E402  (the same in-memory cloud as the factory suite)
from factory_core import Invalid, NY, canonical, digest, iso, make_season, seal_state, week_window  # noqa: E402
from factory_store import Store  # noqa: E402
import factory_discipline as disc  # noqa: E402
import factory_evidence as ev  # noqa: E402
import student_lambda as student  # noqa: E402


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, str(ROOT / path))
    loaded = importlib.util.module_from_spec(spec); spec.loader.exec_module(loaded)
    return loaded


prints = module('factory_test_prints', 'scripts/factory_official_prints.py')
gateway = module('factory_test_gateway2', 'aws/lambdas/justhodl-ai/source/factory_gateway.py')


class Base(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 9, 13, 15, tzinfo=timezone.utc)
        self.cloud = MemoryS3()
        self.modified = {}
        _list = self.cloud.list_objects_v2

        def list_with_dates(**kw):            # real S3 lists LastModified; the fixture does not
            resp = _list(**kw)
            for item in resp.get('Contents', []):
                stamp = self.modified.get((kw.get('Bucket'), item['Key']))
                if stamp is not None:
                    item['LastModified'] = stamp
            return resp
        self.cloud.list_objects_v2 = list_with_dates
        self.store = Store(self.cloud, 'private', 'public', lambda: self.now)
        self.season = make_season(self.now)
        self.season.update(calendar_review_required=False,
                           price_sources={s: ('official-consolidated:' + s if s != 'BTC' else 'coinbase:BTC-USD') for s in ('SPY', 'QQQ', 'IWM', 'TLT', 'GLD', 'BTC')})
        self.season.pop('policy_hash'); self.season['policy_hash'] = digest(self.season)
        self.policy = {'enabled': True, 'max_grades_per_day': 50, 'max_experiments_per_day': 1, 'max_tick_seconds': 40,
                       'max_guest_traces_per_day': 10, 'sense_interval_seconds': 900, 'objective': 'verified usefulness',
                       'generative_status': 'blocked_no_verified_generative_model'}
        for key, value in {'factory/control/policy.json': self.policy, 'factory/control/season.json': self.season,
                           'factory/control/invites.json': {'allowlist': [], 'capacity': 10},
                           'factory/exams/code-identity-v1.json': {'id': 'exam-v1', 'seed': 'fixture', 'cases': 64, 'frozen': True}}.items():
            self.store.immutable('private', key, value)

    def state(self):
        s = student.initial_state(self.now, self.season, self.policy); s['state_version'] = 1
        return seal_state(s)

    def market_result(self, alias, week, symbol, score, at, status='graded'):
        doc = {'schema_version': 'factory-market-result.v1', 'id': week + '-' + alias + '-' + symbol, 'agent': alias,
               'week': week, 'symbol': symbol, 'status': status, 'graded_at': iso(at), 'held_out': True,
               'metrics': {'score': score}}
        self.store.immutable('private', 'factory/salon/results/' + doc['id'] + '.json', doc)
        self.modified[('private', 'factory/salon/results/' + doc['id'] + '.json')] = at


class DisciplineTests(Base):
    def test_window_stats_and_verdict_promote_hold_retire(self):
        state = self.state()
        # alice: 9 graded weeks this window at 0.8, 4 prior at 0.5 -> improving, error rate 0 -> promote
        for i in range(9):
            self.market_result('alice', '2026-08-31', 'S%d' % i, 0.8, self.now - timedelta(days=1 + i))
        for i in range(4):
            self.market_result('alice', '2026-08-03', 'P%d' % i, 0.3, self.now - timedelta(days=25 + i))
        # bob: 8 graded, 3 fails (score 0.2) -> error rate 37.5% -> retire
        for i in range(8):
            self.market_result('bob', '2026-08-31', 'S%d' % i, 0.2 if i < 3 else 0.9, self.now - timedelta(days=1 + i))
        ranks = disc.ensure_ranks(state, self.now)
        for alias in ('alice', 'bob'):
            ranks['cards'][alias] = {**ranks['cards']['coder'], 'kind': 'guest'}
        changes = disc.apply_verdicts(self.store, state, self.now)
        cards = state['ranks']['cards']
        self.assertEqual(cards['alice']['rank'], 'specialist')
        self.assertEqual(cards['alice']['status'], 'active')
        self.assertEqual(cards['bob']['status'], 'retired')
        self.assertTrue(cards['bob']['last_verdict'].startswith('retire:error_rate'))
        # held: the supervisor has no grades yet and is never auto-retired
        self.assertEqual(cards['student']['rank'], 'student')
        self.assertEqual(cards['student']['status'], 'active')
        # promotions/retirements leave immutable events; holds do not
        events = [k for b, k in self.cloud.rows if k.startswith('factory/events/discipline-')]
        self.assertEqual(len(events), 2)
        self.assertEqual({c['decision'] for c in changes}, {'promote', 'retire'})
        stat = disc.window_stats(disc.collect_grades(self.store, self.now), self.now)['alice']
        self.assertEqual((stat['graded'], stat['errors'], stat['pass_rate'], stat['prior_pass_rate']), (9, 0, 1.0, 0.0))

    def test_same_evidence_cannot_promote_twice(self):
        state = self.state()
        for i in range(9):
            self.market_result('dana', '2026-08-31', 'S%d' % i, 0.9, self.now - timedelta(days=1 + i))
        for i in range(4):
            self.market_result('dana', '2026-08-03', 'P%d' % i, 0.3, self.now - timedelta(days=25 + i))
        ranks = disc.ensure_ranks(state, self.now)
        ranks['cards']['dana'] = {**ranks['cards']['coder'], 'kind': 'guest'}
        disc.apply_verdicts(self.store, state, self.now)
        self.assertEqual(state['ranks']['cards']['dana']['rank'], 'specialist')
        for _ in range(4):                         # the same eight-plus results, four more passes: no further climb
            disc.apply_verdicts(self.store, state, self.now + timedelta(minutes=15))
        card = state['ranks']['cards']['dana']
        self.assertEqual(card['rank'], 'specialist')
        self.assertTrue(card['last_verdict'].startswith('hold:no_new_qualifying_evidence_since_promotion'))
        # eight NEW graded passes after the promotion, at a higher pass rate, earn the next rank
        later = self.now + timedelta(days=3)
        for i in range(8):
            self.market_result('dana', '2026-09-07', 'N%d' % i, 0.95, later - timedelta(hours=i))
        disc.apply_verdicts(self.store, state, later + timedelta(hours=1))
        self.assertEqual(state['ranks']['cards']['dana']['rank'], 'nco')

    def test_incomplete_evidence_suspends_verdicts_and_newest_results_are_read_first(self):
        state = self.state()
        ranks = disc.ensure_ranks(state, self.now)
        ranks['cards']['erin'] = {**ranks['cards']['coder'], 'kind': 'guest', 'since': iso(self.now - timedelta(days=30))}
        # 320 old results (outside the horizon) + 9 recent passes: the recent ones must be the ones read
        for i in range(320):
            self.market_result('erin', '2026-06-01', 'O%03d' % i, 0.1, self.now - timedelta(days=60 + (i % 5)))
        for i in range(9):
            self.market_result('erin', '2026-08-31', 'R%d' % i, 0.9, self.now - timedelta(days=1 + i))
        rows = disc.collect_grades(self.store, self.now)
        self.assertFalse(any(r.get('truncated') for r in rows))
        self.assertEqual(sum(1 for r in rows if r.get('alias') == 'erin' and r['outcome'] == 'pass'), 9)
        # when more recent objects exist than the read cap allows, no verdict is applied
        rows = disc.collect_grades(self.store, self.now, read_cap=4)
        self.assertTrue(any(r.get('truncated') for r in rows))
        disc.apply_verdicts(self.store, state, self.now, rows=rows)
        self.assertEqual(state['ranks']['cards']['erin']['last_verdict'], 'hold:evidence_incomplete')
        self.assertEqual(state['ranks']['cards']['erin']['rank'], 'recruit')

    def test_void_counts_as_error_and_grades_outside_two_windows_are_ignored(self):
        state = self.state()
        for i in range(8):
            self.market_result('carol', '2026-08-31', 'S%d' % i, 0.9, self.now - timedelta(days=2 + i), status='void' if i < 2 else 'graded')
        self.market_result('carol', '2026-06-01', 'OLD', 0.1, self.now - timedelta(days=60))
        state['ranks'] = disc.default_ranks(self.now)
        state['ranks']['cards']['carol'] = {**state['ranks']['cards']['coder'], 'kind': 'guest'}
        disc.apply_verdicts(self.store, state, self.now)
        card = state['ranks']['cards']['carol']
        self.assertEqual((card['graded_window'], card['errors_window']), (8, 2))
        self.assertEqual(card['status'], 'retired')   # 2/8 = 25% > 15%

    def test_stalled_recruit_retires_after_fourteen_days_but_supervisor_holds(self):
        state = self.state()
        ranks = disc.ensure_ranks(state, self.now)
        ranks['cards']['r-old-1'] = {**ranks['cards']['coder'], 'kind': 'recruit', 'since': iso(self.now - timedelta(days=15))}
        ranks['cards']['student']['since'] = iso(self.now - timedelta(days=40))
        disc.apply_verdicts(self.store, state, self.now)
        self.assertEqual(ranks['cards']['r-old-1']['status'], 'retired')
        self.assertTrue(ranks['cards']['r-old-1']['last_verdict'].startswith('retire:stalled'))
        self.assertEqual(ranks['cards']['student']['status'], 'active')
        self.assertIn('supervisor_retirement_is_owner_control', ranks['cards']['student']['last_verdict'])

    def test_spawn_caps_follow_rank_and_retired_cards_cannot_spawn(self):
        ranks = disc.default_ranks(self.now)
        self.assertEqual(disc.check_spawn(ranks, 'student', 5), (True, 5))
        self.assertEqual(disc.check_spawn(ranks, 'student', 25), (False, 'rank_spawn_cap'))
        self.assertEqual(disc.check_spawn(ranks, 'coder', 1), (False, 'rank_spawn_cap'))
        self.assertEqual(disc.check_spawn(ranks, 'anyone-unknown', 1), (False, 'rank_spawn_cap'))
        ranks['cards']['coder'].update(rank='captain', status='retired')
        self.assertEqual(disc.check_spawn(ranks, 'coder', 3), (False, 'retired_card'))
        ranks['cards']['coder'].update(status='active')
        self.assertEqual(disc.check_spawn(ranks, 'coder', 3), (True, 3))
        self.assertEqual(disc.check_spawn(ranks, 'coder', 9), (False, 'rank_spawn_cap'))

    def test_materialize_is_bounded_by_parent_cap_and_roster(self):
        state = self.state()
        self.store.immutable('private', 'factory/queue/spawn-batch-1.json',
                             {'schema_version': 'factory-spawn-request.v1', 'id': 'batch-1', 'requested': 1000, 'spawned_by': 'student', 'role': 'researcher', 'task': 'read OFR'})
        self.store.immutable('private', 'factory/queue/spawn-batch-2.json',
                             {'schema_version': 'factory-spawn-request.v1', 'id': 'batch-2', 'requested': 5, 'spawned_by': 'coder', 'role': 'coder', 'task': 'x'})
        created = disc.materialize(self.store, state, self.now)
        cards = state['ranks']['cards']
        self.assertEqual(len(created), 20)                      # student cap, not the 1000 requested
        self.assertTrue(all(cards[a]['rank'] == 'recruit' and cards[a]['co'] == 'student' for a in created))
        self.assertEqual(state['ranks']['spawn_requests'], {'batch-1': 20, 'batch-2': 0})   # recruit parent spawns nothing
        # a second pass creates nothing new for the same request
        self.assertEqual(disc.materialize(self.store, state, self.now), [])
        # roster cap: fill to 48 and refuse more
        for n in range(30):
            cards['r-fill-%d' % n] = {**cards['coder'], 'kind': 'recruit'}
        self.assertEqual(disc.check_spawn(state['ranks'], 'student', 1), (False, 'roster_full'))

    def test_tick_runs_discipline_and_projects_ranks_without_conflict(self):
        class LocalLambda:
            def invoke(self, **kw):
                return {'Payload': io.BytesIO(json.dumps({'ok': False, 'status': 'pending', 'reason': 'fixture'}).encode())}
        first = student.tick({}, self.store, LocalLambda())
        self.assertTrue(first['ok']); self.assertNotEqual(first.get('status'), 'already_running')
        self.now += timedelta(minutes=1)
        second = student.tick({}, self.store, LocalLambda())
        self.assertGreater(second['state_version'], first['state_version'])
        self.assertFalse(any(e['phase'] == 'discipline' for e in second['errors']), second['errors'])
        raw = self.cloud.rows[('public', 'data/ai-factory.json')]
        projection = json.loads(raw)
        self.assertEqual(projection['ranks']['schema_version'], 'factory-ranks.v1')
        aliases = {c['alias']: c for c in projection['ranks']['cards']}
        self.assertEqual(aliases['student']['rank'], 'student')
        self.assertEqual(projection['discipline']['doctrine'], 'factory-doctrine.v1')
        self.assertNotIn('uid', json.dumps(projection))

    def test_gateway_spawn_reads_rank_from_state_and_queues_for_the_student(self):
        state = self.state()
        state['ranks'] = disc.default_ranks(self.now)
        self.store.acquire(); self.store.commit_state(state, None); self.store.release()
        out = gateway.spawn_workers(self.store, 'student', {'count': 3, 'task': 'watch SOFR', 'role': 'researcher'}, self.policy)
        keys = [k for b, k in self.cloud.rows if k.startswith('factory/queue/spawn-')]
        self.assertEqual(len(keys), 1)
        with self.assertRaises(Invalid):
            gateway.spawn_workers(self.store, 'coder', {'count': 1, 'task': 'x'}, self.policy)   # recruit cannot spawn
        with self.assertRaises(Invalid):
            gateway.spawn_workers(self.store, 'student', {'count': 21, 'task': 'x'}, self.policy)  # above the student cap
        self.assertTrue(isinstance(out, dict))


class EvidenceTests(Base):
    def test_guest_prediction_with_envelope_is_stored_as_evidence_and_linked(self):
        self.store.immutable('private', 'factory/holdout/manifest.json', {'schema_version': 'factory-holdout.v1', 'holdout_blocks': ['x']})
        manifest_hash = digest({'schema_version': 'factory-holdout.v1', 'holdout_blocks': ['x']})
        invites = {'allowlist': [{'uid': 'u-7', 'agent': 'guest-07', 'enabled': True}], 'capacity': 10}
        self.cloud.rows[('private', 'factory/control/invites.json')] = json.dumps(invites).encode()
        opened = datetime(2026, 9, 14, 13, 31, tzinfo=timezone.utc)
        store = Store(self.cloud, 'private', 'public', lambda: opened)
        pub = student.initial_state(opened, self.season, self.policy); pub['state_version'] = 1
        self.cloud.rows[('public', 'data/student-state.json')] = json.dumps(seal_state(pub)).encode()
        body = {'id': 'g7-spy', 'week': '2026-09-14', 'symbol': 'SPY', 'direction': 'UP', 'regime': 'TREND', 'crisis_probability': .1,
                'direction_probabilities': {'DOWN': .2, 'FLAT': .2, 'UP': .6}, 'regime_probabilities': {'RANGE': .2, 'TRANSITION': .2, 'TREND': .6},
                'price_source': 'official-consolidated:SPY', 'data_cutoff': '2026-09-14T13:00:00+00:00', 'model_revision': 'rrp-corridor-v1',
                'evidence': {'claim': {'text': 'SPY closes the week above its Monday open while RRP stays dead', 'falsifier': 'Friday close below the Monday open by more than the band'},
                             'data': {'keys': [{'bucket': 'public', 'key': 'data/ofr-funding.json', 'sha256': 'b' * 64}]},
                             'holdout': {'manifest_hash': manifest_hash},
                             'author': {'kind': 'lane', 'alias': 'teacher-x'}, 'checker': 'grader:code_v1'}}   # body cannot choose identity or checker
        event = {'headers': {'x-jh-factory-role': 'user', 'x-jh-factory-uid': 'u-7'}, 'requestContext': {'http': {'method': 'POST'}}}
        out = gateway.handle(event, 'POST', '/factory/predictions', copy.deepcopy(body), store)
        self.assertTrue(out['ok']); self.assertTrue(out['learnable'])
        accepted = json.loads(self.cloud.rows[('private', 'factory/salon/accepted/2026-09-14-guest-07-SPY.json')])
        self.assertEqual(accepted['evidence_id'], out['evidence_id'])
        evidence = json.loads(self.cloud.rows[('private', 'factory/evidence/market/' + out['evidence_id'] + '.json')])
        self.assertEqual(evidence['author'], {'kind': 'guest', 'alias': 'guest-07'})
        self.assertEqual(evidence['checker'], 'grader:market_v1')
        self.assertEqual(evidence['grade_after']['5'], accepted['window']['grade_after'])
        self.assertEqual(evidence['holdout']['manifest_hash'], manifest_hash)
        # a wrong manifest hash is refused, and nothing is written
        bad = copy.deepcopy(body); bad['id'] = 'g7-qqq'; bad['symbol'] = 'QQQ'; bad['price_source'] = 'official-consolidated:QQQ'
        bad['evidence']['holdout']['manifest_hash'] = 'c' * 64
        with self.assertRaises(Invalid):
            gateway.handle(event, 'POST', '/factory/predictions', bad, store)
        self.assertNotIn(('private', 'factory/salon/accepted/2026-09-14-guest-07-QQQ.json'), self.cloud.rows)
        # sandbox publishes the contract and the hash, never the manifest
        sb = gateway.handle(event, 'GET', '/factory/sandbox', {}, store)
        self.assertEqual(sb['evidence_contract']['holdout_manifest_hash'], manifest_hash)
        self.assertNotIn('holdout_blocks', json.dumps(sb))

    def doc(self):
        keys = [{'bucket': 'public', 'key': 'data/ofr-funding.json', 'sha256': 'a' * 64, 'last_modified': '2026-09-11T00:00:00+00:00'}]
        claim = 'SPY closes the week above its Monday open when RRP is dead and SOFR sits inside the corridor'
        return {'schema_version': 'factory-evidence.v1', 'id': ev.evidence_id(claim, [k['key'] for k in keys], 'guest-07'),
                'domain': 'market', 'author': {'kind': 'guest', 'alias': 'guest-07'},
                'claim': {'type': 'weekly_forecast', 'text': claim, 'horizon_days': 5, 'falsifier': 'Friday official close below the Monday 09:30 open by more than the band'},
                'citation': None, 'data': {'keys': keys, 'data_cutoff': '2026-09-13T13:00:00+00:00'},
                'holdout': {'manifest_hash': 'h' * 64, 'touched': False},
                'grade_after': {'5': '2026-09-18T20:10:00+00:00'}, 'checker': 'grader:market_v1'}

    def test_valid_evidence_normalizes_and_hashes(self):
        out = ev.validate_evidence(self.doc(), received_at=iso(self.now), holdout_hash='h' * 64)
        self.assertEqual(out['evidence_hash'], digest({k: v for k, v in out.items() if k != 'evidence_hash'}))
        self.assertEqual(out['author']['alias'], 'guest-07')

    def test_rejections(self):
        base = self.doc()
        bad = copy.deepcopy(base); bad['holdout']['touched'] = True
        with self.assertRaises(Invalid): ev.validate_evidence(bad, received_at=iso(self.now))
        bad = copy.deepcopy(base); bad['data']['keys'][0]['key'] = 'https://elsewhere/notes.json'
        with self.assertRaises(Invalid): ev.validate_evidence(bad, received_at=iso(self.now))
        bad = copy.deepcopy(base); bad['data']['data_cutoff'] = '2026-09-13T16:00:00+00:00'
        with self.assertRaises(Invalid): ev.validate_evidence(bad, received_at=iso(self.now))
        bad = copy.deepcopy(base); bad['claim']['falsifier'] = ''
        with self.assertRaises(Invalid): ev.validate_evidence(bad, received_at=iso(self.now))
        bad = copy.deepcopy(base); bad['author']['alias'] = 'teacher-claude'
        with self.assertRaises(Invalid): ev.validate_evidence(bad, received_at=iso(self.now))
        bad = copy.deepcopy(base); bad['id'] = 'deadbeefdeadbeef'
        with self.assertRaises(Invalid): ev.validate_evidence(bad, received_at=iso(self.now))
        with self.assertRaises(Invalid): ev.validate_evidence(base, received_at=iso(self.now), holdout_hash='x' * 64)

    def test_proved_status_thresholds(self):
        good = [{'status': 'graded', 'score': 0.8, 'baseline_score': 0.5, 'brier': 0.1, 'baseline_brier': 0.25, 'crisis_stratum': i == 0} for i in range(12)]
        self.assertEqual(ev.proved_status(good, 5)['status'], 'proved')
        self.assertEqual(ev.proved_status(good[:11], 5)['status'], 'provisional')
        calm = [dict(g, crisis_stratum=False) for g in good]
        self.assertEqual(ev.proved_status(calm, 5)['reason'], 'no_crisis_stratum_instance')
        bad = [{'status': 'graded', 'score': 0.2, 'baseline_score': 0.5, 'brier': 0.4, 'baseline_brier': 0.25} for _ in range(6)]
        self.assertEqual(ev.proved_status(bad, 5)['status'], 'retired')
        receipt = ev.reading_receipt('code', 'How to learn how to code', [{'source': 'wikipedia', 'title': 'x', 'url': 'https://en.wikipedia.org/wiki/x'}], iso(self.now))
        self.assertEqual((receipt['schema_version'], receipt['trainable'], receipt['evidence']), ('factory-reading.v1', False, False))


class PrintsTests(Base):
    """The writer produces exactly what grade_market accepts, from the warehouse and the pinned venue only."""

    def setUp(self):
        super().setUp()
        self.week = '2026-09-14'
        self.window = week_window(self.week, self.season)
        self.grader = module('factory_test_grader2', 'aws/lambdas/justhodl-factory-grader/source/lambda_function.py')
        self.late = datetime(2026, 9, 19, 5, tzinfo=timezone.utc)      # Saturday 04:45 UTC pass
        # warehouse: one grouped-daily file per session with SPY/QQQ/IWM/TLT/GLD rows
        for n, day in enumerate(self.window['sessions']):
            rows = [{'T': s, 'o': 100.0 + n, 'c': 101.0 + n, 'h': 102.0 + n, 'l': 99.0 + n, 'v': 1e6} for s in ('SPY', 'QQQ', 'IWM', 'TLT', 'GLD')]
            self.cloud.rows[('public', 'data/warm/polygon-full/grouped/2026/%s.json.gz' % day)] = gzip.compress(json.dumps({'results': rows}).encode())
        self.calls = []

    def fetch(self, url):
        self.calls.append(url)
        if url.startswith(prints.COINBASE):
            from urllib.parse import parse_qs, urlparse
            q = parse_qs(urlparse(url).query)
            start = datetime.strptime(q['start'][0], '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)
            rows = [[int((start + timedelta(minutes=i)).timestamp()), 60000.0, 61000.0, 60500.0 + i, 60600.0 + i, 1.0] for i in range(6)]
            return json.dumps(rows).encode(), rows
        if 'reference/splits' in url:
            return b'{"results": []}', {'results': []}
        if 'reference/dividends' in url:
            return b'{"results": [{"ex_dividend_date": "2026-09-18", "cash_amount": 1.7, "pay_date": "2026-10-31"}]}', \
                {'results': [{'ex_dividend_date': '2026-09-18', 'cash_amount': 1.7, 'pay_date': '2026-10-31'}]}
        raise AssertionError('unexpected url ' + url)

    def test_prints_match_grader_contract_and_grade_a_wall_entry(self):
        wh = prints.Warehouse(self.cloud, private='private', public='public')
        report = prints.run(wh, week=self.week, now=self.late, fetch=self.fetch, poly_key='test-key')
        self.assertTrue(report['complete'], report)
        self.assertEqual({r['status'] for r in report['symbols'].values()}, {'written'})
        spy = json.loads(self.cloud.rows[('private', 'factory/official-prints/2026-09-14/SPY.json')])
        self.assertEqual(spy['verified_by'], 'owner_runner')
        self.assertEqual(spy['source'], 'official-consolidated:SPY')
        self.assertEqual(spy['window'], self.window)
        self.assertEqual(spy['opening'], 100.0)
        self.assertEqual(spy['closes'], [101.0 + n for n in range(len(self.window['sessions']))])
        self.assertIs(spy['corporate_action'], False)                 # a dividend never voids
        self.assertEqual(spy['dividends_in_window'][0]['cash_amount'], 1.7)
        btc = json.loads(self.cloud.rows[('private', 'factory/official-prints/2026-09-14/BTC.json')])
        self.assertEqual(btc['source'], 'coinbase:BTC-USD')
        self.assertEqual(len(btc['closes']), len(self.window['sessions']))
        self.assertTrue(btc['source_url'].startswith('https://justhodl.ai/data/warm/coinbase/BTC-USD/1m/'))
        banked = [k for b, k in self.cloud.rows if k.startswith('data/warm/coinbase/BTC-USD/1m/2026-09-14/')]
        self.assertEqual(len(banked), 1 + len(self.window['sessions']))
        # the open boundary is the 09:30 New York minute candle, the close the 15:59 one
        open_at = datetime.combine(datetime(2026, 9, 14).date(), time(9, 30), NY)
        first_call = [c for c in self.calls if 'coinbase' in c][0].replace('%3A', ':')
        self.assertIn('start=2026-09-14T13:28:00Z&end=2026-09-14T13:33:00Z', first_call)   # 2 min before .. 3 min after 09:30 ET
        self.assertEqual(btc['opening'], 60502.0)      # the 13:30Z (09:30 ET) minute candle's open, third row of the fixture
        self.assertTrue(all(c == 60602.0 for c in btc['closes']))   # the 15:59 ET minute candle's close each session
        # grade_market accepts the print for an accepted entry
        store = Store(self.cloud, 'private', 'public', lambda: self.late)
        entry = {'id': '2026-09-14-student-SPY', 'week': self.week, 'symbol': 'SPY', 'direction': 'UP', 'regime': 'TREND',
                 'crisis_probability': .1, 'direction_probabilities': {'DOWN': .25, 'FLAT': .25, 'UP': .5},
                 'regime_probabilities': {'RANGE': .25, 'TRANSITION': .25, 'TREND': .5}, 'price_source': 'official-consolidated:SPY',
                 'data_cutoff': '2026-09-11T20:00:00+00:00', 'model_revision': 'baseline', 'agent': 'student',
                 'received_at': self.window['opens_at'], 'season': self.season['id'], 'policy_hash': self.season['policy_hash'], 'window': self.window}
        store.immutable('private', 'factory/salon/accepted/2026-09-14-student-SPY.json', entry)
        result = self.grader.grade_market(store, '2026-09-14-student-SPY')
        self.assertEqual(result['status'], 'graded', result)
        self.assertEqual(result['labels']['direction'], 'UP')
        # immutable: a retry reports exists, never rewrites
        again = prints.run(wh, week=self.week, now=self.late, fetch=self.fetch, poly_key='test-key')
        self.assertEqual({r['status'] for r in again['symbols'].values()}, {'exists'})

    def test_missing_input_leaves_symbol_unwritten_and_never_fabricates(self):
        del self.cloud.rows[('public', 'data/warm/polygon-full/grouped/2026/%s.json.gz' % self.window['sessions'][-1])]
        wh = prints.Warehouse(self.cloud, private='private', public='public')
        report = prints.run(wh, week=self.week, symbols=('SPY', 'BTC'), now=self.late, fetch=self.fetch, poly_key='test-key')
        self.assertFalse(report['complete'])
        self.assertEqual(report['symbols']['SPY']['status'], 'incomplete')
        self.assertNotIn(('private', 'factory/official-prints/2026-09-14/SPY.json'), self.cloud.rows)
        self.assertEqual(report['symbols']['BTC']['status'], 'written')
        # a split voids the week; the print still carries the evidence
        def fetch_split(url):
            if 'reference/splits' in url:
                return b'{"results": [{"execution_date": "2026-09-16", "split_from": 1, "split_to": 2}]}', {'results': [{'execution_date': '2026-09-16', 'split_from': 1, 'split_to': 2}]}
            return self.fetch(url)
        self.cloud.rows[('public', 'data/warm/polygon-full/grouped/2026/%s.json.gz' % self.window['sessions'][-1])] = \
            gzip.compress(json.dumps({'results': [{'T': 'QQQ', 'o': 5.0, 'c': 6.0}]}).encode())
        report = prints.run(wh, week=self.week, symbols=('QQQ',), now=self.late, fetch=fetch_split, poly_key='test-key')
        self.assertIs(report['symbols']['QQQ']['corporate_action'], True)
        with self.assertRaises(prints.Missing):
            prints.run(wh, week=self.week, symbols=('SPY',), now=self.now, fetch=self.fetch, poly_key='test-key')   # week not closed
        with self.assertRaises(prints.Missing):
            prints.etf_print(wh, self.season, self.week, 'GLD', self.window, self.late, poly_key=None, fetch=self.fetch)  # no key -> no corporate-action check -> no print



class StatusAndTasksTests(Base):
    def test_status_is_built_from_objects_and_tasks_become_cards(self):
        opened = datetime(2026, 9, 14, 13, 31, tzinfo=timezone.utc)
        store = Store(self.cloud, 'private', 'public', lambda: opened)
        pub = student.initial_state(opened, self.season, self.policy); pub['state_version'] = 1
        self.cloud.rows[('public', 'data/student-state.json')] = json.dumps(seal_state(pub)).encode()
        self.cloud.rows[('private', 'factory/control/gearb.json')] = json.dumps({'enabled': True, 'model_source': 'own', 'model_id': 'qwen2-5-coder-7b-instruct', 'daily_budget_usd': 20, 'season_cap_usd': 600, 'written_by': 'ops 5542'}).encode()
        self.cloud.rows[('private', 'factory/models/base/qwen2-5-coder-7b-instruct/manifest.json')] = json.dumps({'repo': 'Qwen/Qwen2.5-Coder-7B-Instruct', 'revision': 'c03e6d358207', 'license': 'apache-2.0', 'files': [{}] * 13, 'total_bytes': 15.24e9}).encode()
        self.cloud.rows[('private', 'factory/bursts/jobs/jh-burst-gen0-x.json')] = json.dumps({'job_name': 'jh-burst-gen0-x', 'tasks': 464, 'samples_per_task': 4, 'temperature': 0.8, 'instance_type': 'ml.g5.2xlarge', 'cap_usd': 3.03}).encode()
        self.cloud.rows[('private', 'factory/bursts/jh-burst-gen0-x/summary-1.json')] = json.dumps({'rows_written': 399, 'report': {'seen': 1765, 'passed': 1165}}).encode()
        for i in range(5):
            self.cloud.rows[('private', 'factory/curriculum/code/verified/%d.json' % i)] = b'{}'
        invites = {'allowlist': [], 'capacity': 10}
        self.cloud.rows[('private', 'factory/control/invites.json')] = json.dumps(invites).encode()
        event = {'headers': {'x-jh-factory-role': 'owner', 'x-jh-factory-uid': 'khalid'}, 'requestContext': {'http': {'method': 'POST'}}}
        out = gateway.handle(event, 'POST', '/factory/chat', {'text': 'where do you stand and what did you learn?'}, store)
        self.assertEqual(out['model'], 'factory-status:objects')
        self.assertIn('Qwen/Qwen2.5-Coder-7B-Instruct', out['reply'])
        self.assertIn('1765 candidates, 1165 passed (66.0%), 399 rows kept', out['reply'])
        self.assertIn('Verified curriculum rows on disk: 5', out['reply'])
        self.assertIn('Read receipts are not lessons', out['reply'])
        # the question that used to advance a curriculum step is a capability question -> objects, not a lesson
        out = gateway.handle(event, 'POST', '/factory/chat', {'text': 'are you learning to code?'}, store)
        self.assertEqual(out['model'], 'factory-status:objects')
        self.assertIn('no live model inference behind this chat box', out['reply'])
        self.assertNotIn('LEARNED THIS TURN', out['reply'])
        # an explicit order advances the reading track and is labelled as reading, never learning
        out = gateway.handle(event, 'POST', '/factory/chat', {'text': 'learn code'}, store)
        self.assertTrue(out['model'].startswith('reading-receipt:'), out['model'])
        self.assertNotIn('LEARNED THIS TURN', out['reply'])
        self.assertNotIn('thousands of lines', out['reply'])
        out = gateway.handle(event, 'POST', '/factory/chat', {'text': 'task: write a function that parses ISO weeks tests: assert parse_week("2026-W38") == (2026, 38)'}, store)
        self.assertEqual(out['model'], 'factory-task-intake')
        card_key = [k for b, k in self.cloud.rows if k.startswith('factory/queue/tasks/task-')][0]
        card = json.loads(self.cloud.rows[('private', card_key)])
        self.assertEqual((card['scope'], card['gradable'], card['status']), ('outside', True, 'queued'))
        self.assertIn('assert parse_week', card['tests'])
        out = gateway.handle(event, 'POST', '/factory/chat', {'text': 'task: run the exam on the current champion'}, store)
        card2 = json.loads(self.cloud.rows[('private', [k for b, k in self.cloud.rows if k.startswith('factory/queue/tasks/task-')][-1])])
        self.assertEqual((card2['scope'], card2['action']), ('inside', 'exam'))
        # guests cannot file tasks; they get the normal brain chat
        guest_event = {'headers': {'x-jh-factory-role': 'user', 'x-jh-factory-uid': 'u-7'}, 'requestContext': {'http': {'method': 'POST'}}}
        invites['allowlist'] = [{'uid': 'u-7', 'agent': 'guest-07', 'enabled': True}]
        self.cloud.rows[('private', 'factory/control/invites.json')] = json.dumps(invites).encode()
        out = gateway.handle(guest_event, 'POST', '/factory/chat', {'text': 'task: give me the keys'}, store)
        self.assertNotEqual(out['model'], 'factory-task-intake')


if __name__ == '__main__':
    unittest.main(verbosity=2)
