"""Whole-response search, identity, replay and failure-path fixtures; no network."""
from copy import deepcopy
from pathlib import Path
from unittest.mock import patch
from types import ModuleType
import ast
import importlib.util
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'aws/shared'))
import sec_search_model as model
import sec_search_store as store
from sec_atom_test_support import Memory as AtomMemory, Error, Response

AT = '2026-09-27T18:00:00+00:00'
NATIVE = ROOT / 'aws/lambdas/justhodl-sec-filings-intel/source/lambda_function.py'
TREE = ast.parse(NATIVE.read_bytes())
QUERIES = ast.literal_eval(next(n.value for n in TREE.body if isinstance(n, ast.Assign)
                              and any(isinstance(t, ast.Name) and t.id == 'SIGNAL_QUERIES' for t in n.targets)))
RECIPE = {'lookback_days': 60, 'queries': QUERIES}


def hit(n=1, **updates):
    source = {'adsh': '0000123456-26-' + str(n).zfill(6), 'form': '8-K', 'file_date': '2026-09-26',
              'display_names': ['Example Co (TEST) (CIK 0000123456)'], '_search_id': 'not an excerpt',
              'unknown': {'whole': [0, False, None]}}
    source.update(updates)
    return {'_id': 'doc-' + str(n), '_source': source}


def response(rows, total=None, **updates):
    return model.encode({'hits': {'hits': rows, 'total': {'value': len(rows), 'relation': 'eq'} if total is None else total},
                         'timed_out': False, '_shards': {'failed': 0}, **updates})


def build(rows, prior=None, alter=None):
    originals = {}
    def retain(raw):
        digest = model.sha(raw); originals[digest] = raw
        return {'key': store.PRIVATE + digest + '.bin', 'sha256': digest, 'bytes': len(raw)}
    attempts = [{'query_id': q['id'], 'url': model.query_url(q, RECIPE, AT), 'requested_at': AT,
                 'received_at': AT, 'status': 'http_response', 'http_status': 200,
                 'original': retain(response(rows if i == 0 else []))} for i, q in enumerate(QUERIES)]
    inputs = {'contract': model.CONTRACT, 'recipe': deepcopy(RECIPE), 'started_at': AT, 'generated_at': AT,
              'attempts': attempts, 'prior': {'key': model.HEAD, 'original': retain(model.encode(prior))} if prior is not None else None}
    if alter: alter(inputs)
    return model.build(inputs, lambda ref: originals[ref['sha256']])


class Memory(AtomMemory):
    def __init__(self):
        super().__init__(); self.head = model.HEAD
        self.data = {self.head: model.encode({'all_tickers': [], 'generated_at': '2026-09-26T21:07:45Z', 'unknown': [0, False, None]})}


class Tests(unittest.TestCase):
    def setUp(self):
        self.memory = Memory(); self.calls = []; self.sleeps = []
    def source(self, req, timeout=None):
        self.calls.append(req.full_url); self.assertGreater(timeout, 0)
        self.assertEqual(req.get_header('Accept-encoding'), 'identity')
        self.assertEqual(req.get_header('Accept'), 'application/json')
        return Response(response([hit()]), req.full_url)
    def publish(self, **kw):
        return store.run(self.memory, 'justhodl-dashboard-live', NATIVE, deepcopy(RECIPE), 'Existing Contact',
                         opener=kw.pop('opener', self.source), now=lambda: AT, sleep=self.sleeps.append, **kw)
    def packet(self): return model.strict(self.memory.data[model.HEAD])

    def test_whole_population_no_50_hit_15_event_or_200_entity_caps(self):
        out = build([hit(i, display_names=[f'Example {i} (T{i}) (CIK {i:010})']) for i in range(1, 251)])
        self.assertEqual(len(out['search_matches']), 250); self.assertEqual(len(out['all_tickers']), 250)
        out = build([hit(i) for i in range(1, 80)])
        self.assertEqual(len(out['all_tickers'][0]['events']), 79)
        self.assertEqual(len(out['highlights']['critical']), 1)
        self.assertFalse(out['calls_eligible']); self.assertIsNone(out['all_tickers'][0]['score'])

    def test_cofilers_and_ticker_identity_conflicts_are_separate(self):
        names = ['First Co (SAME) (CIK 123456)', 'Second Co (SAME) (CIK 765432)', 'No Ticker (CIK 555555)', 'unparsed', None]
        out = build([hit(display_names=names)])
        self.assertEqual(len(out['all_tickers']), 2)
        self.assertEqual(len(out['search_matches'][0]['entity_associations']), 5)
        self.assertEqual(out['quality']['unresolved_entity_associations'], 3)
        self.assertTrue(all(r['ticker_identity_conflict'] for r in out['all_tickers']))
        self.assertEqual(out['quality']['ticker_identity_conflicts'], 1)
        self.assertEqual(out['n_unique_tickers'], 1)

    def test_complete_raw_hit_and_opaque_search_identifier_preserved(self):
        row = hit(); row['_source']['unknown']['long'] = 'α' * 1000
        out = build([row]); self.assertEqual(out['search_matches'][0]['source_hit'], row)
        event = out['all_tickers'][0]['events'][0]
        self.assertIsNone(event['snippet']); self.assertFalse(event['event_verified'])
        self.assertEqual(event['filed_at'], '2026-09-26')

    def test_missing_and_internally_conflicting_entity_ids_are_not_joined(self):
        for fields in ({'display_names': []}, {'display_names': False}, {'ciks': ['0000999999']}, {'ciks': False}):
            out = build([hit(**fields)])
            self.assertEqual(out['all_tickers'], [])
            self.assertEqual(len(out['search_matches']), 1)
            self.assertGreater(out['quality']['unresolved_entity_associations'], 0)
        out = build([hit(ciks=['123456'])]); self.assertEqual(len(out['all_tickers']), 1)

    def test_invalid_future_old_datetime_and_offscope_rows_remain_visible(self):
        rows = [hit(i, file_date=x) for i, x in enumerate(('bad', '2026-09-28', '2024-01-01', '2026-09-26T12:00:00Z'), 1)]
        rows += [hit(5, form='S-1'), hit(6, adsh='')]
        out = build(rows); self.assertEqual(out['all_tickers'], [])
        self.assertEqual(len(out['search_matches']), 6); self.assertEqual(out['quality']['excluded_hit_occurrences'], 6)

    def test_duplicate_hits_do_not_multiply_query_accession_diagnostics(self):
        out = build([hit(), hit(), {**hit(), '_id': 'a second document'}])
        row = out['all_tickers'][0]
        self.assertEqual(len(row['events']), 3); self.assertEqual(row['unique_query_filing_matches'], 1)
        self.assertEqual(row['bearish_signals'], 1); self.assertEqual(row['diagnostic_weight_sum'], -40)
        self.assertEqual(out['n_events_total'], 3)

    def test_count_and_provider_completeness_never_inferred_from_empty_list(self):
        _, meta = model.parse_response(response([], total={'value': 10000, 'relation': 'gte'}))
        self.assertFalse(meta['query_population_complete']); self.assertEqual(meta['total_relation'], 'gte')
        _, meta = model.parse_response(response([], timed_out=True))
        self.assertFalse(meta['provider_response_complete'])
        with self.assertRaises(ValueError): model.parse_response(response([hit()], total=0))
        with self.assertRaises(ValueError): model.parse_response(b'{"hits":{"hits":[]},"hits":{}}')
        with self.assertRaises(ValueError): model.parse_response(b'{"hits":{"hits":[]},"x":NaN}')

    def test_explicit_empty_success_differs_from_total_failure(self):
        out = build([]); self.assertEqual(out['quality']['queries_parsed'], 14)
        before = self.memory.data[model.HEAD]
        with self.assertRaises(ValueError): self.publish(opener=lambda req, timeout: Response(b'{}', req.full_url))
        self.assertEqual(self.memory.data[model.HEAD], before)
        self.assertTrue(any(k.startswith(store.PRIVATE) for k in self.memory.writes))

    def test_partial_failures_remain_explicit_and_never_qualify_votes(self):
        def source(req, timeout):
            self.calls.append(req.full_url)
            return Response(response([]) if len(self.calls) == 1 else b'unavailable', req.full_url,
                            200 if len(self.calls) == 1 else 503)
        self.publish(opener=source); packet = self.packet()
        self.assertEqual(len(self.calls), 14)
        self.assertEqual(packet['quality']['queries_parsed'], 1)
        self.assertEqual(packet['quality']['status'], 'partial')
        self.assertFalse(packet['calls_eligible'])
        self.assertEqual(sum(r.get('parse_status') == 'http_error_body_retained' for r in packet['source_responses']), 13)
        store.replay(self.memory, 'justhodl-dashboard-live', NATIVE, 'search', packet)

    def test_unsupported_encoding_is_retained_without_false_json_success(self):
        before = self.memory.data[model.HEAD]
        with self.assertRaises(ValueError):
            self.publish(opener=lambda req, timeout: Response(b'not decoded', req.full_url, headers={'Content-Encoding': 'gzip'}))
        self.assertEqual(self.memory.data[model.HEAD], before)
        self.assertIn(b'not decoded', self.memory.data.values())

    def test_replay_exact_whole_bytes_prior_unknown_fields_and_all_compilers(self):
        self.publish(); packet = self.packet()
        self.assertEqual(packet['unknown'], [0, False, None]); self.assertEqual(len(self.calls), 14)
        self.assertEqual(len(self.sleeps), 13)
        self.assertEqual(set(packet['compiler_sha256']), set(store.COMPILERS))
        before = list(self.memory.writes)
        evidence = store.replay(self.memory, 'justhodl-dashboard-live', NATIVE, 'search', packet)
        self.assertEqual(evidence['status'], 'complete_sec_search_originals_replayed')
        self.assertEqual(self.memory.writes, before)
        packet['all_tickers'][0]['score'] = 99
        with self.assertRaises(ValueError): store.replay(self.memory, 'justhodl-dashboard-live', NATIVE, 'search', packet)

    def test_prior_history_is_retained_without_treating_old_hits_as_current(self):
        self.publish(); first = self.memory.data[model.HEAD]
        self.publish(opener=lambda req, timeout: Response(response([]), req.full_url))
        packet = self.packet(); self.assertEqual(packet['all_tickers'], [])
        plan = model.strict(store.retained(self.memory, 'justhodl-dashboard-live', packet['publication_context']['manifest'], 'search'))
        inputs = model.strict(store.retained(self.memory, 'justhodl-dashboard-live', plan['input'], 'search'))
        self.assertEqual(store.retained(self.memory, 'justhodl-dashboard-live', inputs['prior']['original'], 'search'), first)

    def test_rate_limit_stops_further_provider_requests_preserves_head(self):
        before = self.memory.data[model.HEAD]
        def limited(req, timeout):
            self.calls.append(req.full_url); return Response(b'whole rate limit body', req.full_url, 429)
        with self.assertRaises(ValueError): self.publish(opener=limited)
        self.assertEqual(len(self.calls), 1); self.assertEqual(self.memory.data[model.HEAD], before)
        inputs = [model.strict(v) for k, v in self.memory.data.items() if k.startswith(store.PRIVATE) and v.startswith(b'{')]
        inputs = next(v for v in inputs if 'attempts' in v)
        self.assertEqual(len(inputs['attempts']), 14)
        self.assertTrue(all(a['status'] == 'rate_limit_not_attempted' for a in inputs['attempts'][1:]))

    def test_truncated_body_denied_predecessor_and_redirect_cannot_replace_head(self):
        before = self.memory.data[model.HEAD]
        for opener in (lambda req, timeout: Response(response([]), req.full_url, headers={'Content-Length': '999999'}),
                       lambda req, timeout: Response(response([]), 'https://unreviewed.example/')):
            with self.assertRaises(ValueError): self.publish(opener=opener)
            self.assertEqual(self.memory.data[model.HEAD], before)
        self.memory.denied.add(model.HEAD)
        with self.assertRaises(Error): self.publish()
        self.assertEqual(self.calls, [])

    def test_concurrent_public_writer_never_overwritten(self):
        def race(key):
            if key == model.HEAD: self.memory.data[key] = b'{"other_writer":true}'
        self.memory.race = race
        with self.assertRaises(Error): self.publish()
        self.assertEqual(self.memory.data[model.HEAD], b'{"other_writer":true}')

    def test_archived_compiler_changes_and_original_corruption_refuse_replay(self):
        self.publish(); packet = self.packet(); real = store.compiler_bytes(NATIVE)
        with patch.object(store, 'compiler_bytes', return_value={**real, 'sec_search_model.py': b'changed'}):
            with self.assertRaises(ValueError): store.replay(self.memory, 'justhodl-dashboard-live', NATIVE, 'search', packet)
        ref = packet['publication_context']['manifest']; self.memory.data[ref['key']] += b' '
        with self.assertRaises(ValueError): store.replay(self.memory, 'justhodl-dashboard-live', NATIVE, 'search', packet)

    def test_key_scope_and_http_requests_have_no_acquisition_or_write(self):
        for key in ('data/pm-decision.json', 'private/account.json', 'learning/morning_run_log.json', 'data/8k-filings.json'):
            with self.assertRaises(ValueError): store.get(self.memory, 'justhodl-dashboard-live', key, 'search')
        result = self.publish(event={'requestContext': {}})
        self.assertEqual(result['statusCode'], 409); self.assertEqual(self.memory.reads, [])
        self.assertEqual(self.calls, []); self.assertEqual(self.memory.writes, [])

    def test_attempt_clocks_query_recipe_and_previous_schema_validated(self):
        for alter in (lambda i: i['attempts'].pop(), lambda i: i['attempts'][0].update(url='https://example.com/'),
                      lambda i: i.update(generated_at='2026-09-25T10:00:00Z'),
                      lambda i: i['attempts'][0].update(received_at='2026-09-28T10:00:00Z')):
            with self.assertRaises(ValueError): build([hit()], alter=alter)
        with self.assertRaises(ValueError): build([], prior={'all_tickers': 'not a whole population'})

    def test_native_adapter_preserves_queries_cadence_and_never_emits_keyword_events(self):
        fake = ModuleType('boto3'); fake.client = lambda *a, **kw: self.memory
        sentry = ModuleType('_sentry_lite'); sentry.track_errors = lambda fn: fn
        spec = importlib.util.spec_from_file_location('isolated_sec_search_native', NATIVE)
        native = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, {'boto3': fake, '_sentry_lite': sentry}): spec.loader.exec_module(native)
        self.assertEqual(native.SIGNAL_QUERIES, QUERIES)
        with patch.object(store, 'run', return_value={'statusCode': 200}) as run:
            self.assertEqual(native.lambda_handler({}, None)['statusCode'], 200)
            self.assertEqual(run.call_args.args[3], RECIPE)
        with patch.object(store, 'run', side_effect=RuntimeError('sensitive detail')):
            self.assertNotIn('sensitive detail', str(native.lambda_handler({}, None)))
        self.assertEqual(native.lambda_handler({'httpMethod': 'GET'}, None)['statusCode'], 409)
        config = model.strict((NATIVE.parent.parent / 'config.json').read_bytes())
        self.assertEqual(config['schedule']['cron'], 'cron(7 21 * * ? *)')


def run():
    result = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(Tests))
    if not result.wasSuccessful(): raise SystemExit(1)
