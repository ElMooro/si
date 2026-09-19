from pathlib import Path
from unittest.mock import patch
import ast
import gzip
import hashlib
import io
import json
import unittest

import flow_desk_research as m
import flow_desk_store as s
from evidence_store import public_source_url

AT = '2026-09-19T12:00:00+00:00'
SOURCE_AT = '2026-09-19T11:00:00+00:00'
DATES = ['2026-09-10', '2026-09-11', '2026-09-14', '2026-09-15', '2026-09-16', '2026-09-17', '2026-09-18']


def fixture(flows=None, claim=True, period_end=None):
    raw = {}
    def put(prefix, doc):
        body = m.encoded(doc); sha = m.digest(doc); key = prefix + sha + '.json'
        raw[key] = body
        return {'key': key, 'sha256': sha, 'bytes': len(body), **({'rows': len(doc['rows'])} if 'rows' in doc else {})}
    classifications = {
        '1': {'portfolioId': 1, 'localExchangeTicker': 'IVV', 'isin': 'US4642872000', 'fundName': 'Test US equity',
              'aladdinAssetClass': 'Equity', 'aladdinSubAssetClass': 'Large/Mid Cap'},
        '2': {'portfolioId': 2, 'localExchangeTicker': 'AGG', 'isin': 'US4642872265', 'fundName': 'Test bond fund',
              'aladdinAssetClass': 'Fixed Income', 'aladdinSubAssetClass': 'Multi Sectors'},
    }
    body = m.encoded(classifications); sha = hashlib.sha256(body).hexdigest()
    url = public_source_url(m.ISSUER_URL); request = hashlib.sha256(url.encode()).hexdigest()
    original_key = f'data/evidence/etf_original/{request}/{sha}.bin.gz'
    raw[original_key] = body
    original = {'url': m.ISSUER_URL, 'acquired_at': SOURCE_AT,
                'evidence': {'contract': 'source-evidence.v1', 'provider': 'etf_original', 'captured': True,
                             'key': original_key, 'sha256': sha, 'bytes': len(body), 'source_url': url,
                             'first_received_at': SOURCE_AT}}
    original_input = put(m.ETF + 'inputs/', {'originals': {'ishares_catalog': original}})
    reference = put(m.ETF + 'histories/', {'contract': 'etf-issuer-reference-dates.v1',
                    'reference_fund': 'IVV', 'rows': [{'date': v} for v in DATES]})
    entries = {}
    flows = flows if flows is not None else [None, '10', '20', '30', '40', '50', '60']
    for pid, ticker in (('1', 'IVV'), ('2', 'AGG')):
        item = classifications[pid]
        identity = {'issuer': 'iShares', 'portfolio_id': int(pid), 'isin': item['isin'],
                    'fund_name': item['fundName'], 'currency': 'USD'}
        rows = [{'date': day, 'nav_valued_share_change_decimal': value,
                 'flow_status': 'descriptive_estimate' if value is not None else 'excluded'}
                for day, value in zip(DATES, flows)]
        history = put(m.ETF + 'histories/', {'contract': 'etf-native-history.v1',
                                           'ticker': ticker, 'identity': identity, 'rows': rows})
        window = sum((m.dec(v) for v in flows[1:6]), m.Decimal(0)) if all(v is not None for v in flows[1:6]) else None
        entries[ticker] = {'identity': identity, 'source_status': 'retained_native_history', 'history': history,
            'category': 'BROAD_EQUITY_US' if ticker == 'IVV' else 'CREDIT', 'quality': {'status': 'recent_source_check'},
            'observation_date': DATES[-1], 'net_flow_5d_usd': float(window) if claim and window is not None else None,
            'flow_windows': {'5d': {'start_date': DATES[0], 'end_date': period_end or DATES[5], 'value_decimal': m.ds(window)}}}
    entries['QQQ'] = {'identity': {'issuer': None}, 'history': None, 'source_status': 'no_reviewed_native_history',
                      'category': 'BROAD_EQUITY_US', 'quality': {'status': 'unavailable'}}
    output = {'contract': 'etf-original-research.v1', 'generated_at': SOURCE_AT, 'by_etf': entries,
              'reference_calendar': {**reference, 'latest_date': DATES[-1]},
              'aggregation_period': {'end_date': DATES[5]}, **m.PERMISSION}
    output_ref = put(m.ETF + 'outputs/', output)
    manifest = put(m.ETF + 'runs/', {'contract': 'etf-original-replay.v1', 'generated_at': SOURCE_AT,
                    'input': original_input, 'output': output_ref, 'output_sha256': output_ref['sha256']})
    current = {**output, 'replay': {'manifest_key': manifest['key'], 'output_sha256': output_ref['sha256']}}
    inputs = {'contract': 'flow-desk-inputs.v1', 'etf_manifest': manifest, 'contexts': {},
              'tic_manifest': None, 'legacy': {'fixture': True}}
    return raw, inputs, current


class StorageError(Exception):
    def __init__(self, code):
        self.response = {'Error': {'Code': code}}


class Storage:
    def __init__(self):
        self.data = {}
        self.writes = []

    def get_object(self, **kw):
        if kw['Key'] not in self.data:
            raise StorageError('NoSuchKey')
        body = self.data[kw['Key']]
        return {'Body': io.BytesIO(body), 'ETag': hashlib.sha256(body).hexdigest()}

    def put_object(self, **kw):
        old = self.data.get(kw['Key'])
        if kw.get('IfNoneMatch') == '*' and old is not None:
            raise StorageError('PreconditionFailed')
        if kw.get('IfMatch') and (old is None or hashlib.sha256(old).hexdigest() != kw['IfMatch']):
            raise StorageError('PreconditionFailed')
        self.data[kw['Key']] = kw['Body']
        self.writes.append(kw['Key'])


class Tests(unittest.TestCase):
    def test_actual_http_handler_never_collects_or_calls_ai(self):
        path = Path(s.__file__).with_name('lambda_function.py')
        node = next(v for v in ast.parse(path.read_text()).body if isinstance(v, ast.FunctionDef) and v.name == 'lambda_handler')
        client = Storage(); client.data[m.CURRENT] = m.encoded({'contract': m.CONTRACT, 'call': None})
        scope = {'json': json, 's3': client, 'BUCKET': 'fixture'}
        exec(compile(ast.Module(body=[node], type_ignores=[]), str(path), 'exec'), scope)
        with patch.object(s, 'run', side_effect=AssertionError('HTTP must not collect')):
            result = scope['lambda_handler']({'requestContext': {'http': {'method': 'GET'}}})
            self.assertEqual(result['statusCode'], 200)
            self.assertEqual(result['headers']['Cache-Control'], 'no-store')
            self.assertEqual(json.loads(result['body']), json.loads(client.data[m.CURRENT]))
            client.data[m.CURRENT] = m.encoded({'legacy': True})
            self.assertEqual(scope['lambda_handler']({'httpMethod': 'GET'})['statusCode'], 503)

    def test_same_period_subtotals_and_issuer_taxonomy(self):
        raw, inputs, _ = fixture()
        output, histories = m.build(inputs, raw.__getitem__, AT)
        group = output['groups']['configured:EQUITY_US']
        self.assertEqual(group['value_decimal'], '150')
        self.assertEqual(group['covered_members'], ['IVV'])
        self.assertEqual(group['unavailable_members'], ['QQQ'])
        self.assertEqual(group['period'], {'start_date': DATES[0], 'end_date': DATES[5]})
        self.assertEqual(output['funds']['AGG']['issuer_classification']['sub_asset_class'], 'Multi Sectors')
        self.assertIn('Mixed bond', output['groups']['configured:CREDIT']['label'])
        history = json.loads(histories[group['history']['key']])
        self.assertEqual(len(history['rows']), len(DATES))
        self.assertEqual(history['rows'][-2]['five_observation_decimal'], '150')
        self.assertEqual(history['rows'][-1]['five_observation_decimal'], '200')
        self.assertFalse(output['calls_eligible'])
        self.assertIsNone(output['inst_vs_retail']['institutional'])
        self.assertIsNone(output['hot_money']['call'])
        self.assertEqual(output['dependency_roots'], ['ETF_ISSUER_OBSERVATIONS'])

    def test_zero_is_an_observation_not_a_missing_or_warming_fund(self):
        raw, inputs, _ = fixture([None, '0', '0', '0', '0', '0', '0'])
        output, _ = m.build(inputs, raw.__getitem__, AT)
        row = output['groups']['configured:EQUITY_US']
        self.assertEqual(row['direction'], 'ZERO')
        self.assertEqual(row['value_decimal'], '0')
        self.assertEqual(row['coverage_count'], 1)
        missing = output['groups']['configured:GOLD_WRAPPERS']
        self.assertIsNone(missing['value_decimal'])
        self.assertIsNone(missing['direction'])

    def test_excluded_observation_never_compresses_five_date_window(self):
        raw, inputs, _ = fixture([None, '10', '20', None, '40', '50', '60'])
        output, histories = m.build(inputs, raw.__getitem__, AT)
        group = output['groups']['configured:EQUITY_US']
        self.assertIsNone(group['net_flow_5d_usd'])
        history = json.loads(histories[group['history']['key']])
        self.assertIsNone(history['rows'][-1]['five_observation_decimal'])

    def test_claimed_source_window_must_match_full_history(self):
        raw, inputs, _ = fixture(period_end=DATES[-1])
        with self.assertRaisesRegex(ValueError, 'five-observation'):
            m.build(inputs, raw.__getitem__, AT)

    def test_source_withheld_estimate_is_not_reinstated_from_history(self):
        raw, inputs, _ = fixture(claim=False)
        output, histories = m.build(inputs, raw.__getitem__, AT)
        self.assertIsNone(output['groups']['configured:EQUITY_US']['value_decimal'])
        self.assertEqual(output['quality']['aligned_funds'], 0)
        group = output['groups']['configured:EQUITY_US']
        self.assertEqual(json.loads(histories[group['history']['key']])['rows'][-2]['five_observation_decimal'], '150')

    def test_expired_source_keeps_history_but_current_estimates_abstain(self):
        raw, inputs, _ = fixture()
        output, histories = m.build(inputs, raw.__getitem__, '2026-09-22T12:00:00+00:00')
        self.assertEqual(output['quality']['status'], 'stale_source')
        self.assertEqual(output['quality']['aligned_funds'], 0)
        self.assertIsNone(output['groups']['configured:CREDIT']['net_flow_5d_usd'])
        self.assertTrue(histories)
        with self.assertRaises(ValueError):
            m.build(inputs, raw.__getitem__, '2026-09-18T12:00:00+00:00')

    def test_issuer_original_hash_and_history_hash_are_both_required(self):
        for kind in ('original', 'history'):
            raw, inputs, _ = fixture()
            key = next(k for k in raw if ('/evidence/' in k if kind == 'original' else '/histories/' in k))
            raw[key] = b'{}'
            with self.assertRaises(ValueError):
                m.build(inputs, raw.__getitem__, AT)

    def test_excluded_row_cannot_carry_a_numeric_flow(self):
        history = {'contract': 'etf-native-history.v1', 'rows': [
            {'date': DATES[0], 'flow_status': 'corporate_action_review', 'nav_valued_share_change_decimal': '100'}]}
        with self.assertRaises(ValueError):
            m.series_values(history, DATES)

    def test_overlapping_members_are_explicit_and_never_new_votes(self):
        raw, inputs, _ = fixture()
        output, _ = m.build(inputs, raw.__getitem__, AT)
        self.assertGreater(len(output['overlapping_memberships']['IVV']), 1)
        self.assertIn('configured:GOLD_WRAPPERS', output['overlapping_memberships']['IAU'])
        self.assertEqual(output['additional_independent_votes'], 0)
        self.assertTrue(all(v['additional_independent_votes'] == 0 for v in output['groups'].values()))

    def test_nonfinite_json_context_retained_without_promoting_it(self):
        client = Storage()
        source = 'data/dark-pool.json'
        client.data[source] = b'{"score":NaN}'
        with patch.object(s, 'now', return_value=AT):
            contexts = s.capture_contexts(client, 'fixture')
        self.assertEqual(contexts[source]['status'], 'unparseable')
        output = m.retained_contexts({'contexts': contexts}, s.reader(client, 'fixture'), AT)
        self.assertEqual(output[source]['source_status'], 'retained_unparseable_context')
        self.assertFalse(output[source]['calls_eligible'])
        self.assertEqual(client.data[contexts[source]['artifact']['key']], client.data[source])

    def test_current_pointer_must_be_exact_retained_output(self):
        raw, inputs, current = fixture()
        s.current_matches(current, inputs['etf_manifest'], m.ETF, raw.__getitem__)
        current['by_etf']['IVV']['net_flow_5d_usd'] = 99999
        with self.assertRaises(ValueError):
            s.current_matches(current, inputs['etf_manifest'], m.ETF, raw.__getitem__)

    def test_complete_legacy_history_is_preserved_without_rewriting_it(self):
        client = Storage()
        client.data[m.CURRENT] = b'{"legacy":"full packet"}'
        client.data[s.LEGACY_HISTORY] = m.encoded({str(i): {'score': i} for i in range(400)})
        marker = s.preserve(client, 'fixture', s.reader(client, 'fixture'))
        self.assertEqual(len(marker['objects']), 2)
        for ref in marker['objects']:
            self.assertEqual(client.data[s.PRIVATE + ref['sha256'] + '.bin'], client.data[ref['source']])
        self.assertNotIn(s.LEGACY_HISTORY, client.writes)

    def test_real_store_replays_and_http_size_stays_bounded(self):
        raw, _, current = fixture()
        client = Storage()
        for key, body in raw.items():
            client.data[key] = gzip.compress(body, mtime=0) if key.endswith('.gz') else body
        client.data[m.CURRENT] = m.encoded({'legacy': True})
        client.data[s.LEGACY_HISTORY] = m.encoded({'2020-01-01': {'score': 99}})
        client.data['data/etf-true-flows.json'] = m.encoded(current)
        with patch.object(s, 'now', return_value=AT):
            result = s.run(client, 'fixture')
        self.assertTrue(result['published'])
        read = s.reader(client, 'fixture')
        manifest = json.loads(read(result['replay']['manifest_key']))
        output = s.replay(manifest, read)
        self.assertEqual(output['contract'], m.CONTRACT)
        self.assertLess(len(client.data[m.CURRENT]), 4 * 1024 * 1024)
        history = next(iter(output['groups'].values()))['history']['key']
        client.data[history] = b'{}'
        with self.assertRaises(ValueError):
            s.replay(manifest, s.reader(client, 'fixture'))

    def test_publication_refuses_older_source_and_same_clock_conflict(self):
        client = Storage()
        packet = {'contract': m.CONTRACT, 'generated_at': AT, 'source_generated_at': SOURCE_AT}
        client.data[m.CURRENT] = m.encoded(packet)
        self.assertFalse(s.publish(client, 'fixture', {**packet, 'source_generated_at': '2026-09-18T00:00:00+00:00'}))
        with self.assertRaises(ValueError):
            s.publish(client, 'fixture', {**packet, 'changed': True})
        self.assertEqual(json.loads(client.data[m.CURRENT]), packet)

    def test_failed_candidate_keeps_complete_previous_publication(self):
        raw, _, current = fixture(); client = Storage()
        for key, body in raw.items():
            client.data[key] = gzip.compress(b'{}' if '/evidence/' in key else body, mtime=0) if key.endswith('.gz') else body
        before = m.encoded({'legacy': 'complete preceding packet'})
        client.data[m.CURRENT] = before
        client.data[s.LEGACY_HISTORY] = m.encoded({'2020-01-01': {'score': 99}})
        client.data['data/etf-true-flows.json'] = m.encoded(current)
        with patch.object(s, 'now', return_value=AT):
            with self.assertRaises(ValueError):
                s.run(client, 'fixture')
        self.assertEqual(client.data[m.CURRENT], before)
        self.assertTrue(any('/attempts/' in key for key in client.writes))
