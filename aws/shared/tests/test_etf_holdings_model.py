"""Whole source rows remain inspectable; membership never becomes fund buying."""
from pathlib import Path
from unittest import mock
import copy, io, json, sys, time, unittest, urllib.error
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import etf_holdings_native as native
import etf_holdings_model as model
import etf_holdings_collect as collector
from test_etf_holdings_native import collection, row, GENERATED


class Projection(unittest.TestCase):
    def inputs(self):
        current, raw = collection([row(), row(constituent_ticker=None, figi=None, constituent_name='Cash',
            weight=0, shares_held=None, market_value=None, constituent_rank=2)])
        prior, old = collection([row(processed_date='2026-08-21', effective_date='2026-07-31')], processed='2026-08-21')
        prior['cutoff'] = '2026-08-22';prior['selection']['url'] = native.selection_url('SPY', '2026-08-22')
        raw.update(old)
        return {'contract': 'etf-holdings-inputs.v1', 'kind': 'holdings', 'generated_at': GENERATED,
            'query_date': '2026-09-21', 'collections': {'SPY': {'current': current, 'prior': prior}},
            'contexts': {}, 'previous': None, 'provider_requests': 4, 'original_provider_bytes': sum(map(len, raw.values()))}, raw

    def test_full_pagination_index_and_missing_identifier_rows_survive(self):
        inputs, raw = self.inputs();artifacts = {}
        with mock.patch.dict(model.catalog.ETF_UNIVERSE, {'SPY': {'category': 'broad'}}, clear=True), mock.patch.object(model, 'CHUNK_ROWS', 1):
            packet = model.build(inputs, raw.__getitem__, lambda k, v: artifacts.__setitem__(k, v))
            replayed = model.build(inputs, raw.__getitem__, lambda k, v: self.assertEqual(artifacts[k], v))
        self.assertEqual(packet, replayed);self.assertEqual(packet['quality']['reconstructed_current_rows'], 2)
        for key, body in artifacts.items():self.assertIn(native.sha(body), key)
        def artifact(ref):
            b = artifacts[ref['key']];self.assertEqual(len(b), ref['bytes']);self.assertEqual(native.sha(b), ref['sha256'])
            return json.loads(b)
        snapshot = artifact(packet['funds']['SPY']['current']['snapshot'])
        self.assertEqual(len(snapshot['parts']), 2);self.assertEqual(snapshot['indexed_rows'], 2)
        self.assertEqual(len([r for ref in snapshot['index_parts'] for r in artifact(ref)['rows']]), 2)
        rows = [r for ref in snapshot['parts'] for r in artifact(ref)['rows']]
        self.assertEqual(rows[1]['constituent_name'], 'Cash');self.assertIsNone(rows[1]['identity_key'])
        directory = artifact(packet['security_directory']);self.assertEqual(directory['security_count'], 1)
        securities = [r for ref in directory['parts'] for r in artifact(ref)['securities']]
        self.assertEqual(len(securities), 1)
        security = securities[0];members = artifact(directory['buckets'][security['bucket']])['securities'][0]
        self.assertEqual(members['observed_funds'], ['SPY']);self.assertIsNone(members['portfolio_weight'])
        self.assertFalse(members['memberships'][0]['weight_unit_certified']);self.assertIsNone(members['inferred_trade_usd'])
        self.assertTrue(all(packet[k] is False for k in model.PERMISSIONS))
        projected = model.lookthrough({**packet, 'replay': {'fixture': 'retained'}}, GENERATED, {'fixture': 'protected'}, None)
        self.assertEqual(projected['funds'], packet['funds']);self.assertEqual(projected['provider_requests'], 0)
        self.assertEqual(projected['additional_independent_investment_votes'], 0);self.assertEqual(projected['top_picks'], [])

    def test_configured_universe_and_cutoffs_cannot_shrink_silently(self):
        inputs, raw = self.inputs()
        with mock.patch.dict(model.catalog.ETF_UNIVERSE, {'SPY': {}, 'VOO': {}}, clear=True):
            with self.assertRaises(ValueError):model.build(inputs, raw.__getitem__, lambda *a: None)
        inputs['collections']['SPY']['prior']['cutoff'] = '2026-08-23'
        inputs['collections']['SPY']['prior']['selection']['url'] = native.selection_url('SPY', '2026-08-23')
        with mock.patch.dict(model.catalog.ETF_UNIVERSE, {'SPY': {}}, clear=True):
            with self.assertRaises(ValueError):model.build(inputs, raw.__getitem__, lambda *a: None)


class Collection(unittest.TestCase):
    def test_missing_credential_and_expired_deadline_make_no_http_call(self):
        c = collector.Collector('', lambda _: None, time.monotonic() + 60, '2026-09-21')
        with mock.patch.object(collector.urllib.request, 'build_opener') as open_:
            pair = c.fund('SPY');self.assertEqual(pair['current']['status'], 'credential_unavailable')
            self.assertEqual(pair['prior']['cutoff'], '2026-08-22');open_.assert_not_called()
        c = collector.Collector('fixture-only', lambda _: None, time.monotonic() - 1)
        with mock.patch.object(collector.urllib.request, 'build_opener') as open_:
            self.assertEqual(c.snapshot('SPY', '2026-09-21')['status'], 'acquisition_deadline');open_.assert_not_called()

    def test_error_body_and_credential_reflection_are_not_saved(self):
        saved=[];c=collector.Collector('fixture-secret', saved.append, time.monotonic()+60)
        class Body:
            def read(self,*a):raise AssertionError('Error response bodies are not evidence')
            def close(self):pass
        opener=mock.Mock();opener.open.side_effect=urllib.error.HTTPError(native.ENDPOINT,403,'private',{},Body())
        with mock.patch.object(collector.urllib.request,'build_opener',return_value=opener):
            result=c.snapshot('SPY','2026-09-21')
        self.assertEqual(result['status'],'provider_http_error');self.assertEqual(saved,[])
        opener.open.side_effect=None;opener.open.return_value=io.BytesIO(b'{"echo":"fixture-secret"}')
        with mock.patch.object(collector.urllib.request,'build_opener',return_value=opener):
            result=c.snapshot('SPY','2026-09-21')
        self.assertEqual(result['status'],'response_rejected');self.assertEqual(saved,[])

    def test_terminal_page_failure_keeps_complete_original_prefix_without_certification(self):
        c=collector.Collector('fixture',lambda _:None,time.monotonic()+60)
        page={'url':native.snapshot_url('SPY','2026-09-18'),'acquired_at':GENERATED,'original':{'fixture':True}}
        next_=native.ENDPOINT+'?cursor=next'
        responses=[({'results':[row()]},{'selection':True}),({'results':[row()],'next_url':next_},page),
                   (None,{'status':'provider_http_error','http_status':503})]
        with mock.patch.object(c,'page',side_effect=responses):result=c.snapshot('SPY','2026-09-21')
        self.assertEqual(result['pages'],[page]);self.assertEqual(result['status'],'provider_http_error')


if __name__=='__main__':unittest.main(verbosity=2)
