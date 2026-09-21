"""Profile audit retains raw units and fails closed on unreviewed destinations."""
from pathlib import Path
from decimal import Decimal, localcontext
from collections import Counter
import ast, json, sys, types, unittest, urllib.parse

ROOT = Path(__file__).resolve().parents[1]
PATH = ROOT/'aws/ops/staged/ops_5982_etf_desk_profile_preflight.py'


def functions():
    sys.path.insert(0,str(ROOT/'aws/shared'))
    import etf_holdings_native as native
    namespace = {'urllib': types.SimpleNamespace(parse=urllib.parse), 'Decimal': Decimal, 'localcontext':localcontext, 'Counter': Counter,'native':native}
    tree = ast.parse(PATH.read_text(encoding='utf-8'))
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id in ('EXPOSURES', 'NUMERIC') for t in node.targets):
            exec(compile(ast.Module(body=[node], type_ignores=[]), str(PATH), 'exec'), namespace)
        if isinstance(node, ast.FunctionDef) and node.name in ('checked_url', 'describe', 'probe'):
            exec(compile(ast.Module(body=[node], type_ignores=[]), str(PATH), 'exec'), namespace)
    return namespace


class ProfileEvidence(unittest.TestCase):
    def setUp(self): self.ns = functions()

    def test_only_bound_profile_queries_can_receive_credentials(self):
        check = self.ns['checked_url']
        allowed = 'https://api.polygon.io/etf-global/v1/profiles?composite_ticker=SPY&limit=1'
        self.assertEqual(check(allowed), allowed)
        for url in ('http://api.polygon.io/etf-global/v1/profiles?limit=1',
                'https://api.polygon.io.evil.invalid/etf-global/v1/profiles?limit=1',
                'https://api.polygon.io@evil.invalid/etf-global/v1/profiles?limit=1',
                'https://user@api.polygon.io/etf-global/v1/profiles?limit=1',
                'https://api.polygon.io:444/etf-global/v1/profiles?limit=1',
                'https://api.polygon.io/etf-global/v1/fund-flows?limit=1',
                allowed+'&apiKey=anything', allowed+'&limit=2', allowed+'#fragment',
                'https://api.polygon.io/etf-global/v1/profiles?cursor=', allowed+'&cursor='+'x'*8192):
            with self.subTest(url=url), self.assertRaises(AssertionError): check(url)

    def test_zero_small_fee_negative_leverage_remain_exact_and_unqualified(self):
        row = {'composite_ticker': 'SQQQ', 'aum': Decimal('0'), 'net_expenses': Decimal('0.0009'),
            'management_fee': Decimal('0.0945'), 'bid_ask_spread': Decimal('0.000042'), 'levered_amount': Decimal('-3')}
        original = dict(row); out = self.ns['describe']([row])[0]['numeric_fields']
        self.assertEqual(row, original)
        for key, value in row.items():
            if isinstance(value, Decimal):
                self.assertEqual(out[key]['value'], str(value)); self.assertTrue(out[key]['present'])
                self.assertFalse(out[key]['unit_certified'])
        self.assertFalse(out['num_holdings']['present']); self.assertIsNone(out['num_holdings']['value'])

    def test_exposure_shapes_and_missing_fields_are_distinguished(self):
        row = {'sector_exposure': {'cash': Decimal('0'), 'equity': Decimal('1.1'), 'short': Decimal('-0.1'), 'unknown': None},
            'currency_exposure': [{'currency': 'usd', 'weight': Decimal('1.003')}]}
        out = self.ns['describe']([row])[0]['exposures']
        self.assertEqual(out['sector_exposure']['raw_numeric_sum'], '1.0')
        self.assertEqual(out['sector_exposure']['numeric_entries'], 3)
        self.assertEqual(out['sector_exposure']['non_numeric_entries'], 1)
        self.assertEqual(out['currency_exposure']['type'], 'list')
        self.assertEqual(out['currency_exposure']['child_field_counts'], {'currency': 1, 'weight': 1})
        self.assertFalse(out['maturity_exposure']['present'])
        self.assertTrue(all(v['unit_certified'] is False for v in out.values()))

    def arrange_probe(self, pages):
        self.ns['ENDPOINT'] = 'https://api.polygon.io/etf-global/v1/profiles'
        seen = []
        def request(client, credential, url, budget):
            seen.append(url); return pages.pop(0), {'status': 'retained', 'original': {'sha256': 'fixture'}}
        self.ns['request_original'] = request
        return self.ns['probe'], seen

    def test_duplicate_profiles_are_retained_and_not_arbitrarily_selected(self):
        row = {'composite_ticker': 'SPY', 'processed_date': '2026-09-18', 'effective_date': '2026-09-17'}
        probe, seen = self.arrange_probe([{'results': [row]}, {'results': [row, {**row, 'aum': Decimal('2')}]}])
        out = probe(None, 'fixture', 'SPY', '2026-09-21', {})
        self.assertEqual(out['rows'], 2); self.assertFalse(out['single_profile_unambiguous'])
        self.assertEqual(out['status'], 'complete_returned_profile_snapshot'); self.assertEqual(len(out['source_rows']), 2)
        self.assertNotIn('apiKey', ''.join(seen))

    def test_wrong_ticker_or_future_processed_date_rejected(self):
        for ticker, processed in [('VOO', '2026-09-18'), ('SPY', '2026-09-22')]:
            probe, _ = self.arrange_probe([{'results': [{'composite_ticker': ticker, 'processed_date': processed}]}])
            with self.assertRaises(AssertionError): probe(None, 'fixture', 'SPY', '2026-09-21', {})

    def test_followup_failure_cannot_be_complete(self):
        row = {'composite_ticker': 'SPY', 'processed_date': '2026-09-18', 'effective_date': '2026-09-17'}
        probe, _ = self.arrange_probe([{'results': [row]},
            {'results': [row], 'next_url': 'https://api.polygon.io/etf-global/v1/profiles?cursor=next'}, None])
        out = probe(None, 'fixture', 'SPY', '2026-09-21', {})
        self.assertEqual(out['status'], 'incomplete'); self.assertEqual(len(out['pages']), 1)
        self.assertNotIn('source_rows', out)

    def test_extreme_numeric_exposure_has_no_misleading_sum(self):
        out=self.ns['describe']([{'sector_exposure':{'valid':Decimal('0.5'),'extreme':Decimal('1e-1000000')}}])[0]
        rec=out['exposures']['sector_exposure']
        self.assertIsNone(rec['raw_numeric_sum']);self.assertFalse(rec['sum_numeric_bounds_valid'])
        self.assertEqual(rec['numeric_entries'],2)

    def test_multiple_date_selection_rows_are_not_arbitrarily_reduced(self):
        row={'composite_ticker':'SPY','processed_date':'2026-09-18'}
        probe,seen=self.arrange_probe([{'results':[row,dict(row)]}])
        with self.assertRaises(AssertionError):probe(None,'fixture','SPY','2026-09-21',{})
        self.assertEqual(len(seen),1)

    def test_repeated_page_is_rejected_before_third_read(self):
        row={'composite_ticker':'SPY','processed_date':'2026-09-18','effective_date':'2026-09-17'}
        url='https://api.polygon.io/etf-global/v1/profiles?composite_ticker=SPY&processed_date=2026-09-18&limit=5000'
        probe,seen=self.arrange_probe([{'results':[row]},{'results':[row],'next_url':url}])
        with self.assertRaises(AssertionError):probe(None,'fixture','SPY','2026-09-21',{})
        self.assertEqual(len(seen),2)


if __name__ == '__main__': unittest.main(verbosity=2)
