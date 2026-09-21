"""Adversarial source reconstruction and unadjusted position comparisons."""
from pathlib import Path
import copy, json, sys, unittest
from decimal import Decimal
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import etf_holdings_native as n

GENERATED = '2026-09-21T07:00:00+00:00'


def row(**kw):
    return {'composite_ticker': 'SPY', 'processed_date': '2026-09-18', 'effective_date': '2026-09-17',
        'constituent_ticker': 'ABC', 'constituent_name': 'Synthetic common equity', 'figi': 'TEST-ID-A',
        'currency_traded': 'USD', 'asset_class': 'Equity', 'security_type': 'Common Stock',
        'shares_held': 120, 'market_value': 6000, 'weight': .2, 'constituent_rank': 1, **kw}


def collection(rows=None, processed='2026-09-18', acquired='2026-09-21T06:00:00+00:00', pages=1):
    rows = rows if rows is not None else [row()]
    rawmap = {}
    def page(url, values, following=None):
        doc = {'status': 'OK', 'results': values, 'count': len(values)}
        if following: doc['next_url'] = following
        raw = json.dumps(doc).encode();digest = n.sha(raw);key = n.PRIVATE + digest + '.bin';rawmap[key] = raw
        return {'url': url, 'original': {'key': key, 'sha256': digest, 'bytes': len(raw)}, 'acquired_at': acquired}
    selection = page(n.selection_url('SPY', '2026-09-21'), [row(processed_date=processed)])
    url = n.snapshot_url('SPY', processed)
    split = max(1, len(rows) // pages)
    entries = []
    for i in range(pages):
        part = rows[i * split:] if i == pages - 1 else rows[i * split:(i + 1) * split]
        following = n.ENDPOINT + '?cursor=fixture' + str(i + 1) if i < pages - 1 else None
        entries.append(page(url, part, following));url = following
    return {'ticker': 'SPY', 'cutoff': '2026-09-21', 'selection': selection, 'pages': entries,
            'status': 'complete_returned_snapshot'}, rawmap


class Holdings(unittest.TestCase):
    def test_decimal_expansion_is_bounded_before_formatting_without_erasing_zero(self):
        for value in ('1e-1000000000', '1e1000000000', '1.'+'2'*128,
                '1000000000000000000000000000000.0001'):
            with self.subTest(value=value), self.assertRaises(ValueError): n.decimal(Decimal(value))
        for value in ('0e-1000000000', '-0e1000000000'):
            self.assertEqual(n.ds(n.decimal(Decimal(value))), '0')
        self.assertEqual(n.ds(n.decimal(Decimal('0.0000000000012300'))), '0.0000000000012300')
        self.assertEqual(len(n.ds(n.decimal(Decimal('1e-128')))), 130)
        self.assertEqual(n.ds(n.decimal(Decimal('9007199254740993.123400'))), '9007199254740993.123400')

    def test_extreme_nonzero_field_is_named_and_other_original_values_survive(self):
        source={'page':0,'row_index':0,'sha256':'f'*64}
        normalized=n.normalize(row(weight=Decimal('1e-1000000000'),market_value=0), 'SPY','2026-09-18',source)
        self.assertEqual(normalized['field_errors'], ['weight'])
        self.assertIsNone(normalized['weight_raw_decimal'])
        self.assertEqual(normalized['market_value_raw_decimal'], '0')
        self.assertEqual(normalized['shares_held_raw_decimal'], '120')
        self.assertEqual(normalized['source'], source)

    def test_unrepresentable_json_exponent_is_quarantined_as_source_content(self):
        raw=b'{"status":"OK","results":[{"weight":1e999999999999999999999999}]}'
        digest=n.sha(raw);key=n.PRIVATE+digest+'.bin'
        with self.assertRaises(n.SourceRejected): n.original({'key':key,'sha256':digest,'bytes':len(raw)},lambda _:raw)

    def test_all_asset_classes_missing_tickers_zero_and_negative_values_survive(self):
        c, raw = collection([row(), row(constituent_ticker=None, figi=None, asset_class='Cash',
            security_type='Cash', weight=0, shares_held=0, market_value=0, constituent_rank=2),
            row(constituent_ticker=None, figi='DERIVATIVE-ID', asset_class='Derivative',
                security_type='Swap', weight=-.1, shares_held=-5, market_value=-20, constituent_rank=3)], pages=2)
        p = n.reconstruct(c, raw.__getitem__, GENERATED)
        self.assertEqual(len(p['rows']), 3);self.assertEqual(p['quality']['missing_ticker_rows'], 2)
        self.assertEqual(p['weight_audit']['raw_observed_sum_decimal'], '0.1')
        self.assertEqual(p['rows'][1]['weight_raw_decimal'], '0')
        self.assertEqual(p['rows'][2]['shares_held_raw_decimal'], '-5')
        self.assertFalse(p['weight_audit']['normalized']);self.assertFalse(p['quality']['weight_unit_certified'])
        self.assertFalse(p['quality']['market_value_currency_certified']);self.assertFalse(p['quality']['current_holdings_confirmed'])
        self.assertEqual(p['quality']['effective_age_days'], {'2026-09-17': 4})
        for index, r in enumerate(p['rows']):
            source = p['originals'][r['source']['page']]
            original = json.loads(raw[source['key']])['results'][r['source']['row_index']]
            self.assertEqual(original['shares_held'], int(r['shares_held_raw_decimal']))
            self.assertEqual(r['source']['sha256'], source['sha256'])

    def test_missing_numeric_is_not_zero_and_bad_numeric_is_named(self):
        c, raw = collection([row(weight=None, market_value='6000', shares_held=False)])
        p = n.reconstruct(c, raw.__getitem__, GENERATED);r = p['rows'][0]
        self.assertIsNone(r['weight_raw_decimal']);self.assertIsNone(p['weight_audit']['raw_observed_sum_decimal'])
        self.assertEqual(r['field_errors'], ['market_value', 'shares_held'])
        self.assertEqual(p['quality']['rows_with_field_errors'], 1)

    def test_date_selection_snapshot_chain_and_digest_are_exact(self):
        c, raw = collection([row(), row(figi='OTHER', constituent_rank=2)], pages=2)
        for mutate in (
            lambda x: x['pages'].reverse(),
            lambda x: x['pages'].pop(),
            lambda x: x['pages'][0]['original'].update(bytes=1),
            lambda x: x['selection'].update(url=n.selection_url('SPY', '2026-09-20')),
            lambda x: x['pages'][1].update(acquired_at='2026-09-22T06:00:00+00:00')):
            changed = copy.deepcopy(c);mutate(changed)
            with self.assertRaises(ValueError): n.reconstruct(changed, raw.__getitem__, GENERATED)
        first = c['pages'][0]['original']['key'];raw[first] += b' '
        with self.assertRaises(ValueError): n.reconstruct(c, raw.__getitem__, GENERATED)

    def test_partial_chain_never_certifies_complete(self):
        c, raw = collection([row(), row(figi='OTHER', constituent_rank=2)], pages=2)
        c['pages'].pop();c['status'] = 'incomplete'
        p = n.reconstruct(c, raw.__getitem__, GENERATED)
        self.assertEqual(p['quality']['status'], 'incomplete');self.assertFalse(p['quality']['pagination_complete'])
        self.assertEqual(len(p['rows']), 1)

    def test_wrong_fund_or_processing_date_does_not_silently_drop_rows(self):
        for r in (row(composite_ticker='VOO'), row(processed_date='2026-09-17'),
                  row(effective_date='2026-09-19'), row(effective_date='2026-02-30')):
            c, raw = collection([row(), r])
            with self.assertRaises(ValueError): n.reconstruct(c, raw.__getitem__, GENERATED)

    def test_source_quarantine_is_distinct_from_corrupted_retained_bytes(self):
        c, raw = collection([row(composite_ticker='OTHER')])
        rejected = n.reconstruct_or_reject(c, raw.__getitem__, GENERATED)
        self.assertEqual(rejected['quality']['status'], 'source_rejected')
        self.assertEqual(rejected['retained_attempt'], c)
        self.assertIsNone(rejected['quality']['source_row_count'])
        key = c['pages'][0]['original']['key'];raw[key] += b' '
        with self.assertRaises(ValueError): n.reconstruct_or_reject(c, raw.__getitem__, GENERATED)

    def test_lagged_holdings_do_not_become_current_on_processing_day(self):
        c, raw = collection([row(effective_date='2026-07-31')])
        p = n.reconstruct(c, raw.__getitem__, GENERATED)
        self.assertEqual(p['effective_dates'], {'2026-07-31': 1});self.assertEqual(p['quality']['effective_age_days']['2026-07-31'], 52)
        self.assertFalse(p['quality']['current_holdings_confirmed']);self.assertFalse(p['quality']['source_check_overdue'])
        late = n.reconstruct(c, raw.__getitem__, '2026-09-23T07:00:00Z')
        self.assertTrue(late['quality']['source_check_overdue'])
        self.assertEqual(late['source_valid_until'], p['source_valid_until'])

    def test_original_json_rejects_duplicate_fields_nonfinite_and_wrong_count(self):
        for raw in (b'{"status":"OK","results":[],"status":"OK"}',
                    b'{"status":"OK","results":[],"x":NaN}',
                    b'{"status":"OK","results":[],"count":1}'):
            digest = n.sha(raw);ref = {'key': n.PRIVATE + digest + '.bin', 'sha256': digest, 'bytes': len(raw)}
            with self.assertRaises(ValueError): n.original(ref, lambda _: raw)

    def test_unreviewed_pagination_destination_is_rejected(self):
        for bad in (n.ENDPOINT + '?apiKey=fixture', n.ENDPOINT + '?cursor=x&cursor=y',
            n.ENDPOINT + '?cursor=x#fragment', 'https://api.polygon.io@elsewhere.invalid/etf-global/v1/constituents?cursor=x'):
            with self.assertRaises(ValueError): n.next_url(bad)

    def test_exact_identity_comparison_withholds_trade_inference_and_missing_side_zero(self):
        a, ra = collection([row(), row(figi='ADDED-ID', constituent_rank=2), row(figi=None, constituent_ticker=None)])
        b, rb = collection([row(processed_date='2026-08-20', effective_date='2026-08-19', weight=.15, shares_held=100),
            row(processed_date='2026-08-20', effective_date='2026-08-19', figi='REMOVED-ID', constituent_rank=2)], processed='2026-08-20')
        current = n.reconstruct(a, ra.__getitem__, GENERATED);prior = n.reconstruct(b, rb.__getitem__, GENERATED)
        out = n.compare(current, prior)
        matched = next(r for r in out['rows'] if r['status'] == 'observed_in_both')
        self.assertEqual(matched['shares_held_change_raw_decimal'], '20');self.assertEqual(matched['weight_change_raw_decimal'], '0.05')
        self.assertTrue(out['comparable_snapshots']);self.assertEqual(len(out['current_unidentified_rows']), 1)
        for r in out['rows']:
            self.assertIsNone(r['inferred_trade_usd'])
            if r['status'] != 'observed_in_both': self.assertIsNone(r['shares_held_change_raw_decimal'])

    def test_ticker_collision_duplicate_identity_and_same_date_are_not_trades(self):
        c, raw = collection([row(), row(figi='OTHER-LISTING', constituent_rank=2), row(constituent_rank=3)])
        current = n.reconstruct(c, raw.__getitem__, GENERATED)
        self.assertEqual(current['quality']['duplicate_identity_rows'], 1)
        b, rb = collection([row(processed_date='2026-08-20', effective_date='2026-08-19')], processed='2026-08-20')
        prior = n.reconstruct(b, rb.__getitem__, GENERATED);out = n.compare(current, prior)
        self.assertEqual(out['identity_status_counts']['ambiguous_identity'], 1)
        self.assertEqual(out['identity_status_counts']['observed_only_in_current'], 1)
        same = n.compare(current, current)
        self.assertFalse(same['comparable_snapshots'])
        self.assertTrue(all(r['shares_held_change_raw_decimal'] is None for r in same['rows']))


if __name__ == '__main__': unittest.main(verbosity=2)
