"""Real complete filings plus adversarial boundaries for disclosure comparisons."""
from copy import deepcopy
from pathlib import Path
import hashlib
import json
import unittest

import holdings_native as m

FIXTURES = Path(__file__).parent / 'fixtures'
SOURCES = json.loads((FIXTURES / 'sources.json').read_bytes())['filings']
PRIOR = '0001193125-26-226661'
CURRENT = '0001193125-26-352200'
BAUPOST = '0001061768-26-000010'


def original_filing(accession):
    source = SOURCES[accession]
    cover_doc = next(v for v in source['sources'] if v['kind'] == 'cover_or_other_xml')
    table_doc = next(v for v in source['sources'] if v['kind'] == 'information_table')
    cover = m.parse_cover((FIXTURES / cover_doc['file']).read_bytes(), source['filing'], source['cik'])
    rows = m.parse_table((FIXTURES / table_doc['file']).read_bytes(), table_doc['reference'], accession, cover['usd_multiplier'])
    assert len(rows) == cover['reported_rows']
    assert m.total(v['reported_value'] for v in rows) == m.Decimal(cover['reported_value_sum'])
    return {**source['filing'], 'cover': cover, 'rows': rows}


def amended(original, accession, number, kind, rows=None):
    result = deepcopy(original)
    result.update(accession=accession, accepted_at='2026-09-%02dT20:00:00Z' % number,
                  filed_at='2026-09-%02d' % number, form='13F-HR/A')
    result['cover'].update(is_amendment=True, amendment_number=number, amendment_type=kind)
    if rows is not None:
        result['rows'] = deepcopy(rows)
    return result


class NativeHoldings(unittest.TestCase):
    def test_complete_original_fixture_hashes(self):
        for source in SOURCES.values():
            for document in source['sources']:
                raw = (FIXTURES / document['file']).read_bytes()
                ev = document['reference']['evidence']
                self.assertEqual(hashlib.sha256(raw).hexdigest(), ev['sha256'])
                self.assertEqual(len(raw), ev['bytes'])

    def test_berkshire_coca_cola_price_change_is_not_add(self):
        prior, current = original_filing(PRIOR), original_filing(CURRENT)
        a, b = m.resolve_period([current], True), m.resolve_period([prior], True)
        result = m.compare(a, b)
        row = next(v for v in result['rows'] if v['identity']['cusip'] == '191216100')
        self.assertEqual(row['status'], 'reported_quantity_unchanged')
        self.assertEqual(row['reported_quantity_change'], '0')
        self.assertEqual(row['reported_value_change_usd'], '2088000000')
        self.assertIsNone(row['inferred_purchase_usd'])
        self.assertFalse(row['execution_inferred'])
        self.assertEqual(a['positions'][row['position_id']]['reported_quantity'], '400000000')
        self.assertEqual(a['positions'][row['position_id']]['native_rows'], 10)
        self.assertEqual(b['positions'][row['position_id']]['reported_value_usd'], '30420000000')

    def test_price_fall_with_unchanged_quantity_is_not_trim(self):
        prior = original_filing(PRIOR); current = deepcopy(prior)
        for row in current['rows']:
            row['reported_value_usd'] = '0'
        result = m.compare(m.resolve_period([current], True), m.resolve_period([prior], True))
        self.assertTrue(all(v['status'] == 'reported_quantity_unchanged' for v in result['rows']))
        self.assertTrue(any(m.Decimal(v['reported_value_change_usd']) < 0 for v in result['rows']))

    def test_baupost_is_not_silently_multiplied_by_1000(self):
        filing = original_filing(BAUPOST)
        self.assertEqual(filing['cover']['usd_multiplier'], 1)
        self.assertIsNotNone(filing['cover']['valuation_review'])
        self.assertEqual(m.total(v['reported_value_usd'] for v in filing['rows']), m.Decimal('5415853'))
        self.assertFalse(filing['cover']['valuation_verified'])

    def test_units_use_filing_date_including_old_quarter_amendment(self):
        source = SOURCES[CURRENT]
        document = next(v for v in source['sources'] if v['kind'] == 'cover_or_other_xml')
        raw = (FIXTURES / document['file']).read_bytes()
        old = raw.replace(b'06-30-2026', b'09-30-2022')
        filing = {**source['filing'], 'period_of_report': '2022-09-30', 'filed_at': '2022-11-14'}
        self.assertEqual(m.parse_cover(old, filing, source['cik'])['usd_multiplier'], 1000)
        filing['filed_at'] = '2023-01-03'
        self.assertEqual(m.parse_cover(old, filing, source['cik'])['usd_multiplier'], 1)

    def test_missing_prior_is_not_zero_or_new(self):
        current = m.resolve_period([original_filing(CURRENT)], True)
        comparison = m.compare(current, {'status': 'not_acquired', 'positions': {}})
        self.assertFalse(comparison['eligible_for_disclosure_comparison'])
        self.assertTrue(all(v['status'] == 'comparison_unavailable' for v in comparison['rows']))
        self.assertTrue(all(v['reported_quantity_change'] is None for v in comparison['rows']))

    def test_disappearing_row_is_not_proven_sale(self):
        prior = m.resolve_period([original_filing(PRIOR)], True)
        current = deepcopy(prior); removed = next(iter(current['positions']))
        del current['positions'][removed]
        row = next(v for v in m.compare(current, prior)['rows'] if v['position_id'] == removed)
        self.assertEqual(row['status'], 'not_present_in_current_public_disclosure')
        self.assertIsNone(row['inferred_sale_usd'])
        self.assertIsNone(row['reported_quantity_change'])

    def test_put_call_class_and_principal_do_not_collide(self):
        row = original_filing(CURRENT)['rows'][0]
        rows = [deepcopy(row) for _ in range(5)]
        rows[1]['identity']['put_call'] = 'PUT'
        rows[2]['identity']['put_call'] = 'CALL'
        rows[3]['identity']['quantity_type'] = 'PRN'
        rows[4]['identity']['class'] = 'CLASS B'
        self.assertEqual(len(m.groups(rows)), 5)

    def test_same_security_multiple_managers_retains_each_native_row(self):
        filing = original_filing(CURRENT)
        row = next(v for v in m.groups(filing['rows']).values() if v['identity']['cusip'] == '191216100')
        self.assertEqual(len(row['row_ids']), 10)
        self.assertGreater(len(row['other_managers']), 1)
        self.assertIsNone(row['symbol'])
        self.assertGreater(filing['cover']['included_manager_count'], 1)

    def test_restatement_replaces_original_without_double_count(self):
        original = original_filing(CURRENT)
        updated = amended(original, '0000000001-26-000001', 1, 'RESTATEMENT', original['rows'][:2])
        result = m.resolve_period([updated, original], True)
        self.assertEqual(result['effective_native_rows'], 2)
        self.assertEqual(result['effective_accessions'], [updated['accession']])
        self.assertEqual(len(result['chain']), 2)

    def test_supplement_adds_disclosed_positions_and_overlap_is_reviewed(self):
        original = original_filing(CURRENT)
        addition = deepcopy(original['rows'][0]); addition['identity']['cusip'] = '000000001'
        supplement = amended(original, '0000000001-26-000001', 1, 'NEW HOLDINGS', [addition])
        result = m.resolve_period([original, supplement], True)
        self.assertEqual(result['effective_native_rows'], len(original['rows']) + 1)
        supplement['rows'] = deepcopy(original['rows'][:1])
        result = m.resolve_period([original, supplement], True)
        self.assertEqual(result['status'], 'supplemental_identity_overlap_requires_review')
        self.assertEqual(result['positions'], {})

    def test_restatement_after_supplement_does_not_guess_overlap(self):
        original = original_filing(CURRENT)
        addition = deepcopy(original['rows'][0]); addition['identity']['cusip'] = '000000001'
        supplement = amended(original, '0000000001-26-000001', 1, 'NEW HOLDINGS', [addition])
        restatement = amended(original, '0000000001-26-000002', 2, 'RESTATEMENT')
        result = m.resolve_period([original, supplement, restatement], True)
        self.assertEqual(result['status'], 'restatement_after_supplement_requires_review')

    def test_incomplete_amendment_chain_cannot_compare(self):
        original = original_filing(CURRENT)
        second = amended(original, '0000000001-26-000002', 2, 'RESTATEMENT')
        self.assertEqual(m.resolve_period([original, second], True)['positions'], {})
        self.assertEqual(m.resolve_period([second], True)['status'], 'missing_original')
        self.assertEqual(m.resolve_period([original], False)['status'], 'incomplete_chain')

    def test_mixed_report_periods_cannot_be_summed(self):
        with self.assertRaisesRegex(ValueError, 'report periods'):
            m.resolve_period([original_filing(CURRENT), original_filing(PRIOR)], True)

    def test_unknown_optional_fields_retained_in_original_order(self):
        source = SOURCES[BAUPOST]
        document = next(v for v in source['sources'] if v['kind'] == 'information_table')
        raw = (FIXTURES / document['file']).read_bytes()
        # Test the lossless field walker independently of source namespace syntax.
        fields = m.native_fields(m.tree(b'<row><x a="1">one</x><x>two</x></row>'))
        self.assertEqual(fields, [['/row/x', 'one', {'a': '1'}], ['/row/x', 'two', {}]])
        self.assertTrue(m.parse_table(raw, document['reference'], BAUPOST, 1)[0]['native_fields'])

    def test_xml_entities_and_duplicate_required_fields_rejected(self):
        with self.assertRaises(ValueError):
            m.tree(b'<!DOCTYPE x [<!ENTITY y "bad">]><x>&y;</x>')
        with self.assertRaises(ValueError):
            m.text(m.tree(b'<x><value>1</value><value>2</value></x>'), 'value')

    def test_decimal_arithmetic_never_rounds_to_machine_precision(self):
        self.assertEqual(m.ds(m.total(['999999999999999999999999999999.999999999999', '0.000000000001'])),
                         '1000000000000000000000000000000.000000000000')
        for invalid in ('NaN', 'Infinity', '-1', '1e3', '1,000', None, True):
            with self.assertRaises(ValueError):
                m.decimal(invalid)

    def test_duplicate_json_and_nonfinite_rejected(self):
        for raw in (b'{"x":1,"x":2}', b'{"x":NaN}'):
            with self.assertRaises(ValueError):
                m.decode(raw)

    def test_receipt_tampering_future_and_request_identity_rejected(self):
        doc = SOURCES[CURRENT]['sources'][0]
        ref = doc['reference']; raw = (FIXTURES / doc['file']).read_bytes()
        self.assertEqual(m.original(ref, lambda _: raw, ref['url'], '2026-09-20T00:00:00Z'), raw)
        for body, url, at in ((raw + b' ', ref['url'], '2026-09-20T00:00:00Z'),
                              (raw, ref['url'] + 'wrong', '2026-09-20T00:00:00Z'),
                              (raw, ref['url'], '2026-09-18T00:00:00Z')):
            with self.assertRaises(ValueError):
                m.original(ref, lambda _: body, url, at)

    def test_acceptance_and_effective_filing_date_remain_distinct(self):
        result = m.submission_rows({'accessionNumber': ['0001104659-26-104387'], 'form': ['13F-HR/A'],
            'reportDate': ['2026-06-30'], 'filingDate': ['2026-09-02'], 'primaryDocument': ['primary_doc.xml'],
            'acceptanceDateTime': ['2026-09-01T22:14:00.000Z']})
        self.assertEqual(result[0]['filed_at'], '2026-09-02')
        self.assertEqual(result[0]['accepted_at'], '2026-09-01T22:14:00.000Z')

    def test_quarterly_calendar_does_not_make_latest_day_daily_signal(self):
        self.assertEqual(m.expected_report_period('2026-07-01T00:00:00Z'), '2026-03-31')
        self.assertEqual(m.expected_report_period('2026-09-19T00:00:00Z'), '2026-06-30')
        self.assertEqual(m.PERMISSION['additional_independent_votes'], 0)
        self.assertIsNone(m.PERMISSION['call'])


if __name__ == '__main__':
    unittest.main()
