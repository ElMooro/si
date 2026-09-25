from pathlib import Path
from copy import deepcopy
import json, sys, unittest
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'aws/shared'))
import statement_research_source as source
import statement_research_model as model
import statement_measurements as measurements
from test_statement_measurements import bundle


class Fixture:
    def __init__(self, labels=('ABC',), modify=None):
        self.files, self.capsules = {}, {}
        captures = {}; count = empty = 0
        for label in labels:
            for period in ('annual', 'quarter'):
                for endpoint in source.ENDPOINTS:
                    spec = source.spec(label, endpoint, period)
                    row = deepcopy(bundle()[endpoint]['values'])
                    row.update(symbol=label, period='FY' if period == 'annual' else 'Q4')
                    rows = [row]
                    if modify:
                        rows = modify(label, period, endpoint, rows)
                    count += len(rows); empty += not rows
                    cap = {'status': 'response_retained', 'spec': spec, 'http_status': 200,
                        'requested_at': '2026-03-01T12:00:00+00:00', 'received_at': '2026-03-01T12:00:01+00:00',
                        'headers': {}, 'original': self.retain(source.encoded(rows))}
                    captures[spec['url']] = self.retain(source.encoded(cap))
                    self.capsules[spec['url']] = cap
        self.manifest = {'contract': 'financial-statement-complete-source-campaign.v1', 'status': 'complete',
            'generated_at': '2026-03-01T12:00:00+00:00', 'completed_at': '2026-03-01T12:10:00+00:00',
            'universe': self.retain(source.encoded({'rows': [{'symbol': label} for label in labels]})),
            'reported_symbols': sorted(labels), 'captures': captures, 'planned_sources': len(captures),
            'counts': {'complete_sources': len(captures), 'provider_rows': count, 'unavailable_empty_responses': empty}}
        self.ref = self.retain(source.encoded(self.manifest))

    def retain(self, body):
        ref = {'key': source.PRIVATE + source.sha(body) + '.bin', 'bytes': len(body), 'sha256': source.sha(body)}
        self.files[ref['key']] = body
        return ref

    def compile(self):
        return model.compile_output(self.ref, self.files.__getitem__)


class Tests(unittest.TestCase):
    def test_complete_universe_and_all_coordinates_reconstruct_deterministically(self):
        f = Fixture(('ABC', 'XYZ')); value = f.compile()
        self.assertEqual(value, f.compile())
        packet = value['packet']
        self.assertEqual((packet['reported_names'], packet['provider_responses'], packet['provider_rows'], packet['records']), (2,12,12,4))
        self.assertEqual(packet['complete_aligned_records'], 4)
        self.assertEqual(packet['multiple_labels_per_issuer'], {'sec-cik:0000000123': ['ABC','XYZ']})
        self.assertEqual(packet['independent_investment_votes'], 0)
        self.assertFalse(packet['quality']['original_sec_filings_replayed'])
        for symbol, shard in value['shards'].items():
            self.assertEqual(shard['source_row_count'],6)
            for record in shard['records']:
                self.assertEqual(record['measurements']['metrics']['gross_margin_pct']['value'],'40.000000000000')
                self.assertEqual(len(record['source_rows']),3)
                self.assertTrue(all(c['source_row'] == 0 for c in record['source_rows']))

    def test_empty_responses_and_invalid_identity_rows_are_retained(self):
        def change(label, period, endpoint, rows):
            if label == 'XYZ': return []
            if endpoint == measurements.C: rows[0]['cik'] = '0'
            return rows
        value = Fixture(('ABC','XYZ'), change).compile(); packet = value['packet']
        self.assertEqual(packet['reported_names'],2);self.assertEqual(packet['empty_responses'],6)
        self.assertEqual(packet['rows_with_identity_problems'],2)
        self.assertEqual(value['shards']['XYZ']['records'],[])
        self.assertEqual(packet['issuers'][1]['status'],'no_provider_statements')
        rows = value['shards']['ABC']['records']
        self.assertEqual(sum(len(v['source_rows']) for v in rows),6)
        self.assertEqual(sum(v['measurements'] is None for v in rows),2)

    def test_filing_vintage_disagreement_creates_distinct_records_not_false_alignment(self):
        def change(label, period, endpoint, rows):
            if endpoint == measurements.C: rows[0]['acceptedDate'] = '2026-02-15 13:00:00'
            return rows
        value = Fixture(modify=change).compile()
        self.assertEqual(value['packet']['complete_aligned_records'],0)
        self.assertEqual(value['packet']['records'],4)
        for record in value['shards']['ABC']['records']:
            self.assertIsNone(record['measurements']['metrics']['cash_conversion_multiple']['value'])

    def test_duplicates_are_not_dropped_or_arbitrarily_selected(self):
        def change(label, period, endpoint, rows):
            if endpoint == measurements.B:
                rows.append(deepcopy(rows[0]));rows[1]['totalDebt'] = 0
            return rows
        value = Fixture(modify=change).compile(); packet = value['packet']
        self.assertEqual(packet['provider_rows'],8);self.assertEqual(packet['records_with_repeated_endpoints'],2)
        for record in value['shards']['ABC']['records']:
            self.assertEqual(len(record['source_rows']),4)
            self.assertEqual(record['duplicate_endpoints'],[measurements.B])
            self.assertIsNone(record['measurements']['metrics']['net_debt_derived']['value'])
            self.assertEqual(record['measurements']['metrics']['gross_margin_pct']['value'],'40.000000000000')

    def test_future_dates_and_wrong_request_labels_cannot_calculate(self):
        for field, changed in (('date','2027-12-31'),('filingDate','2027-12-31'),('acceptedDate','2027-12-31 12:00:00'),('symbol','XYZ'),('period','Q1')):
            def change(label, period, endpoint, rows):
                if period == 'annual' and endpoint == measurements.I: rows[0][field] = changed
                return rows
            value = Fixture(modify=change).compile()
            self.assertEqual(value['packet']['rows_with_identity_problems'],1,field)
            self.assertEqual(sum(v['measurements'] is None for v in value['shards']['ABC']['records']),1,field)

    def test_original_corruption_missing_responses_wrong_counts_and_universe_fail_closed(self):
        f=Fixture(); cap=next(iter(f.capsules.values()));f.files[cap['original']['key']]+=b' '
        with self.assertRaises(ValueError):f.compile()
        for mutate in (lambda m:m['captures'].pop(next(iter(m['captures']))),
                       lambda m:m['counts'].update(provider_rows=0),
                       lambda m:m.update(reported_symbols=['XYZ']),
                       lambda m:m.update(completed_at='2025-01-01T00:00:00Z')):
            f=Fixture();mutate(f.manifest);f.ref=f.retain(source.encoded(f.manifest))
            with self.assertRaises(ValueError):f.compile()
        for raw in (b'{"x":0,"x":1}',b'[NaN]',b'[Infinity]'):
            with self.assertRaises(ValueError):source.strict(raw)

    def test_response_order_never_selects_the_pairing(self):
        def change(label, period, endpoint, rows):
            previous=deepcopy(rows[0]);previous.update(date='2024-12-31',fiscalYear='2024',filingDate='2025-02-15',acceptedDate='2025-02-15 12:00:00')
            if endpoint==measurements.I:previous.update(revenue=200,grossProfit=50)
            return [previous,*rows] if endpoint==measurements.B else [*rows,previous]
        value=Fixture(modify=change).compile()
        self.assertEqual(value['packet']['complete_aligned_records'],4)
        for record in value['shards']['ABC']['records']:
            expected='25.000000000000' if record['identity']['fiscalYear']=='2024' else '40.000000000000'
            self.assertEqual(record['measurements']['metrics']['gross_margin_pct']['value'],expected)


if __name__ == '__main__': unittest.main(verbosity=2)
