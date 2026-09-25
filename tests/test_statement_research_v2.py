from pathlib import Path
from copy import deepcopy
import sys, unittest
sys.path[:0] = [str(Path(__file__).resolve().parents[1] / p) for p in ('aws/shared', 'aws/ops/checks')]
import statement_research_source as source
import statement_research_v2 as model
import statement_research_identity as identity
import statement_research_arithmetic_v2 as independent
import statement_measurements as measurements
from test_statement_research_model import Fixture as PreviousFixture


class Fixture(PreviousFixture):
    def __init__(self, labels=('ABC',), modify=None, index_change=None):
        super().__init__(labels, modify)
        rows = {str(i): {'ticker': 'FIXTURE' + str(i), 'cik_str': 1000+i, 'title': 'Synthetic test issuer'} for i in range(1000)}
        for i, label in enumerate(labels):
            rows[str(i)] = {'ticker': label, 'cik_str': 123, 'title': 'Synthetic ' + label}
        if index_change: index_change(rows)
        self.index = rows
        self.capture = {'url': identity.URL, 'status': 'response_retained', 'http_status': 200,
            'requested_at': '2026-03-01T12:15:00+00:00', 'received_at': '2026-03-01T12:15:01+00:00',
            'headers': {}, 'original': self.retain(source.encoded(rows))}
        self.identity_ref = self.retain(source.encoded(self.capture))

    def compile(self):
        return model.compile_output(self.ref, self.identity_ref, self.files.__getitem__)

    def verify(self, compiled):
        return independent.verify(self.ref, self.identity_ref, compiled, self.files.__getitem__)


class Tests(unittest.TestCase):
    def test_complete_population_compiler_and_independent_arithmetic(self):
        f=Fixture(('ABC','XYZ')); value=f.compile(); proof=f.verify(value)
        self.assertEqual(value,f.compile())
        self.assertEqual(proof['metric_comparisons'],68)
        self.assertEqual(proof['identity_metadata_rows_checked'],12)
        self.assertEqual(value['packet']['generated_at'],f.capture['received_at'])
        self.assertEqual(value['packet']['statement_campaign_completed_at'],f.manifest['completed_at'])
        self.assertEqual(value['packet']['multiple_labels_per_issuer'],{'sec-cik:0000000123':['ABC','XYZ']})
        self.assertFalse(value['packet']['quality']['historical_security_continuity_verified'])

    def test_wrong_company_cik_is_preserved_but_cannot_calculate(self):
        def change(symbol,period,endpoint,rows):
            if symbol=='PGR' and endpoint==measurements.B:rows[0]['cik']='456'
            if symbol=='T':rows[0]['cik']='456'
            return rows
        f=Fixture(('PGR','T'),change,lambda rows:rows['1'].update(cik_str=456))
        value=f.compile();proof=f.verify(value)
        self.assertEqual(value['packet']['current_identity_quarantined_rows'],2)
        self.assertEqual(proof['invalid_or_uncorroborated_identity_rows_checked'],2)
        self.assertNotIn('sec-cik:0000000456',value['packet']['multiple_labels_per_issuer'])
        self.assertEqual(value['packet']['provider_reported_cik_groups']['sec-cik:0000000456'],['PGR','T'])
        bad=[v for v in value['shards']['PGR']['records'] if not v['current_ticker_cik_corroborated']]
        self.assertEqual(len(bad),2)
        for record in bad:
            self.assertIsNone(record['measurements'])
            self.assertEqual(record['identity_evidence'][0]['reported_identity']['cik'],'456')
            self.assertEqual(record['identity_evidence'][0]['current_sec_ciks'],['0000000123'])

    def test_invalid_dates_missing_metadata_and_zero_cik_remain_visible(self):
        def change(symbol,period,endpoint,rows):
            if endpoint==measurements.I: rows[0]['acceptedDate']='2025-01-01 12:00:00'
            elif endpoint==measurements.B: rows[0]['cik']=0
            else: del rows[0]['filingDate']
            return rows
        f=Fixture(modify=change);value=f.compile();proof=f.verify(value)
        self.assertEqual(proof['identity_metadata_rows_checked'],6)
        self.assertEqual(value['packet']['records_without_calculations'],6)
        items=[v['identity_evidence'][0] for v in value['shards']['ABC']['records']]
        self.assertEqual(sum('filingDate' in v['missing_fields'] for v in items),2)
        self.assertEqual(sum(v['reported_identity']['cik']==0 for v in items),2)
        self.assertEqual(value['packet']['clock_issues']['acceptedDate_precedes_period_end'],2)

    def test_unmapped_and_ambiguous_current_tickers_cannot_supply_company_measurements(self):
        for change,status in ((lambda rows:rows['0'].update(ticker='OTHER'),'not_in_current_sec_ticker_index'),
            (lambda rows:rows['999'].update(ticker='ABC',cik_str=456),'ambiguous_current_sec_ticker_index')):
            f=Fixture(index_change=change);value=f.compile();f.verify(value)
            self.assertEqual(value['packet']['current_identity_statuses'],{status:6})
            self.assertTrue(all(v['measurements'] is None for v in value['shards']['ABC']['records']))

    def test_universe_classification_comes_from_retained_row_and_is_not_claimed_verified(self):
        f=Fixture(('ABC','XYZ'))
        universe={'generated_at':'2026-03-01T11:00:00Z','rows':[
            {'symbol':'XYZ','sector':'Financials','industry':'Banks'},{'symbol':'ABC','sector':'Technology','industry':None}]}
        f.manifest['universe']=f.retain(source.encoded(universe));f.ref=f.retain(source.encoded(f.manifest))
        value=f.compile();f.verify(value)
        row=value['packet']['issuers'][0]['universe_record']
        self.assertEqual(row['source_row'],1)
        self.assertEqual(row['reported_classification'],{'sector':'Technology','industry':None})
        self.assertFalse(row['classification_independently_verified'])

    def test_tampered_metadata_correspondence_calculation_or_population_fails_independently(self):
        f=Fixture();original=f.compile()
        mutations=(lambda c:c['shards']['ABC']['records'][0]['identity_evidence'][0]['reported_identity'].update(cik='456'),
            lambda c:c['shards']['ABC']['records'][0]['identity_evidence'][0].update(current_sec_ciks=['0000000456']),
            lambda c:c['shards']['ABC']['records'][0]['identity_evidence'][0].update(clock_issues=['invented']),
            lambda c:c['shards']['ABC']['records'][0]['measurements']['metrics']['gross_margin_pct'].update(value='41.000000000000'),
            lambda c:c['shards']['ABC']['records'].pop(),
            lambda c:c['packet']['issuers'][0]['universe_record'].update(source_row=99))
        for change in mutations:
            value=deepcopy(original);change(value)
            with self.assertRaises((AssertionError,IndexError)):f.verify(value)

    def test_corrupt_truncated_ambiguous_or_undated_sec_source_cannot_compile(self):
        for change in (lambda cap:cap.update(http_status=403),lambda cap:cap.update(url='https://example.test/index'),
            lambda cap:cap.update(received_at='2026-03-01T12:14:00Z'),lambda cap:cap['headers'].update({'content-length':'1'})):
            f=Fixture();change(f.capture);f.identity_ref=f.retain(source.encoded(f.capture))
            with self.assertRaises(ValueError):f.compile()
        f=Fixture();f.files[f.capture['original']['key']]+=b' '
        with self.assertRaises(ValueError):f.compile()
        for raw in (b'{}',b'{"1":{},"1":{}}',b'[NaN]'):
            with self.assertRaises(ValueError):identity.index(raw)


if __name__=='__main__':unittest.main(verbosity=2)
