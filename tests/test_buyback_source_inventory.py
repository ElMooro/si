from pathlib import Path
import copy,sys,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'aws/ops/checks'))
import buyback_source_inventory as model


def fixture():
    p={key:{} for key in model.INPUTS}
    p[model.CURRENT]={'generated_at':'2026-09-25T13:30:00Z','tickers':{'ZZZ':{'symbol':'OTHER','gross_repurchases_ttm':0,'net_buyback_ttm':None},'BRK.B':{'symbol':'BRK.B'}}}
    p[model.ATTENTION]={'tickers':{f'A{i:03}':{} for i in range(220)}}
    p[model.SCANNER]={'top_opportunities':[{'ticker':'ZZA','announcement_date':'2026-09-24','filing_url':'https://www.sec.gov/cgi-bin/browse-edgar?CIK=123'},
        {'ticker':'ZZA','announcement_date':'2026-09-26','filing_url':'https://www.sec.gov/Archives/edgar/data/123/000123456/doc.htm'}]}
    return p


class Tests(unittest.TestCase):
    def test_all_names_outside_legacy_cap_and_duplicate_announcements_survive(self):
        p=fixture();before=copy.deepcopy(p);out=model.inventory(p,'2026-09-25')
        self.assertEqual(p,before);self.assertEqual(out['current_row_count'],2)
        self.assertEqual(out['authorization_rows'],2);self.assertEqual(len(out['reported_label_occurrences']['ZZA']),2)
        self.assertIn('A219',out['outside_legacy_request']);self.assertIn('ZZZ',out['outside_legacy_request'])
        self.assertIn('BRK.B',out['provider_request_eligible_labels']);self.assertIn('ZZA',out['legacy_requested_labels'])
        self.assertEqual(out['current_rows'][0]['reported_symbol'],'OTHER')
        self.assertFalse(out['forecast_qualified']);self.assertFalse(out['unreported_filings_complete'])

    def test_zero_null_identity_clocks_and_browse_urls_are_distinct(self):
        out=model.inventory(fixture(),'2026-09-25')
        self.assertEqual(out['field_population']['gross_repurchases_ttm'],1);self.assertNotIn('net_buyback_ttm',out['field_population'])
        self.assertIsNone(out['current_rows'][0]['declared_row_as_of'])
        self.assertEqual([a['age_days'] for a in out['authorizations']],[1,-1])
        self.assertFalse(out['authorizations'][0]['exact_sec_archive_url_shape'])
        self.assertTrue(out['authorizations'][1]['exact_sec_archive_url_shape'])
        self.assertFalse(out['authorizations'][1]['original_filing_bytes_verified'])

    def test_malformed_and_missing_sources_are_explicit_never_dropped(self):
        p=fixture();p[model.SCANNER]['top_opportunities'] += [{'ticker':None,'announcement_date':'bad'}, {'ticker':'odd label'}]
        p['data/share-flows.json']=None;out=model.inventory(p,'2026-09-25')
        self.assertEqual(out['authorization_rows'],4);self.assertEqual(len(out['shape_issues']),1)
        self.assertIn('odd label',out['reported_labels_not_provider_eligible']);self.assertIn('data/share-flows.json',out['missing_inputs'])
        for mutate in (lambda p:p.pop(model.INPUTS[-1]),lambda p:p[model.CURRENT]['tickers'].update(BAD=None),
                lambda p:p[model.SCANNER].update(top_opportunities=[42]),lambda p:p[model.ATTENTION].update(tickers=[])):
            p=fixture();mutate(p)
            with self.assertRaises(ValueError):model.inventory(p,'2026-09-25')

    def test_archive_url_shape_does_not_certify_a_filing_and_malformed_urls_survive(self):
        p=fixture();urls=['https://[bad','https://www.sec.gov.evil.test/Archives/edgar/data/1/2/doc.htm',
            'http://www.sec.gov/Archives/edgar/data/1/2/doc.htm','https://www.sec.gov/Archives/edgar/data/1/2/doc.htm?x=1']
        p[model.SCANNER]['top_opportunities']=[{'ticker':'ABC','filing_url':url} for url in urls]
        out=model.inventory(p,'2026-09-25')
        self.assertEqual([row['filing_url'] for row in out['authorizations']],urls)
        self.assertTrue(all(not row['exact_sec_archive_url_shape'] and not row['original_filing_bytes_verified'] for row in out['authorizations']))


if __name__=='__main__':unittest.main(verbosity=2)
