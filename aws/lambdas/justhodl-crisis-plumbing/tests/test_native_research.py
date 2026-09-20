from pathlib import Path
from copy import deepcopy
from decimal import localcontext,ROUND_DOWN
import json,sys,unittest
ROOT=Path(__file__).resolve().parents[4]
sys.path[:0]=[str(ROOT/'aws/shared'),str(Path(__file__).resolve().parents[1]/'source')]
import plumbing_research_model as m
FIX=Path(__file__).parent/'fixtures'
AT='2026-09-20T11:00:00+00:00'


def fixtures():
    macro=json.loads((FIX/'macro.json').read_bytes());entries=json.loads((FIX/'selected-macro-inputs.json').read_bytes())
    originals={sid:{**entry,**{part:json.loads((FIX/'originals'/ref['sha256']).read_bytes()) for part,ref in entry['evidence'].items()}}
               for sid,entry in entries.items()}
    funding=json.loads((FIX/'funding.json').read_bytes());entry=json.loads((FIX/'funding-input.json').read_bytes())['originals']['ofr_fsi']
    return macro,originals,funding,{**entry,'raw':(FIX/'originals'/entry['evidence']['sha256']).read_bytes()}


class NativeResearch(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source,cls.originals,cls.funding,cls.ofr_original=fixtures()
        cls.native=m.native(cls.source,cls.originals,AT)
        cls.ofr=m.ofr_native(cls.funding,cls.ofr_original,AT)
        cls.output,cls.histories=m.build(cls.source,cls.originals,AT,cls.ofr)

    def test_originals_replace_earliest_page_claims(self):
        self.assertGreaterEqual(self.native['DEXJPUS']['observation_date'],'2026-09-01')
        self.assertGreaterEqual(self.native['NFCICREDIT']['observation_date'],'2026-09-01')
        self.assertGreaterEqual(self.native['TOTBKCR']['observation_date'],'2026-09-01')

    def test_original_decimal_tampering_rejected(self):
        originals=deepcopy(self.originals);originals['SOFR']['observations']['observations'][0]['value']='99'
        with self.assertRaisesRegex(ValueError,'reconstruction differs'):m.native(self.source,originals,AT)

    def test_measurement_wrapper_tampering_rejected(self):
        source=deepcopy(self.source);source['measurements']['SOFR']['current_decimal']='0'
        with self.assertRaisesRegex(ValueError,'content differs'):m.native(source,self.originals,AT)

    def test_duplicate_original_dates_rejected(self):
        originals=deepcopy(self.originals);doc=originals['SOFR']['observations']['observations'];doc.append(deepcopy(doc[0]))
        with self.assertRaisesRegex(ValueError,'duplicate'):m.native(self.source,originals,AT)

    def test_future_compilation_rejected(self):
        with self.assertRaisesRegex(ValueError,'clock'):m.native(self.source,self.originals,'2026-09-19T00:00:00+00:00')

    def test_missing_and_discontinued_identities_survive(self):
        self.assertEqual(len(self.native),53)
        for sid in ('OFRFSI','DRTSCLM','WCBSL'):
            self.assertIsNone(self.native[sid]['value_decimal']);self.assertEqual(self.native[sid]['quality']['status'],'unavailable')
        self.assertIsNone(self.native['USSLIND']['value_decimal']);self.assertEqual(self.native['USSLIND']['observation_date'],'2020-02-01')
        self.assertTrue(self.native['USSLIND']['rows'])

    def test_zero_is_a_real_observation(self):
        self.assertEqual(self.native['DRTSCILM']['value_decimal'],'0')
        self.assertEqual(self.native['DRTSCILM']['quality']['status'],'fresh')

    def test_native_units_are_distinct(self):
        self.assertEqual(self.native['WTREGEN']['unit'],'Millions of U.S. Dollars')
        self.assertEqual(self.native['DPSACBW027SBOG']['unit'],'Billions of U.S. Dollars')
        self.assertEqual(self.native['DPCREDIT']['unit'],'Percent')
        self.assertEqual(self.native['BUSLOANS']['frequency'],'M')

    def test_pairs_use_anchor_day_without_old_substitution(self):
        pair=self.output['comparisons']['sofr_iorb'];self.assertEqual(pair['value_decimal'],'-5.00')
        self.assertEqual(pair['observation_date'],'2026-09-17')
        rows=deepcopy(self.native);rows['IORB']['rows']=[r for r in rows['IORB']['rows'] if r['date']!='2026-09-17']
        changed=m.comparison(rows,'SOFR','IORB','Test','Test')
        self.assertIsNone(changed['value_decimal']);self.assertIn('no forward fill',changed['reason'])

    def test_monthly_rate_not_forward_filled_into_daily_basis(self):
        self.assertIsNone(self.output['comparisons']['usd_jpy_three_month']['value_decimal'])
        self.assertEqual(self.native['IR3TIB01JPM156N']['frequency'],'M')
        self.assertIn('cross_currency_basis',self.output['unavailable_claims'])

    def test_ofr_original_columns_share_one_index(self):
        self.assertEqual(len(self.ofr),9);self.assertEqual(self.ofr['ofr_fsi:ofr_fsi']['value_decimal'],'-2.411')
        self.assertEqual(self.ofr['ofr_fsi:ofr_fsi']['observation_date'],'2026-09-16')
        self.assertEqual(self.output['dependency_graph']['independent_votes'],0)

    def test_ofr_value_and_column_tampering_rejected(self):
        bad=deepcopy(self.funding);bad['measurements']['ofr_fsi:ofr_fsi']['value_decimal']='0'
        with self.assertRaisesRegex(ValueError,'binding differs'):m.ofr_native(bad,self.ofr_original,AT)
        original={**self.ofr_original,'raw':self.ofr_original['raw'].replace(b'OFR FSI',b'Other FSI',1)}
        with self.assertRaisesRegex(ValueError,'columns differ'):m.ofr_native(self.funding,original,AT)

    def test_old_acquisition_withholds_current_values(self):
        native=m.native(self.source,self.originals,'2026-09-22T11:00:00+00:00')
        self.assertIsNone(native['SOFR']['value_decimal']);self.assertTrue(native['SOFR']['rows'])
        ofr=m.ofr_native(self.funding,self.ofr_original,'2026-09-20T16:00:00+00:00')
        self.assertIsNone(ofr['ofr_fsi:ofr_fsi']['value_decimal']);self.assertEqual(ofr['ofr_fsi:ofr_fsi']['quality']['status'],'stale_source')

    def test_compact_histories_retain_exact_records(self):
        self.assertLess(len(m.encoded(self.output)),400000)
        row=self.output['measurements']['SOFR'];ref=row['history'];history=json.loads(self.histories[ref['key']])
        self.assertEqual(m.digest(history),ref['sha256']);self.assertEqual(len(self.histories[ref['key']]),ref['bytes'])
        self.assertEqual(history['rows'],[[r['date'],r['value_decimal'],r['source_row_index']] for r in self.native['SOFR']['rows']])

    def test_ranks_require_real_native_span(self):
        row=deepcopy(self.native['NFCI']);row['rows']=row['rows'][-52:]
        self.assertIsNone(m.official_rank(row,10)['value'])
        self.assertIsNotNone(self.output['official_historical_ranks']['NFCI']['10']['fraction'])

    def test_arithmetic_context_cannot_change_replay(self):
        with localcontext() as context:
            context.prec=7;context.rounding=ROUND_DOWN
            output,histories=m.build(self.source,self.originals,AT,m.ofr_native(self.funding,self.ofr_original,AT))
        self.assertEqual(m.digest(output),m.digest(self.output));self.assertEqual(histories,self.histories)

    def test_research_does_not_authorize_crisis_or_portfolio(self):
        self.assertEqual(self.output['decision'],{'verb':'WAIT','meaning':'abstain'})
        self.assertIsNone(self.output['composite']['composite_stress_score'])
        self.assertFalse(self.output['portfolio_consequences']['forced_liquidation'])
        for key in m.AUTHORITY:self.assertIs(self.output[key],False)


if __name__=='__main__':unittest.main()
