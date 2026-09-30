"""Complete invented books: absent classifications are never an economic sector."""
from copy import deepcopy
from decimal import Decimal
import gzip,hashlib,json,sys,unittest
from pathlib import Path
from test_risk_model import fixture,evaluate,model,NOW
from portfolio_sector_exposure import build_sector_exposure as sectors
ROOT=Path(__file__).resolve().parents[4]


def book(labels,values):
    return sectors([{'symbol':'X'+str(i),'sector':label} for i,label in enumerate(labels)],values)


class SectorCoverage(unittest.TestCase):
    def test_complete_current_predecessor_reproductions_repair_missing_and_partial_hhi(self):
        raw=(ROOT/'tests/fixtures/pre-portfolio-sector-coverage/complete-synthetic.json.gz').read_bytes()
        audit=json.loads((ROOT/'docs/audit/2026-09-30/portfolio-sector-coverage.json').read_bytes())
        self.assertEqual(hashlib.sha256(raw).hexdigest(),audit['fixtures']['complete_synthetic']['sha256'])
        cases=json.loads(gzip.decompress(raw))['cases']
        for name,case in cases.items():
            out=model.build(**deepcopy(case['inputs']))
            if name in ('missing','all_unknown','partial','whitespace','sentinel'):
                self.assertIsNone(out['concentration_hhi']);self.assertIsNone(out['alerts_summary']['sector_concentration_breach'])
                self.assertGreater(case['output']['concentration_hhi'],0)
            self.assertFalse(out['permissions']['sizing_eligible'])
    def test_all_unknown_preserves_every_record_and_full_unknown_exposure(self):
        out=book([None,'Unknown','N/A'],[10,20,30])
        self.assertEqual(out['unclassified_weight_pct'],100);self.assertEqual(out['known_sectors'],[])
        self.assertEqual([r['input_index'] for r in out['records']],[0,1,2]);self.assertIsNone(out['concentration_hhi'])
    def test_partial_known_exposure_does_not_renormalize_known_labels(self):
        out=book(['Technology',None],[35,65])
        self.assertEqual(out['known_sectors'][0]['weight_pct'],35);self.assertEqual(out['classification_coverage_pct'],35)
        self.assertIsNone(out['known_sector_above_40pct']);self.assertIsNone(out['maximum_sector_weight_pct'])
    def test_known_sector_can_exceed_threshold_despite_partial_classification(self):
        out=book(['Technology',None],[65,35])
        self.assertTrue(out['known_sector_above_40pct']);self.assertIsNone(out['concentration_hhi'])
    def test_threshold_uses_unrounded_values_and_strict_comparison(self):
        for value,expected in [(40,False),(40.000001,True),(39.999999,False)]:
            out=book(['A','B','C'],[value,30,70-value]);self.assertIs(out['known_sector_above_40pct'],expected)
        self.assertIsNone(book(['A',None],[40,60])['known_sector_above_40pct'])
        snapshot,packets=fixture();template=snapshot['positions'][0]
        snapshot['positions']=[{**template,'symbol':symbol,'sector':sector,'qty':value/100,'market_value':round(value,2)}
            for symbol,sector,value in [('AAA','Technology',40.000001),('BBB','Healthcare',30),('CCC','Energy',29.999999)]]
        out=evaluate(snapshot,packets);self.assertTrue(out['alerts_summary']['sector_concentration_breach'])
        self.assertAlmostEqual(out['sector_exposure']['maximum_known_sector_weight_pct'],40.000001)
        self.assertEqual(out['sector_exposure']['records'][0]['reported_market_value'],40)
    def test_long_short_gross_denominator_does_not_cancel(self):
        out=book(['Technology','Healthcare'],[100,-100])
        self.assertEqual(out['gross_marked_value'],200);self.assertEqual(out['concentration_hhi'],5000)
        self.assertEqual([v['weight_pct'] for v in out['known_sectors']],[50,50])
    def test_complete_known_labels_match_independent_decimal_hhi(self):
        out=book(['A','B','C'],[100,200,300]);expected=sum((Decimal(v)/Decimal(600)*100)**2 for v in [100,200,300])
        self.assertAlmostEqual(out['concentration_hhi'],float(expected),places=9)
        self.assertFalse(out['classification_verified']);self.assertFalse(out['sizing_eligible'])
    def test_zero_unknown_lot_retains_record_without_erasing_known_hhi(self):
        out=book(['Technology',None],[100,0]);self.assertEqual(out['concentration_hhi'],10000)
        self.assertEqual(out['unclassified_position_count'],1);self.assertEqual(out['unclassified_weight_pct'],0)
    def test_empty_and_zero_books_do_not_claim_concentration_or_percentages(self):
        for out in (book([],[]),book([None,'Technology'],[0,0])):
            self.assertEqual(out['status'],'NO_GROSS_EXPOSURE');self.assertIsNone(out['concentration_hhi']);self.assertIsNone(out['classification_coverage_pct']);self.assertIsNone(out['known_sector_above_40pct'])
        snapshot,packets=fixture();snapshot['positions']=[]
        self.assertEqual(evaluate(snapshot,packets)['sector_exposure']['status'],'NO_GROSS_EXPOSURE')
    def test_partial_marks_preserve_priced_amounts_but_no_full_book_weights(self):
        out=book(['Technology',None],[100,None]);self.assertEqual(out['priced_gross_marked_value'],100)
        self.assertIsNone(out['gross_marked_value']);self.assertIsNone(out['known_sectors'][0]['weight_pct'])
        self.assertEqual(out['status'],'VALUATION_INCOMPLETE')
    def test_missing_typed_and_sentinel_labels_remain_full_original_records(self):
        labels=[None,'  ','Unknown',' UNKNOWN ','N/A','—','ETF','Fund','Other',True,[],{'full':['π',0,None]}]
        out=book(labels,[1]*len(labels));self.assertEqual(out['known_sectors'],[])
        self.assertEqual([r['reported_sector'] for r in out['records']],labels)
    def test_trimmed_known_label_is_separate_from_original_spelling(self):
        out=book([' Technology ','Technology'],[10,20]);self.assertEqual(len(out['known_sectors']),1)
        self.assertEqual(out['records'][0]['reported_sector'],' Technology ');self.assertEqual(out['known_sectors'][0]['input_indices'],[0,1])
    def test_conflicting_same_instrument_labels_become_unknown_without_dropping_lots(self):
        rows=[{'symbol':'AAA','sector':'Technology'},{'symbol':'AAA','sector':'Healthcare'},{'symbol':'AAA','sector':None}]
        out=sectors(rows,[10,-20,30]);self.assertEqual(out['unclassified_gross_marked_value'],60)
        self.assertEqual([r['classification_reason'] for r in out['records']],['CONFLICTING_INSTRUMENT_LABELS']*3)
    def test_invalid_values_never_coerce_to_measured_zero_or_finite_amounts(self):
        for value in (True,False,'10',{},[],float('inf'),float('nan'),10**400,9007199254740992):
            out=book(['Technology'],[value]);self.assertIsNone(out['gross_marked_value']);self.assertEqual(out['priced_position_count'],0)
    def test_population_types_and_lengths_must_be_exact(self):
        for rows,values,complete in [(None,[],True),([],None,True),([{}],[],True),([],[],1)]:
            with self.assertRaises(ValueError):sectors(rows,values,valuation_complete=complete)
    def test_risk_replay_binds_shared_classifier_and_preserves_input_without_sizing(self):
        s,p=fixture();s['positions'][0]['sector']=None;before=deepcopy(s)
        frozen,out=model.freeze(s,p,NOW.isoformat(),{});self.assertEqual(model.replay(frozen),out);self.assertEqual(before,s)
        self.assertIn('portfolio_sector_exposure.py',frozen['code']);self.assertGreater(out['holdings_risk']['var_1d_99_dollars'],0)
        self.assertIsNone(out['concentration_hhi']);self.assertFalse(out['permissions']['may_recommend_trades'])


if __name__=='__main__':unittest.main()
