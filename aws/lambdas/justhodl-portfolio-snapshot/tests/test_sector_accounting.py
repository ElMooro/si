"""Shared classification coverage through current reviewed accounting only."""
from copy import deepcopy
import unittest
from test_accounting import position,enriched,NOW
from test_watchlist_sync import load_current


class SectorAccounting(unittest.TestCase):
    def setUp(self):self.mod=load_current()
    def calc(self,rows,marks):return self.mod.build_holdings_accounting(rows,marks,NOW)
    def test_missing_sector_never_becomes_unknown_sector_group(self):
        rows,summary,groups,gross=self.calc([position()],enriched())
        self.assertEqual(groups,[]);self.assertEqual(summary['sector_exposure']['unclassified_weight_pct'],100)
        self.assertIsNone(rows[0]['sector']);self.assertEqual(rows[0]['sector_source'],'UNAVAILABLE')
    def test_book_label_fallback_is_explicit_and_preserved_in_output_rows(self):
        rows,summary,groups,_=self.calc([position(sector='Technology')],enriched())
        self.assertEqual(rows[0]['sector'],'Technology');self.assertEqual(rows[0]['sector_source'],'SOURCE_BOOK')
        self.assertEqual(groups[0]['weight_pct'],100);self.assertFalse(summary['sector_exposure']['classification_verified'])
    def test_reported_research_label_precedence_does_not_relabel_unknown_as_book_known(self):
        rows,summary,groups,_=self.calc([position(sector='Technology')],enriched(sector='Unknown'))
        self.assertEqual(rows[0]['sector'],'Unknown');self.assertEqual(rows[0]['sector_source'],'RESEARCH_ENRICHMENT');self.assertEqual(groups,[])
    def test_long_short_groups_use_gross_lots_and_keep_signed_values(self):
        marks=enriched(sector='Technology');marks['BBB']={**marks['AAA'],'symbol':'BBB','sector':'Healthcare'}
        rows,summary,groups,gross=self.calc([position(),position(symbol='BBB',qty=-10)],marks)
        self.assertEqual(gross,2200);self.assertEqual([g['weight_pct'] for g in groups],[50,50])
        self.assertEqual(sorted(g['value'] for g in groups),[-1100,1100]);self.assertTrue(all(g['weight_scope']=='GROSS_MARKED_LOTS_NOT_NAV' for g in groups))
    def test_partial_marks_do_not_hide_unpriced_exposure_in_the_denominator(self):
        rows,summary,groups,gross=self.calc([position(),position(symbol='BBB')],enriched(sector='Technology'))
        self.assertEqual(summary['sector_exposure']['position_count'],2);self.assertIsNone(groups[0]['weight_pct'])
        self.assertIsNone(summary['sector_exposure']['gross_marked_value']);self.assertEqual(gross,1100)
    def test_original_records_unchanged_and_zero_empty_evidence_distinct(self):
        inputs=[position(sector={'raw':['unknown',False]})];before=deepcopy(inputs)
        rows,summary,groups,_=self.calc(inputs,enriched());self.assertEqual(inputs,before)
        self.assertEqual(summary['sector_exposure']['records'][0]['reported_sector'],inputs[0]['sector'])
        rows,summary,groups,gross=self.calc([],{});self.assertEqual(summary['sector_exposure']['status'],'NO_GROSS_EXPOSURE');self.assertEqual(gross,0)


if __name__=='__main__':unittest.main()
