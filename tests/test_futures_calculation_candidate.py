from pathlib import Path
import json,sys,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'tests'),str(ROOT/'aws/shared'),str(ROOT/'aws/ops/staged')]
import futures_research_model as model
import ops_6011_futures_calendar_candidate as audit
from test_futures_research_model import fixture,compile_,change,add_calendar
class Tests(unittest.TestCase):
    def data(self):
        originals,sources=fixture();out,artifacts=compile_(originals,sources)
        return out,sources,{**originals,**artifacts}
    def test_complete_original_inventory_calculations_and_curves_reconcile(self):
        out,sources,blobs=self.data();r=audit.independent(out,sources,blobs.__getitem__)
        self.assertEqual(r['products'],7);self.assertEqual(r['datasets'],35);self.assertEqual(r['original_rows'],343)
        self.assertEqual(r['exact_price_comparisons'],84);self.assertEqual(r['exact_common_session_spreads'],14)
    def test_tampered_exact_or_displayed_arithmetic_rejected(self):
        for field,value in (('absolute_change_decimal','99'),('percent_change_exact',{'numerator':'99','denominator':'1'})):
            out,sources,blobs=self.data();out['products']['ES']['contracts'][0]['comparisons']['close']['5'][field]=value
            with self.assertRaises(AssertionError):audit.independent(out,sources,blobs.__getitem__)
    def test_wrong_endpoint_or_curve_session_rejected(self):
        for edit in (lambda p:p['contracts'][0]['comparisons']['close']['5']['from'].update(ordinal=0),
            lambda p:p['matched_curves'][0].update(session_end_date='2020-01-01')):
            out,sources,blobs=self.data();edit(out['products']['ES'])
            with self.assertRaises(AssertionError):audit.independent(out,sources,blobs.__getitem__)
    def test_self_consistently_rehashed_record_still_fails_original_reconciliation(self):
        out,sources,blobs=self.data();d=out['datasets']['ES:bars:ESZ6'];old=d['records'][0]
        doc=json.loads(blobs[old['key']]);doc['rows'][0]['values']['close']['number']['decimal']='999'
        raw=model.encoded(doc);ref=model.ref(raw,'records');blobs[ref['key']]=raw;d['records'][0]=ref
        with self.assertRaises(AssertionError):audit.independent(out,sources,blobs.__getitem__)
    def test_unit_coverage_clock_and_authority_mutations_rejected(self):
        edits=[lambda o:o['products']['ES']['specification'].update(usd_value_per_price_unit_per_contract_decimal='999'),
            lambda o:o['products']['ES']['contracts'][0]['coverage'].update(missing_settlement_rows=1),
            lambda o:o.update(source_capture_completed_at=o['generated_at']),lambda o:o.update(sizing_eligible=True)]
        for edit in edits:
            out,sources,blobs=self.data();edit(out)
            with self.assertRaises(AssertionError):audit.independent(out,sources,blobs.__getitem__)
    def test_missing_settlement_and_sparse_history_reconcile_without_fabrication(self):
        originals,sources=fixture()
        change(originals,sources,'ES:bars:ESZ6',lambda d:d.update(results=d['results'][-2:]))
        change(originals,sources,'ES:bars:ESH7',lambda d:d['results'][-1].pop('settlement_price'))
        out,artifacts=compile_(originals,sources);r=audit.independent(out,sources,{**originals,**artifacts}.__getitem__)
        self.assertLess(r['exact_price_comparisons'],84);self.assertLess(r['exact_common_session_spreads'],14)
    def test_calendar_and_completed_only_views_reconcile_against_original_event_rows(self):
        blobs,sources=fixture();add_calendar(blobs,sources);out,artifacts=compile_(blobs,sources)
        r=audit.independent(out,sources,{**blobs,**artifacts}.__getitem__)
        self.assertGreater(r['exact_price_comparisons'],84)
    def test_changed_calendar_or_promotion_of_partial_session_is_rejected(self):
        for edit in (lambda p:p['session_calendar']['sessions']['2026-09-21'].update(scheduled_close_utc='2026-09-21T18:00:00Z'),
            lambda p:p['contracts'][0]['coverage'].update(scheduled_ended_rows=23)):
            blobs,sources=fixture();add_calendar(blobs,sources);out,artifacts=compile_(blobs,sources);edit(out['products']['ES'])
            with self.assertRaises(AssertionError):audit.independent(out,sources,{**blobs,**artifacts}.__getitem__)
if __name__=='__main__':unittest.main(verbosity=2)
