from pathlib import Path
import json,sys,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'tests'),str(ROOT/'aws/shared'),str(ROOT/'aws/ops/staged')]
import fx_research_model as model
import ops_6006_fx_calculation_candidate as audit
from test_fx_research_model import fixture,compile_


class Tests(unittest.TestCase):
    def data(self):
        originals,sources=fixture();out,artifacts=compile_(originals,sources)
        return out,sources,{**originals,**artifacts}
    def test_every_reported_field_and_both_rate_orientations_reconcile(self):
        out,sources,blobs=self.data();result=audit.independent(out,sources,blobs.__getitem__)
        self.assertEqual(result,{'pairs':19,'original_rows':437,'reported_numeric_field_slots':3496,'exact_rate_comparisons':114})
    def test_rounded_or_exact_return_tampering_fails(self):
        for field,value in (('quoted_rate_change_decimal','99'),('inverse_rate_change_exact',{'numerator':'99','denominator':'1'})):
            out,sources,blobs=self.data();out['pairs']['EUR_USD']['comparisons']['5'][field]=value
            with self.assertRaises(AssertionError):audit.independent(out,sources,blobs.__getitem__)
    def test_wrong_endpoint_identity_fails_even_when_value_was_copied(self):
        out,sources,blobs=self.data();out['pairs']['EUR_USD']['comparisons']['5']['from']['ordinal']=0
        with self.assertRaises(AssertionError):audit.independent(out,sources,blobs.__getitem__)
    def test_self_consistently_rehashed_row_value_still_fails_original_reconciliation(self):
        out,sources,blobs=self.data();pair=out['pairs']['EUR_USD'];old=pair['bar_blocks'][0]
        body=json.loads(blobs[old['key']]);body['rows'][0]['values']['v']['decimal']='999'
        raw=model.encoded(body);ref=model.ref(raw,'bars');blobs[ref['key']]=raw;pair['bar_blocks'][0]=ref
        with self.assertRaises(AssertionError):audit.independent(out,sources,blobs.__getitem__)
    def test_missing_pair_or_comparison_is_not_qualified_coverage(self):
        for mode in ('pair','comparison'):
            out,sources,blobs=self.data()
            if mode=='pair':out['pairs'].pop('USD_TRY')
            else:out['pairs']['EUR_USD']['comparisons'].pop('20')
            with self.assertRaises(AssertionError):audit.independent(out,sources,blobs.__getitem__)
    def test_authority_cannot_be_added_to_a_measurement(self):
        out,sources,blobs=self.data();out['pairs']['EUR_USD']['comparisons']['1']['sizing_eligible']=True
        with self.assertRaises(AssertionError):audit.independent(out,sources,blobs.__getitem__)
    def test_wrong_metadata_is_rejected_even_when_returns_are_right(self):
        mutations=[lambda o:o.update(source_capture_completed_at=o['generated_at']),
            lambda o:o['pairs']['EUR_USD'].update(price_unit='USD'),
            lambda o:o['pairs']['XAU_USD'].update(metal_base_quantity_unit_verified=True),
            lambda o:o['pairs']['EUR_USD']['latest_reported_row'].update(reported_close_decimal='999'),
            lambda o:o['pairs']['EUR_USD']['comparisons']['5']['from'].update(close_observed_at=o['generated_at']),
            lambda o:o['pairs']['EUR_USD']['coverage'].update(positive_close_rows=1),
            lambda o:o['pairs']['EUR_USD']['comparisons']['5'].update(positive_close_rows_in_window=1)]
        for mutate in mutations:
            out,sources,blobs=self.data();mutate(out)
            with self.assertRaises(AssertionError):audit.independent(out,sources,blobs.__getitem__)


if __name__=='__main__':unittest.main(verbosity=2)
