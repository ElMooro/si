from pathlib import Path
import sys,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'aws/ops/staged'))
import ops_5989_option_contract_original_replay as audit
from test_option_contract_research import row,page

class Tests(unittest.TestCase):
    def fixture(self):
        c,p=row('call'),row('put');c['open_interest']=10;p['open_interest']=20
        pages=[page([c,p])];raw=audit.model.capture.decode(pages[0]['raw'])['results']
        out=audit.model.compile_rows('SPY',pages,True)
        return raw,pages,out
    def test_original_arithmetic_passes(self):
        raw,pages,out=self.fixture();counts=audit.verify_fields(raw,pages,out)
        self.assertEqual(counts['original_rows'],2);self.assertEqual(counts['quantity_fields'],4)
        self.assertEqual(counts['dated_volume_groups'],1)
    def test_tampered_quantity_rejected(self):
        raw,pages,out=self.fixture();out['rows'][0]['metrics']['open_interest']['value']='11'
        with self.assertRaises(AssertionError):audit.verify_fields(raw,pages,out)
    def test_tampered_aggregation_rejected(self):
        raw,pages,out=self.fixture();out['daily_bar_update_groups'][0]['calls']['value']='11'
        with self.assertRaises(AssertionError):audit.verify_fields(raw,pages,out)
    def test_ratio_cannot_self_justify_with_changed_numerator(self):
        raw,pages,out=self.fixture();out['reported_open_interest']['call_put_ratio'].update(numerator='200',value='10')
        with self.assertRaises(AssertionError):audit.verify_fields(raw,pages,out)
    def test_wrong_original_row_reference_rejected(self):
        raw,pages,out=self.fixture();out['rows'][1]['evidence']['row_index']=0
        with self.assertRaises(AssertionError):audit.verify_fields(raw,pages,out)
    def test_fabricated_oi_clock_rejected(self):
        raw,pages,out=self.fixture();out['rows'][0]['open_interest_date']='2026-09-18'
        with self.assertRaises(AssertionError):audit.verify_fields(raw,pages,out)
    def test_negative_gamma_is_not_repaired_to_zero(self):
        c=row();c['greeks']['gamma']=-.001;pages=[page([c,row('put')])]
        raw=audit.model.capture.decode(pages[0]['raw'])['results'];out=audit.model.compile_rows('SPY',pages,True)
        self.assertEqual(audit.verify_fields(raw,pages,out)['negative_gamma_retained_and_excluded'],1)
        out['rows'][0]['metrics']['vendor_gamma'].update(value='0',state='reported_zero')
        with self.assertRaises(AssertionError):audit.verify_fields(raw,pages,out)

if __name__=='__main__':unittest.main()
