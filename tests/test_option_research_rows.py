from pathlib import Path
import copy,sys,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'aws/shared'))
import option_research_rows as codec
from test_option_contract_research import compute,row

class Tests(unittest.TestCase):
    def records(self):
        c,p=row('call'),row('put');c['greeks']['gamma']=-.00001;p['open_interest']=None
        return compute([c,p])['rows']
    def test_lossless_values_null_zero_and_invalid_reported_value(self):
        records=self.records();block=codec.pack(records)
        self.assertEqual(codec.unpack(block),records)
        self.assertEqual(block['rows'][0]['cells']['vendor_gamma']['reported_value'],'-0.00001')
        self.assertNotIn('reported_value',block['rows'][0]['cells']['open_interest'])
        self.assertLess(len(codec.encoded(block)),len(codec.encoded(records)))
    def test_altered_value_cannot_pass_expanded_digest(self):
        block=codec.pack(self.records());block['rows'][0]['cells']['strike']['value']='101'
        with self.assertRaises(ValueError):codec.unpack(block)
    def test_cells_cannot_override_unit(self):
        block=codec.pack(self.records());block['rows'][0]['cells']['strike']['unit']='percent'
        with self.assertRaises(ValueError):codec.unpack(block)
    def test_metadata_cannot_override_common_authority(self):
        block=codec.pack(self.records());block['rows'][0]['record']['calls_eligible']=True
        with self.assertRaises(ValueError):codec.unpack(block)
    def test_cross_source_page_refused(self):
        records=self.records();records[1]['evidence']['page']=2
        with self.assertRaises(ValueError):codec.pack(records)
    def test_duplicate_original_row_refused(self):
        records=self.records();records[1]['evidence']=copy.deepcopy(records[0]['evidence'])
        with self.assertRaises(ValueError):codec.pack(records)
    def test_nonobject_source_row_and_missing_metrics_survive(self):
        records=compute([row(),None])['rows'];self.assertEqual(codec.unpack(codec.pack(records)),records)
    def test_row_count_tampering_rejected(self):
        block=codec.pack(self.records());block['row_count']=1
        with self.assertRaises(ValueError):codec.unpack(block)
    def test_record_pointer_cannot_be_relabelled(self):
        records=self.records();records[1]['evidence']['row_pointer']='/results/0'
        with self.assertRaises(ValueError):codec.pack(records)
    def test_limit_is_one_provider_page(self):
        with self.assertRaises(ValueError):codec.pack([])
        with self.assertRaises(ValueError):codec.pack(self.records()*126)

if __name__=='__main__':unittest.main()
