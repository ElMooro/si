"""Independent audit detects profile/source drift before production integration."""
from pathlib import Path
from decimal import Decimal,localcontext
import ast,copy,json,sys,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/shared/tests')]
import etf_profile_native as native
from test_etf_profile_native import fixture,row,AT
PATH=ROOT/'aws/ops/staged/ops_5984_etf_profile_original_replay.py'
node=next(n for n in ast.parse(PATH.read_text(encoding='utf-8')).body if isinstance(n,ast.FunctionDef) and n.name=='independent')
ns={'native':native,'Decimal':Decimal,'localcontext':localcontext,'json':json}
exec(compile(ast.Module(body=[node],type_ignores=[]),str(PATH),'exec'),ns)
check=ns['independent']

class OriginalProfileReplay(unittest.TestCase):
    def arrange(self):
        c,objects,_=fixture([row(currency_exposure={'usd':3.171},fee_waivers=0)])
        return c,objects,native.reconstruct(c,objects.__getitem__,AT)
    def test_exact_originals_and_every_supported_field(self):
        c,objects,result=self.arrange();counts=check(c,result,objects.__getitem__)
        self.assertEqual(counts['profiles'],1);self.assertEqual(counts['numeric_fields'],19)
        self.assertEqual(counts['exposure_entries'],3);self.assertEqual(counts['exposure_sums'],2)
    def test_fee_conversion_and_exposure_normalization_are_detected(self):
        for mode in ('fee','sum','date','unit','source'):
            c,objects,result=self.arrange();p=result['profiles'][0]
            if mode=='fee':p['numeric']['net_expenses']['value_decimal']='3'
            elif mode=='sum':p['exposures']['currency_exposure']['raw_observed_sum_decimal']='1'
            elif mode=='date':p['effective_date']='2026-09-21'
            elif mode=='unit':p['numeric']['aum']['unit_certified']=True
            elif mode=='source':p['numeric']['net_expenses']['source']['field']='management_fee'
            with self.subTest(mode=mode),self.assertRaises(AssertionError):check(c,result,objects.__getitem__)
    def test_missing_value_cannot_be_replaced_by_zero(self):
        c,objects,result=self.arrange();result['profiles'][0]['numeric']['total_expenses']['value_decimal']='0'
        with self.assertRaises(AssertionError):check(c,result,objects.__getitem__)

if __name__=='__main__':unittest.main(verbosity=2)
