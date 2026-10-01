"""Investigate current funding defects with invented HTTP bodies only."""
from pathlib import Path
import ast,io,json,math,unittest,urllib.request
from unittest.mock import Mock,patch
R=Path(__file__).resolve().parents[1]/'tests/fixtures/crypto-funding-observations/before'
def run_funding(row):
 p=R/'aws/lambdas/justhodl-crypto-intel/source/lambda_function.py.txt';node=next(n for n in ast.parse(p.read_text(encoding='utf-8')).body if isinstance(n,ast.FunctionDef) and n.name=='fetch_funding')
 scope={'urllib':urllib,'json':json,'http_get':Mock(side_effect=AssertionError('Unexpected fallback'))};exec(compile(ast.Module(body=[node],type_ignores=[]),'current-funding-producer','exec'),scope)
 def response(request,**kw):
  return io.BytesIO(json.dumps({'code':'0','data':[row]}).encode())
 with patch('urllib.request.urlopen',side_effect=response) as get:
  result=scope['fetch_funding']();assert get.call_count==10
 scope['http_get'].assert_not_called();return result
class Reproductions(unittest.TestCase):
 def test_missing_rate_becomes_reported_zero_and_short(self):
  out=run_funding({'fundingTime':'1577836800000','nextFundingTime':'1577840400000'})
  self.assertEqual(out['status'],'success');self.assertEqual(out['rates'][0]['funding_rate'],0);self.assertEqual(out['rates'][0]['sentiment'],'SHORT')
 def test_one_hour_contract_still_gets_eight_hour_annualization(self):
  out=run_funding({'fundingRate':'0.0001','fundingTime':'1577836800000','nextFundingTime':'1577840400000'})
  self.assertEqual(out['rates'][0]['annualized_pct'],10.95);self.assertNotIn('fundingTime',out['rates'][0]);self.assertNotIn('interval_hours',out['rates'][0])
 def test_nonfinite_provider_string_poisons_public_json(self):
  out=run_funding({'fundingRate':'NaN'})
  self.assertTrue(math.isnan(out['avg_rate_pct']))
  with self.assertRaises(ValueError):json.dumps(out,allow_nan=False)
 def test_page_field_is_absent_even_when_valid_rate_is_present(self):
  out=run_funding({'fundingRate':'0.0001'})
  self.assertEqual(out['avg_rate_pct'],.01);self.assertNotIn('avg_funding',out);self.assertEqual(out.get('avg_funding',0),0)
if __name__=='__main__':unittest.main(verbosity=2)
