from pathlib import Path
from base64 import b64decode
import hashlib,json,runpy,unittest
from decimal import localcontext,Decimal
W=Path(__file__).resolve().parents[1]/'aws/shared';M=runpy.run_path(str(W/'crypto_stablecoin_observations.py'))
def row(ident='one',value=10,prior=None,peg='peggedUSD'):
 d={'id':ident,'name':ident,'symbol':ident,'pegType':peg,'circulating':{peg:value}}
 if prior is not None:d['circulatingPrevWeek']={peg:prior}
 return d
def build(rows):return M['build'](json.dumps({'peggedAssets':rows}).encode())
class Stock(unittest.TestCase):
 def test_zero_is_real_and_missing_is_unavailable(self):
  rows=build([row('zero',0),row('absent',None)])['stablecoins']
  self.assertEqual(rows[0]['snapshots']['current']['value'],0);self.assertIsNone(rows[1]['snapshots']['current']['value'])
  self.assertIsNone(rows[0]['comparisons']['reported_previous_week']['percent_decimal'])
 def test_missing_prior_cannot_create_zero_change_or_stable_signal(self):
  out=build([row()]);self.assertIsNone(out['stablecoins'][0]['comparisons']['reported_previous_week']['difference_usd_decimal']);self.assertEqual(out['net_signal'],'UNAVAILABLE');self.assertIsNone(out['minting_count']);self.assertIsNone(out['stable_count'])
 def test_twenty_six_rows_are_all_preserved_without_size_filter(self):
  out=build([row(str(i),i) for i in range(26)])
  self.assertEqual(len(out['stablecoins']),26);self.assertEqual(out['identified_rows'],26);self.assertIsNone(out['total_mcap'])
 def test_euro_peg_is_retained_under_its_reported_field(self):
  out=build([row('euro',12,10,'peggedEUR')])['stablecoins'][0]
  self.assertEqual(out['snapshots']['current']['value'],12);self.assertEqual(out['peg_type'],'peggedEUR');self.assertEqual(out['comparisons']['reported_previous_week']['percent_decimal'],'20.0')
 def test_non_measurements_cannot_become_stock_values(self):
  for value in [True,False,None,'12',{},[],-1]:
   out=build([row(value=value)]);self.assertIsNone(out['stablecoins'][0]['snapshots']['current']['value'])
 def test_duplicate_or_malformed_rows_are_retained_unresolved(self):
  out=build([row(),row(),[1],False]);self.assertEqual(out['reported_rows'],4);self.assertEqual(out['unresolved_rows'],4);self.assertEqual(len(out['stablecoins']),4)
 def test_snapshot_arithmetic_keeps_basis_and_no_cash_flow(self):
  out=build([row(value=5,prior=10)]);c=out['stablecoins'][0]['comparisons']['reported_previous_week'];self.assertEqual(c['difference_usd_decimal'],'-5');self.assertEqual(c['percent_decimal'],'-50.0');self.assertFalse(c['period_dates_verified']);self.assertFalse(c['cash_flow_verified'])
 def test_zero_prior_preserves_absolute_difference_without_percent(self):
  c=build([row(value=3,prior=0)])['stablecoins'][0]['comparisons']['reported_previous_week'];self.assertEqual(c['difference_usd_decimal'],'3');self.assertIsNone(c['percent_decimal']);self.assertEqual(c['reason'],'zero_comparison_denominator')
 def test_exact_originals_survive_invalid_payload_and_arithmetic(self):
  for raw in [b'{}',b'{"a":NaN}',b'\xff',b'{"peggedAssets":[],"peggedAssets":[1]}',json.dumps({'peggedAssets':[row()]}).encode()]:
   out=M['build'](raw);self.assertEqual(b64decode(out['original_response_base64']),raw);self.assertEqual(out['original_response_sha256'],hashlib.sha256(raw).hexdigest());json.dumps(out,allow_nan=False)
 def test_every_output_denies_investment_authority(self):
  out=build([row()]);self.assertTrue(all(out[k]==v for k,v in M['DENIED'].items()));self.assertTrue(all(out['stablecoins'][0][k]==v for k,v in M['DENIED'].items()))
 def test_non_terminating_percent_is_reproducible_from_exact_operands(self):
  c=build([row(value=4,prior=3)])['stablecoins'][0]['comparisons']['reported_previous_week'];self.assertEqual(c['percent_numerator_usd_decimal'],'1');self.assertEqual(c['percent_denominator_usd_decimal'],'3');self.assertEqual(c['percent_precision_digits'],34)
 def test_arithmetic_is_independent_of_ambient_decimal_context(self):
  with localcontext() as ctx:
   ctx.prec=2
   out=build([row(value=1e308,prior=5e-324)])
  c=out['stablecoins'][0]['comparisons']['reported_previous_week']
  with localcontext() as ctx:
   ctx.prec=1024
   self.assertEqual(Decimal(c['difference_usd_decimal']),Decimal('1e308')-Decimal('5e-324'))
  self.assertEqual(c['percent_denominator_usd_decimal'],'5E-324')
 def test_unrepresentable_json_exponent_is_retained_as_invalid(self):
  raw=b'{"peggedAssets":[],"value":1e9999999999999999999999999999999999}'
  out=M['build'](raw)
  self.assertEqual(out['reason'],'invalid_original_response');self.assertEqual(b64decode(out['original_response_base64']),raw)
 def test_oversized_population_refuses_projection_without_truncating_original(self):
  rows=[row(str(i)) for i in range(M['MAX_ROWS']+1)];raw=json.dumps({'peggedAssets':rows}).encode();out=M['build'](raw)
  self.assertEqual(out['status'],'unavailable');self.assertFalse(out['projection_complete']);self.assertEqual(out['stablecoins'],[])
  self.assertEqual(out['source_rows_received'],len(rows));self.assertEqual(b64decode(out['original_response_base64']),raw)
if __name__=='__main__':unittest.main(verbosity=2)
