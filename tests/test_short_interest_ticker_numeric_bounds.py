from pathlib import Path
from decimal import Decimal, localcontext
import copy,hashlib,importlib.util,json,sys,unittest
R=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(R/'aws/shared'),str(R/'tests')]
import short_interest_tickers as t
import short_interest_measurements as m
from test_short_interest_ticker_identity import record,shards

class NumericBounds(unittest.TestCase):
 def test_complete_predecessor_and_both_reported_failures_are_reproducible(self):
  fixture=R/'tests/fixtures/short-interest-numeric-bounds/predecessor.py.txt'
  manifest=json.loads((R/'tests/fixtures/short-interest-numeric-bounds/edits.json').read_bytes())
  raw=fixture.read_bytes();self.assertEqual(hashlib.sha256(raw).hexdigest(),manifest['predecessor_sha256'])
  old={};exec(compile(raw,str(fixture),'exec'),old)
  self.assertEqual(len(format(old['_decimal_or_none']('1e40000'),'f')),40001)
  rows={key:{'ticker':key,'latest':True,'short_interest':amount,'dtc_effective':'1.000000000001'} for key,amount in [('LOW','10000000000000000000000000000.1'),('HIGH','10000000000000000000000000000.2')]}
  self.assertEqual([r['ticker'] for r in old['build_ranked_lists'](rows)['top_squeeze_risk']],['LOW','HIGH'])
  text=raw.decode('utf-8')
  for before,after in manifest['edits']:self.assertEqual(text.count(before),1);text=text.replace(before,after)
  self.assertEqual(text,Path(t.__file__).read_text(encoding='utf-8'))
  previous=json.loads((R/'tests/fixtures/short-interest-numeric-bounds/previous-preservation.json').read_bytes())
  previous['files'][manifest['target']]['edits'].extend(manifest['edits'])
  self.assertEqual(previous,json.loads((R/'tests/fixtures/short-interest-ticker-projection/preservation.json').read_bytes()))
 def test_small_exponent_tokens_cannot_expand_unbounded_display(self):
  for value in ['1e1000000000','1e40000','1e-40000','0e40000','-0e-40000',Decimal('1e40000')]:
   with self.subTest(value=str(value)):self.assertIsNone(t._decimal_or_none(value))
 def test_strict_ascii_decimal_grammar_rejects_non_numbers(self):
  for value in [None,True,False,[],{},'',' 1','1 ','1_000','NaN','sNaN','Infinity','１２','1\n',float('inf')]:
   with self.subTest(value=str(value)):self.assertIsNone(t._decimal_or_none(value))
 def test_parse_does_not_call_arbitrary_object_string_conversion(self):
  class Unexpected:
   def __str__(self):raise AssertionError('must not call user-defined conversion')
  self.assertIsNone(t._decimal_or_none(Unexpected()))
 def test_long_strings_big_integers_and_coefficients_reject_before_conversion(self):
  self.assertIsNone(t._decimal_or_none('1'*129));self.assertIsNone(t._decimal_or_none(10**1000))
  self.assertIsNone(t._decimal_or_none(Decimal('0.'+'1'*129)))
 def test_genuine_zero_small_fractions_and_reported_precision_are_exact(self):
  for value in ['0','-0.000000000000000000000000','1e-24','999999999999999999999999999999.123456789012345678901234',0,0.125]:
   with self.subTest(value=value):self.assertEqual(t._decimal_or_none(value),Decimal(str(value)))
 def test_canonical_largest_derived_values_are_not_clipped_to_provider_bound(self):
  large=Decimal('999999999999999999999999999999.999999999999999999999999')
  tiny=Decimal('0.000000000000000000000001')
  for value in [m.quotient(large,tiny),m.quotient(large,tiny,100),m.quotient(tiny,large)]:
   self.assertEqual(t._decimal_or_none(value),Decimal(value));self.assertLess(len(format(t._decimal_or_none(value),'f')),150)
 def test_invalid_reconstructed_ratio_stays_unavailable_without_losing_original(self):
  r=record();point=r['observations'][0]
  point[m.POINT_FIELDS.index('days_to_cover_status')]='provider_differs_from_reconstructed_ratio'
  point[m.POINT_FIELDS.index('reconstructed_position_to_reported_adv_days')]='1e40000'
  original=copy.deepcopy(r);row=t.build_tickers_view(shards(r))['TEST']
  self.assertIsNone(row['dtc_effective']);self.assertEqual(row['dtc_reconstructed'],'1e40000');self.assertEqual(r,original)
 def test_invalid_provider_ratio_can_use_only_valid_reconstructed_measurement(self):
  r=record();point=r['observations'][0];point[m.POINT_FIELDS.index('daysToCoverQuantity')]='1e40000'
  row=t.build_tickers_view(shards(r))['TEST'];self.assertEqual(row['dtc_effective'],'0.500000000000');self.assertEqual(row['days_to_cover'],'1e40000')
 def test_malformed_composite_input_does_not_crash_or_enter_sorted_lists(self):
  rows={'TEST':{'ticker':'TEST','latest':True,'short_interest':'1e999999','dtc_effective':'1e999999','change_pct':'-1e999999'}}
  self.assertTrue(all(v==[] for v in t.build_ranked_lists(rows).values()))
 def test_product_order_distinguishes_values_beyond_default_28_digit_precision(self):
  rows={key:{'ticker':key,'latest':True,'short_interest':amount,'dtc_effective':'1.000000000001'} for key,amount in [('LOW','10000000000000000000000000000.1'),('HIGH','10000000000000000000000000000.2')]}
  expected=['HIGH','LOW']
  for precision in (6,28,128):
   with localcontext() as context:
    context.prec=precision
    self.assertEqual([r['ticker'] for r in t.build_ranked_lists(rows)['top_squeeze_risk']],expected)
 def test_product_does_not_depend_on_ambient_exponent_limit(self):
  with localcontext() as context:
   context.prec=3;context.Emax=5;context.Emin=-5
   self.assertEqual(t._exact_crowding_product(Decimal('1e54'),Decimal('1e24')),Decimal('1e78'))
 def test_bounds_limit_fixed_point_output_and_preserve_negative_change(self):
  for value in ['1e79','1e-64','-1e79','-1e-64','0e80']:
   parsed=t._decimal_or_none(value);self.assertIsNotNone(parsed);self.assertLessEqual(len(format(parsed,'f')),146)
  self.assertIsNone(t._decimal_or_none('1e80'));self.assertIsNone(t._decimal_or_none('1e-65'))
  row={'ticker':'TEST','latest':True,'short_interest':'0','dtc_effective':None,'change_pct':'-100'}
  self.assertEqual(t.build_ranked_lists({'TEST':row})['top_covering'][0]['ticker'],'TEST')

if __name__=='__main__':unittest.main(verbosity=2)
