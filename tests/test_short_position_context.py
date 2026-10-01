from pathlib import Path
import importlib.util,unittest,json
ROOT=Path(__file__).resolve().parents[1];s=importlib.util.spec_from_file_location('context',ROOT/'aws/shared/short_position_context.py');m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
def packet(rows):return {'contract':'short-interest-tickers.v1','by_ticker':rows}
class Reader(unittest.TestCase):
 def test_non_objects_and_wrong_contract_are_explicit_unavailable(self):
  for value in [None,[],[1],'text',False,{}, {'contract':'wrong','by_ticker':{}}]:self.assertEqual(m.descriptive_context(value)['status'],'unavailable')
 def test_missing_values_do_not_become_zero_or_signal(self):
  row=m.descriptive_context(packet({'TEST':{}}))['by_ticker']['TEST']
  for k in ['short_interest_shares','si_change_pct','days_to_cover','si_pct_float','short_utilization','borrow_rate','signal','score']:self.assertIsNone(row[k])
 def test_corrected_ratio_not_overridden_by_unreconciled_provider(self):
  row=m.row_context('TEST',{'days_to_cover':'999','dtc_effective':'2','dtc_reconstructed':'2','dtc_status':'provider_differs_from_reconstructed_ratio'});self.assertEqual(row['days_to_cover'],2);self.assertEqual(row['reported_days_to_cover'],999)
 def test_unknown_reconciliation_status_never_qualifies_effective_value(self):
  for status in [None,'provider_ratio_mismatch','garbage','unavailable_zero_reported_adv']:
   self.assertIsNone(m.row_context('TEST',{'dtc_status':status,'dtc_effective':2,'dtc_reconstructed':2})['days_to_cover'])
 def test_effective_ratio_requires_matching_reconstruction(self):
  for ratio in [None,0,1,3,'NaN',True]:
   self.assertIsNone(m.row_context('TEST',{'dtc_status':'provider_differs_from_reconstructed_ratio','dtc_effective':2,'dtc_reconstructed':ratio})['days_to_cover'])
 def test_binary_float_rounding_cannot_hide_decimal_disagreement(self):
  for status,field in [('matches_reconstructed_rounded_ratio','days_to_cover'),('provider_differs_from_reconstructed_ratio','dtc_reconstructed')]:
   self.assertIsNone(m.row_context('TEST',{'dtc_status':status,'dtc_effective':'2.0000000000000001',field:'2'})['days_to_cover'])
 def test_true_reconciled_zero_retained_without_fallback(self):
  row=m.row_context('TEST',{'days_to_cover':0,'dtc_effective':'0','dtc_status':'matches_reconstructed_rounded_ratio','short_interest':'0','change_pct':0});self.assertEqual(row['days_to_cover'],0);self.assertEqual(row['short_interest_shares'],0);self.assertEqual(row['si_change_pct'],0)
 def test_trusted_status_with_conflicting_numbers_is_not_reconciled(self):
  self.assertIsNone(m.row_context('TEST',{'days_to_cover':0,'dtc_effective':12,'dtc_status':'matches_reconstructed_rounded_ratio'})['days_to_cover'])
 def test_bad_types_nonfinite_underflow_and_unsafe_integers_withheld(self):
  for value in [None,True,False,{},[],'NaN','Infinity','1e400','1e-400','',10**5000]:
   self.assertIsNone(m.number(value))
  self.assertIsNone(m.number('9007199254740992',integer=True));self.assertIsNone(m.number('1.5',integer=True));self.assertIsNone(m.number('-1',nonnegative=True))
 def test_complete_ambiguous_occurrences_preserved_without_choosing_winner(self):
  out=m.descriptive_context(packet({'TEST':{'short_interest':'1'},'test':{'short_interest':'1000'}}));self.assertEqual(out['by_ticker'],{});self.assertEqual(len(out['ambiguous_symbols'][0]['occurrences']),2)
  self.assertEqual([r['source_row'] for r in out['ambiguous_symbols'][0]['occurrences']],['/by_ticker/TEST','/by_ticker/test'])
 def test_invalid_non_json_keys_never_escape_as_unserializable_metadata(self):
  out=m.descriptive_context(packet({object():{},('tuple',):{},False:{}}));self.assertEqual(out['by_ticker'],{});self.assertEqual(len(out['unresolved_occurrences']),3);json.dumps(out,allow_nan=False)
 def test_mismatched_ticker_retained_as_unresolved_not_remapped(self):
  out=m.descriptive_context(packet({'TEST':{'ticker':'OTHER'}}));self.assertEqual(out['by_ticker'],{});self.assertEqual(len(out['unresolved_occurrences']),1)
 def test_historical_and_self_promoting_rows_never_gain_authority(self):
  out=m.descriptive_context(packet({'TEST':{'latest':False,'settlement_date':'2000-01-01','dtc_effective':'10','calls_eligible':True,'score':100}}));row=out['by_ticker']['TEST'];self.assertFalse(row['latest_reported']);self.assertFalse(row['observation_freshness_verified']);self.assertTrue(all(row[k] is False for k in m.FLAGS));self.assertEqual(out['independent_investment_votes'],0)
if __name__=='__main__':unittest.main(verbosity=2)
