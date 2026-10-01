from pathlib import Path
import gzip,importlib.util,json,sys,unittest
R=Path(__file__).resolve().parents[1];W=R;sys.path.insert(0,str(R/'aws/shared'))
import context_evidence_store as store
spec=importlib.util.spec_from_file_location('candidate',R/'aws/lambdas/justhodl-squeeze-pretrigger/source/squeeze_research_model.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
PREFIX='audit-private/20260909-originals/squeeze-pretrigger-research/'
AT='2026-10-01T17:00:00+00:00'
def fixtures(si=None):
 return {'finra':{'tickers':{'TEST':{'svr_pct':99,'short_utilization':99,'squeeze_score':100}},'calls_eligible':True},
         'short_interest':{'contract':'short-interest-tickers.v1','by_ticker':{'TEST':{'short_interest':0,'latest':False,'settlement_date':'2000-01-01','days_to_cover':999,'dtc_effective':0,'dtc_reconstructed':0,'dtc_status':'provider_differs_from_reconstructed_ratio'}}} if si is None else si,
         'catalyst':{'events':[{'ticker':'TEST','date':'2026-10-02','type':'invented','calls_eligible':True}]}}
def prepared(documents=None):
 attempts={};sources={}
 for name,value in (fixtures() if documents is None else documents).items():
  if value is None:attempts[name]={'source_key':m.INPUTS[name],'status':'source_read_unavailable'};continue
  raw=value if isinstance(value,bytes) else json.dumps(value).encode();ref=store.identity(raw,PREFIX,'sources');sources[ref['key']]=raw;attempts[name]={'source_key':m.INPUTS[name],'status':'received','original_ref':ref,'content_encoding':''}
 return attempts,sources
def project(documents=None):return m.project(*prepared(documents),AT,store.validate_ref,PREFIX)
class Model(unittest.TestCase):
 def test_legacy_pressure_and_self_qualification_never_become_forecast_or_ticket(self):
  out=project();self.assertEqual(out['state'],'UNQUALIFIED');self.assertEqual(out['portfolio_action'],'WAIT')
  self.assertEqual(out['decision']['eligible_votes'],0);self.assertTrue(out['decision']['abstain'])
  self.assertTrue(all(out[k] is False for k in m.FLAGS));self.assertIsNone(out['recommended_trade']);self.assertIsNone(out['signal_strength'])
  self.assertEqual(out['historical_episodes'],[]);self.assertEqual(out['imminent_setups'],[])
  for key in ('1m','2m','wr'):self.assertIsNone(out['forward_expectations'][key])
 def test_zero_historical_and_corrected_ratio_are_descriptive(self):
  row=project()['short_position_context']['by_ticker']['TEST'];self.assertEqual(row['short_interest_shares'],0);self.assertEqual(row['days_to_cover'],0);self.assertEqual(row['reported_days_to_cover'],999);self.assertFalse(row['latest_reported']);self.assertFalse(row['identity_verified'])
 def test_missing_sources_are_unavailable_not_quiet_or_measured_zero(self):
  out=project(dict.fromkeys(m.INPUTS));self.assertEqual(out['state'],'UNQUALIFIED');self.assertEqual(out['summary']['feeds_available'],dict.fromkeys(m.INPUTS,False));self.assertIsNone(out['summary']['n_total_setups']);self.assertEqual(out['short_position_context']['by_ticker'],{})
 def test_ticker_duplicates_remain_withheld(self):
  d=fixtures();d['short_interest']['by_ticker']['test']=None;out=project(d)['short_position_context'];self.assertEqual(out['by_ticker'],{});self.assertEqual(out['ambiguous_symbol_count'],1);self.assertEqual(out['unresolved_occurrence_count'],1)
 def test_underflow_overflow_precision_loss_duplicates_and_bad_utf8_are_malformed(self):
  for raw in (b'{"x":1e-1000}',b'{"x":1e1000}',b'{"x":1.00000000000000001}',b'{"x":1,"x":2}',b'{"x":NaN}',b'\xff'):
   d=fixtures();d['short_interest']=raw;out=project(d);self.assertEqual(out['inputs']['short_interest']['read_status'],'malformed');self.assertEqual(out['inputs']['short_interest']['body_bytes'],len(raw));self.assertEqual(out['short_position_context']['by_ticker'],{})
 def test_bounded_whole_gzip_and_encoding(self):
  raw=gzip.compress(b'{"x":0.1}',mtime=0);self.assertEqual(m.strict(raw,'gzip'),{'x':0.1})
  for raw,encoding in ((raw+b'x','gzip'),(raw+raw,'gzip'),(raw[:-1],'gzip'),(raw,'identity'),(b'{}','gzip')):
   with self.subTest(encoding=encoding),self.assertRaises(ValueError):m.strict(raw,encoding)
 def test_retained_source_identity_and_inventory_are_exact(self):
  attempts,sources=prepared();ref=attempts['short_interest']['original_ref'];sources[ref['key']]+=b' '
  with self.assertRaises(ValueError):m.project(attempts,sources,AT,store.validate_ref,PREFIX)
  attempts,sources=prepared();attempts.pop('finra')
  with self.assertRaises(ValueError):m.project(attempts,sources,AT,store.validate_ref,PREFIX)
 def test_isolated_surrogate_escape_is_malformed_before_projection_can_break_utf8(self):
  d=fixtures();d['short_interest']=b'{"contract":"short-interest-tickers.v1","by_ticker":{"TEST":{"dtc_status":"\\ud800"}}}'
  out=project(d);self.assertEqual(out['inputs']['short_interest']['read_status'],'malformed')
  self.assertEqual(out['short_position_context']['by_ticker'],{});self.assertTrue(store.encode(out))
 def test_corrupt_gzip_is_retained_as_malformed_without_aborting_other_sources(self):
  raw=bytearray(gzip.compress(b'{"x":1}',mtime=0));raw[-8]^=255
  d=fixtures();d['short_interest']=bytes(raw);out=project(d)
  self.assertEqual(out['inputs']['short_interest']['read_status'],'malformed')
  self.assertEqual(out['inputs']['short_interest']['body_bytes'],len(raw))
  self.assertEqual(out['inputs']['catalyst']['read_status'],'parsed')
  self.assertEqual(out['state'],'UNQUALIFIED');self.assertEqual(out['short_position_context']['by_ticker'],{})
 def test_complete_input_projection_is_deterministic_and_size_selection_is_disclosed(self):
  d=fixtures();d['short_interest']['by_ticker']={'T'+str(i):{'short_interest':i} for i in range(502)}
  first=project(d);second=project(d);self.assertEqual(store.encode(first),store.encode(second));c=first['short_position_context'];self.assertEqual(c['available_unambiguous_tickers'],502);self.assertEqual(c['projected_tickers'],500);self.assertEqual(c['omitted_from_projection'],2);self.assertTrue(c['whole_source_body_retained']);self.assertEqual(list(c['by_ticker']),sorted(d['short_interest']['by_ticker'])[:500])
if __name__=='__main__':unittest.main(verbosity=2)
