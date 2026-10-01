from pathlib import Path
import copy,hashlib,importlib.util,json,unittest
W=Path(__file__).resolve().parents[1]/'source'
spec=importlib.util.spec_from_file_location('status',W/'research_status.py');mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod)
SPECS={name:{'key':'data/'+name+'.json'} for name in ['signal_board','auction_crisis','auction_crisis_ai','macro_frontrun','crisis_brief','bonds','repo','regime','correlations','global_stress','sentiment','volatility']}
STAMP='2026-10-01T13:00:00Z';DIGEST='a'*64
def loaded():return {'_availability':{'read_status':'parsed','body_sha256':DIGEST,'body_bytes':123,'storage_last_modified':STAMP},'calls_eligible':True,'score':100,'regime':'RISK_ON'}
class Status(unittest.TestCase):
 def run_status(self,value):return mod.compile_status(SPECS,value,STAMP,DIGEST)
 def test_empty_keeps_all_twelve_inputs_and_abstains(self):
  value=self.run_status({});self.assertEqual(len(value['input_status']),12);self.assertEqual(value['engines_loaded'],0);self.assertEqual(value['decision']['action'],'WAIT');self.assertEqual(value['synthesis']['global_posture'],'WAIT')
 def test_all_parsed_self_promoting_inputs_still_have_zero_votes(self):
  value=self.run_status({n:loaded() for n in SPECS});self.assertEqual(value['engines_loaded'],12);self.assertEqual(value['decision']['eligible_votes'],0);self.assertTrue(all(value[f] is False for f in mod.FLAGS));self.assertEqual(set(value['snapshot_age_min'].values()),{None});self.assertNotIn('RISK_ON',json.dumps(value))
 def test_partial_availability_is_not_zero_filled_or_hidden(self):
  value=self.run_status({'bonds':loaded(),'repo':{'_availability':{'read_status':'missing'}}});self.assertEqual(value['engines_loaded'],1);self.assertEqual(value['input_status']['repo']['read_status'],'missing');self.assertIn('1 of 1',value['synthesis']['per_page_focus']['bonds'])
 def test_incomplete_hash_or_count_cannot_claim_a_parsed_artifact(self):
  for key,bad in [('body_sha256','wrong'),('body_bytes',True),('body_bytes',0),('body_bytes','123')]:
   raw=loaded();raw['_availability'][key]=bad;self.assertEqual(self.run_status({'bonds':raw})['engines_loaded'],0)
 def test_untrusted_donor_text_never_becomes_a_narrative(self):
  raw=loaded();raw.update(headline='<img src=x onerror=bad()>',thesis='buy with all capital');value=self.run_status({'bonds':raw});self.assertNotIn('onerror',json.dumps(value));self.assertNotIn('all capital',json.dumps(value))
 def test_storage_and_claimed_dates_never_certify_observation_freshness(self):
  raw=loaded();raw['_availability'].update(reported_generated_at='2099-01-01T00:00:00Z',reported_as_of='bad');row=self.run_status({'bonds':raw})['input_status']['bonds'];self.assertTrue(row['reported_generated_at'].startswith('2099'));self.assertIsNone(row['reported_as_of']);self.assertFalse(row['observation_freshness_verified'])
 def test_metadata_replays_exactly_and_is_digest_bound(self):
  inputs={'bonds':loaded()};one=self.run_status(inputs);two=self.run_status(copy.deepcopy(inputs));self.assertEqual(one,two);self.assertEqual(one['input_status_sha256'],hashlib.sha256(json.dumps(one['input_status'],sort_keys=True,separators=(',',':')).encode()).hexdigest())
 def test_invalid_compiler_identity_or_naive_clock_rejected(self):
  for stamp,digest in [('2026-10-01T13:00:00',DIGEST),(STAMP,'bad')]:
   with self.assertRaises(ValueError):mod.compile_status(SPECS,{},stamp,digest)
 def test_complete_empty_body_retains_its_measured_zero_size_and_digest(self):
  digest=hashlib.sha256(b'').hexdigest();value=self.run_status({'bonds':{'_availability':{'read_status':'malformed','body_sha256':digest,'body_bytes':0}}})
  row=value['input_status']['bonds'];self.assertEqual(row['read_status'],'malformed');self.assertEqual(row['body_bytes'],0);self.assertEqual(row['body_sha256'],digest);self.assertEqual(value['engines_loaded'],0)
if __name__=='__main__':unittest.main()
