from pathlib import Path
import json,hashlib,sys,unittest
from unittest.mock import patch
R=Path(__file__).resolve().parents[4]
sys.path.insert(0,str(R/'tests'))
from synthesis_status_test_support import Memory,load,Error,store

class Whole(unittest.TestCase):
 def setUp(self):self.mem=Memory();self.mod,self.kernel=load(self.mem)
 def packet(self):return json.loads(self.mem.objects[self.mem.head])
 def fill(self,packet):
  for key in self.mem.inputs:self.mem.objects[key]=json.dumps(packet).encode()
 def run_handler(self):
  with patch('urllib.request.urlopen',side_effect=AssertionError('No provider/model HTTP')),patch('socket.create_connection',side_effect=AssertionError('No network')):
   return self.mod.lambda_handler({'output':'private/account.json','model':'paid'},None)
 def safe(self,p):
  self.assertEqual(p['contract'],'website-research-status.v1');self.assertEqual(p['synthesis']['global_posture'],'WAIT');self.assertIsNone(p['call'])
  self.assertTrue(all(p[f] is False for f in self.kernel.FLAGS));self.assertEqual(len(p['input_status']),12);self.assertEqual(p['decision']['eligible_votes'],0)
  self.assertEqual(p['model_requests'],0);self.assertEqual(p['notifications_sent'],0);self.assertEqual(p['alert_info']['sent'],False)
  self.assertFalse(any('_alerts/' in key for key in self.mem.reads));self.assertFalse(any('/archive/ai-website-synthesis/' in w['Key'] for w in self.mem.writes))
  self.assertTrue(all(b.closed for b in self.mem.streams))
 def test_empty_inputs_publish_explicit_zero_parsed_not_freshness_or_neutral(self):
  out=self.run_handler();p=self.packet();self.safe(p);self.assertEqual(out['statusCode'],200);self.assertEqual(p['engines_loaded'],0)
  self.assertEqual(set(self.mem.inputs),set(key for key in self.mem.reads if key in self.mem.inputs));self.assertTrue(all(v is None for v in p['snapshot_age_min'].values()))
 def test_all_self_promoting_inputs_remain_unqualified(self):
  self.fill({'regime':'STRONG_BUY','quality':{'status':'fresh'},'calls_eligible':True,'sizing_eligible':True,'thesis':'secret donor narrative'})
  self.run_handler();p=self.packet();self.safe(p);self.assertEqual(p['engines_loaded'],12);self.assertNotIn('STRONG_BUY',json.dumps(p));self.assertNotIn('secret donor',json.dumps(p))
 def test_partial_inputs_count_without_changing_abstention(self):
  for key in sorted(self.mem.inputs)[:5]:self.mem.objects[key]=b'{"as_of":"2026-09-30"}'
  self.run_handler();p=self.packet();self.safe(p);self.assertEqual(p['engines_loaded'],5);self.assertEqual(sum(v['reported_as_of']=='2026-09-30' for v in p['input_status'].values()),5)
 def test_malformed_bodies_retained_but_never_parsed(self):
  values=[b'null',b'[]',b'false',b'{"x":NaN}',b'{"x":1e400}',b'{"x":1,"x":2}',b'\xff',b'{',b'"text"']
  for key,raw in zip(sorted(self.mem.inputs),values):self.mem.objects[key]=raw
  self.run_handler();p=self.packet();self.safe(p);self.assertEqual(p['engines_loaded'],0);self.assertEqual(sum(v['read_status']=='malformed' for v in p['input_status'].values()),len(values))
 def test_reported_errors_not_counted_and_error_text_not_published(self):
  self.fill({'status':'error','error':'private donor debug text'});self.run_handler();p=self.packet();self.safe(p);self.assertEqual(p['engines_loaded'],0);self.assertNotIn('private donor',json.dumps(p))
 def test_source_dates_remain_claims_and_do_not_make_age(self):
  self.fill({'generated_at':'2099-01-01T00:00:00Z','as_of':'2026-02-30'});self.run_handler();p=self.packet();self.safe(p)
  for row in p['input_status'].values():self.assertEqual(row['reported_generated_at'],'2099-01-01T00:00:00+00:00');self.assertIsNone(row['reported_as_of']);self.assertFalse(row['observation_freshness_verified'])
 def test_complete_sources_and_compilers_precede_head_and_replay_exactly(self):
  self.fill({'generated_at':'2026-10-01T00:00:00Z','as_of':'2026-09-30','arbitrary_untruncated':[1]*100})
  self.run_handler();p=self.packet();self.safe(p);self.assertEqual(self.mem.writes[-1]['Key'],self.mem.head)
  manifest=json.loads(self.mem.objects[p['replay']['input_ref']['key']]);self.assertEqual(len(manifest['attempts']),12);self.assertEqual(len(manifest['source_files']),5)
  sources={a['original_ref']['key']:self.mem.objects[a['original_ref']['key']] for a in manifest['attempts'].values()}
  replay=self.mod._project(manifest['attempts'],sources,manifest['generated_at']);candidate={k:v for k,v in p.items() if k not in ('replay','acquisition_started_at')};self.assertEqual(replay,candidate)
  self.assertTrue(all(json.loads(raw)['arbitrary_untruncated']==[1]*100 for raw in sources.values()))
 def test_previous_head_retained_and_conditionally_replaced(self):
  previous=b'{"legacy":"invented only"}';self.mem.objects[self.mem.head]=previous;self.run_handler();p=self.packet();self.safe(p)
  self.assertEqual(self.mem.objects[p['replay']['previous_publication']['key']],previous);self.assertIn('IfMatch',self.mem.writes[-1])
 def test_source_retention_failure_raises_before_head_write(self):
  previous=b'old';self.mem.objects[self.mem.head]=previous;self.fill({'source':'invented'});self.mem.fail=('put_kind','sources')
  with self.assertRaises(Error):self.run_handler()
  self.assertEqual(self.mem.objects[self.mem.head],previous);self.assertFalse(any(w['Key']==self.mem.head for w in self.mem.writes))
 def test_prior_access_denied_is_not_treated_as_missing(self):
  self.mem.fail=('get',self.mem.head)
  with self.assertRaises(Error):self.run_handler()
  self.assertEqual(self.mem.writes,[])
 def test_concurrent_head_never_silently_overwritten_or_retried(self):
  self.mem.race=b'newer competing publication'
  with self.assertRaises(store.PublicationUncertain):self.run_handler()
  self.assertEqual(self.mem.objects[self.mem.head],self.mem.race);self.assertEqual(sum(w['Key']==self.mem.head for w in self.mem.writes),1)
 def test_lost_acknowledgement_does_not_claim_success_or_preservation(self):
  self.mem.fail=('ack',self.mem.head)
  with self.assertRaises(store.PublicationUncertain):self.run_handler()
  self.assertEqual(sum(w['Key']==self.mem.head for w in self.mem.writes),1);self.safe(self.packet())
 def test_oversize_source_does_not_become_partial_parsed_data(self):
  self.mem.objects[sorted(self.mem.inputs)[0]]=b' '*(store.MAX_BYTES+1);self.run_handler();p=self.packet();self.safe(p);self.assertEqual(p['engines_loaded'],0)
 def test_corrupt_retained_original_ref_rejected(self):
  self.fill({'x':1});self.run_handler();p=self.packet();manifest=json.loads(self.mem.objects[p['replay']['input_ref']['key']]);sources={a['original_ref']['key']:self.mem.objects[a['original_ref']['key']] for a in manifest['attempts'].values()};sources[next(iter(sources))]=b'{}'
  with self.assertRaises(ValueError):self.mod._project(manifest['attempts'],sources,manifest['generated_at'])
 def test_retained_all_input_specs_match_predecessor(self):
  import ast
  source=ast.parse((R/'tests/fixtures/website-research-status/pre513/aws/lambdas/justhodl-ai-website-synthesis/source/lambda_function.py.txt').read_bytes())
  spec=next(n.value for n in source.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='ENGINE_INPUTS' for t in n.targets));self.assertEqual(ast.literal_eval(spec),self.mod.ENGINE_INPUTS)
 def test_compatibility_functions_never_call_models_or_notifications(self):
  self.assertFalse(self.mod.send_telegram('hello'));self.assertFalse(self.mod.maybe_alert_posture_change({'global_posture':'RISK_ON'})['sent'])
  with self.assertRaises(RuntimeError):self.mod.call_anthropic('s','u')
  self.assertEqual(self.mem.clients,[]);self.assertEqual(self.mem.reads,[]);self.assertEqual(self.mem.writes,[])
 def test_bond_signal_board_and_gsi_boundaries_survive(self):
  self.fill({'composite_signal':99,'global_stress_index':99,'regime':'BUY','calls_eligible':True,'quality':{'status':'fresh'}});self.mod.s3=self.mem
  for name in ('bonds','signal_board','global_stress'):
   name,snap=self.mod.fetch_engine(name,self.mod.ENGINE_INPUTS[name]);self.assertEqual(snap['status'],'ABSTAIN');self.assertFalse(snap['calls_eligible']);self.assertIsNone(snap['_age_min']);self.assertNotIn('regime',snap);self.assertNotIn('BUY',self.mod.build_user_prompt({name:snap}))
 def test_undeclared_source_event_keys_cannot_change_acquisition(self):
  with self.assertRaises(ValueError):self.mod.fetch_engine('signal_board',{'key':'data/private-account.json'})
  self.run_handler();self.assertTrue(all(key in self.mem.inputs or key==self.mem.head or key.startswith(self.mem.prefix) for key in self.mem.reads))
 def test_client_timeout_and_retry_policy_bounded(self):
  self.run_handler();self.assertEqual(len(self.mem.clients),1);self.assertEqual(self.mem.clients[0]['config'],{'connect_timeout':2,'read_timeout':3,'retries':{'max_attempts':0}})

if __name__=='__main__':unittest.main(verbosity=2)
