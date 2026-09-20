from fomc_fixture import *
import importlib.util

def rows(values):return [{'date':d,'value':Decimal(str(v)) if v is not None else None,'original_row_index':i} for i,(d,v) in enumerate(values)]
class Sources(unittest.TestCase):
 def test_exact_event_date_does_not_substitute_a_sunday_or_missing_value(self):
  r=rows([('2026-03-13',4),('2026-03-16',4.25),('2026-03-17',None)])
  for day in ('2026-03-15','2026-03-17'):self.assertFalse(model.event_change(r,day,'yield')['available'])
  v=model.event_change(r,'2026-03-16','yield');self.assertEqual(v['value'],25);self.assertEqual(v['calendar_days'],3)
 def test_adjacent_missing_or_large_gap_does_not_become_a_zero_change(self):
  for pairs in ([('2026-03-16',None),('2026-03-17',4)],[('2026-03-10',4),('2026-03-17',4)]):
   o=model.event_change(rows(pairs),'2026-03-17','yield');self.assertFalse(o['available']);self.assertIsNone(o['value'])
 def test_horizons_retain_exact_endpoints_missing_rows_and_intervening_events(self):
  r=rows([('2026-03-16',100),('2026-03-17',101),('2026-03-18',None),('2026-03-19',110),('2026-03-20',105)])
  o=model.outcome(r,'2026-03-16',2,'equity_price_index',['2026-03-18']);self.assertEqual(o['value'],10)
  self.assertEqual(o['end']['date'],'2026-03-19');self.assertEqual(o['missing_reported_rows_inside_span'],1);self.assertEqual(o['intervening_scheduled_meetings'],['2026-03-18'])
  self.assertFalse(model.outcome(r,'2026-03-16',5,'equity_price_index',[])['available'])
 def test_negative_yield_levels_and_zero_moves_are_numeric(self):
  r=rows([('2026-03-16',-.25),('2026-03-17',-.25),('2026-03-18',-.5)])
  self.assertEqual(model.event_change(r,'2026-03-17','yield')['value'],0)
  self.assertEqual(model.outcome(r,'2026-03-17',1,'yield',[])['value'],-25)
 def test_samples_expose_exact_membership_and_overlap_without_claiming_independence(self):
  _,_,p=packet();summary=next(s for s in p['summaries'] if s['series_id']=='DGS2' and s['condition']=='all' and s['horizon_reported_numeric_observations']==63)
  self.assertGreater(summary['numeric_outcomes'],0);self.assertTrue(summary['sample_overlap_pairs']);self.assertEqual(summary['numeric_outcomes'],len(summary['event_dates']))
  self.assertIsNone(summary['forecast_probability']);self.assertIsNone(summary['independent_sample_size']);self.assertFalse(summary['calls_eligible'])
 def test_missing_history_preserves_requested_identity_and_null_outcomes(self):
  _,_,p=packet(('NASDAQCOM',));self.assertEqual(p['quality']['available_histories'],3)
  self.assertEqual(p['measurements']['NASDAQCOM']['history']['returned_rows'],0)
  self.assertTrue(all(not e['assets']['NASDAQCOM']['forward_changes']['1']['available'] for e in p['events']))
 def test_calendar_body_and_dates_must_reconstruct_exactly(self):
  s,i,macro,originals=fixture();fw=json.loads(s.objects['data/fedwatch.json']);raw=calendar_raw()
  fw['calendar']['meetings'][0]['end_date']='2026-01-29'
  with self.assertRaises(ValueError):model.build(macro,originals,fw,raw,STAMP)
  fw=json.loads(s.objects['data/fedwatch.json'])
  with self.assertRaises(ValueError):model.build(macro,originals,fw,raw+b' ',STAMP)
 def test_source_definitions_and_future_clocks_cannot_be_relabelled(self):
  s,i,macro,originals=fixture();fw=json.loads(s.objects['data/fedwatch.json']);macro['measurements']['DGS2']['unit']='USD'
  with self.assertRaises(ValueError):model.build(macro,originals,fw,calendar_raw(),STAMP)
  s,i,macro,originals=fixture();fw['generated_at']='2026-09-21T00:00:00+00:00'
  with self.assertRaises(ValueError):model.build(macro,originals,fw,calendar_raw(),STAMP)
 def test_no_tone_stale_surprise_or_legacy_forecast_survives(self):
  _,_,p=packet();self.assertIsNone(p['surprise']['label']);self.assertIsNone(p['surprise']['statement_tone']);self.assertEqual(p['reaction_map'],{})
  self.assertEqual(p['self_grading']['n'],0);self.assertIsNone(p['self_grading']['directional_accuracy_pct']);self.assertEqual(p['portfolio_action'],'WAIT')
  self.assertTrue(all(p[k] is False for k in model.PERMISSIONS));self.assertEqual(p['quality']['independent_investment_votes'],0)

class Store(unittest.TestCase):
 def test_whole_source_replay_and_actual_cli(self):
  s,i,p=packet();read=store.reader(s,'b');self.assertEqual(store.replay(p['replay'],read),{k:v for k,v in p.items() if k!='replay'})
  spec=importlib.util.spec_from_file_location('replay_native_fomc',ROOT/'scripts/replay_fomc_research.py');cli=importlib.util.module_from_spec(spec);spec.loader.exec_module(cli)
  self.assertTrue(cli.verify(p,read)['replayed'])
  for k,ref in i['legacy'].items():self.assertEqual(read(ref['key']),s.objects[k])
 def test_tampered_calendar_canonical_and_compiler_originals_fail(self):
  for kind in ('calendar','fred','compiler'):
   s,i,p=packet();fw=json.loads(s.objects['data/fedwatch.json'])
   if kind=='calendar':key=fw['calendar']['original']['key']
   elif kind=='fred':key=next(k for k in s.objects if k.startswith('data/evidence/fred/'))
   else:key=json.loads(s.objects[p['replay']['manifest_key']])['compilers']['fomc_research_model']['key']
   s.objects[key]+=b'changed'
   with self.assertRaises(Exception):store.replay(p['replay'],store.reader(s,'b'))
 def test_calendar_manifest_and_body_cannot_self_relabel(self):
  s,i,p=packet();fw=json.loads(s.objects['data/fedwatch.json']);fw['calendar']['meetings'][0]['end_date']='2026-01-29'
  with self.assertRaises(ValueError):store.calendar_source(fw,store.reader(s,'b'))
 def test_request_idempotency_and_failure_preserve_the_prior_publication(self):
  s,i,_,_=fixture()
  with patch.object(store,'now',return_value=STAMP):
   result=store.run(s,'b','once','aws-first');self.assertTrue(result['published']);n=len(s.writes)
   self.assertEqual(store.run(s,'b','once','aws-second'),result);self.assertEqual(len(s.writes),n)
   before=s.objects[store.CURRENT]
   with patch.object(store,'compile_output',side_effect=ValueError('private failure detail')):
    with self.assertRaises(RuntimeError):store.run(s,'b','fail','aws-fail')
   self.assertEqual(s.objects[store.CURRENT],before);self.assertNotIn('private failure',s.objects[store.request_key('fail')].decode())
 def test_publication_cannot_roll_back_an_upstream_calendar_vintage(self):
  s,i,p=packet();new=deepcopy(p);new['calendar_generated_at']='2026-09-20T23:41:00+00:00';s.objects[store.CURRENT]=model.encoded(new)
  p['generated_at']='2026-09-20T23:42:00+00:00';self.assertFalse(store.publish(s,'b',p));self.assertEqual(json.loads(s.objects[store.CURRENT]),new)
 def test_http_is_read_only_and_validation_is_aws_free(self):
  import lambda_function as handler
  s,i,p=packet();s.objects[store.CURRENT]=model.encoded(p)
  with patch.object(handler.boto3,'client',return_value=s),patch.object(handler,'run',side_effect=AssertionError('HTTP cannot publish')):
   self.assertEqual(json.loads(handler.lambda_handler({'httpMethod':'GET'})['body']),p)
  with patch.object(handler.boto3,'client',side_effect=AssertionError('AWS forbidden')):
   self.assertEqual(handler.lambda_handler({'validate_only':True})['statusCode'],200)
 def test_reader_excludes_accounts_arbitrary_urls_and_unreviewed_histories(self):
  for k in ('portfolio/state.json','https://other.invalid/data.json','data/fomc-reaction-log/private.json','data/fomc-research/../account.json'):
   self.assertFalse(store.allowed(k))

if __name__=='__main__':unittest.main(verbosity=2)
