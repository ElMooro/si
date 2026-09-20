"""Dated measurement semantics, actual original replay and publication boundaries."""
from implied_fixture import *
import importlib.util,os,subprocess

class Native(unittest.TestCase):
 @classmethod
 def setUpClass(cls):
  cls.s,cls.inputs,cls.macro,cls.originals=fixture()
  cls.output=store.compile_output(cls.inputs,store.reader(cls.s,'b'))

 def test_complete_declared_identity_inventory_and_no_legacy_authority(self):
  p=self.output;self.assertEqual(p['quality']['within_age_ceiling'],18)
  self.assertEqual(set(p['measurements']),set(model.SERIES));self.assertEqual(set(model.SERIES),set(model.SPECS))
  for k in model.PERMISSIONS:self.assertIs(p[k],False)
  self.assertIsNone(p['recession']['ny_fed_12m_prob_pct']);self.assertIsNone(p['fed']['meeting_probabilities'])
  self.assertIsNone(p['fed']['near_term_stance']);self.assertIsNone(p['fed']['next_meeting'])
  for key in ('spy','qqq','btc'):
   for metric in ('iv_30d','iv_90d','moves_30d','moves_90d','density_moves_30d'):self.assertIsNone(p[key][metric])
  self.assertFalse(p['option_snapshot_context']['available']);self.assertEqual(p['portfolio_action'],'WAIT')
  self.assertTrue(all(not x['measurement_eligible'] for x in p['legacy_context'].values()))

 def test_actual_common_dates_and_independent_midpoint_arithmetic(self):
  p=self.output;m=p['descriptive_comparisons']['target_midpoint'];self.assertTrue(m['available'])
  refs=m['inputs'];lo=Decimal(refs['DFEDTARL']['exact_value']);hi=Decimal(refs['DFEDTARU']['exact_value'])
  self.assertEqual(m['value'],float((lo+hi)/2));self.assertEqual(p['fed']['current_rate'],m['value'])
  gap=p['descriptive_comparisons']['dff_minus_target_midpoint'];v=gap['inputs']
  expected=100*(Decimal(v['DFF']['exact_value'])-(Decimal(v['DFEDTARL']['exact_value'])+Decimal(v['DFEDTARU']['exact_value']))/2)
  self.assertEqual(gap['value'],float(expected));self.assertEqual(gap['unit'],'basis_points')
  self.assertEqual({r['date'] for r in v.values()},{gap['observation_date']})

 def test_missing_bound_does_not_become_zero_or_midpoint(self):
  p=deepcopy(self.macro);p['measurements'].pop('DFEDTARL');originals=dict(self.originals,DFEDTARL=None)
  o=model.build(p,originals,{},STAMP);self.assertIsNone(o['fed']['current_rate'])
  self.assertFalse(o['descriptive_comparisons']['target_midpoint']['available'])
  self.assertIsNone(o['measurements']['DFEDTARL']['value'])

 def test_crossed_bounds_refused_and_zero_rates_survive(self):
  with self.assertRaisesRegex(ValueError,'crossed'):model.target_midpoint({'DFEDTARL':Decimal(2),'DFEDTARU':Decimal(1)})
  self.assertEqual(model.target_midpoint({'DFEDTARL':Decimal(0),'DFEDTARU':Decimal(0)}),0)
  rows,h=model.observed(self.macro,self.originals,STAMP)
  for sid in ('DFEDTARL','DFEDTARU'):
   h[sid][-1]['value']=Decimal(0)
  d=model.comparison(rows,h,('DFEDTARL','DFEDTARU'),'test','test',model.target_midpoint,'percent')
  self.assertTrue(d['available']);self.assertEqual(d['value'],0)

 def test_common_day_must_be_fresh_at_evaluation_not_only_near_latest(self):
  rows,h=model.observed(self.macro,self.originals,STAMP)
  for sid in ('DFEDTARL','DFEDTARU'):
   h[sid]=[{'date':'2026-09-07','value':Decimal(5),'original_row_index':0}]
  self.assertEqual(model.matched(rows,h,('DFEDTARL','DFEDTARU')),(None,{}))

 def test_common_date_does_not_forward_fill_or_interpolate(self):
  rows,h=model.observed(self.macro,self.originals,STAMP)
  h['DFEDTARL']=[{'date':'2026-09-17','value':Decimal(5),'original_row_index':0}]
  h['DFEDTARU']=[{'date':'2026-09-18','value':Decimal(6),'original_row_index':0}]
  self.assertEqual(model.matched(rows,h,('DFEDTARL','DFEDTARU')),(None,{}))

 def test_recession_change_uses_exact_twelve_months_and_percentage_points(self):
  x=self.output['descriptive_comparisons']['recession_estimate_12m_change'];self.assertTrue(x['available'])
  self.assertEqual(x['baseline_date'],'2025-08-01');self.assertEqual(x['observation_date'],'2026-08-01')
  current=Decimal(self.output['measurements']['RECPROUSM156N']['exact_value'])
  self.assertAlmostEqual(x['value'],float(current-Decimal(str(x['baseline_value']))))
  self.assertEqual(x['unit'],'percentage_points')
  rows,h=model.observed(self.macro,self.originals,STAMP)
  h['RECPROUSM156N']=[r for r in h['RECPROUSM156N'] if r['date']!='2025-08-01']
  self.assertFalse(model.recession_change(rows,h)['available'])

 def test_source_definitions_and_probability_domain_checked(self):
  for field,bad in [('unit','Billions'),('frequency','W')]:
   p=deepcopy(self.macro);p['measurements']['DFF'][field]=bad
   with self.assertRaisesRegex(ValueError,'definition'):model.build(p,self.originals,{},STAMP)
  p=deepcopy(self.macro);p['measurements']['RECPROUSM156N']['current_decimal']='101'
  with self.assertRaisesRegex(ValueError,'range'):model.build(p,self.originals,{},STAMP)

 def test_inversion_counts_reported_observations_and_breaks_on_missing(self):
  rows,h=model.observed(self.macro,self.originals,STAMP);rows['T10Y3M'].update(value=-.25,exact_value='-0.25')
  h['T10Y3M']=[{'date':'2026-09-14','value':Decimal(-1),'original_row_index':4},
   {'date':'2026-09-15','value':None,'original_row_index':3},
   {'date':'2026-09-16','value':Decimal(-.1),'original_row_index':2},
   {'date':'2026-09-17','value':Decimal(-.25),'original_row_index':1}]
  out=model.inversion(rows,h);self.assertEqual(out['consecutive_reported_negative_observations'],2)
  self.assertEqual(out['spread_basis_points'],-25);self.assertFalse(out['history_left_censored'])
  h['T10Y3M']=h['T10Y3M'][-2:];self.assertTrue(model.inversion(rows,h)['history_left_censored'])

 def test_volatility_underlying_and_horizon_are_source_identities(self):
  identities=self.output['volatility_identities']
  self.assertEqual((identities['VIXCLS']['underlying'],identities['VIXCLS']['index_horizon_calendar_days']),('SPX',30))
  self.assertEqual((identities['VXVCLS']['underlying'],identities['VXVCLS']['index_horizon_calendar_days']),('SPX',90))
  self.assertEqual(identities['VXNCLS']['underlying'],'NDX');self.assertEqual(identities['OVXCLS']['underlying'],'USO')

 def test_sample_statistics_show_count_not_invented_ten_years(self):
  m=self.output['measurements']['BAMLH0A0HYM2'];stats=m['statistics']
  vals=[float(x['value']) for x in model.history(self.originals['BAMLH0A0HYM2'],m['observation_date']) if x['value'] is not None][-252:]
  self.assertAlmostEqual(stats['mean'],statistics.mean(vals),places=7)
  self.assertAlmostEqual(stats['sample_sd'],statistics.stdev(vals),places=7)
  self.assertEqual(stats['numeric_observations'],252);self.assertEqual(len(stats['original_row_indices']),252)
  self.assertFalse(m['history_coverage']['point_in_time_history'])

 def test_stale_and_future_canonical_data_cannot_gain_freshness(self):
  later=(AT+timedelta(hours=27)).isoformat();o=model.build(self.macro,self.originals,{},later)
  self.assertEqual(o['quality']['within_age_ceiling'],0)
  with self.assertRaisesRegex(ValueError,'Future'):model.build(self.macro,self.originals,{},(AT-timedelta(seconds=1)).isoformat())

 def test_unchanged_originals_replay_and_cli_verifies_actual_packet(self):
  s,i,p=packet();self.assertEqual(store.replay(p['replay'],store.reader(s,'b')),{k:v for k,v in p.items() if k!='replay'})
  sys.path.insert(0,str(ROOT/'scripts'));import replay_implied_research
  self.assertTrue(replay_implied_research.verify(p,store.reader(s,'b'))['replayed'])
  p['fed']['current_rate']=0
  with self.assertRaisesRegex(ValueError,'differs'):replay_implied_research.verify(p,store.reader(s,'b'))

 def test_original_bytes_and_compiler_identity_are_checked(self):
  s,i,p=packet();read=store.reader(s,'b');run=json.loads(read(p['replay']['manifest_key']))
  ref=run['compilers']['implied_research_catalog'];s.objects[ref['key']]+=b'\n'
  with self.assertRaisesRegex(ValueError,'compiler'):store.replay(p['replay'],read)
  s,i,_,_=fixture();key=next(k for k in s.objects if k.endswith('.bin.gz'));s.objects[key]=gzip.compress(b'{}')
  with self.assertRaisesRegex(ValueError,'bytes'):store.compile_output(i,store.reader(s,'b'))

 def test_private_path_and_retained_identity_are_constrained(self):
  s,i,_,_=fixture()
  for key in ('portfolio/state.json','data/private-positions.json',store.PRIVATE+'../../bad'):
   with self.assertRaisesRegex(ValueError,'Unapproved'):store.reader(s,'b')(key)
  i['macro']['sha256']='0'*64
  with self.assertRaises(ValueError):store.compile_output(i,store.reader(s,'b'))

 def test_publication_retains_whole_predecessor_and_refuses_older_source(self):
  s,i,p=packet();prior=s.objects[store.CURRENT];self.assertTrue(store.publish(s,'b',p))
  self.assertEqual(s.objects[store.PRIVATE+model.sha(prior)+'.bin'],prior)
  old=deepcopy(p);old['generated_at']=(AT+timedelta(seconds=1)).isoformat()
  old['source_generated_at']=(AT-timedelta(seconds=1)).isoformat();self.assertFalse(store.publish(s,'b',old))
  self.assertEqual(json.loads(s.objects[store.CURRENT]),p)

 def test_idempotency_and_failure_preserve_publication(self):
  s,_,_,_=fixture()
  with patch.object(store,'now',return_value=STAMP):
   a=store.run(s,'b','review-1','aws-1');before=len(s.writes);b=store.run(s,'b','review-1','aws-2')
   self.assertEqual(a,b);self.assertEqual(len(s.writes),before);self.assertTrue(a['published'])
  before=s.objects[store.CURRENT]
  with patch.object(store,'compile_output',side_effect=ValueError('secret vendor URL')):
   with self.assertRaisesRegex(RuntimeError,'Native implied'):store.run(s,'b','review-2','aws-3')
  self.assertEqual(s.objects[store.CURRENT],before)
  status=json.loads(s.objects[store.request_key('review-2')]);self.assertEqual(status['status'],'failed')
  self.assertNotIn('secret',json.dumps(status))

 def test_http_and_validation_do_not_publish_or_acquire(self):
  spec=importlib.util.spec_from_file_location('implied_handler_test',HERE.parent/'source/lambda_function.py')
  handler=importlib.util.module_from_spec(spec);spec.loader.exec_module(handler)
  with patch.object(handler.boto3,'client',side_effect=AssertionError('No AWS in validation')):
   self.assertEqual(handler.lambda_handler({'validate_only':True})['statusCode'],200)
  s,_,p=packet();s.objects[store.CURRENT]=model.encoded(p);before=len(s.writes)
  with patch.object(handler.boto3,'client',return_value=s),patch.object(handler,'run',side_effect=AssertionError('No HTTP publication')):
   self.assertEqual(handler.lambda_handler({'httpMethod':'GET'})['statusCode'],200)
  self.assertEqual(len(s.writes),before)

 def test_standalone_replay_help_requires_no_aws_or_preloaded_modules(self):
  env=os.environ.copy();env.pop('PYTHONPATH',None)
  result=subprocess.run([sys.executable,str(ROOT/'scripts/replay_implied_research.py'),'--help'],env=env,capture_output=True,text=True)
  self.assertEqual(result.returncode,0,result.stderr);self.assertIn('--packet',result.stdout)

if __name__=='__main__':unittest.main()
