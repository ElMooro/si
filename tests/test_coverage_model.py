"""Complete invented inventory projection inputs; no I/O or real packets."""
from copy import deepcopy
from pathlib import Path
import importlib.util,json,unittest

HERE=Path(__file__).resolve().parent
SOURCE=HERE if (HERE/'coverage_model.py').exists() else HERE.parent/'aws/lambdas/justhodl-coverage-gap-report/source'
spec=importlib.util.spec_from_file_location('coverage_candidate',SOURCE/'coverage_model.py')
model=importlib.util.module_from_spec(spec);spec.loader.exec_module(model)
NOW='2026-10-01T01:00:00+00:00'
DOCS={'symbology':{'as_of':'2001-01-01T00:00:00Z','n_tickers':99,'by_ticker':{
 'TEST':{'cik':'0000000123','figi':'BBG000000001','cusip':'123456789'},
 'TEST2':{'cik':'0000000123','figi':'BBG000000001','cusip':'123456789'}}},
 'edgar':{'n_filings':2,'complete':False},'nyfed':{'rates':{name.lower():{'n_obs':2 if name=='SOFR' else 0} for name in model.RATE_NAMES}},
 'rollup':{'global_feed_counts':{'fred':3}}}

def project(docs=None,available=True):
 docs=deepcopy(DOCS if docs is None else docs)
 attempts={name:{'source_key':key,'status':'received' if available else 'source_read_unavailable',
  'requested_at':NOW,'received_at':NOW,'original_ref':{'key':'invented-private/'+name,'sha256':'0'*64,'bytes':1} if available else None} for name,key in model.INPUTS.items()}
 return model.project(attempts,docs,NOW)

def metrics(doc):return {m['metric']:m for m in doc['metrics']}

class Tests(unittest.TestCase):
 def test_failures_never_become_measured_zero(self):
  result=project({name:None for name in model.INPUTS},False)
  self.assertTrue(all(m['actual'] is None and m['pct_of_target'] is None for m in result['metrics']))
  self.assertEqual(result['quality']['unavailable_metric_count'],6)
 def test_observed_empty_population_has_scoped_zero_and_no_market_percentage(self):
  docs=deepcopy(DOCS);docs['symbology']={'by_ticker':{}};rows=metrics(project(docs))
  for name in ('us_tickers','figi_ids','cusips'):
   self.assertEqual(rows[name]['actual'],0);self.assertIsNone(rows[name]['pct_of_target'])
   self.assertFalse(rows[name]['market_coverage_qualified'])
 def test_declared_count_cannot_override_received_population(self):
  row=metrics(project())['us_tickers'];self.assertEqual(row['actual'],2)
  self.assertFalse(row['count_reconciliation']['matches_population']);self.assertEqual(row['count_reconciliation']['reported_n_tickers'],99)
 def test_identifier_grains_separate_rows_from_unique_values(self):
  rows=metrics(project())
  for name in ('figi_ids','cusips'):
   self.assertEqual(rows[name]['actual'],1);self.assertEqual(rows[name]['rows_matching_character_pattern'],2)
   self.assertIsNone(rows[name]['target']);self.assertFalse(rows[name]['identifier_relationships_qualified'])
 def test_bool_missing_and_malformed_identifiers_never_count_as_valid_values(self):
  for value in (True,False,0,{},[], '', 'bad'):
   docs=deepcopy(DOCS)
   for row in docs['symbology']['by_ticker'].values():row.update(figi=value,cusip=value)
   rows=metrics(project(docs))
   for name in ('figi_ids','cusips'):
    self.assertEqual(rows[name]['actual'],0);self.assertEqual(rows[name]['rows_not_matching_character_pattern'],2)
 def test_null_identifiers_are_unavailable_values_not_malformed(self):
  docs=deepcopy(DOCS)
  for row in docs['symbology']['by_ticker'].values():row.update(figi=None,cusip=None)
  rows=metrics(project(docs))
  self.assertEqual(rows['figi_ids']['rows_not_matching_character_pattern'],0)
 def test_figi_character_rules_do_not_assume_only_one_issuer_prefix_or_verify_assignment(self):
  for value in ('ZZG000000001','BBG000000001'):
   docs=deepcopy(DOCS)
   for row in docs['symbology']['by_ticker'].values():row['figi']=value
   record=metrics(project(docs))['figi_ids'];self.assertEqual(record['actual'],1)
   self.assertFalse(record['assignment_verified']);self.assertFalse(record['checksum_verified'])
  for value in ('BBG00000000A','BBG00000A001','BAG000000001','BBX000000001'):
   docs=deepcopy(DOCS)
   for row in docs['symbology']['by_ticker'].values():row['figi']=value
   self.assertEqual(metrics(project(docs))['figi_ids']['actual'],0)
 def test_cusip_character_pattern_requires_final_digit(self):
  docs=deepcopy(DOCS)
  for row in docs['symbology']['by_ticker'].values():row['cusip']='12345678A'
  self.assertEqual(metrics(project(docs))['cusips']['actual'],0)
 def test_malformed_population_is_not_filtered_to_a_misleading_count(self):
  for value in (None,[],{'TEST':1},{'TEST':{ },'BAD KEY':{}}):
   docs=deepcopy(DOCS);docs['symbology']['by_ticker']=value;rows=metrics(project(docs))
   for name in ('us_tickers','figi_ids','cusips'):self.assertIsNone(rows[name]['actual'])
 def test_unqualified_global_benchmarks_never_form_percentages(self):
  rows=metrics(project())
  for name,target in (('us_tickers',320000),('cusips',500000),('fred_feeds_in_use',45000)):
   self.assertEqual(rows[name]['legacy_unqualified_target'],target);self.assertIsNone(rows[name]['target']);self.assertIsNone(rows[name]['pct_of_target'])
 def test_non_integer_summary_counts_are_not_measurements(self):
  for value in (True,False,1.0,'2',-1,{},[],None):
   docs=deepcopy(DOCS);docs['edgar']['n_filings']=value;docs['rollup']['global_feed_counts']['fred']=value
   rows=metrics(project(docs));self.assertIsNone(rows['edgar_filings_qtd']['actual']);self.assertIsNone(rows['fred_feeds_in_use']['actual'])
 def test_edgar_reported_count_never_claims_quarterly_completeness(self):
  row=metrics(project())['edgar_filings_qtd'];self.assertEqual(row['actual'],2)
  self.assertFalse(row['completeness_verified']);self.assertFalse(row['quarter_scope_verified'])
 def test_named_rate_slots_exclude_unknown_keys_and_invalid_observation_counts(self):
  docs=deepcopy(DOCS);docs['nyfed']['rates']={'SOFR':{'n_obs':True},'EFFR':{'n_obs':2},'UNKNOWN':{'n_obs':300}}
  row=metrics(project(docs))['nyfed_reference_rates'];self.assertIsNone(row['actual']);self.assertEqual(row['known_nonempty_slot_count'],1);self.assertEqual(row['target'],5)
  self.assertEqual(row['unrecognized_rate_keys'],['UNKNOWN']);self.assertIsNone(row['slot_states']['SOFR']['reported_observations'])
 def test_actual_lowercase_producer_keys_support_the_complete_defined_slot_set(self):
  row=metrics(project())['nyfed_reference_rates']
  self.assertEqual(row['actual'],1);self.assertEqual(row['slot_states']['SOFR']['source_keys'],['sofr'])
 def test_conflicting_alias_or_source_failure_cannot_be_counted_as_loaded(self):
  for mutate in (lambda rates:rates.update(SOFR={'n_obs':2}),lambda rates:rates['sofr'].update(data_unavailable=True)):
   docs=deepcopy(DOCS);mutate(docs['nyfed']['rates']);row=metrics(project(docs))['nyfed_reference_rates']
   self.assertIsNone(row['actual']);self.assertIsNone(row['pct_of_target']);self.assertFalse(row['slot_states']['SOFR']['count_known'])
 def test_malformed_rate_row_makes_population_unavailable(self):
  docs=deepcopy(DOCS);docs['nyfed']['rates']['BGCR']=False
  self.assertIsNone(metrics(project(docs))['nyfed_reference_rates']['actual'])
 def test_generation_cannot_refresh_old_reported_source_time(self):
  result=project();source=result['sources']['symbology']
  self.assertEqual(source['reported_clocks']['as_of']['value'],'2001-01-01T00:00:00Z')
  self.assertFalse(source['freshness_verified']);self.assertFalse(result['quality']['source_freshness_verified'])
  self.assertEqual(result['as_of_basis'],'report generation, not source observation time')
 def test_no_input_objects_are_modified_and_generation_replays_exactly(self):
  original=deepcopy(DOCS);a=project(DOCS);b=project(DOCS)
  self.assertEqual(DOCS,original);self.assertEqual(a,b);json.dumps(a,allow_nan=False)
 def test_missing_input_set_or_unaware_generation_is_rejected(self):
  attempts={name:{'source_key':key,'status':'received'} for name,key in model.INPUTS.items()}
  for clock in ('2026-10-01','not a clock',None):
   with self.assertRaises(ValueError):model.project(attempts,DOCS,clock)
  with self.assertRaises(ValueError):model.project({},DOCS,NOW)
 def test_source_binding_cannot_change_arbitrarily(self):
  attempts={name:{'source_key':key,'status':'received'} for name,key in model.INPUTS.items()};attempts['symbology']['source_key']='data/not-reviewed.json'
  with self.assertRaises(ValueError):model.project(attempts,DOCS,NOW)
 def test_extreme_reported_clock_is_retained_without_overflowing_utc_conversion(self):
  docs=deepcopy(DOCS);docs['symbology']['as_of']='9999-12-31T23:59:59-23:00'
  result=project(docs);clock=result['sources']['symbology']['reported_clocks']['as_of']
  self.assertEqual(clock['value'],docs['symbology']['as_of']);self.assertIsNone(clock['aware_clock'])
  self.assertEqual(metrics(result)['us_tickers']['actual'],len(docs['symbology']['by_ticker']))

if __name__=='__main__':unittest.main()
