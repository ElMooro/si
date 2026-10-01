from pathlib import Path
from datetime import datetime,timezone,timedelta
from unittest.mock import patch
import contextlib,copy,importlib.util,importlib.machinery,io,json,math,runpy,sys,types,unittest
W=Path(__file__).parent;R=Path(__file__).resolve().parents[4]
spec=importlib.util.spec_from_file_location('contract_fixtures',W/'test_finra_contract.py');fixtures=importlib.util.module_from_spec(spec);spec.loader.exec_module(fixtures)
class Frozen(datetime):
 @classmethod
 def now(cls,tz=None):return cls(2026,10,1,21,tzinfo=timezone.utc)
def run(n=90,other=None,corporate=None,treasury=None,history=None,legacy=False,fail_key=None):
 writes=[];messages=[];queries=[];acquisition=[];reads=[]
 raw={s:[{'t':int((datetime(2026,7,1,tzinfo=timezone.utc)+timedelta(days=i)).timestamp()*1000),'o':100+i*.1,'h':102+i*.1,'l':99+i*.1,'c':101+i*.1,'v':1000+i} for i in range(other if s=='LQD' and other is not None else n)] for s in ['HYG','LQD','JNK','TLT','ANGL']}
 store={'data/bond-trace.json':{'regime':'ELEVATED'},'data/trace-bond-prints-history.json':[{'trade_date':'2026-09-29','dealer_share':.9,'breadth_net_pct':-80}], 'data/finra-aggregate-history-v1.json':history or {}}
 corp=copy.deepcopy(corporate) if corporate is not None else [fixtures.corporate(category=c) for c in ['all securities','investment grade','high yield','convertibles']]
 tsy=copy.deepcopy(treasury) if treasury is not None else [fixtures.treasury(),fixtures.treasury(productCategory='Bills',yearsToMaturity=None,benchmark=None,dealerCustomerVolume=20,atsInterdealerVolume=30,volumeWeightedAveragePrice=None)]
 class S3:
  def get_object(self,Bucket,Key):reads.append(Key);return {'Body':io.BytesIO(json.dumps(copy.deepcopy(store[Key]),allow_nan=False).encode())}
  def put_object(self,**kw):
   if kw['Key']==fail_key:raise OSError('invented publication failure')
   body=json.loads(kw['Body']);writes.append({'key':kw['Key'],'body':body,'cache':kw['CacheControl']});store[kw['Key']]=copy.deepcopy(body)
 boto=types.ModuleType('boto3');boto.client=lambda *a,**kw:S3()
 source=R/'tests/fixtures/bond-trace-contract/pre512/aws/shared/finra_trace.py.txt' if legacy else R/'aws/shared/finra_trace.py'
 spec=importlib.util.spec_from_loader('finra_trace',importlib.machinery.SourceFileLoader('finra_trace',str(source)));helper=importlib.util.module_from_spec(spec);spec.loader.exec_module(helper);helper._weekday_window=lambda n:('2026-09-25','2026-10-01')
 def query(name,**kw):
  queries.append({'name':name,'args':copy.deepcopy(kw)})
  if name=='treasuryDailyAggregates':return copy.deepcopy(tsy)
  if name=='corporateMarketBreadth':return copy.deepcopy(corp)
  if legacy and name in ('trace','corporateDebtMarketBreadth'):return None
  raise AssertionError(name)
 helper.query=query
 source=R/'tests/fixtures/bond-trace-contract/pre512/aws/lambdas/justhodl-bond-trace/source/lambda_function.py.txt' if legacy else R/'aws/lambdas/justhodl-bond-trace/source/lambda_function.py'
 with patch.dict(sys.modules,{'boto3':boto,'_fred_shim':types.ModuleType('_fred_shim'),'finra_trace':helper}),patch('urllib.request.urlopen',side_effect=AssertionError('Unexpected real network')):
  engine=runpy.run_path(str(source));g=engine['lambda_handler'].__globals__;g['datetime']=Frozen;g['time']=types.SimpleNamespace(time=lambda:0)
  def aggs(s,n):acquisition.append(s);return copy.deepcopy(raw[s])
  g['fetch_aggs']=aggs;g['fred_get']=lambda *a,**kw:[{'date':f'2026-09-{30-i:02d}','value':3.0} for i in range(30)];g['maybe_telegram']=lambda msg:messages.append(msg)
  with contextlib.redirect_stdout(io.StringIO()):result=engine['lambda_handler']({},None)
 return {'return':result,'writes':writes,'messages':messages,'queries':queries,'acquisition':acquisition,'reads':reads,'input':raw,'corporate_source':corp,'treasury_source':tsy}
def output(result,key='data/bond-trace.json'):return next(row['body'] for row in reversed(result['writes']) if row['key']==key)
def history_row(day,value):return {'contract_version':'finra-aggregates-v1','observation_date':day,'value':value,'response_sha256':'invented-prior-response'}
class WholeContract(unittest.TestCase):
 def test_whole_handler_retains_all_sources_and_separate_dates(self):
  result=run();out=output(result);standalone=output(result,'data/trace-bond-prints.json')
  self.assertEqual(result['return']['statusCode'],200);self.assertEqual(out['trace']['observation_dates'],{'treasury':'2026-09-29','corporate':'2026-09-30'});self.assertEqual(standalone['source_snapshots']['corporate']['source_rows'],result['corporate_source']);self.assertEqual(standalone['source_snapshots']['treasury']['source_rows'],result['treasury_source'])
 def test_aggregate_not_subset_sum_and_benchmark_label_not_price(self):
  result=run();out=output(result);s=output(result,'data/trace-bond-prints.json');self.assertEqual(out['trace']['corporate_breadth_net_pct'],30);self.assertEqual(out['trace']['dealer_customer_volume_share'],.4);self.assertEqual(s['treasury']['buckets'][0]['benchmark'],'On-the-run');self.assertIsNone(out['trace']['vwap_dislocation']);self.assertIsNone(out['trace']['dealer_positioning_z']);self.assertEqual(len(s['corporate']['categories']),4)
 def test_deprecated_prints_never_queried_and_remain_unavailable(self):
  result=run();self.assertEqual([q['name'] for q in result['queries']],['treasuryDailyAggregates','corporateMarketBreadth']);self.assertIsNone(output(result)['trace']['trace_n_prints']);self.assertIsNone(output(result)['trace']['trace_total_volume']);self.assertIsNone(output(result,'data/trace-bond-prints.json')['trace_aggregates'])
 def test_missing_all_securities_does_not_sum_grade_categories(self):
  result=run(corporate=[fixtures.corporate(category='investment grade'),fixtures.corporate(category='high yield')]);self.assertIsNone(output(result)['trace']['corporate_breadth_net_pct'])
 def test_malformed_corporate_does_not_remove_valid_treasury(self):
  for bad in [fixtures.corporate(7),fixtures.corporate('2026-10-02'),fixtures.corporate('2026-09-31')]:
   result=run(corporate=[fixtures.corporate(),bad]);self.assertEqual(output(result)['trace']['treasury_n_buckets'],2);self.assertIsNone(output(result)['trace']['corporate_breadth_net_pct'])
 def test_null_count_is_unavailable_not_zero(self):
  for value in [None,True,'0',-1,.5]:
   result=run(corporate=[fixtures.corporate(advances=value)]);self.assertIsNone(output(result)['trace']['corporate_breadth_net_pct'])
 def test_real_zero_breadth_and_zero_denominator_are_distinct(self):
  self.assertEqual(output(run(corporate=[fixtures.corporate(advances=0,declines=0,unchanged=10)]))['trace']['corporate_breadth_net_pct'],0)
  self.assertIsNone(output(run(corporate=[fixtures.corporate(advances=0,declines=0,unchanged=0)]))['trace']['corporate_breadth_net_pct'])
 def test_missing_or_nonfinite_treasury_volume_cannot_be_zero_filled(self):
  for value in [None,True,'40',-1,1e308]:
   result=run(treasury=[fixtures.treasury(dealerCustomerVolume=value,atsInterdealerVolume=1e308)]);self.assertIsNone(output(result)['trace']['dealer_customer_volume_share'])
 def test_zero_share_is_real_and_all_zero_volume_is_unavailable(self):
  self.assertEqual(output(run(treasury=[fixtures.treasury(dealerCustomerVolume=0)]))['trace']['dealer_customer_volume_share'],0)
  self.assertIsNone(output(run(treasury=[fixtures.treasury(dealerCustomerVolume=0,atsInterdealerVolume=0)]))['trace']['dealer_customer_volume_share'])
 def test_unknown_category_retained_but_not_aggregated(self):
  result=run(treasury=[fixtures.treasury(productCategory='invented unqualified scope')]);self.assertIsNone(output(result)['trace']['dealer_customer_volume_share']);self.assertEqual(len(output(result,'data/trace-bond-prints.json')['treasury']['buckets']),1)
 def test_legacy_history_is_not_read_or_migrated(self):
  result=run();self.assertNotIn('data/trace-bond-prints-history.json',result['reads']);self.assertIsNone(output(result)['trace']['breadth_momentum'])
 def test_comparisons_only_use_earlier_same_definition_dates(self):
  history={'contract_version':'finra-aggregates-v1','corporate':[history_row('2026-09-29',20),history_row('2026-09-30',99),history_row('2026-10-01',-99),dict(history_row('2026-09-28',-10),contract_version='old')]}
  result=run(history=history);self.assertEqual(output(result)['trace']['breadth_momentum'],10);stored=output(result,'data/finra-aggregate-history-v1.json')['corporate'];self.assertEqual(next(r['value'] for r in stored if r['observation_date']=='2026-09-30'),30);self.assertEqual(stored[-1]['value'],-99)
 def test_duplicate_prior_dates_do_not_inflate_evidence(self):
  history={'contract_version':'finra-aggregates-v1','corporate':[history_row('2026-09-29',20),history_row('2026-09-29',21)]}
  self.assertIsNone(output(run(history=history))['trace']['breadth_momentum'])
 def test_malformed_history_cannot_remove_current_valid_measurements(self):
  history={'contract_version':'finra-aggregates-v1','corporate':[None,[],{},history_row('bad-date',3),history_row('2026-09-29',10**400),history_row('2026-09-28',True)]}
  result=run(history=history);self.assertEqual(output(result)['trace']['corporate_breadth_net_pct'],30);self.assertIsNone(output(result)['trace']['breadth_momentum'])
 def test_ambiguous_treasury_scope_retained_but_not_aggregated(self):
  for row in [fixtures.treasury(benchmark=None),fixtures.treasury(yearsToMaturity=None),fixtures.treasury(productCategory='Bills')]:
   result=run(treasury=[row]);self.assertIsNone(output(result)['trace']['dealer_customer_volume_share']);self.assertEqual(output(result,'data/trace-bond-prints.json')['source_snapshots']['treasury']['rows'],[row])
 def test_same_date_repeat_does_not_add_observation(self):
  first=run();history=output(first,'data/finra-aggregate-history-v1.json');again=run(history=history);self.assertEqual(output(again,'data/finra-aggregate-history-v1.json'),history);self.assertIsNone(output(again)['trace']['breadth_momentum'])
 def test_30_row_boundary_recovers_and_31_ordinary_proxy_preserved(self):
  for n,other in [(30,30),(30,31),(31,30)]:self.assertIsNone(output(run(n,other))['hyg_lqd_ratio']['change_30d_pct'])
  for n in [31,90]:
   candidate=run(n);legacy=run(n,legacy=True)
   for key in ['hyg','lqd','hyg_lqd_ratio','jnk_hyg_divergence_5d_pct','tlt_perf_5d_pct','hy_oas_pct','hy_oas_5d_change_bp','hy_oas_30d_change_bp','composite_stress','regime','top_reasons']:
    self.assertEqual(output(candidate).get(key),output(legacy).get(key),key)
   self.assertEqual(candidate['acquisition'],legacy['acquisition'])
 def test_calls_and_sizing_withheld_even_when_all_rows_available(self):
  out=output(run());self.assertFalse(out['calls_eligible']);self.assertFalse(out['sizing_eligible']);self.assertIsNone(out['call']);self.assertEqual(out['quality']['status'],'unqualified');self.assertFalse(out['trace']['quality']['calls_eligible'])
 def test_both_sources_missing_preserves_proxy_without_claiming_finra(self):
  result=run(corporate=[],treasury=[]);out=output(result);self.assertEqual(out['trace']['provenance'],'proxy-fallback');self.assertNotIn('authoritative',out['trace']['note']);self.assertEqual(len(result['writes']),1)
 def test_failed_standalone_publication_cannot_claim_published_layer(self):
  result=run(fail_key='data/trace-bond-prints.json');self.assertEqual(output(result)['trace']['provenance'],'proxy-fallback')
if __name__=='__main__':unittest.main()
