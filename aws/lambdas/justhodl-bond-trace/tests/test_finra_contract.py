from pathlib import Path
from unittest.mock import patch
import copy,hashlib,importlib.util,json,unittest
R=Path(__file__).resolve().parents[4]
spec=importlib.util.spec_from_file_location('finra_contract',R/'aws/shared/finra_trace.py');mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod)
def corporate(day='2026-09-30',category='all securities',**kw):
 return dict({'tradeReportDate':day,'productCategory':category,'advances':6,'declines':3,'unchanged':1,'totalTrades':30,'totalVolume':123.5,'fiftyTwoWeekHigh':2,'fiftyTwoWeekLow':1},**kw)
def treasury(day='2026-09-29',**kw):
 return dict({'tradeDate':day,'productCategory':'Nominal Coupons','yearsToMaturity':'<= 2 years','benchmark':'On-the-run','dealerCustomerVolume':40,'atsInterdealerVolume':60,'dealerCustomerCount':7,'atsInterdealerCount':9,'volumeWeightedAveragePrice':100.5},**kw)
class Contracts(unittest.TestCase):
 def setUp(self):
  self.window=patch.object(mod,'_weekday_window',return_value=('2026-09-25','2026-10-01'));self.window.start();self.addCleanup(self.window.stop)
  self.network=patch('urllib.request.urlopen',side_effect=AssertionError('No real network permitted'));self.network.start();self.addCleanup(self.network.stop)
 def fetch(self,rows,fn='fetch_corporate_breadth',*args):
  with patch.object(mod,'query',return_value=copy.deepcopy(rows)) as query:
   result=getattr(mod,fn)(*args)
  return result,query
 def test_documented_dataset_and_date_field(self):
  result,q=self.fetch([corporate()]);self.assertEqual(q.call_args.args,('corporateMarketBreadth',));self.assertEqual(q.call_args.kwargs['date_range_filters'][0]['fieldName'],'tradeReportDate');self.assertEqual(result['observation_date'],'2026-09-30')
 def test_all_categories_retained_without_adding_subsets(self):
  rows=[corporate(category=c) for c in ['all securities','investment grade','high yield','convertibles']]
  result,_=self.fetch(rows);self.assertEqual(result['rows'],rows);self.assertEqual(result['source_rows'],rows);self.assertEqual(result['numberOfIssues'],10);self.assertEqual(result['numberOfTrades'],30);self.assertEqual(result['parValueTraded'],123.5);self.assertIsNone(result['averagePriceChange'])
 def test_unsorted_older_rows_preserved_latest_selected(self):
  rows=[corporate('2026-09-29'),corporate('2026-09-30'),corporate('2026-09-28')];result,_=self.fetch(rows);self.assertEqual(result['rows'],[rows[1]]);self.assertEqual(result['source_rows'],rows)
 def test_hash_binds_entire_response(self):
  rows=[corporate(),corporate('2026-09-29')];result,_=self.fetch(rows);self.assertEqual(result['response_sha256'],hashlib.sha256(json.dumps(rows,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest())
 def test_mixed_missing_future_invalid_dates_reject_whole_response(self):
  for d in [None,7,True,'2026-09-31','2026-10-02','2026-09-24','2026-9-30','2026-09-30T00:00:00Z']:
   with self.subTest(d=d):self.assertIsNone(self.fetch([corporate(),corporate(d)])[0])
 def test_duplicate_category_date_rejects_even_identical_rows(self):
  self.assertIsNone(self.fetch([corporate(),corporate()])[0])
 def test_bad_category_types_and_nondict_rows_reject(self):
  for row in [None,3,[],{},corporate(category=None),corporate(category=True),corporate(category='')]:
   with self.subTest(row=row):self.assertIsNone(self.fetch([corporate(),row])[0])
 def test_unknown_category_and_fields_are_preserved(self):
  row=corporate(category='invented future category',new_field='literal');result,_=self.fetch([row]);self.assertEqual(result['rows'],[row]);self.assertIsNone(result['numberOfIssues']);self.assertIsNone(result['parValueTraded'])
 def test_saturated_response_never_claims_complete(self):
  self.assertIsNone(self.fetch([corporate()]*500)[0]);self.assertIsNone(self.fetch([treasury()]*5000,'fetch_treasury_latest')[0])
 def test_nonfinite_source_rejected_not_serialized_as_json_nan(self):
  for value in [float('nan'),float('inf'),-float('inf')]:self.assertIsNone(self.fetch([corporate(totalVolume=value)])[0])
 def test_treasury_observation_is_not_run_date(self):
  row=treasury();result,q=self.fetch([row],'fetch_treasury_latest');self.assertEqual(result['observation_date'],'2026-09-29');self.assertNotIn('sort_fields',q.call_args.kwargs);self.assertEqual(result['rows'][0]['benchmark'],'On-the-run')
 def test_treasury_duplicate_identity_rejected(self):
  self.assertIsNone(self.fetch([treasury(),treasury(dealerCustomerVolume=900)],'fetch_treasury_latest')[0])
 def test_treasury_exact_date_validated_and_preserves_source(self):
  row=treasury();result,q=self.fetch([row],'fetch_treasury_daily','2026-09-29');self.assertEqual(result,[row]);self.assertEqual(q.call_args.kwargs['compare_filters'][0]['compareType'],'EQUAL')
  self.assertIsNone(self.fetch([row],'fetch_treasury_daily','2026-09-30')[0])
 def test_invalid_requested_date_and_deprecated_prints_never_query(self):
  with patch.object(mod,'query',side_effect=AssertionError('must not query')):
   self.assertIsNone(mod.fetch_treasury_daily('wrong'));self.assertIsNone(mod.fetch_trace_aggregates('2026-09-30'))
 def test_numeric_contract_preserves_zero_and_rejects_coercion(self):
  self.assertEqual(mod._finite_number(0),0)
  for value in [None,True,'0',-1,float('nan'),10**400]:self.assertIsNone(mod._finite_number(value))
  self.assertIsNone(mod._finite_number(.5,count=True));self.assertIsNone(mod._finite_number(2**53,count=True))
 def test_no_rows_remains_unavailable(self):
  for rows in [[],None,{},'[]']:
   self.assertIsNone(self.fetch(rows)[0]);self.assertIsNone(self.fetch(rows,'fetch_treasury_latest')[0])
if __name__=='__main__':unittest.main()
