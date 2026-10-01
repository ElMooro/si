from pathlib import Path
import copy,importlib.util,json,sys,unittest
R=Path(__file__).resolve().parents[1];W=R
sys.path.insert(0,str(R/'aws/shared'))
import short_interest_tickers as t
import short_interest_measurements as m

def record(symbol='TEST',name='Invented Class A',stamp='2026-09-15',latest=True,quantity=100):
 row=dict(accountingYearMonthNumber=20260915,symbolCode=symbol,issueName=name,
          issuerServicesGroupExchangeCode='Q',marketClassCode='NMS',
          currentShortPositionQuantity=quantity,previousShortPositionQuantity=80,
          averageDailyVolumeQuantity=200,daysToCoverQuantity=1,stockSplitFlag=None,
          revisionFlag=None,changePercent=25,changePreviousNumber=20,settlementDate=stamp)
 row=m.normalized(row);point=m.compact_point(row,'2026-08-31',0,0)
 return dict(identity={key:row[key] for key in m.GRAIN},observations=[point],
             dates=['2026-08-31','2026-09-15'],latest_settlement_present=latest)

def shards(*records):return {'aa':{'records':{str(i):r for i,r in enumerate(records)}}}

class Identity(unittest.TestCase):
 def test_distinct_issues_are_retained_but_not_collapsed_to_ticker(self):
  data=shards(record(),record(name='Invented Class B',quantity=1000));out=t.build_tickers_projection(data)
  self.assertEqual(out['by_ticker'],{});self.assertEqual(out['source_occurrences'],2)
  self.assertEqual([r['reported_identity']['issueName'] for r in out['ambiguous_symbols'][0]['occurrences']],['Invented Class A','Invented Class B'])
  self.assertFalse(out['identity_verified']);self.assertEqual(t.build_tickers_view(data),{})
 def test_identical_duplicate_occurrences_still_withhold_projection(self):
  out=t.build_tickers_projection(shards(record(),record()))
  self.assertEqual(out['by_ticker'],{});self.assertEqual(len(out['ambiguous_symbols'][0]['occurrences']),2)
 def test_case_collision_and_malformed_issue_cannot_disappear_before_grouping(self):
  bad=record(symbol='test');bad['observations']=[];out=t.build_tickers_projection(shards(record(),bad))
  self.assertEqual(out['by_ticker'],{});self.assertEqual(out['ambiguous_symbols'][0]['ticker'],'TEST');self.assertEqual(len(out['unresolved_occurrences']),1)
 def test_non_boolean_flag_never_enters_latest_rankings(self):
  for value in ['false','true',1,0,None,[],{}]:
   with self.subTest(value=value):
    rows=t.build_tickers_view(shards(record(latest=value)));self.assertIsNone(rows['TEST']['latest'])
    self.assertEqual(rows['TEST']['latest_flag_status'],'invalid_or_missing');self.assertTrue(all(v==[] for v in t.build_ranked_lists(rows).values()))
 def test_genuine_zero_and_boolean_false_are_preserved(self):
  rows=t.build_tickers_view(shards(record(quantity=0,latest=False)))
  self.assertEqual(rows['TEST']['short_interest'],'0');self.assertIs(rows['TEST']['latest'],False)
  self.assertTrue(all(v==[] for v in t.build_ranked_lists(rows).values()))
 def test_malformed_dates_never_win_a_lexical_comparison(self):
  for stamp in ['9999-99-99','2026-02-30','2026-9-15','garbage',None]:
   with self.subTest(stamp=stamp):
    r=record();r['observations'][0][m.POINT_FIELDS.index('settlementDate')]=stamp
    self.assertEqual(t.build_tickers_view(shards(r)),{})
 def test_duplicate_settlement_and_mismatched_point_identity_are_rejected(self):
  r=record();r['observations'].append(copy.deepcopy(r['observations'][0]));self.assertEqual(t.build_tickers_view(shards(r)),{})
  r=record();r['observations'][0][m.POINT_FIELDS.index('issueName')]='Different issue';self.assertEqual(t.build_tickers_view(shards(r)),{})
 def test_latest_observation_selected_from_complete_valid_history(self):
  r=record();older=record(stamp='2026-09-01')['observations'][0];r['observations'].insert(0,older)
  self.assertEqual(t.build_tickers_view(shards(r))['TEST']['settlement_date'],'2026-09-15')
 def test_bad_symbols_and_record_containers_stay_visible(self):
  data=shards(record(symbol='bad/symbol'),None);data['bb']=None;out=t.build_tickers_projection(data)
  self.assertEqual(out['source_occurrences'],2);self.assertEqual(out['by_ticker'],{});self.assertEqual(len(out['unresolved_occurrences']),3)
 def test_optional_symbol_selection_never_resolves_an_issue_collision(self):
  data=shards(record(),record(name='Other issue'),record(symbol='XYZ'))
  out=t.build_tickers_projection(data,['test']);self.assertEqual(out['by_ticker'],{});self.assertEqual(len(out['ambiguous_symbols']),1);self.assertEqual(out['source_occurrences'],3)
 def test_input_objects_are_not_modified(self):
  data=shards(record(),record(name='Other'));old=copy.deepcopy(data);t.build_tickers_projection(data);self.assertEqual(old,data)

if __name__=='__main__':unittest.main(verbosity=2)
