from pathlib import Path
from datetime import datetime,timedelta,timezone
import base64,hashlib,json,runpy,unittest
M=runpy.run_path(str(Path(__file__).resolve().parents[1]/'aws/shared/crypto_sentiment_observations.py'))
NOW=datetime(2020,2,1,tzinfo=timezone.utc)
def point(days,value='17'):
 return {'value':value,'value_classification':'Invented','timestamp':str(int((NOW-timedelta(days=days)).timestamp()))}
def build(rows,**kwargs):
 raw=json.dumps({'name':'Fear and Greed Index','data':rows,'metadata':{'error':None}},allow_nan=False).encode()
 return M['build'](raw,source_url=M['SOURCES'][0],acquired_at=NOW.isoformat(),**kwargs)
class Observations(unittest.TestCase):
 def test_projection_budget_withholds_whole_population_and_retains_original_bytes(self):
  globals_=M['build'].__globals__;before=globals_['MAX_ROWS'];globals_['MAX_ROWS']=3
  try:out=build([point(i) for i in range(4)])
  finally:globals_['MAX_ROWS']=before
  self.assertEqual(out['received_rows'],4);self.assertFalse(out['projection_complete']);self.assertEqual(out['history'],[]);self.assertIsNone(out['current']);self.assertEqual(len(json.loads(base64.b64decode(out['original_response_base64']))['data']),4);self.assertFalse(out['calls_eligible'])
 def test_missing_and_boolean_values_are_unavailable_not_fifty_or_one(self):
  for value in [None,True,False,'','NaN','Infinity','-1','101',[],{},'00',' 1','1e1',1.5]:
   result=build([point(0,value)]);self.assertIsNone(result['current']);self.assertIsNone(result['avg_7d']);self.assertEqual(result['unresolved_rows'],1)
 def test_zero_and_scale_endpoints_are_observations(self):
  for value in ['0','100',0,100,17]:
   result=build([point(0,value)]);self.assertEqual(result['current'],int(value));self.assertEqual(result['resolved_rows'],1)
 def test_single_observation_never_claims_calendar_week_or_month(self):
  result=build([point(0)]);self.assertIsNone(result['avg_7d']);self.assertIsNone(result['avg_30d']);self.assertIsNone(result['averages']['7d']['denominator'])
 def test_exact_calendar_mean_retains_its_numerator_and_members(self):
  result=build([point(i,str(i)) for i in range(7)])
  self.assertEqual(result['avg_7d'],3);self.assertEqual(result['averages']['7d']['sum_index_points'],21);self.assertEqual(result['averages']['7d']['denominator'],7)
  self.assertEqual(len(result['averages']['7d']['observation_references']),7);self.assertIsNone(result['avg_30d'])
 def test_all_thirty_calendar_dates_required_and_zero_mean_is_zero(self):
  rows=[point(i,'0') for i in range(30)];out=build(rows);self.assertEqual(out['avg_7d'],0);self.assertEqual(out['avg_30d'],0)
  out=build(rows[:15]+rows[16:]);self.assertIsNone(out['avg_30d']);self.assertEqual(out['avg_7d'],0)
 def test_unsorted_source_is_retained_and_latest_selected_by_timestamp(self):
  rows=[point(2,'4'),point(0,'8'),point(1,'6')];out=build(rows)
  self.assertEqual([p['value'] for p in out['history']],[4,8,6]);self.assertEqual([p['value'] for p in out['series']],[4,6,8]);self.assertEqual(out['current'],8)
 def test_unresolved_date_prevents_claiming_latest(self):
  for stamp in [None,True,False,'bad','-1','1.5',{},1.5]:
   row=point(0);row['timestamp']=stamp;out=build([row,point(1,'23')]);self.assertIsNone(out['current']);self.assertTrue(out['undated_source_rows']);self.assertEqual(len(out['history']),2)
 def test_future_observation_is_retained_but_never_a_current_value(self):
  out=build([point(-1,'20'),point(0,'40')]);self.assertIsNone(out['current']);self.assertEqual(out['history'][0]['reason'],'future_source_observation');self.assertEqual(out['history'][1]['value'],40)
 def test_exact_duplicates_retain_occurrences_and_do_not_inflate_average(self):
  rows=[point(i,str(i)) for i in range(7)];out=build(rows+[rows[0]])
  self.assertEqual(out['avg_7d'],3);self.assertEqual(len(out['history']),8);self.assertEqual(len(out['series']),7);self.assertEqual(out['series'][-1]['source_occurrences'],2)
 def test_conflicting_duplicate_clock_is_not_last_writer_wins(self):
  for rows in [[point(0,'1'),point(0,'2')],[point(0,'1'),point(0,True)]]:
   out=build(rows);self.assertIsNone(out['current']);self.assertEqual(out['series'][0]['status'],'unresolved_source_occurrences');self.assertEqual(len(out['series'][0]['source_rows']),2)
 def test_more_than_one_source_timestamp_in_a_date_withholds_daily_mean(self):
  rows=[point(i,str(i)) for i in range(7)];extra=point(1,'99');extra['timestamp']=str(int(extra['timestamp'])+1)
  out=build(rows+[extra]);self.assertIsNone(out['avg_7d']);self.assertEqual(len(out['series']),8)
 def test_original_complete_bytes_and_every_row_are_retained(self):
  rows=[point(i,'0') for i in range(1000)];rows[50]=None;out=build(rows);raw=base64.b64decode(out['original_response_base64'])
  self.assertEqual(json.loads(raw)['data'],rows);self.assertEqual(hashlib.sha256(raw).hexdigest(),out['original_response_sha256']);self.assertEqual(len(raw),out['original_response_bytes']);self.assertEqual(len(out['history']),1000);self.assertEqual(out['synthetic_rows_added'],0);self.assertEqual(out['sampled_rows_removed'],0)
 def test_duplicate_json_nonfinite_and_provider_error_are_not_partial_success(self):
  for raw in [b'{"data":[],"data":[],"metadata":{"error":null}}',b'{"data":[NaN],"metadata":{"error":null}}',b'{"data":[],"metadata":{"error":"rate limited"}}',b'{"data":[]}',b'[]',b'\xff']:
   out=M['build'](raw,source_url=M['SOURCES'][0],acquired_at=NOW.isoformat());self.assertEqual(out['status'],'unavailable');self.assertIsNone(out['current']);self.assertEqual(base64.b64decode(out['original_response_base64']),raw)
 def test_empty_valid_population_differs_from_unavailable_response(self):
  out=build([]);self.assertEqual(out['status'],'descriptive');self.assertEqual(out['returned_rows'],0);self.assertIsNone(out['current']);self.assertEqual(out['series'],[])
 def test_foreign_or_missing_provider_series_name_is_not_the_named_index(self):
  for name in ['Different index',None,True]:
   raw=json.dumps({'name':name,'data':[point(0)],'metadata':{'error':None}}).encode()
   out=M['build'](raw,source_url=M['SOURCES'][0],acquired_at=NOW.isoformat());self.assertEqual(out['status'],'unavailable');self.assertIsNone(out['current'])
 def test_invalid_acquisition_or_source_identity_cannot_be_projected(self):
  for stamp in [None,'2020-01-01','2020-01-01T00:00:00-04:00','bad']:
   with self.assertRaises((ValueError,TypeError)):M['build'](b'{}',source_url=M['SOURCES'][0],acquired_at=stamp)
  with self.assertRaises(ValueError):M['build'](b'{}',source_url='https://example.invalid',acquired_at=NOW.isoformat())
 def test_descriptive_values_never_acquire_forecast_or_portfolio_authority(self):
  out=build([point(0,'100')]);self.assertIsNone(out['first_publication_at'])
  for name,value in M['DENIED'].items():self.assertIs(out[name],value)
if __name__=='__main__':unittest.main(verbosity=2)
