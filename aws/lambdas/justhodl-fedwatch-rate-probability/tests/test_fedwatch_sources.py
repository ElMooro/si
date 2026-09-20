from pathlib import Path
from copy import deepcopy
from datetime import datetime,timedelta,timezone
import json,sys,unittest
HERE=Path(__file__).resolve().parent;ROOT=HERE.parents[3]
sys.path[:0]=[str(HERE.parent/'source'),str(ROOT/'aws/shared')]
import fedwatch_research_model as m
from research_brief_model import SOURCE_CONTRACT
START='2026-09-20T22:55:00+00:00';RECEIVED='2026-09-20T22:55:01+00:00';GENERATED='2026-09-20T22:55:03+00:00'
def seconds(text):return int(datetime.fromisoformat(text).timestamp())
def calendar_raw():
 rows=[('January','27-28'),('March','17-18*'),('April','28-29'),('June','16-17*'),('July','28-29'),('September','15-16*'),('October','27-28'),('December','8-9*')]
 return ('<html><h4><a>2026 FOMC Meetings</a></h4>'+''.join('<div class="fomc-meeting__month"><strong>'+mth+'</strong></div><div class="fomc-meeting__date">'+days+'</div>' for mth,days in rows)+'</html>').encode()
def page(url,raw):return {'request_url':url,'request_sent':True,'http_status':200,'started_at':START,'received_at':RECEIVED,'original':{'key':m.PRIVATE+m.sha(raw)+'.bin','sha256':m.sha(raw),'bytes':len(raw)}}
def chart_doc(symbol):
 return {'chart':{'error':None,'result':[{'meta':{'symbol':symbol,'currency':'USD','exchangeName':'CBT','instrumentType':'FUTURE','dataGranularity':'1d','exchangeTimezoneName':'America/New_York','firstTradeDate':seconds('2021-09-01T00:00:00+00:00'),
  'regularMarketPrice':96.105,'regularMarketTime':seconds('2026-09-20T22:54:00+00:00')},
  'timestamp':[seconds('2026-09-18T04:00:00+00:00'),seconds('2026-09-20T22:53:00+00:00')],
  'indicators':{'quote':[{'open':[96.1,96.1],'high':[96.2,96.2],'low':[96,96],'close':[96.1,96.105],'volume':[1000,16]}],'adjclose':[{'adjclose':[999,999]}]}}]}}
def macro():
 rows={}
 for sid,(_,freq) in m.SPECS.items():
  value={'DFEDTARU':'4','DFEDTARL':'3.75','DFF':'3.88','FEDFUNDS':'3.63'}[sid]
  rows[sid]={'series_id':sid,'contract':SOURCE_CONTRACT,'current_decimal':value,'unit':'Percent','frequency':freq,'date':'2026-08-01' if freq=='M' else '2026-09-18',
    'acquired_at':START,'current_row_index':0,'quality':{'status':'fresh'},'definition':{'seasonal_adjustment_short':'NSA'},'evidence':{'definition':{},'observations':{}}}
 return {'generated_at':START,'measurements':rows},{sid:{'fixture':True} for sid in m.SERIES}
def collection():
 expected=m.plan(START);raw=calendar_raw();pages={};bodies={'calendar':raw};cal=page(m.CALENDAR_URL,raw)
 for item in expected:
  raw=m.encoded(chart_doc(item['symbol']));bodies[item['symbol']]=raw;pages[item['symbol']]=page(item['request_url'],raw)
 return {'started_at':START,'completed_at':RECEIVED,'plan':expected,'calendar':cal,'quotes':pages,'provider_requests':13,'source_bytes':sum(len(x) for x in bodies.values())},bodies

class Sources(unittest.TestCase):
 def setUp(self):
  self.item=m.plan(START)[1];self.doc=chart_doc(self.item['symbol'])
 def read(self,doc=None,received=RECEIVED):
  raw=m.encoded(doc or self.doc);p=page(self.item['request_url'],raw);p['received_at']=received
  return m.chart(raw,p,self.item)
 def test_twelve_month_contract_identity_and_year_roll(self):
  p=m.plan(START);self.assertEqual(len(p),12);self.assertEqual(p[0]['symbol'],'ZQU26.CBT');self.assertEqual(p[4]['symbol'],'ZQF27.CBT');self.assertEqual(p[-1]['contract_month'],'2027-08')
  for s in ('ZQ=F','ZQV26.CBT?apikey=x','../private'):self.assertRaises(ValueError,m.quote_url,s)
 def test_official_calendar_span_stays_distinct_from_effective_date(self):
  raw=calendar_raw();o=m.calendar(raw,page(m.CALENDAR_URL,raw),START);self.assertEqual(o['scheduled_counts_by_year'],{'2026':8})
  last=o['meetings'][-1];self.assertEqual(last['end_date'],'2026-12-09');self.assertTrue(last['projections_marked']);self.assertIsNone(last['policy_effective_date'])
  raw=raw.replace(b'April',b'Apr/May').replace(b'28-29',b'30-1',1);o=m.calendar(raw,page(m.CALENDAR_URL,raw),START)
  self.assertEqual(o['meetings'][2]['start_date'],'2026-04-30');self.assertEqual(o['meetings'][2]['end_date'],'2026-05-01')
 def test_calendar_unknown_date_duplicate_and_missing_year_refused(self):
  for raw in (calendar_raw().replace(b'October',b'Unknown'),calendar_raw().replace(b'2026 FOMC Meetings',b'2027 FOMC Meetings'),calendar_raw().replace(b'27-28',b'31-35',1),calendar_raw().replace(b'December',b'January').replace(b'8-9*',b'27-28')):
   self.assertRaises(ValueError,m.calendar,raw,page(m.CALENDAR_URL,raw),START)
 def test_live_bar_and_metadata_mark_have_different_retained_clocks(self):
  o=self.read();self.assertTrue(o['available']);self.assertEqual(o['latest_bar']['original_row_index'],1)
  self.assertEqual(o['latest_bar']['provider_bar_timestamp'],'2026-09-20T22:53:00+00:00');self.assertEqual(o['provider_market_mark']['provider_market_time'],'2026-09-20T22:54:00+00:00')
  self.assertFalse(o['official_settlement_verified']);self.assertFalse(o['exchange_session_completion_verified']);self.assertFalse(o['adjusted_close_used'])
  self.assertAlmostEqual(o['latest_bar']['rate_equivalent_percent'],3.895);self.assertIsNone(o['meeting_probabilities'])
 def test_last_nonmissing_close_preserves_original_timestamp_index(self):
  self.doc['chart']['result'][0]['indicators']['quote'][0]['close'][1]=None
  o=self.read();self.assertEqual(len(o['bars']),2);self.assertEqual(o['latest_bar']['original_row_index'],0)
  self.assertEqual(o['latest_bar']['provider_bar_timestamp'],'2026-09-18T04:00:00+00:00');self.assertEqual(o['provider_market_mark']['price'],96.105)
 def test_contract_definition_mismatch_is_not_a_proxy(self):
  for key,value in [('symbol','ZQX26.CBT'),('currency','EUR'),('exchangeName','NYQ'),('instrumentType','ETF'),('dataGranularity','1m')]:
   d=deepcopy(self.doc);d['chart']['result'][0]['meta'][key]=value;self.assertRaises(ValueError,self.read,d)
 def test_misaligned_duplicate_and_future_bars_refused(self):
  d=deepcopy(self.doc);d['chart']['result'][0]['indicators']['quote'][0]['close'].pop();self.assertRaises(ValueError,self.read,d)
  d=deepcopy(self.doc);d['chart']['result'][0]['timestamp'][1]=d['chart']['result'][0]['timestamp'][0];self.assertRaises(ValueError,self.read,d)
  d=deepcopy(self.doc);d['chart']['result'][0]['timestamp'][1]=seconds('2026-09-21T00:00:00+00:00');self.assertRaises(ValueError,self.read,d)
 def test_ohlc_volume_and_nonfinite_number_validation(self):
  for key,value in [('high',95),('low',97),('volume',-1),('volume',.5),('close',True)]:
   d=deepcopy(self.doc);d['chart']['result'][0]['indicators']['quote'][0][key][0]=value;self.assertRaises(ValueError,self.read,d)
  raw=m.encoded(self.doc).replace(b'96.105',b'NaN');self.assertRaises(ValueError,m.chart,raw,page(self.item['request_url'],raw),self.item)
 def test_missing_stale_bars_and_negative_rate_equivalent_are_not_clipped(self):
  o=self.read(received='2026-09-29T22:55:01+00:00');self.assertFalse(o['available']);self.assertIsNotNone(o['latest_bar'])
  d=deepcopy(self.doc);q=d['chart']['result'][0]['indicators']['quote'][0]
  for key in ('open','high','low','close'):q[key][1]=100.125
  self.assertEqual(self.read(d)['latest_bar']['rate_equivalent_percent'],-.125)
  p=page(self.item['request_url'],b'');p['http_status']=429;self.assertFalse(m.chart(b'',p,self.item)['available'])
 def test_target_bounds_require_equal_dates_and_allow_real_zero(self):
  p,o=macro();rows,t=m.policy_rows(p,o,GENERATED);self.assertEqual(t['midpoint'],3.875);self.assertEqual(rows['DFF']['value'],3.88)
  p['measurements']['DFEDTARL']['date']='2026-09-17';self.assertIsNone(m.policy_rows(p,o,GENERATED)[1]['midpoint'])
  p,o=macro();p['measurements']['DFEDTARL']['current_decimal']='0';p['measurements']['DFEDTARU']['current_decimal']='0';self.assertEqual(m.policy_rows(p,o,GENERATED)[1]['midpoint'],0)
  p['measurements']['DFEDTARL']['current_decimal']='1';self.assertRaises(ValueError,m.policy_rows,p,o,GENERATED)
 def test_target_missing_and_stale_acquisition_remain_unavailable(self):
  p,o=macro();p['measurements'].pop('DFEDTARL');o.pop('DFEDTARL');self.assertIsNone(m.policy_rows(p,o,GENERATED)[1]['midpoint'])
  p,o=macro();p['measurements']['DFF']['acquired_at']='2026-09-18T00:00:00+00:00';self.assertIsNone(m.policy_rows(p,o,GENERATED)[0]['DFF']['value'])
 def test_complete_research_has_no_legacy_probability_or_timing_authority(self):
  p,o=macro();c,b=collection();out=m.build(p,o,c,b,GENERATED)
  self.assertEqual(out['quality']['dated_contracts_within_age_ceiling'],12);self.assertIsNone(out['next_6mo_summary']['scenario'])
  self.assertTrue(all(out[k] is False for k in m.PERMISSIONS));self.assertEqual(out['portfolio_action'],'WAIT')
  self.assertTrue(all(r['probabilities_pct'] is None and r['implied_post_meeting_rate_pct'] is None for r in out['meetings_ahead']))
  self.assertEqual(out['portfolio_consequences']['dollars_per_index_point'],4167)
 def test_request_inventory_bytes_and_capture_clocks_are_exact(self):
  p,o=macro()
  for mutate in (lambda c:c.update(provider_requests=12),lambda c:c.update(source_bytes=1),lambda c:c.update(completed_at='2026-09-20T22:59:00+00:00'),lambda c:c['quotes'].pop('ZQU26.CBT'),lambda c:c['calendar'].update(received_at='2026-09-20T22:59:00+00:00')):
   c,b=collection();mutate(c);self.assertRaises(ValueError,m.build,p,o,c,b,GENERATED)

if __name__=='__main__':unittest.main(verbosity=2)
