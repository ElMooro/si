import copy
from datetime import datetime,timezone,timedelta
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'aws/shared'))
from bottom_context import context_rows
NOW=datetime(2026,9,9,15,tzinfo=timezone.utc)
def valid():
 return {'engine':'justhodl-bottom','generated_at':NOW.isoformat(),'session':'2026-09-09','board_all':[{'ticker':'SPY','state':'TRIGGERED','frame':'W','weekly_state':'TRIGGERED','score':90,'bars_in_state':0,'st_vol_ratio_sc':0}]}
def test_fresh_bottom_preserves_zero_bars_and_stays_context_only():
 rows,health=context_rows(valid(),now=NOW)
 assert health['usable'] and health['execution_eligible'] is False
 assert rows['SPY']['bars_in_state']==0 and rows['SPY']['st_vol_ratio_sc']==0
 assert rows['SPY']['use']=='DESCRIPTIVE_CONTEXT_ONLY'
def test_stale_future_wrong_producer_and_invalid_session_cannot_feed_alerts_or_structure():
 for change in ({'generated_at':(NOW-timedelta(hours=31)).isoformat()},{'generated_at':(NOW+timedelta(hours=1)).isoformat()},{'engine':'other'},{'session':'2026-01-01'},{'session':'bad'}):
  doc={**valid(),**change};rows,health=context_rows(doc,now=NOW)
  assert not rows and not health['usable']
def test_bottom_invalid_and_duplicate_rows_cannot_be_usable_evidence():
 doc=valid();doc['board_all'][0]['score']=float('nan');rows,health=context_rows(doc,now=NOW)
 assert not rows and health['invalid_rows']==1
 doc=valid();doc['board_all'].append(copy.deepcopy(doc['board_all'][0]));rows,health=context_rows(doc,now=NOW)
 assert not rows and not health['usable']
