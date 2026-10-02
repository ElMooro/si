from pathlib import Path
from unittest.mock import patch
from datetime import datetime,timezone,timedelta
import hashlib,importlib.util,io,json,math,socket,sys,unittest
R=Path(__file__).resolve().parents[4]
sys.path[:0]=[str(R/'aws/lambdas/justhodl-ai/source'),str(R/'aws/shared')]
class Fixed(datetime):
 @classmethod
 def now(cls,tz=None):return cls(2026,10,2,14,0,tzinfo=timezone.utc)
class S3:
 def __init__(self,docs):self.docs=docs
 def get_object(self,**kw):return {'Body':io.BytesIO(json.dumps(self.docs[kw['Key']]).encode())}
class BoardInputTests(unittest.TestCase):
 def setUp(self):
  guard=patch.object(socket.socket,'connect',side_effect=AssertionError('No actual network'));guard.start();self.addCleanup(guard.stop)
  spec=importlib.util.spec_from_file_location('invented_board_candidate',R/'aws/lambdas/justhodl-ai/source/market_read.py');self.m=importlib.util.module_from_spec(spec);spec.loader.exec_module(self.m)
  timer=patch.object(self.m,'datetime',Fixed);timer.start();self.addCleanup(timer.stop)
 def board(self,docs):return self.m.build_board(S3(docs),'invented-public')
 def test_booleans_text_and_nonfinite_values_cannot_become_numeric_measurements(self):
  for value in (True,False,'0','12.3','NaN',None,[],{},float('nan'),float('inf'),float('-inf')):self.assertIsNone(self.m._num(value))
 def test_finite_zero_sign_tiny_value_and_integer_precision_survive(self):
  for value in (0,1,-1,1e-7,-1e-7,.123456789,9007199254740993):self.assertEqual(self.m._num(value),value)
  self.assertLess(math.copysign(1,self.m._num(-0.0)),0)
 def test_reported_zero_and_null_do_not_borrow_aliases(self):
  for value in (0,None,False,'',[],{}):self.assertEqual(self.m.pick({'score':value,'legacy':99},'score','legacy'),value)
  self.assertEqual(self.m.pick({'legacy':0},'score','legacy'),0)
 def test_nested_present_values_and_array_indices_are_preserved(self):
  self.assertIsNone(self.m.pick({'a':{'b':None},'legacy':99},'a.b','legacy'))
  self.assertEqual(self.m.pick({'a':[{'b':0}],'legacy':99},'a.0.b','legacy'),0)
  self.assertEqual(self.m.pick({'a':[],'legacy':99},'a.0.b','legacy'),99)
 def test_board_bool_risk_and_cap_withheld_not_converted(self):
  b=self.board({'data/risk-gate.json':{'composite':{'score':True}},'data/khalid-risk.json':{'cap_pct':False}})
  self.assertIsNone(b['regime']['risk_gate_composite']);self.assertIsNone(b['regime']['authority_cap_pct'])
 def test_score_zero_null_and_alias_absence_reach_both_stock_desks(self):
  for value in (0,None,False,''):
   row={'ticker':'QAONLY','tier':'KATLIN_PRIME','score':value,'katlin_score':99}
   b=self.board({'data/katlin.json':{'picks':[row]},'data/fortress.json':{'top':[dict(row,fortress_score=99)]}})
   expected=0 if type(value) is int else None
   self.assertEqual(b['stocks']['katlin_prime'][0]['score'],expected);self.assertEqual(b['stocks']['fortress_top'][0]['score'],expected)
  b=self.board({'data/katlin.json':{'picks':[{'ticker':'QAONLY','tier':'READY','katlin_score':7}]}});self.assertEqual(b['stocks']['katlin_ready'][0]['score'],7)
 def test_invalid_primary_numeric_value_cannot_borrow_valid_legacy(self):
  b=self.board({'data/risk-gate.json':{'composite':{'score':None},'composite_score':90,'sizing_multiplier':False,'sizing':{'multiplier':1}}})
  self.assertIsNone(b['regime']['risk_gate_composite']);self.assertIsNone(b['regime']['risk_gate_sizing'])
 def test_zero_confidence_is_not_replaced_by_half_in_fusion_sort(self):
  b=self.board({'data/jh-fusion.json':{'entities':{'stock:ZERO':{'fusion_score':100,'confidence':0},'stock:POS':{'fusion_score':1,'confidence':.1}}}})
  self.assertEqual([r['ticker'] for r in b['stocks']['fusion_top']],['POS','ZERO']);self.assertEqual(b['stocks']['fusion_top'][1]['confidence'],0)
 def test_clock_threshold_uses_full_precision_at_each_source_sla(self):
  for name,(key,sla,private) in self.m.SOURCES.items():
   if private:continue
   for delta,status in ((-.0001,'FRESH'),(0,'FRESH'),(.0001,'STALE')):
    age=sla+delta;stamp=(Fixed.now()-timedelta(hours=age)).isoformat();row=self.board({key:{'generated_at':stamp}})['sources'][name]
    self.assertEqual(row['status'],status,(name,age,row));self.assertAlmostEqual(row['age_h'],age,places=7)
 def test_future_skew_uses_existing_five_minute_boundary_without_rounding(self):
  for minutes,status in ((4,'FRESH'),(5,'FRESH'),(5.001,'FUTURE'),(6,'FUTURE')):
   stamp=(Fixed.now()+timedelta(minutes=minutes)).isoformat();row=self.board({'data/risk-gate.json':{'generated_at':stamp}})['sources']['risk_gate']
   self.assertEqual(row['status'],status);self.assertAlmostEqual(row['age_h'],-minutes/60,places=7)
 def test_missing_invalid_and_old_clocks_do_not_become_fresh(self):
  self.assertEqual(self.board({})['sources']['risk_gate']['status'],'MISSING')
  for value in (None,'',False,'invalid','2000-01-01'):
   self.assertEqual(self.board({'data/risk-gate.json':{'generated_at':value}})['sources']['risk_gate']['status'],'STALE')
 def test_explicit_offset_and_microseconds_preserved_in_age(self):
  self.assertEqual(self.m._age_h('2026-10-02T10:00:00-04:00'),0)
  self.assertAlmostEqual(self.m._age_h('2026-10-02T13:59:59.999999Z'),1/3600000000,places=15)
 def test_source_changes_are_exactly_the_recorded_six_replacements(self):
  transition=json.loads((R/'docs/audit/2026-10-02/ai-board-projection-transition.json').read_bytes());s=(R/'tests/fixtures/market-read-pre-arithmetic-20261002.py').read_text(encoding='utf-8');self.assertEqual(hashlib.sha256(s.encode()).hexdigest(),transition['after_sha256'])
  for edit in reversed(transition['edits']):self.assertEqual(s.count(edit['after']),edit['count']);s=s.replace(edit['after'],edit['before'])
  self.assertEqual(hashlib.sha256(s.encode()).hexdigest(),transition['before_sha256']);self.assertEqual(s,(R/'tests/fixtures/market-read-pre-board-projection-20261002.py').read_text(encoding='utf-8'))
 def test_retained_source_reproduces_original_faults(self):
  source=R/'tests/fixtures/market-read-pre-board-projection-20261002.py';self.assertEqual(hashlib.sha256(source.read_bytes()).hexdigest(),'d5cc5600fb4e86d7dd0ff3de4440d9034689158bdafb2fe0803abc86e812edf1')
  spec=importlib.util.spec_from_file_location('original_board_projection',source);old=importlib.util.module_from_spec(spec);spec.loader.exec_module(old)
  self.assertEqual(old._num(True),1.0);self.assertEqual(old._num(1e-7),0.0);self.assertEqual(old.pick({'score':None,'legacy':99},'score','legacy'),99)
  with patch.object(old,'datetime',Fixed):
   docs={'data/katlin.json':{'picks':[{'ticker':'QAONLY','tier':'KATLIN_PRIME','score':0,'katlin_score':99}]},'data/risk-gate.json':{'generated_at':(Fixed.now()-timedelta(hours=36.04)).isoformat()}}
   board=old.build_board(S3(docs),'invented-public');self.assertEqual(board['stocks']['katlin_prime'][0]['score'],99);self.assertEqual(board['sources']['risk_gate']['status'],'FRESH')
   self.assertEqual(old._age_h((Fixed.now()+timedelta(minutes=4)).isoformat()),-.1)
if __name__=='__main__':unittest.main(verbosity=2)
