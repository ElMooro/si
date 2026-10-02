from pathlib import Path
from unittest.mock import patch
import hashlib,importlib.util,json,math,socket,sys,unittest
R=Path(__file__).resolve().parents[4]
sys.path[:0]=[str(R/'aws/lambdas/justhodl-ai/source'),str(R/'aws/shared'),str(Path(__file__).parent)]
from test_market_read_board import Fixed,S3
SOURCE=R/'aws/lambdas/justhodl-ai/source/market_read.py'
PRIOR=R/'tests/fixtures/market-read-pre-arithmetic-20261002.py'
class ArithmeticTests(unittest.TestCase):
 def setUp(self):
  guard=patch.object(socket.socket,'connect',side_effect=AssertionError('No actual network'));guard.start();self.addCleanup(guard.stop)
  self.m=self.load(SOURCE)
 def load(self,path):
  spec=importlib.util.spec_from_file_location('invented_arithmetic_'+path.stem,path);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);m.datetime=Fixed;return m
 def board(self,entities,m=None):return (m or self.m).build_board(S3({'data/jh-fusion.json':{'entities':{'stock:'+k:v for k,v in entities.items()}}}),'invented-public')
 def symbols(self,entities,m=None,side='fusion_top'):return [r['ticker'] for r in self.board(entities,m)['stocks'][side]]
 def test_oversized_positive_and_negative_integer_withheld(self):
  for value in (10**309,-10**309,10**400,-10**400):self.assertIsNone(self.m._num(value))
 def test_finite_integer_precision_and_subnormal_values_preserved(self):
  for value in (2**53+1,10**308,-10**308,sys.float_info.max,5e-324,-5e-324,0,-0.0):
   self.assertEqual(self.m._num(value),value);self.assertIs(type(self.m._num(value)),type(value))
  self.assertLess(math.copysign(1,self.m._num(-0.0)),0)
 def test_whole_board_survives_oversized_score_among_valid_rows(self):
  rows={'HUGE':{'fusion_score':10**400,'confidence':.5},'SMALL':{'fusion_score':3,'confidence':.5},'NEG':{'fusion_score':-10**400,'confidence':.5}}
  self.assertEqual(self.symbols(rows),['SMALL']);self.assertEqual(self.symbols(rows,side='fusion_bottom'),[])
  with self.assertRaises(OverflowError):self.board(rows,self.load(PRIOR))
 def test_oversized_confidence_is_withheld_before_existing_missing_policy(self):
  rows={'QAONLY':{'fusion_score':2.0,'confidence':10**400}}
  self.assertIsNone(self.board(rows)['stocks']['fusion_top'][0]['confidence'])
  with self.assertRaises(OverflowError):self.board(rows,self.load(PRIOR))
 def test_overflowing_products_keep_relative_rank_in_both_sides(self):
  for sign,side in ((1,'fusion_top'),(-1,'fusion_bottom')):
   rows={'LOW':{'fusion_score':sign*1e308,'confidence':2.0},'HIGH':{'fusion_score':sign*1e308,'confidence':3.0}}
   self.assertEqual(self.symbols(rows,side=side),['HIGH','LOW']);self.assertEqual(self.symbols(rows,self.load(PRIOR),side),['LOW','HIGH'])
 def test_underflowing_products_keep_relative_rank(self):
  rows={'LOW':{'fusion_score':5e-324,'confidence':.1},'HIGH':{'fusion_score':5e-324,'confidence':.2}}
  self.assertEqual(self.symbols(rows),['HIGH','LOW']);self.assertEqual(self.symbols(rows,self.load(PRIOR)),['LOW','HIGH'])
 def test_integer_precision_is_not_lost_during_float_weighting(self):
  rows={'LOW':{'fusion_score':2**53,'confidence':.5},'HIGH':{'fusion_score':2**53+1,'confidence':.5}}
  self.assertEqual(self.symbols(rows),['HIGH','LOW']);self.assertEqual(self.symbols(rows,self.load(PRIOR)),['LOW','HIGH'])
 def test_zero_missing_negative_weights_and_ties_preserve_existing_policy(self):
  rows={'ZERO':{'fusion_score':100,'confidence':0},'MISSING':{'fusion_score':2},'NEG':{'fusion_score':4,'confidence':-.5},'TIE':{'fusion_score':1,'confidence':1}}
  self.assertEqual(self.symbols(rows),['MISSING','TIE','ZERO','NEG'])
  self.assertEqual(self.symbols(rows),self.symbols(rows,self.load(PRIOR)))
 def test_rank_working_values_do_not_leak_into_public_json(self):
  rows={'QAONLY':{'fusion_score':10**308,'confidence':1e308}}
  result=self.board(rows);json.dumps(result,allow_nan=False)
  row=result['stocks']['fusion_top'][0];self.assertEqual(row['fusion_score'],10**308);self.assertEqual(row['confidence'],1e308)
 def test_complete_source_reverses_only_reviewed_arithmetic_edits(self):
  transition=json.loads((R/'docs/audit/2026-10-02/ai-board-arithmetic-transition.json').read_bytes());s=SOURCE.read_text(encoding='utf-8')
  self.assertEqual(hashlib.sha256(s.encode()).hexdigest(),transition['after_sha256'])
  for edit in reversed(transition['edits']):self.assertEqual(s.count(edit['after']),edit['count']);s=s.replace(edit['after'],edit['before'])
  self.assertEqual(s,PRIOR.read_text(encoding='utf-8'));self.assertEqual(hashlib.sha256(s.encode()).hexdigest(),transition['before_sha256'])
  self.assertEqual(transition['before_sha256'],'ea0d38f88975095091943e358d16ea3ccb635985b18afd75299e17fc49a91a90')
if __name__=='__main__':unittest.main(verbosity=2)
