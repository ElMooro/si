"""Invented source-only reproductions; no provider or application packet reads."""
from pathlib import Path
from unittest.mock import Mock
import ast,unittest
R=Path(__file__).resolve().parents[1]/'tests/fixtures/crypto-stablecoin-stocks/before'
source=R/'aws/lambdas/justhodl-crypto-intel/source/lambda_function.py.txt'
node=next(n for n in ast.parse(source.read_text(encoding='utf-8')).body if isinstance(n,ast.FunctionDef) and n.name=='fetch_stablecoins')
def row(name,current,prior=None):
 out={'id':name,'name':name,'symbol':name,'pegType':'peggedUSD','circulating':{'peggedUSD':current}}
 if prior is not None:out['circulatingPrevWeek']={'peggedUSD':prior}
 return out
def run(rows):
 get=Mock(return_value={'peggedAssets':rows});scope={'http_get':get,'fmt':str,'sr':lambda v:round(v,2)}
 exec(compile(ast.Module(body=[node],type_ignores=[]),str(source),'exec'),scope)
 out=scope['fetch_stablecoins']();assert get.call_count==1;return out
class ExistingDefects(unittest.TestCase):
 def test_missing_history_fabricates_zero_change_and_stable_state(self):
  out=run([row('A',100_000_000)])
  self.assertEqual(out['stablecoins'][0]['change_7d'],0);self.assertEqual(out['stablecoins'][0]['signal'],'STABLE')
 def test_inflow_vote_can_coexist_with_massive_total_contraction(self):
  rows=[row('BIG',10_000_000_000,20_000_000_000)]+[row('SMALL'+str(i),100_000_000,50_000_000) for i in range(4)]
  out=run(rows);self.assertEqual(out['net_signal'],'INFLOW');self.assertLess(sum(r['circulating']['peggedUSD']-r['circulatingPrevWeek']['peggedUSD'] for r in rows),0)
 def test_total_and_displayed_population_differ(self):
  out=run([row('A',100_000_000),row('B',40_000_000)])
  self.assertEqual(out['total_mcap'],140_000_000);self.assertEqual(sum(r['mcap'] for r in out['stablecoins']),100_000_000)
 def test_twenty_five_name_cutoff_silently_omits_returned_population(self):
  out=run([row(str(i),100_000_000) for i in range(26)])
  self.assertEqual(out['total_mcap'],2_500_000_000);self.assertEqual(len(out['stablecoins']),25)
 def test_boolean_is_counted_as_a_monetary_measurement(self):
  out=run([row('BOOLEAN',True)])
  self.assertEqual(out['total_mcap'],1);self.assertEqual(out['stable_count'],1)
if __name__=='__main__':unittest.main(verbosity=2)
