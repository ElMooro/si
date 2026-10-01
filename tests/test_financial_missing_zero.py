from pathlib import Path
from datetime import datetime,timezone
import ast,copy,hashlib,json,math,unittest
R=Path(__file__).resolve().parents[1];D=R/'tests/fixtures/financial-missing-zero'
def load(old=False):
 p=D/'predecessor.py.txt' if old else R/'aws/shared/equity_enrich.py';tree=ast.parse(p.read_bytes());nodes=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in ('num','first_num','fetch_financials')]
 ns={'math':math,'datetime':datetime,'timezone':timezone};exec(compile(ast.Module(body=nodes,type_ignores=[]),str(p),'exec'),ns);return ns
def fixture():
 return {'profile':[{'companyName':'Invented','price':10,'mktCap':100,'pe':10}],
 'income-statement':[{'calendarYear':'2024','date':'2024-12-31','revenue':100,'netIncome':10,'grossProfit':40,'operatingIncome':20,'epsdiluted':2,'eps':3,'weightedAverageShsOutDil':5,'weightedAverageShsOut':6,'ebitda':20,'interestExpense':2}],
 'cash-flow-statement':[{'calendarYear':'2024','freeCashFlow':8,'operatingCashFlow':9,'netCashProvidedByOperatingActivities':90,'acquisitionsNet':2}],
 'balance-sheet-statement':[{'totalAssets':100,'totalDebt':10,'cashAndCashEquivalents':5,'totalCurrentAssets':20,'totalCurrentLiabilities':10}],
 'ratios-ttm':[{'priceToEarningsRatioTTM':15}],'ratios':[{'priceToEarningsRatio':10}],'earnings':[],'revenue-product-segmentation':[],'insider-trading/search':[],'historical-price-eod/light':[]}
def run(data,old=False):
 ns=load(old);ns['fmp']=lambda path,params:copy.deepcopy(data[path]);return ns['fetch_financials']('INVENTED')
class FinancialNulls(unittest.TestCase):
 def test_predecessor_silently_admits_bool_nonfinite_and_replaces_zero(self):
  n=load(True)['num'];self.assertEqual(n(True),1);self.assertTrue(math.isnan(n('NaN')))
  d=fixture();d['income-statement'][0]['epsdiluted']=0;d['cash-flow-statement'][0]['operatingCashFlow']=0
  old=run(d,True);self.assertEqual(old['financials'][0]['eps'],3);self.assertEqual(old['accruals'],-80)
 def test_numeric_type_and_finiteness_are_required_without_object_coercion(self):
  n=load()['num']
  class Trap:
   def __float__(self):raise AssertionError('arbitrary coercion')
  for value in [True,False,None,[],{},Trap(),float('inf'),float('nan'),'NaN','Infinity','1e999',10**1000,'1'*513]:self.assertIsNone(n(value))
  for value in [0,0.0,'0',-2,'2.5',4]:self.assertEqual(n(value),float(value))
 def test_real_zero_wins_over_conflicting_numeric_fallback(self):
  d=fixture();d['income-statement'][0].update(epsdiluted=0,weightedAverageShsOutDil=0);d['cash-flow-statement'][0].update(operatingCashFlow=0,acquisitionsNet=0);d['profile'][0]['pe']=0;d['balance-sheet-statement'][0]['totalCurrentAssets']=0
  p=run(d);self.assertEqual(p['financials'][0]['eps'],0);self.assertEqual(p['financials'][0]['shares'],0);self.assertEqual(p['accruals'],10);self.assertEqual(p['acq_pct'],0);self.assertEqual(p['cur_ratio'],0);self.assertEqual(p['pe'],0)
 def test_missing_balance_sheet_leg_is_never_manufactured_as_zero(self):
  for field in ['totalDebt','cashAndCashEquivalents']:
   d=fixture();d['balance-sheet-statement'][0].pop(field);self.assertIsNone(run(d)['ev_ebitda'])
  d=fixture();d['balance-sheet-statement'][0].pop('cashAndCashEquivalents');self.assertIsNotNone(run(d,True)['net_debt_ebitda']);self.assertIsNone(run(d)['net_debt_ebitda'])
 def test_explicit_zero_balance_sheet_legs_remain_measured(self):
  d=fixture();d['balance-sheet-statement'][0].update(totalDebt=0,cashAndCashEquivalents=0);p=run(d);self.assertEqual(p['net_debt_ebitda'],0);self.assertEqual(p['ev_ebitda'],5)
 def test_complete_healthy_projection_and_inputs_are_unchanged(self):
  d=fixture();before=copy.deepcopy(d);self.assertEqual(run(d),run(d,True));self.assertEqual(d,before)
 def test_whole_predecessor_and_only_named_edits_are_preserved(self):
  p=json.loads((D/'edits.json').read_bytes());raw=(D/'predecessor.py.txt').read_bytes();self.assertEqual(hashlib.sha256(raw).hexdigest(),p['predecessor_sha256']);s=raw.decode()
  for a,b in p['edits']:self.assertEqual(s.count(a),1);s=s.replace(a,b)
  self.assertEqual(s,(R/'aws/shared/equity_enrich.py').read_text(encoding='utf-8'));self.assertEqual(hashlib.sha256(s.encode()).hexdigest(),p['candidate_sha256'])
  previous=json.loads((D/'previous-policy-preservation.json').read_bytes());previous['edits'][p['target']].extend(p['edits']);self.assertEqual(previous,json.loads((R/'tests/fixtures/no-paid-research/preservation.json').read_bytes()))
if __name__=='__main__':unittest.main(verbosity=2)
