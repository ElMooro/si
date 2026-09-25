from pathlib import Path
from copy import deepcopy
from datetime import date,timedelta
from fractions import Fraction
from decimal import localcontext, ROUND_UP
import sys,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops/checks','aws/shared','aws/lambdas/justhodl-crisis-composite/tests')]
import liquidity_flow_candidate as model
from test_native_research import fixtures


class Tests(unittest.TestCase):
 @classmethod
 def setUpClass(cls):
  cls.source,cls.originals=fixtures()
  cls.output=model.build(cls.source,cls.originals,cls.source['generated_at'])

 def test_every_original_row_is_retained_and_every_calendar_slot_has_traceable_coordinates(self):
  out=self.output
  self.assertEqual(len(out['calendar_history_180d']),180)
  dates=[date.fromisoformat(r['valuation_date']) for r in out['calendar_history_180d']]
  self.assertTrue(all(b-a==timedelta(days=1) for a,b in zip(dates,dates[1:])))
  for sid,row in out['series'].items():
   originals=self.originals[sid]['observations']['observations']
   self.assertEqual(len(row['history']),len(originals))
   for point in row['history']:
    self.assertEqual(point['native_value'],originals[point['original_row']].get('value'))
  for flag in ('calls_eligible','sizing_eligible','execution_eligible','publication_eligible','forecast_qualified','point_in_time_backtest_qualified'):
   self.assertFalse(out[flag])

 def original_value(self,sid,day):
  rows=self.originals[sid]['observations']['observations']
  eligible=[(r['date'],i,r.get('value')) for i,r in enumerate(rows) if r['date']<=day]
  if not eligible:return None,None
  observed,index,value=max(eligible)
  if value in (None,'.','') or (date.fromisoformat(day)-date.fromisoformat(observed)).days>({'WALCL':21,'WTREGEN':21,'RRPONTSYD':10}[sid]):return None,index
  return Fraction(value)/({'WALCL':1000,'WTREGEN':1000,'RRPONTSYD':1}[sid]),index

 def test_independent_rational_arithmetic_for_all_180_days_and_signed_contributions(self):
  for sample in self.output['calendar_history_180d']:
   values={}
   for sid in ('WALCL','WTREGEN','RRPONTSYD'):
    expected,index=self.original_value(sid,sample['valuation_date']);values[sid]=expected
    selected=sample['legs'][sid]['selected'];self.assertEqual(selected['original_row'] if selected else None,index)
    actual=sample['legs'][sid]['value']['exact_decimal'];self.assertEqual(Fraction(actual) if actual is not None else None,expected)
   total=values['WALCL']-values['WTREGEN']-values['RRPONTSYD'] if all(v is not None for v in values.values()) else None
   raw=sample['net']['exact_decimal'];self.assertEqual(Fraction(raw) if raw is not None else None,total)
  for difference in self.output['comparisons'].values():
   changes=[]
   for sid,row in difference['legs'].items():
    a,_=self.original_value(sid,difference['current_valuation_date']);b,_=self.original_value(sid,difference['baseline_valuation_date'])
    expected=(a-b)*(1 if sid=='WALCL' else -1) if a is not None and b is not None else None
    raw=row['signed_formula_contribution']['exact_decimal'];self.assertEqual(Fraction(raw) if raw is not None else None,expected);changes.append(expected)
   raw=difference['change']['exact_decimal'];self.assertEqual(Fraction(raw) if raw is not None else None,sum(changes) if all(v is not None for v in changes) else None)

 def test_missing_latest_row_never_backfills_and_small_levels_still_use_declared_units(self):
  series=deepcopy(self.output['series']['WTREGEN']);series['history']=[{'observation_date':'2026-09-23','native_value':'40000','original_row':0}]
  self.assertEqual(model.point(series,date(2026,9,25))['value']['exact_decimal'],'40.000')
  series['history'].append({'observation_date':'2026-09-24','native_value':'.','original_row':1})
  self.assertEqual(model.point(series,date(2026,9,25))['status'],'missing_source_value')
  self.assertIsNone(model.point(series,date(2026,9,25))['value']['value'])
  series['history'][-1]['native_value']='0';self.assertEqual(model.point(series,date(2026,9,25))['value']['value'],0)

 def test_source_expiry_withholds_current_but_preserves_the_entire_reconstruction(self):
  later=(model.clock(self.source['generated_at'])+timedelta(hours=27)).isoformat()
  out=model.build(self.source,self.originals,later)
  self.assertIsNone(out['current']);self.assertEqual(out['quality']['status'],'source_ineligible')
  self.assertEqual({k:v['history'] for k,v in out['series'].items()},{k:v['history'] for k,v in self.output['series'].items()});self.assertEqual(out['calendar_history_180d'],self.output['calendar_history_180d'])
  self.assertIsNone(out['quality']['publication_date']);self.assertFalse(out['quality']['release_calendar_verified'])

 def test_output_tampering_and_future_source_rejected(self):
  bad=deepcopy(self.source);bad['measurements']['WALCL']['current']=0
  with self.assertRaises(ValueError):model.build(bad,self.originals,self.source['generated_at'])
  bad['replay']['output_sha256']=model.observations.digest({k:v for k,v in bad.items() if k!='replay'})
  with self.assertRaises(ValueError):model.build(bad,self.originals,self.source['generated_at'])
  with self.assertRaises(ValueError):model.build(self.source,self.originals,'2000-01-01T00:00:00+00:00')

 def test_month_windows_use_calendar_dates_and_ignore_ambient_arithmetic(self):
  end=date.fromisoformat(self.output['source_generated_at'][:10])
  self.assertEqual(self.output['comparisons']['1m']['baseline_valuation_date'],str(model.observations.months_before(end,1)))
  self.assertEqual(self.output['comparisons']['3m']['baseline_valuation_date'],str(model.observations.months_before(end,3)))
  for change in self.output['comparisons'].values():
   for leg in change['legs'].values():
    self.assertEqual(leg['current']['valuation_date'],str(end))
    self.assertEqual(leg['baseline']['valuation_date'],change['baseline_valuation_date'])
  with localcontext() as context:
   context.prec=8;context.rounding=ROUND_UP
   actual=model.build(self.source,self.originals,self.source['generated_at'])
  self.assertEqual(actual,self.output)


if __name__=='__main__':unittest.main(verbosity=2)
