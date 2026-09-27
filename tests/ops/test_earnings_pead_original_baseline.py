"""Original PEAD defects reproduced with isolated functions and synthetic data."""
from pathlib import Path
from types import SimpleNamespace
from datetime import datetime,timezone
import ast,calendar,sys,time,unittest
ROOT=Path(__file__).resolve().parents[2];sys.path[:0]=[str(ROOT/p) for p in ('aws/ops/staged','aws/ops','aws/ops/checks')]
import ops_6253_earnings_pead_original_baseline as op
FIXTURE=ROOT/'tests/fixtures/pre-earnings-pead-observations.py.txt'


def events():return [{'symbol':'OTHER','date':'2026-09-01','epsActual':2,'epsEstimated':1,'currency':'JPY','period':'FY'} for _ in range(4)]


def scope(values=None,history=None):
    ns={'time':SimpleNamespace(time=lambda:datetime(2026,9,27,tzinfo=timezone.utc).timestamp(),strptime=time.strptime,mktime=calendar.timegm),
        'DRIFT_WINDOW_DAYS':60,'FMP_KEY':'synthetic','fetch_url':lambda *a,**k:values,
        'fetch_earnings_surprises':lambda *a,**k:values if values is not None else events(),'fetch_recent_history':lambda *a,**k:history}
    tree=ast.parse(FIXTURE.read_bytes());fn=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='evaluate_ticker')
    exec(compile(ast.Module(body=[fn],type_ignores=[]),'<isolated original earnings arithmetic>','exec'),ns);return ns


def evaluate(values=None,history=None):return scope(values,history)['evaluate_ticker']({'symbol':'TEST','cap_bucket':'small','market_cap':1e8})


class Tests(unittest.TestCase):
    def test_original_source_pinned_without_native_import(self):
        self.assertEqual(op.source_check('justhodl-earnings-pead',FIXTURE.read_bytes())['bytes'],13505)
        with self.assertRaises(ValueError):op.source_check('justhodl-earnings-pead',FIXTURE.read_bytes()+b'\n')
        self.assertNotIn('lambda_function',sys.modules)
    def test_same_event_wrong_issuer_annual_records_become_four_quarter_top_tier(self):
        out=evaluate();self.assertEqual(out['beat_streak'],4);self.assertEqual(out['tier'],'TIER_S_PEAD_DRIFT')
        self.assertEqual({r['date'] for r in out['surprise_history']},{'2026-09-01'})
    def test_future_events_still_receive_top_tier(self):
        values=events()
        for row in values:row['date']='2027-01-01'
        out=evaluate(values);self.assertLess(out['metrics']['days_since_earnings'],0);self.assertEqual(out['tier'],'TIER_S_PEAD_DRIFT')
    def test_zero_eps_replaced_by_alias_and_boolean_is_an_earnings_amount(self):
        values=events();values[0].update(epsActual=0,actualEarningResult=3);out=evaluate(values)
        self.assertEqual(out['surprise_history'][0]['actual'],3);self.assertEqual(out['metrics']['latest_surprise_pct'],200)
        for row in values:row.update(epsActual=True,epsEstimated=0.5)
        out=evaluate(values);self.assertEqual(out['surprise_history'][0]['actual'],1);self.assertEqual(out['beat_streak'],4)
    def test_zero_estimate_gap_is_skipped_into_a_consecutive_streak(self):
        values=[{**events()[0],'date':d} for d in ['2026-09-01','2026-06-01','2026-03-01','2025-12-01','2025-09-01']]
        values[1]['epsEstimated']=0;out=evaluate(values)
        self.assertEqual(out['beat_streak'],4);self.assertNotIn('2026-06-01',[r['date'] for r in out['surprise_history']])
    def test_unadjusted_unidentified_event_day_prices_become_confirmed_drift(self):
        history=[{'date':'2026-09-01','close':10},{'date':'2026-09-25','close':20}]
        out=evaluate(history=history);self.assertEqual(out['metrics']['post_earnings_return_pct'],100);self.assertIn('DRIFT_CONFIRMED_5%+',out['flags'])
    def test_event_fetch_does_not_filter_future_events_and_discards_complete_population(self):
        values=[{**events()[0],'date':f'2027-01-{i+1:02d}'} for i in range(12)];ns=scope(values)
        tree=ast.parse(FIXTURE.read_bytes());fn=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='fetch_earnings_surprises')
        exec(compile(ast.Module(body=[fn],type_ignores=[]),'<isolated original fetch projection>','exec'),ns)
        out=ns['fetch_earnings_surprises']('TEST');self.assertEqual(len(out),8);self.assertEqual(out[0]['date'],'2027-01-12')


if __name__=='__main__':unittest.main(verbosity=2)
