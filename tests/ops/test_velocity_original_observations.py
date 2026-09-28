"""Synthetic original velocity arithmetic; no provider, state or native I/O."""
from pathlib import Path
from typing import List,Optional
from datetime import date,timedelta
import ast,json,hashlib,unittest
ROOT=Path(__file__).resolve().parents[2]
RAW=(ROOT/'tests/fixtures/pre-momentum-leaders-velocity-acceleration.py.txt').read_bytes();TREE=ast.parse(RAW)


def original():
    ns={'List':List,'Optional':Optional}
    nodes=[n for n in TREE.body if isinstance(n,ast.FunctionDef) and n.name in ('compute_vol_baseline','compute_acceleration') or
           isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id in ('LOOKBACK_DAYS','BASELINE_DAYS') for t in n.targets)]
    exec(compile(ast.Module(body=nodes,type_ignores=[]),'<isolated velocity calculations>','exec'),ns)
    return ns


def rows(ns):
    result=[];day=date(2026,9,25)
    while len(result)<ns['BASELINE_DAYS']+ns['LOOKBACK_DAYS']:
        if day.weekday()<5:result.append({'date':day.isoformat(),'volume':100,'close':100})
        day-=timedelta(days=1)
    return result[::-1]


class Tests(unittest.TestCase):
    def test_whole_original_matches_actual_captured_source(self):
        baseline=json.loads((ROOT/'docs/audit/2026-09-28/momentum-leaders-original-baseline.json').read_bytes())['source_checks']['justhodl-velocity-acceleration']
        self.assertEqual(len(RAW),baseline['bytes']);self.assertEqual(hashlib.sha256(RAW).hexdigest(),baseline['sha256'])
    def test_real_zero_volume_disappears_from_baseline_denominator(self):
        ns=original();data=rows(ns);data[0]['volume']=0
        self.assertEqual(ns['compute_vol_baseline'](data),100)
        expected=100*(ns['BASELINE_DAYS']-1)/ns['BASELINE_DAYS']
        self.assertNotEqual(ns['compute_vol_baseline'](data),expected)
    def test_latest_zero_or_missing_volume_borrows_previous_current_ratio(self):
        ns=original()
        for missing in (0,None):
            data=rows(ns);data[-2]['volume']=500;data[-1]['volume']=missing
            out=ns['compute_acceleration'](data);self.assertEqual(out['current_ratio'],5)
            self.assertEqual(out['n_sessions_used'],ns['LOOKBACK_DAYS']-1)
    def test_initial_recent_return_is_fabricated_zero_despite_prior_close(self):
        ns=original();data=rows(ns);b=ns['BASELINE_DAYS'];data[b-1]['close']=50
        out=ns['compute_acceleration'](data)
        self.assertEqual(out['accum_ratio'],0) # Actual first recent close return is +100%.
    def test_duplicate_and_future_dates_do_not_stop_session_based_scores(self):
        ns=original();data=rows(ns)
        a=ns['compute_acceleration'](data)
        for r in data:r['date']='2099-01-01'
        self.assertEqual(ns['compute_acceleration'](data),a)
        self.assertEqual(a['n_sessions_used'],ns['LOOKBACK_DAYS'])
    def test_baseline_report_truncates_the_actual_calculation_denominator(self):
        ns=original();data=rows(ns);data[0]['volume']=101
        actual=ns['compute_vol_baseline'](data);out=ns['compute_acceleration'](data)
        self.assertGreater(actual,100);self.assertEqual(out['baseline_volume'],100)
    def test_boolean_price_and_volume_are_accepted_as_observations(self):
        ns=original();data=rows(ns);data[-1].update(close=True,volume=True)
        out=ns['compute_acceleration'](data)
        self.assertEqual(out['n_sessions_used'],ns['LOOKBACK_DAYS']);self.assertEqual(out['current_ratio'],.01)


if __name__=='__main__':unittest.main(verbosity=2)
