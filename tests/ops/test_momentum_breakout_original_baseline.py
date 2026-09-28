"""Whole predecessor pins and isolated synthetic momentum calculations."""
from pathlib import Path
from unittest.mock import Mock
import ast,json,sys,unittest
ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops/staged','aws/ops','aws/ops/checks')]
import ops_6263_momentum_breakout_original_baseline as op
TREE=ast.parse((ROOT/'tests/fixtures/pre-momentum-momentum-breakout.py.txt').read_bytes())


def extracted(name,env):
    fn=next(n for n in TREE.body if isinstance(n,ast.FunctionDef) and n.name==name)
    exec(compile(ast.Module(body=[fn],type_ignores=[]),'<synthetic predecessor>','exec'),env);return env[name]


def history(count=90):return [{'symbol':'OTHER','currency':'JPY','date':'2030-01-01','close':100,'volume':1000000} for _ in range(count)]


def compute(rows,spy=None):return extracted('compute_signals',{'MIN_DOLLAR_VOL':5000000})('TEST',rows,spy)


class Tests(unittest.TestCase):
    def test_complete_producer_and_seven_consumers_without_output_reads(self):
        self.assertEqual(len(op.SOURCES),8);self.assertEqual(op.KEYS,('data/momentum-breakout.json',))
        for fn in op.SOURCES:
            raw=(ROOT/'tests/fixtures'/('pre-momentum-'+fn.removeprefix('justhodl-')+'.py.txt')).read_bytes();self.assertFalse(op.source_check(fn,raw)['imported_or_executed'])
        class Denied:
            def get_object(self,**kw):raise AssertionError('Consumer outputs forbidden')
        for key in ('data/best-ideas.json','data/compound-signals.json','data/katlin.json','data/private.json'):
            with self.assertRaises(ValueError):op.capture(Denied(),key)
        self.assertNotIn('lambda_function',sys.modules)
    def test_only_explicit_nonsecret_numeric_controls_leave_runner(self):
        class Lambda:
            def get_function_configuration(self,**kw):return {'Environment':{'Variables':{'MAX_TICKERS':'600','MIN_DOLLAR_VOL':'5000000','SECRET':'never-return'}}}
        p=op.producer_settings(Lambda());self.assertEqual(p['request_limits']['MIN_DOLLAR_VOL'],'5000000');self.assertNotIn('never-return',json.dumps(p))
        self.assertEqual(set(p['request_limits']),{'MAX_TICKERS','N_WORKERS','TIMEOUT_BUDGET_S','MIN_DOLLAR_VOL'})
    def test_future_duplicate_wrong_issuer_rows_enter_score(self):
        out=compute(history());self.assertEqual(out['symbol'],'TEST');self.assertEqual(out['score'],20);self.assertIn('AT_60D_HIGH',out['flags'])
    def test_thirty_rows_fabricate_sixty_observation_high(self):
        out=compute(history(30));self.assertIsNone(out['metrics']['ret_60d_pct']);self.assertIn('AT_60D_HIGH',out['flags'])
        self.assertEqual(out['metrics']['pct_from_60d_high'],0)
    def test_near_high_tolerance_can_label_declining_latest_close_new_high(self):
        r=history();r[-1]['close']=99.6;out=compute(r)
        self.assertIn('AT_60D_HIGH',out['flags']);self.assertEqual(out['metrics']['pct_from_60d_high'],-0.4)
    def test_volume_surprise_contains_its_own_numerator(self):
        r=history();r[-1]['volume']=10000000;out=compute(r)
        self.assertEqual(out['metrics']['vol_ratio_today'],6.9);self.assertNotEqual(out['metrics']['vol_ratio_today'],10)
    def test_missing_benchmark_window_is_subtracted_as_zero(self):
        r=history();r[-1]['close']=110
        out=compute(r,{'60d':999});self.assertEqual(out['metrics']['rs_vs_spy_20d_pct'],10)
        self.assertIsNone(compute(r,None)['metrics']['rs_vs_spy_20d_pct'])
    def test_benchmark_comparison_ignores_all_observation_dates(self):
        a=history();a[-1]['close']=110;b=[dict(r,date='1900-01-01') for r in a]
        self.assertEqual(compute(a,{'20d':2,'60d':3}),compute(b,{'20d':2,'60d':3}))
    def test_boolean_prices_become_nominal_dollar_liquidity_and_zero_denominator_crashes(self):
        out=compute([dict(r,close=True,volume=10000000) for r in history()]);self.assertEqual(out['metrics']['avg_dollar_vol_20d'],10000000)
        r=history();r[-6]['close']=0
        with self.assertRaises(ZeroDivisionError):compute(r)
    def test_whole_response_is_truncated_and_missing_volume_filled_with_zero(self):
        received=[{'symbol':'OTHER','date':'not-a-date','close':True,'volume':None,'unknown':'retain'} for _ in range(110)]
        get=Mock(return_value=received);fetch=extracted('fetch_history',{'_http_get_json':get,'FMP_KEY':'synthetic-fixture'})
        out=fetch('TEST');self.assertEqual(len(out),90);self.assertEqual(out[0],{'date':'not-a-date','close':1.0,'volume':0.0})
    def test_transport_denial_and_unexpected_shape_collapse_to_missing_history(self):
        for get in (Mock(side_effect=RuntimeError('synthetic failure')),Mock(return_value={'error':'denied'}),Mock(return_value=[])):
            self.assertIsNone(extracted('fetch_history',{'_http_get_json':get,'FMP_KEY':'synthetic-fixture'})('TEST'))


if __name__=='__main__':unittest.main(verbosity=2)
