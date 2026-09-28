"""Extracted predecessor calculations and synthetic provider bytes only."""
from pathlib import Path
from unittest.mock import patch
from io import BytesIO
import ast,json,math,sys,unittest,urllib.request
ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops/staged','aws/ops','aws/ops/checks')]
import ops_6261_volatility_squeeze_original_baseline as op
SOURCE=ROOT/'tests/fixtures/pre-volatility-volatility-squeeze-hunter.py.txt'
TREE=ast.parse(SOURCE.read_bytes())

def extracted(name,ns):
    node=next(n for n in TREE.body if isinstance(n,ast.FunctionDef) and n.name==name)
    exec(compile(ast.Module(body=[node],type_ignores=[]),'<isolated original volatility function>','exec'),ns)
    return ns[name]

def history(count=300):
    return [{'date':'2030-01-01','symbol':'OTHER','currency':'JPY','close':100,
        'high':101,'low':99,'volume':1000000} for _ in range(count)]

def compute(rows):return extracted('compute_squeeze_signals',{'math':math})('TEST',rows)

class Tests(unittest.TestCase):
    def test_only_declared_nonsecret_acquisition_controls_are_reported(self):
        class Lambda:
            def get_function_configuration(self,**kw):
                return {'Environment':{'Variables':{'MAX_TICKERS':'1500','N_WORKERS':'12','TIMEOUT_BUDGET_S':'260','SECRET':'never-return'}}}
        value=op.producer_settings(Lambda());self.assertEqual(value['request_limits']['MAX_TICKERS'],1500)
        self.assertNotIn('never-return',json.dumps(value));self.assertEqual(set(value['request_limits']),{'MAX_TICKERS','N_WORKERS','TIMEOUT_BUDGET_S'})
    def test_whole_producer_and_six_consumers_pinned_without_outputs(self):
        self.assertEqual(len(op.SOURCES),7);self.assertEqual(op.KEYS,('data/volatility-squeeze.json',))
        for fn in op.SOURCES:
            raw=(ROOT/'tests/fixtures'/('pre-volatility-'+fn.removeprefix('justhodl-')+'.py.txt')).read_bytes()
            self.assertFalse(op.source_check(fn,raw)['imported_or_executed'])
        class Denied:
            def get_object(self,**kw):raise AssertionError('No consumer output or state may be read')
        for key in ('data/compound-signals.json','data/katlin.json','data/compound-history.json','data/trade-tickets.json'):
            with self.assertRaises(ValueError):op.capture(Denied(),key)
        self.assertNotIn('lambda_function',sys.modules)
    def test_repeated_future_wrong_issuer_rows_produce_ranked_squeeze(self):
        out=compute(history());self.assertEqual(out['symbol'],'TEST');self.assertEqual(out['tier'],'TIER_B_BUILDING')
        self.assertEqual(out['n_signals_firing'],3);self.assertEqual(out['score'],56)
    def test_latest_close_is_omitted_from_bollinger_percentile(self):
        rows=[dict(r,close=100+(i%11)*.1) for i,r in enumerate(history())]
        before=compute(rows);rows[-1].update(close=130,high=131,low=129);after=compute(rows)
        self.assertNotEqual(before['metrics']['today_close'],after['metrics']['today_close'])
        self.assertEqual(before['metrics']['bb_width_percentile'],after['metrics']['bb_width_percentile'])
    def test_tied_ranges_become_thirty_events_and_constant_zero_variance_ranks_highest(self):
        m=compute(history())['metrics'];self.assertEqual(m['nr7_count_30d'],30)
        self.assertEqual(m['inside_day_pct_30d'],100);self.assertEqual(m['bb_width_percentile'],100)
    def test_inverted_ohlc_becomes_negative_drawdown_without_rejection(self):
        out=compute([dict(r,high=90,low=110) for r in history()])
        self.assertEqual(out['metrics']['dd_3m'],-22.2);self.assertEqual(out['tier'],'WATCH')
    def test_boolean_prices_and_unidentified_currency_become_dollar_liquidity(self):
        out=compute([dict(r,close=True,high=True,low=True,volume=10000000) for r in history()])
        self.assertEqual(out['metrics']['today_close'],1);self.assertEqual(out['metrics']['avg_dollar_vol_30d'],10000000)
    def test_missing_ohlc_and_volume_are_fabricated_and_full_source_is_truncated(self):
        rows=[{'date':'not-a-date','symbol':'OTHER','close':True,'volume':None,'unknown':'retain me'} for _ in range(340)]
        fetch=extracted('fetch_history',{'urllib':urllib,'json':json,'FMP_KEY':'synthetic-fixture'})
        with patch.object(urllib.request,'urlopen',return_value=BytesIO(json.dumps(rows).encode())):
            out=fetch('TEST')
        self.assertEqual(len(out),300);self.assertEqual(out[0],{'date':'not-a-date','close':1.0,'high':1.0,'low':1.0,'volume':0.0})
    def test_failed_provider_and_malformed_shape_are_indistinguishable(self):
        fetch=extracted('fetch_history',{'urllib':urllib,'json':json,'FMP_KEY':'synthetic-fixture'})
        with patch.object(urllib.request,'urlopen',side_effect=RuntimeError('synthetic transport failure')):self.assertIsNone(fetch('TEST'))
        with patch.object(urllib.request,'urlopen',return_value=BytesIO(b'{"error":"denied"}')):self.assertIsNone(fetch('TEST'))
    def test_unavailable_year_window_becomes_zero_and_base_count_is_off_by_one(self):
        short=compute(history(220));self.assertEqual(short['metrics']['dd_12m'],0)
        self.assertEqual(compute(history())['metrics']['base_days'],199)

if __name__=='__main__':unittest.main(verbosity=2)
