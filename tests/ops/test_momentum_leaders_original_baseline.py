"""Synthetic predecessor defects, with no native/provider/consumer invocation."""
from pathlib import Path
from typing import List,Optional
from datetime import datetime,timezone,timedelta
from unittest.mock import patch
from io import BytesIO
import ast,json,sys,unittest,urllib.request
ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops/staged','aws/ops','aws/ops/checks')]
import ops_6265_momentum_leaders_original_baseline as op
TREE=ast.parse((ROOT/'tests/fixtures/pre-momentum-leaders-momentum-leaders.py.txt').read_bytes())


def functions(names,extra=None):
    ns={'List':List,'Optional':Optional,**(extra or {})}
    nodes=[n for n in TREE.body if isinstance(n,ast.FunctionDef) and n.name in names]
    exec(compile(ast.Module(body=nodes,type_ignores=[]),'<synthetic original>','exec'),ns);return ns


def rows(n=90):return [{'symbol':'OTHER','date':'2030-01-01','close':100,'high':100,'open':100,'volume':100} for _ in range(n)]


class Tests(unittest.TestCase):
    def test_complete_producer_nine_consumers_and_only_public_producer_capture(self):
        self.assertEqual(len(op.SOURCES),10);self.assertEqual(op.KEYS,('data/momentum-leaders.json',))
        for fn in op.SOURCES:
            raw=(ROOT/'tests/fixtures'/('pre-momentum-leaders-'+fn.removeprefix('justhodl-')+'.py.txt')).read_bytes()
            self.assertFalse(op.source_check(fn,raw)['imported_or_executed'])
        class Forbidden:
            def get_object(self,**kw):raise AssertionError('No other output read')
        for key in ('data/velocity-acceleration.json','data/theme-cascade.json','private/accounts.json'):
            with self.assertRaises(ValueError):op.capture(Forbidden(),key)
    def test_fifty_observations_become_fifty_two_week_high(self):
        ns=functions(['fifty_two_week_high_proximity','derive_tags'],{'PUMP_CONFIRMED_CUTOFF':70})
        prox=ns['fifty_two_week_high_proximity'](rows(50));self.assertEqual(prox,1)
        self.assertIn('AT_52W_HIGH',ns['derive_tags']({'wk52_proximity':prox},0,False))
    def test_missing_spy_becomes_zero_and_dates_are_never_matched(self):
        r=rows();r[-1]['close']=110
        ns=functions(['perf','fifty_two_week_high_proximity','volume_surge','gap_up_count','compute_momentum_dossier'],{'fetch_price_rows':lambda ticker:r})
        p=ns['compute_momentum_dossier']('TEST',[]);self.assertEqual(p['rs_spy_20d_pct'],10)
        spy=[dict(x,date='1900-01-01',symbol='WRONG') for x in rows()]
        self.assertEqual(ns['compute_momentum_dossier']('TEST',spy)['rs_spy_20d_pct'],10)
    def test_boolean_and_future_duplicate_wrong_issuer_prices_are_accepted(self):
        ns=functions(['perf']);self.assertEqual(ns['perf']([dict(x,close=True) for x in rows()],20),0)
        r=rows();r[-1]['close']=110;self.assertEqual(ns['perf'](r,20),10)
    def test_zero_historical_volume_disappears_from_denominator(self):
        r=rows(21)
        for item in r:item['volume']=0
        r[-2]['volume']=100;r[-1]['volume']=100
        ns=functions(['volume_surge']);self.assertEqual(ns['volume_surge'](r),1)
        self.assertEqual(100/(sum(x['volume'] for x in r[:-1])/20),20)
    def test_missing_gap_inputs_become_observed_zero(self):
        ns=functions(['gap_up_count']);self.assertEqual(ns['gap_up_count']([{'date':'unknown'}]*4),0)
    def test_missing_long_window_is_half_rank_and_still_generates_score(self):
        ns=functions(['percentile_rank','compute_composite_score'])
        d={'perf_20d_pct':0,'perf_60d_pct':None,'rs_spy_20d_pct':None,'wk52_proximity':None,'volume_surge':None}
        result=ns['compute_composite_score'](d,[d]);self.assertEqual(result['perf_60d_rank'],.5);self.assertGreater(result['momentum_score'],40)
    def test_warm_price_cache_returns_prior_body_without_reacquisition(self):
        ns=functions(['fetch_price_rows'],{'PRICE_CACHE':{},'datetime':datetime,'timezone':timezone,'timedelta':timedelta,'LOOKBACK_DAYS':90,'FMP_KEY':'SYNTHETIC_ONLY','urllib':urllib,'json':json})
        with patch.object(urllib.request,'urlopen',return_value=BytesIO(json.dumps(rows()).encode())) as call:
            first=ns['fetch_price_rows']('TEST');second=ns['fetch_price_rows']('TEST')
        self.assertIs(first,second);self.assertEqual(call.call_count,1)
        handler=next(n for n in TREE.body if isinstance(n,ast.FunctionDef) and n.name=='lambda_handler')
        self.assertFalse(any(isinstance(n,ast.Attribute) and n.attr=='clear' and isinstance(n.value,ast.Name) and n.value.id=='PRICE_CACHE' for n in ast.walk(handler)))
    def test_failed_price_read_is_cached_and_error_overwrites_good_head(self):
        ns=functions(['fetch_price_rows'],{'PRICE_CACHE':{},'datetime':datetime,'timezone':timezone,'timedelta':timedelta,'LOOKBACK_DAYS':90,'FMP_KEY':'SYNTHETIC_ONLY','urllib':urllib,'json':json})
        with patch.object(urllib.request,'urlopen',side_effect=OSError('synthetic-unavailable')) as call:
            self.assertEqual(ns['fetch_price_rows']('TEST'),[]);self.assertEqual(ns['fetch_price_rows']('TEST'),[])
        self.assertEqual(call.call_count,1)
        writes=[]
        class Store:
            def put_object(self,**kw):writes.append(kw)
        ns=functions(['_write_error'],{'datetime':datetime,'timezone':timezone,'json':json,'s3':Store(),'S3_BUCKET':'fixture','OUTPUT_KEY':'data/momentum-leaders.json'})
        ns['_write_error']('synthetic-unavailable');self.assertEqual(writes[0]['Key'],'data/momentum-leaders.json');self.assertNotIn('IfMatch',writes[0])


if __name__=='__main__':unittest.main(verbosity=2)
