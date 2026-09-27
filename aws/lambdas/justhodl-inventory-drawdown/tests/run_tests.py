"""Actual helper/entrypoint with synthetic sources; no AWS or provider calls."""
from pathlib import Path
from datetime import datetime, timezone, date
from types import SimpleNamespace
from concurrent.futures import ThreadPoolExecutor, as_completed
from copy import deepcopy
import ast
import io
import json
import math
import sys
import time
import unittest

ROOT = Path(__file__).resolve().parents[4]
SOURCE = ROOT/'aws/lambdas/justhodl-inventory-drawdown/source'
sys.path.insert(0, str(SOURCE))
from inventory_measurements import CONTRACT, decode, sector, stock, universe, shift, number


def received(value): return {'status': 'received', 'response': value}


def months():
    return {'observations': [{'date': shift(date(2026, 8, 1), -i).isoformat(), 'value': '2'} for i in range(72)]}


def quarters():
    return [{'symbol': 'TEST', 'cik': '00001', 'date': end, 'startDate': start, 'period': 'Q2',
             'reportedCurrency': 'USD', 'daysOfInventoryOutstanding': value, 'revenuePerShare': 50 if value == 20 else 25}
            for end, start, value in [('2026-06-30','2026-04-01',20), ('2025-06-30','2025-04-01',40)]]


def calculate(rows=None): return stock('TEST', received(quarters() if rows is None else rows), {}, '2026-09-27')


def nodes(path, wanted, namespace):
    tree = ast.parse(path.read_bytes())
    selected = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in wanted]
    exec(compile(ast.Module(body=selected, type_ignores=[]), '<isolated actual source>', 'exec'), namespace)
    return namespace


def native(**extra):
    ns = {'CONTRACT': CONTRACT, 'decode': decode, 'sector': sector, 'stock': stock, 'universe': universe,
          'datetime': datetime, 'timezone': timezone, 'json': json, 'time': time, 'ThreadPoolExecutor': ThreadPoolExecutor,
          'as_completed': as_completed, 'FRED_SECTORS': {'ISRATIO': ('Total', None)}, 'FRED_BASE': 'https://fixture.invalid/fred',
          'FRED_KEY': 'fixture', 'FMP': 'fixture', 'VERSION': '1.1.0', 'BUCKET': 'fixture-only',
          'OUT_KEY': 'data/inventory-drawdown.json', **extra}
    return nodes(SOURCE/'lambda_function.py', {'lambda_handler', '_research_read', '_research_get'}, ns)


class Tests(unittest.TestCase):
    def test_constant_sixty_month_midrank_is_fifty_not_zero(self):
        out = sector('ISRATIO','Total',None,received(months()),'2026-09-27')
        self.assertEqual(out['percentile_5y'],50); self.assertEqual(out['percentile_sample_n'],60)
        self.assertEqual(out['ratio_changes_pct']['6m']['value'],0)
        self.assertEqual(out['as_of'],'2026-08-01'); self.assertEqual(out['observation_age_days'],57)
        self.assertIsNone(out['drawdown_score']); self.assertIsNone(out['chg_6m'])

    def test_latest_missing_never_skipped_and_whole_original_retained(self):
        raw=months();raw['observations'][0]['value']='.'
        out=sector('ISRATIO','Total',None,received(raw),'2026-09-27')
        self.assertIsNone(out['latest_ratio']);self.assertIsNone(out['ratio_changes_pct']['3m']['value'])
        self.assertEqual(out['acquisition']['response'],raw);self.assertEqual(len(out['observations']),72)
        self.assertEqual(out['as_of'],'2026-08-01');self.assertIsNone(out['percentile_5y'])

    def test_exact_month_pair_not_array_offset_and_zero_is_real(self):
        raw=months();raw['observations'][0]['value']='0';del raw['observations'][6]
        out=sector('ISRATIO','Total',None,received(raw),'2026-09-27')
        self.assertEqual(out['latest_ratio'],0);self.assertEqual(out['ratio_changes_pct']['3m']['value'],-100)
        self.assertIsNone(out['ratio_changes_pct']['6m']['value']);self.assertIsNone(out['percentile_5y'])
        raw['observations'].reverse()
        self.assertEqual(sector('ISRATIO','Total',None,received(raw),'2026-09-27')['ratio_changes_pct']['3m']['value'],-100)

    def test_duplicate_invalid_future_and_boolean_sector_observations(self):
        for field,value in [('date','2026-02-30'),('date','2027-01-01'),('date','2026-08-02'),('value',True)]:
            raw=months();raw['observations'][0][field]=value
            self.assertIsNone(sector('ISRATIO','Total',None,received(raw),'2026-09-27')['latest_ratio'])
        raw=months();raw['observations'].append(raw['observations'][0])
        self.assertEqual(sector('ISRATIO','Total',None,received(raw),'2026-09-27')['status'],'duplicate_observation_month')

    def test_exact_annual_pair_and_per_share_not_total_revenue(self):
        out=calculate();self.assertEqual(out['dio_chg_pct'],-50);self.assertEqual(out['dio_change_days'],-20)
        self.assertEqual(out['revenue_per_share_change_pct'],100);self.assertIsNone(out['rev_growth_yoy'])
        self.assertEqual(out['comparison']['prior_date'],'2025-06-30');self.assertFalse(out['forecast_qualified'])
        self.assertIsNone(out['classification']);self.assertEqual(calculate(list(reversed(quarters())))['dio_chg_pct'],-50)

    def test_repeated_same_date_and_missing_duration_cannot_be_yoy(self):
        same=[dict(quarters()[0],daysOfInventoryOutstanding=20+i*5) for i in range(5)]
        self.assertIsNone(calculate(same)['dio_chg_pct']);self.assertEqual(len(calculate(same)['observations']),5)
        rows=quarters();del rows[0]['startDate'];self.assertIsNone(calculate(rows)['dio_chg_pct'])
        self.assertEqual(calculate(rows)['dio_latest'],20)

    def test_zero_boolean_missing_and_nonfinite_dio(self):
        rows=quarters();rows[0]['daysOfInventoryOutstanding']=0
        self.assertEqual(calculate(rows)['dio_chg_pct'],-100);self.assertEqual(calculate(rows)['dio_latest'],0)
        for value in (None,False,'20',-1,float('inf'),float('nan')):
            rows=quarters();rows[0]['daysOfInventoryOutstanding']=value
            out=calculate(rows);self.assertIsNone(out['dio_latest']);self.assertIsNone(out['dio_chg_pct'])
            self.assertEqual(out['as_of'],'2026-06-30')

    def test_unmatched_issuer_quarter_future_and_currency_abstain(self):
        for field,value in [('cik','2'),('period','Q1'),('symbol','OTHER'),('startDate','2025-04-02'),('date','2027-06-30')]:
            rows=quarters();rows[1][field]=value;self.assertIsNone(calculate(rows)['dio_chg_pct'])
        rows=quarters();rows[1]['reportedCurrency']='JPY';self.assertIsNone(calculate(rows)['revenue_per_share_change_pct'])

    def test_zero_prior_or_overflow_cannot_create_infinite_comparison(self):
        rows=quarters();rows[1]['daysOfInventoryOutstanding']=0;self.assertIsNone(calculate(rows)['dio_chg_pct'])
        rows=quarters();rows[0]['daysOfInventoryOutstanding']=1e308;rows[1]['daysOfInventoryOutstanding']=1e-308
        self.assertIsNone(calculate(rows)['dio_chg_pct']);json.dumps(calculate(rows),allow_nan=False)

    def test_full_universe_occurrences_deterministic_cap_without_new_queries(self):
        source={'data/bottleneck-boom.json':{'ranks':[{'ticker':f'A{i}'} for i in range(140)]},
                'data/chokepoint.json':{'all_chokepoints':[{'ticker':'A0'},{'ticker':'BAD&KEY'},None]}}
        out=universe(source);self.assertEqual(len(out['requested']),130);self.assertEqual(len(out['not_attempted']),10)
        self.assertEqual(len(out['occurrences']),143);self.assertEqual(out,universe(deepcopy(source)))
        with self.assertRaises(ValueError):universe({'data/bottleneck-boom.json':{'ranks':{}}})

    def test_native_retains_more_than_forty_names_and_does_not_log_forecasts(self):
        writes=[];requests=[];reads=[]
        source={'ranks':[{'ticker':f'A{i}'} for i in range(135)]}
        def read(**kw):
            reads.append(kw['Key']);return {'Body':io.BytesIO(json.dumps(source if 'bottleneck' in kw['Key'] else {}).encode())}
        def acquire(url):
            requests.append(url)
            if '/fred/' in url:return received(months())
            return received([])
        ns=native(S3=SimpleNamespace(get_object=read,put_object=lambda **kw:writes.append(kw)))
        ns['_research_get']=acquire;result=ns['lambda_handler']()
        self.assertEqual(result['statusCode'],200);self.assertEqual(len(requests),131);self.assertEqual(len(reads),3)
        packet=json.loads(writes[0]['Body']);self.assertEqual(len(packet['stock_drawdown_board']),130)
        self.assertEqual(len(packet['universe_plan']['not_attempted']),5);self.assertEqual(packet['signals_logged'],0)
        self.assertEqual(packet['boom_setups'],[]);self.assertFalse(packet['sizing_eligible'])

    def test_failed_or_empty_acquisition_preserves_previous_publication(self):
        writes=[]
        ns=native(S3=SimpleNamespace(get_object=lambda **kw:{'Body':io.BytesIO(b'{}')},put_object=lambda **kw:writes.append(kw)))
        ns['_research_get']=lambda url:{'status':'unavailable','response':None}
        self.assertEqual(ns['lambda_handler']()['statusCode'],503);self.assertEqual(writes,[])

    def test_denied_malformed_and_duplicate_source_reads_abort(self):
        for raw in (b'{bad',b'[]',b'{"ranks":[],"ranks":[]}'):
            ns=native(S3=SimpleNamespace(get_object=lambda **kw:{'Body':io.BytesIO(raw)}))
            with self.assertRaises(ValueError):ns['_research_read']('fixture')
        class Denied(Exception):response={'Error':{'Code':'AccessDenied'}}
        def denied(**kw):raise Denied()
        with self.assertRaises(Denied):native(S3=SimpleNamespace(get_object=denied))['_research_read']('fixture')

    def test_strict_json_and_safe_acquisition_error(self):
        for raw in (b'{"a":1,"a":2}',b'NaN',b'1e999',b'1e-999',b'"\xff"'):
            with self.assertRaises((ValueError,UnicodeError)):decode(raw)
        def fail(*args,**kw):raise ValueError('https://fixture.invalid?apikey=DO_NOT_PUBLISH')
        out=native(urllib=SimpleNamespace(request=SimpleNamespace(urlopen=fail)))['_research_get']('synthetic')
        self.assertNotIn('DO_NOT_PUBLISH',json.dumps(out));self.assertEqual(out['error_type'],'ValueError')

    def test_existing_consumers_cannot_promote_ratio_changes_to_shortage(self):
        packet={'sector_drawdown':[sector('ISRATIO','Total','XLI',received(months()),'2026-09-27')],
                'boom_setups':[],'stock_drawdown_board':[calculate()],'generated_at':'2026-09-27T00:00:00Z'}
        fn=ROOT/'aws/lambdas/justhodl-bottleneck-boom/source/lambda_function.py'
        ns=nodes(fn,{'inventory_signal'},{'json':json,'S3':SimpleNamespace(get_object=lambda **kw:{'Body':io.BytesIO(json.dumps(packet).encode())}),'BUCKET':'fixture-only'})
        result=ns['inventory_signal']();self.assertFalse(result['inventory_tightening']);self.assertEqual(result['pre_shortage_names'],[])
        fn=ROOT/'aws/lambdas/justhodl-khalid/source/discovery.py'
        ns=nodes(fn,{'_read_inventory'},{});records={};ns['_read_inventory'](records,packet);self.assertEqual(records,{})
        # Isolate the original scarcity consumer's inventory loops, never its logger.
        tree=ast.parse((ROOT/'aws/lambdas/justhodl-scarcity-radar/source/lambda_function.py').read_bytes())
        handler=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='lambda_handler')
        loops=[n for n in handler.body if isinstance(n,ast.For) and 'idr.get' in ast.unparse(n.iter)]
        captured=[];env={'idr':packet,'inv_sector_tight':{},'clamp':lambda n:max(0,min(100,n)),
                         'rec':lambda tk:captured.append(tk)}
        exec(compile(ast.Module(body=loops,type_ignores=[]),'<isolated scarcity inventory loops>','exec'),env)
        self.assertEqual(captured,[]);self.assertTrue(all(v==0 for v in env['inv_sector_tight'].values()))


if __name__=='__main__':unittest.main(verbosity=2)
