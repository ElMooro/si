"""Synthetic, dependency-free tests. AST loading excludes AWS clients/secrets."""
import ast
import copy
from datetime import datetime, timedelta, timezone
import json
import math
from pathlib import Path
from statistics import mean, stdev
import unittest
import io
import contextlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from types import SimpleNamespace

SOURCE = Path(__file__).resolve().parents[1] / 'source/lambda_function.py'


def load(source=None):
    env = dict(datetime=datetime, timedelta=timedelta, timezone=timezone, math=math, mean=mean, stdev=stdev)
    tree = ast.parse(source if source is not None else SOURCE.read_text(encoding='utf-8'))
    exec(compile(ast.Module(body=[n for n in tree.body if isinstance(n, ast.FunctionDef)], type_ignores=[]), str(SOURCE), 'exec'),env)
    return env


def publication(source=None):
    e=load(source)
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return datetime(2026,9,30,22,tzinfo=timezone.utc)
    writes=[]
    e.update(datetime=Clock,json=json,time=SimpleNamespace(time=lambda:100.0),ThreadPoolExecutor=ThreadPoolExecutor,as_completed=as_completed,
             ALL_ETFS=['SPY','TLT','GLD'],ETF_CATEGORIES={'EQUITY':['SPY'],'BONDS':['TLT'],'GOLD':['GLD']},BUCKET='fixture',KEY='data/etf-flows.json',
             S3=SimpleNamespace(put_object=lambda **kwargs:writes.append(kwargs)))
    def bars(ticker,**kwargs):
        return [{'t':(1750000000+i*86400)*1000,'c':200+i*(1 if ticker!='TLT' else -1),'h':202+i,'l':100+i,
                 'v':(1000+i)*(8 if i==64 and ticker!='GLD' else 1)} for i in range(65)]
    e['fetch_polygon_aggs']=bars;e['fetch_polygon_ticker']=lambda ticker:{'name':ticker,'market_cap':1e9}
    with contextlib.redirect_stdout(io.StringIO()):result=e['lambda_handler']()
    assert len(writes)==1
    return json.loads(writes[0]['Body']),result


def legacy_values(value):
    if isinstance(value,list):return [legacy_values(v) for v in value]
    if isinstance(value,dict):return {k:legacy_values(v) for k,v in value.items() if k not in (
        'price_volume_measurement','measurement_type','measurement_source','actual_fund_flows_measured','signal_definitions')}
    return value


class ProxyTests(unittest.TestCase):
    def test_whole_publication_preserves_baseline_numbers_ranking_and_enums(self):
        baseline=json.loads((Path(__file__).parent/'legacy_proxy_baseline.json').read_text(encoding='utf-8'))
        packet,result=publication()
        self.assertEqual(legacy_values(packet),baseline['packet'])
        self.assertEqual(result,baseline['result'])
        self.assertEqual(packet['measurement_type'],'price_volume_proxy')
        self.assertFalse(packet['actual_fund_flows_measured'])
        self.assertIn('not measured fund inflow',packet['signal_definitions']['HEAVY_INFLOW'])

    def test_measured_zero_and_missing_are_distinct_in_additive_measurement(self):
        f=load()['price_volume_measurement']
        for volume,want in [(0,0),(12,1200),(None,None),(False,None),('0',None),(-1,None),(float('nan'),None)]:
            got=f({'c':100,'v':volume,'t':0})
            self.assertEqual(got['close_times_volume_usd'],want)
            self.assertIsNone(got['net_fund_flow_usd'])
            self.assertFalse(got['actual_fund_flows_measured'])
            self.assertEqual('close_times_volume_usd' in got['missing_fields'],want is None)
            json.dumps(got,allow_nan=False)

    def test_bar_clock_is_not_replaced_with_publication_time(self):
        f=load()['price_volume_measurement']
        self.assertEqual(f({'t':0})['as_of'],'1970-01-01T00:00:00+00:00')
        for stamp in [None,True,'0',float('inf'),10**1000,-10**1000]:
            self.assertIsNone(f({'t':stamp})['as_of'])
        self.assertIsNone(f([])['as_of'])

    def test_analyzer_adds_provenance_without_mutating_bars(self):
        e=load();bars=[{'t':i*86400000,'c':100+i,'h':102+i,'l':98+i,'v':1000+i} for i in range(65)]
        before=copy.deepcopy(bars)
        e['fetch_polygon_aggs']=lambda *a,**k:bars
        e['fetch_polygon_ticker']=lambda *a:{'name':'Synthetic','market_cap':1e9}
        row=e['analyze_etf']('SPY','TEST')
        self.assertEqual(bars,before)
        self.assertEqual(row['price_volume_measurement']['close_times_volume_usd'],164*1064)
        self.assertEqual(row['price_volume_measurement']['measurement_type'],'price_volume_proxy')
        self.assertEqual(row['return_1d_pct'],round(100/163,2))

    def test_legacy_classification_thresholds_remain_compatible(self):
        f=load()['classify_flow_signal']
        for z,ret,activity,want in [(3,1,0,'HEAVY_INFLOW'),(3,-1,0,'HEAVY_OUTFLOW'),(3,0,0,'UNUSUAL_VOL'),(-3,0,0,'UNUSUAL_VOL'),(0,1,26,'ROTATION_IN'),(0,-1,26,'ROTATION_OUT'),(0,0,0,'QUIET'),(None,None,None,'QUIET')]:
            self.assertEqual(f({'dvol_z_score':z,'return_1d_pct':ret,'dvol_5d_vs_20d_pct':activity}),want)

if __name__=='__main__':unittest.main()
