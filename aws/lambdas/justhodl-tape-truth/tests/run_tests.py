"""Actual producer/projection replay, all storage/HTTP/LLM calls intercepted."""
import ast
from copy import deepcopy
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import re
import runpy
import sys
import types
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / 'aws/shared'))
from tape_truth_qualification import CONTRACT, PROJECTION, clock, project_tape
NOW = datetime(2026, 10, 1, 22, tzinfo=timezone.utc)
class Frozen(datetime):
    @classmethod
    def now(cls, tz=None):
        return NOW

def load(engine, legacy=False):
    path = ROOT/'aws/lambdas'/engine/('tests/legacy-before.py.txt' if legacy else 'source/lambda_function.py')
    # A boto client here is a stub, never an SDK instance or credential lookup.
    with patch.dict(sys.modules, {'boto3': types.SimpleNamespace(client=lambda *a, **k: object())}):
        ns = runpy.run_path(str(path))
    ns['build'].__globals__['datetime'] = Frozen
    return ns['build'].__globals__

def ledger():
    return {'rows': {t: {f'2026-01-{d:02d}': {'cvd': 100*d, 'close': 100+d,
        'vol': 10000, 'vwap': 100+d, 'o': 100+d, 'h': 102+d, 'l': 99+d, 'c': 100+d}
        for d in range(1, 7)} for t in ['SPY', 'NVDA']}}

def tape(legacy=False, fail=False, key=True, stamp='2026-10-01T20:00:00Z', dates=None, fallback=False):
    ns=load('justhodl-tape-truth',legacy);ns['WATCH']=['SPY','NVDA'];ns['GEX_SYMS']=['SPY','_SPX']
    cv=ledger()
    if dates:
        cv['rows']['SPY']={d:deepcopy(next(iter(cv['rows']['SPY'].values()))) for d in dates}
    store={ns['CVD_LEDGER']:cv, ns['FINRA_LEDGER']:{'rows':{'SPY':{'2026-01-06':0.25}}}}
    calls=[];writes={}
    ns['_g']=lambda k:deepcopy(store.get(k))
    ns['_put']=lambda k,v:writes.update({k:deepcopy(v)})
    def http(url,headers=None):
        calls.append(url)
        if 'polygon.io' in url:
            if fail:raise RuntimeError('mock refresh failure')
            return json.dumps({'results':[{'o':100,'h':102,'l':99,'c':101,'v':100,'vw':100.5}]*150}).encode()
        if 'finra.org' in url:
            if fail or (fallback and '20261001' in url):raise RuntimeError('mock file absent')
            return b'Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume\n20260930|SPY|25|0|100\n'
        if 'cboe.com' in url:
            return json.dumps({'timestamp':stamp,'data':{'current_price':100,'options':[
                {'option':'SPY261002C00100000','gamma':0.02,'open_interest':300,'volume':10},
                {'option':'SPY261002P00105000','gamma':0.03,'open_interest':100,'volume':5}]}}).encode()
        raise AssertionError('Unexpected request '+url)
    ns['http_raw']=http
    with patch.dict(os.environ,{'POLYGON_API_KEY':'mock' if key else ''},clear=True):out=ns['build']()
    return out,writes,calls

def industry(packet,legacy=False):
    ns=load('justhodl-industry-case',legacy);calls=[];writes={}
    stocks=[{'symbol':t,'name':t+' Inc','industry':'Semis','sector':'Technology','market_cap':1e9,'cap_bucket':'SMALL'} for t in ['SPY','NVDA']+['X'+str(i) for i in range(998)]]
    data={'data/universe.json':{'stocks':stocks},'data/tape-truth.json':packet}
    ns['_g']=lambda k:deepcopy(data.get(k));ns['_put']=lambda k,v:writes.update({k:deepcopy(v)})
    def llm(*args):calls.append(args);return 'mock narrative','mock'
    ns['llm_case']=llm
    with patch.dict(os.environ,{},clear=True):out=ns['build']()
    return out,calls,writes

class QualificationTests(unittest.TestCase):
    def test_actual_build_retained_failure_and_new_publication_do_not_refresh_dates(self):
        old,ow,oc=tape(True,fail=True);new,nw,nc=tape(fail=True)
        self.assertEqual(oc,nc);self.assertEqual(old['status'],'LIVE');self.assertEqual(new['status'],'RESEARCH')
        self.assertEqual(old['symbols']['SPY']['verdict']['call'],'GENUINE_UP')
        s=new['symbols']['SPY'];self.assertIsNone(s['verdict']['call']);self.assertIsNone(s['verdict']['conviction'])
        self.assertEqual(s['observation_clocks']['cvd']['value'],'2026-01-06')
        self.assertEqual(s['qualification']['freshness'],'UNKNOWN');self.assertEqual(new['refresh']['cvd'],'ERROR')
        for k in ['data/providers/tape/cvd-daily.json','data/providers/finra/shortvol.json']:
            self.assertEqual(ow.get(k),nw.get(k))

    def test_original_observations_math_fetch_calls_and_ledger_writes_preserved(self):
        for options in [{},{'fail':True},{'key':False},{'fallback':True}]:
            with self.subTest(options=options):
                old,ow,oc=tape(True,**options);new,nw,nc=tape(**options);self.assertEqual(oc,nc)
                for t,s in old['symbols'].items():
                    n=new['symbols'][t]
                    for leg in ['cvd','short_vol','gex']:
                        if s[leg] is None:self.assertIsNone(n[leg]);continue
                        expected=deepcopy(s[leg]);actual=deepcopy(n[leg]);actual.pop('observation_date',None);actual.pop('source_timestamp',None)
                        if expected.get('status')=='LIVE':expected['status']='OBSERVATIONS'
                        self.assertEqual(actual,expected)
                for key in ow:
                    if key!='data/tape-truth.json':self.assertEqual(ow[key],nw[key])
                self.assertTrue(all(s['verdict']['call'] is None for s in new['symbols'].values()))

    def test_finra_fallback_and_gex_original_clocks_preserved(self):
        d,_,_=tape(fallback=True);s=d['symbols']['SPY']
        self.assertEqual(s['short_vol']['observation_date'],'2026-09-30')
        self.assertEqual(s['gex']['source_timestamp'],'2026-10-01T20:00:00Z')
        self.assertEqual(s['observation_clocks']['gex']['status'],'VALID')

    def test_missing_malformed_naive_future_clocks_are_never_fresh(self):
        vectors=[(None,'MISSING'),('', 'INVALID'),(True,'INVALID'),('2026-02-30T12:00:00Z','INVALID'),('2026-10-01T12:00:00','INVALID'),('2026-10-01T12:00:00+00:60','INVALID'),('2026-10-02T00:00:00Z','FUTURE')]
        for stamp,status in vectors:
            with self.subTest(stamp=stamp):
                d,_,_=tape(stamp=stamp);s=d['symbols']['SPY'];self.assertEqual(s['gex']['source_timestamp'],stamp)
                self.assertEqual(s['observation_clocks']['gex']['status'],status)
                self.assertEqual(s['qualification']['freshness'],'UNKNOWN');self.assertIsNone(s['verdict']['call'])
        for value,status in [('2026-02-30','INVALID'),('unknown','INVALID'),('2026-10-03','FUTURE')]:
            d,_,_=tape(fail=True,dates=[value]);s=d['symbols']['SPY']
            self.assertEqual(s['cvd']['series'][0]['d'],value);self.assertEqual(s['observation_clocks']['cvd']['status'],status)

    def test_actual_industry_projection_preserves_observations_and_unrelated_output_and_llm(self):
        source,_,_=tape(fail=True);old,calls,_=industry(source,True);new,ncalls,_=industry(source)
        self.assertEqual(calls,ncalls);self.assertFalse(any('tape' in args[2] for args in ncalls))
        self.assertEqual(old['industries'],new['industries'])
        for t,c in old['cases'].items():
            a=deepcopy(c);b=deepcopy(new['cases'][t]);a.pop('tape',None);b.pop('tape',None);self.assertEqual(a,b)
        p=new['cases']['SPY']['tape'];self.assertEqual(p['contract'],PROJECTION)
        self.assertEqual(p['source_generated_at'],source['generated_at']);self.assertEqual(p['observations']['cvd'],source['symbols']['SPY']['cvd'])
        self.assertEqual(p['observation_clocks']['cvd']['value'],'2026-01-06');self.assertIsNone(p['call'])

    def test_legacy_unknown_contract_and_malformed_publications_unavailable(self):
        new,_,_=tape();legacy,_,_=tape(True)
        variants=[legacy,{}, {'symbols':[]}, {'symbols':{'SPY':True}}]
        for field,value in [('measurement_contract','unknown'),('generated_at',None),('generated_at','bad'),('generated_at','2027-01-01T00:00:00Z'),('qualification',{})]:
            p=deepcopy(new);p[field]=value;variants.append(p)
        for p in variants:
            projection=project_tape(p,'SPY',NOW);self.assertEqual(projection['availability'],'UNAVAILABLE');self.assertIsNone(projection['observations']);self.assertIsNone(projection['call'])
        out,_,_=industry(legacy);self.assertEqual(out['cases']['SPY']['tape']['availability'],'UNAVAILABLE')
        for shape in [[],True,'bad',{'symbols':['SPY']}]:industry(shape)

    def test_known_contract_malformed_legs_and_symbol_collections_are_unavailable(self):
        original,_,_=tape()
        for leg in ['cvd','short_vol','gex']:
            for value in [[],True,'bad',3]:
                d=deepcopy(original);d['symbols']['SPY'][leg]=value
                p=project_tape(d,'SPY',NOW)
                self.assertEqual(p['availability'],'UNAVAILABLE',(leg,value))
                self.assertIsNone(p['observations'])
            d=deepcopy(original);d['symbols']['SPY'][leg]=None
            self.assertEqual(project_tape(d,'SPY',NOW)['availability'],'AVAILABLE')
        d=deepcopy(original);d['symbols']=[d['symbols']['SPY']]
        self.assertEqual(project_tape(d,'0',NOW)['availability'],'UNAVAILABLE')

    def test_old_source_new_projection_and_missing_source_dates(self):
        d,_,_=tape();d['generated_at']='2026-01-06T22:00:00Z';d['symbols']['SPY']['cvd'].pop('last_day');d['symbols']['SPY']['short_vol'].pop('observation_date')
        p=project_tape(d,'SPY',NOW);self.assertEqual(p['source_generated_at'],'2026-01-06T22:00:00Z');self.assertEqual(p['projected_at'],NOW.isoformat())
        self.assertEqual(p['observation_clocks']['cvd']['status'],'MISSING');self.assertEqual(p['observation_clocks']['short_vol']['status'],'MISSING')
        self.assertEqual(p['qualification']['freshness'],'UNKNOWN');self.assertIsNone(p['conviction'])

    def test_frozen_fetch_helpers_llm_prompt_and_why_bus_unchanged(self):
        for engine,names in [('justhodl-tape-truth',['http_raw','completed_session','bar_delta','session_cvd','finra_day','parse_occ','zlast','conviction']),('justhodl-industry-case',['llm_case','tier_of','_g','_put'])]:
            def funcs(path):return {n.name:ast.dump(n,include_attributes=False) for n in ast.parse(path.read_text()).body if isinstance(n,ast.FunctionDef)}
            base=ROOT/'aws/lambdas'/engine;old=funcs(base/'tests/legacy-before.py.txt');new=funcs(base/'source/lambda_function.py')
            for name in names:self.assertEqual(old[name],new[name],name)

    def test_no_strategist_vote_or_causality_scalar(self):
        def pure(path,names,assigns,ns):
            tree=ast.parse((ROOT/path).read_text());nodes=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in names or isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id in assigns for t in n.targets)]
            exec(compile(ast.Module(body=nodes,type_ignores=[]),path,'exec'),ns);return ns
        s=pure('aws/lambdas/justhodl-strategist/source/lambda_function.py',{'classify','scan_text','_verdict_from','_ticks','extract'},{'POS','NEG','NEU','VERDICT_KEYS','TEXT_KEYS','TEXT_POS','TEXT_NEG','SCORE_KEYS','PICK_KEYS','CONTAINERS','_TICK'},{'re':re})
        c=pure('aws/lambdas/justhodl-causality-scanner/source/lambda_function.py',{'extract_scalar'},{'NUMERIC_KEYS'},{'math':math})
        d,_,_=tape();i,_,_=industry(d)
        for p in [d,i]:self.assertIsNone(s['extract'](p));self.assertIsNone(c['extract_scalar'](p))

if __name__=='__main__':
    if '--fixture' in sys.argv:
        d,_,_=tape(fail=True);i,_,_=industry(d);print(json.dumps({'tape':d,'industry':i}))
    else:unittest.main()
