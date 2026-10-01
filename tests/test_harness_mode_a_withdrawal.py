"""Actual-function offline replays. Invented fixtures are not performance evidence."""
import ast
import copy
import gzip
import hashlib
import importlib.util
import io
import json
import math
from pathlib import Path
import random
import re
import sys
import types
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
FIX = ROOT / 'tests/fixtures/harness-mode-a'
sys.path.insert(0, str(ROOT / 'aws/shared'))
from backtest_harness_authority import blocked_qualification, harness_context, alpha_context
from meta_labeler_authority import meta_context


def source(engine):
    return ROOT / 'aws/lambdas' / engine / 'source/lambda_function.py'


def functions(path):
    return {n.name: n for n in ast.parse(path.read_text()).body if isinstance(n, ast.FunctionDef)}


def scope(path):
    tree = ast.parse(path.read_text())
    nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef)]
    for n in tree.body:
        if isinstance(n, ast.Assign):
            try:
                ast.literal_eval(n.value)
            except (ValueError, TypeError):
                if not any(isinstance(t, ast.Name) and t.id == 'RULES' for t in n.targets):
                    continue
            nodes.append(n)
    env = dict(json=json, gzip=gzip, math=math, re=re, datetime=datetime,
               timezone=timezone, timedelta=timedelta,
               time=types.SimpleNamespace(time=lambda: 1780000000.0),
               blocked_qualification=blocked_qualification, harness_context=harness_context,
               alpha_context=alpha_context, meta_context=meta_context)
    from backtest_harness_authority import NOTICE
    env['NOTICE'] = NOTICE
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(path), 'exec'), env)
    return env


class MemoryS3:
    def __init__(self, docs):
        self.docs, self.writes, self.reads = docs, {}, []
    def get_object(self, *, Bucket, Key):
        self.reads.append(Key)
        value = self.docs[Key]
        raw = value if isinstance(value, bytes) else json.dumps(value).encode()
        return {'Body': io.BytesIO(raw)}
    def put_object(self, **kw):
        raw = kw['Body']
        if kw.get('ContentEncoding') == 'gzip': raw = gzip.decompress(raw)
        self.writes[kw['Key']] = json.loads(raw)


def harness_replay(legacy=False, rings=None):
    env = scope(FIX / 'harness.py.txt' if legacy else source('justhodl-backtest-harness'))
    dates = [(datetime(2026, 1, 1) + timedelta(days=i)).date().isoformat() for i in range(40)]
    pxc = {'SPY': {d: 100+i for i,d in enumerate(dates)},
           'X': {d: 100+2*i for i,d in enumerate(dates)}}
    signals = [{'signal_id': f'fixture#{i}', 'signal_type':'fixture', 'ticker':'X',
                'baseline_price':100, 'check_windows':['21d'], 'date':dates[0],
                'confidence':.6, 'predicted_direction':direction}
               for i,direction in enumerate(['UP','UP','DOWN'])]
    signals += [{'signal_id':'pending', 'signal_type':'fixture', 'ticker':'X',
                 'baseline_price':100, 'check_windows':['21d'], 'timestamp':1780000000}]
    rings = rings if rings is not None else {'SPY':[100.]*256, 'X':[100+i for i in range(256)]}
    s3 = MemoryS3({env['UP_STATE']:gzip.compress(json.dumps({'rings':rings,'dv':{t:1e7 for t in rings},'last_date':dates[-1]}).encode()),
                   env['STATE_KEY']:{'pxc':pxc}})
    env.update(S3=s3, DDB=types.SimpleNamespace(Table=lambda name: types.SimpleNamespace(scan=lambda **kw:{'Items':signals})))
    def forbidden(*a, **kw): raise AssertionError('provider/secret acquisition attempted')
    env.update(jget=forbidden, FMP_KEY='invented-unused')
    if not legacy:
        env.update({name: forbidden for name in ('feats','collect_trades','stats','expected_max_sr')})
    with patch('builtins.print'):
        env['lambda_handler']()
    return s3.writes, env


def alpha_replay(legacy=False, packet=None):
    env = scope(FIX/'alpha.py.txt' if legacy else source('justhodl-alpha-decay'))
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None): return cls(2026,10,1,tzinfo=timezone.utc)
    env['datetime'] = Clock
    scorecard = {'scorecard':[
        {'signal_type':'bad','alpha_status':'ALPHA_PROVEN','alpha_mean_excess_pct':-1,'hit_rate':.4,'alpha_n':100,'by_regime':{'RISK_ON':{'excess_mean':-1}}},
        {'signal_type':'better','alpha_status':'ALPHA_PROVEN','alpha_mean_excess_pct':4,'hit_rate':.7,'alpha_n':100},
        {'signal_type':'flat','alpha_status':'BUILDING','alpha_mean_excess_pct':0,'hit_rate':.5,'alpha_n':10}]}
    baseline = {'snapshots':[{'date':'2026-09-01','signals':{n:{'excess':1,'hit':.5} for n in ('bad','better','flat')}}]}
    docs = {'data/signal-scorecard.json':scorecard, env['HIST_KEY']:baseline,
            'data/risk-regime.json':{'regime':'RISK_ON'},'data/backtest-harness.json':packet}
    s3 = MemoryS3(docs); env['s3']=s3
    stub = types.SimpleNamespace(decision_view=lambda obj:obj)
    with patch.dict(sys.modules, {'risk_regime_authority':stub}), patch('builtins.print'):
        env['lambda_handler']({},None)
    return s3.writes, s3.reads


class WithdrawalTests(unittest.TestCase):
    def test_frozen_predecessors_and_protected_consumers(self):
        manifest=json.loads((FIX/'manifest.json').read_text())
        for name,meta in manifest['files'].items():
            self.assertEqual(hashlib.sha256((FIX/name).read_bytes()).hexdigest(),meta['sha256'])
        for path,digest in manifest['preserved'].items():
            # Separately authorized meta-labeler correction retains its original bytes.
            if path == 'aws/lambdas/justhodl-meta-labeler/source/lambda_function.py':
                self.assertEqual(hashlib.sha256((ROOT/'tests/fixtures/meta-labeler/legacy-writer.py.txt').read_bytes()).hexdigest(), digest)
                continue
            self.assertEqual(hashlib.sha256((ROOT/path).read_bytes()).hexdigest(),digest,path)
        old,new=functions(FIX/'harness.py.txt'),functions(source('justhodl-backtest-harness'))
        for name in old.keys()-{'mode_a','lambda_handler'}:
            self.assertEqual(ast.dump(old[name]),ast.dump(new[name]),name)

    def test_future_prices_change_legacy_training_and_selected_configuration(self):
        env=scope(FIX/'harness.py.txt');spy=[100.]*256;rng=random.Random(1)
        entries=[]
        env['collect_trades'](lambda f,i,s,c: entries.append(i) or True,{},
                              {'X':{'p':spy}},{},spy,1,110)
        self.assertEqual(entries,[1,22,43,64,85,106])
        self.assertGreaterEqual(entries[-1]+env['HORIZON'],110)
        before={}
        for t in range(15):
            p=[100.]
            for _ in range(255):p.append(p[-1]*math.exp(rng.gauss(-.002,.045)))
            before[t]=p
        after={t:[v if i<110 else v*(4 if t%2 else .2) for i,v in enumerate(p)] for t,p in before.items()}
        rule=env['RULES']['deep_drawdown_buy'];sf=env['feats'](spy,spy)
        def fit(prices):
            uni={t:env['feats'](p,spy) for t,p in prices.items()}
            stats=[env['stats'](env['collect_trades'](rule['fn'],c,uni,sf,spy,1,110)) for c in rule['cfgs']]
            return stats,rule['cfgs'][max(range(len(stats)),key=lambda i:stats[i]['sr'])]
        a,b=fit(before),fit(after)
        self.assertNotEqual(a[0],b[0]);self.assertEqual(a[1],{'x':45});self.assertEqual(b[1],{'x':35})
        new=scope(source('justhodl-backtest-harness'))
        self.assertEqual(new['mode_a'](before,sf,spy),new['mode_a'](after,sf,spy))

    def test_ticker_order_changes_legacy_drawdown_and_pass(self):
        env=scope(FIX/'harness.py.txt');spy=[100.]*149
        def prices(mult):
            p=[100.]*149
            for i in range(2,len(p)):p[i]=p[i-1]*(mult if (i-1)%21==0 else 1)
            return {'p':p}
        rows={f'{kind}{i}':prices(mult) for kind,mult in [('P',1.5),('N',.6)] for i in range(5)}
        mixed={f'{kind}{i}':rows[f'{kind}{i}'] for i in range(5) for kind in ('P','N')}
        values=[env['collect_trades'](lambda *args:True,{},uni,{},spy,1,127) for uni in (rows,mixed)]
        self.assertEqual(sorted(values[0]),sorted(values[1]))
        a,b=map(env['stats'],values)
        self.assertEqual(a['sr'],b['sr']);self.assertEqual(a['maxdd'],-70.6);self.assertEqual(b['maxdd'],-21.7)
        gate=lambda st:st['sr']>env['expected_max_sr'](9,st['n']) and st['n']>=40 and (st['maxdd'] or -99)>=-40
        self.assertFalse(gate(a));self.assertTrue(gate(b))

    def test_legacy_sharpe_units_and_zero_drawdown_counterexamples(self):
        env=scope(FIX/'harness.py.txt');st=env['stats']([-.1]*490+[.1]*510)
        gate=env['expected_max_sr'](9,1000)
        self.assertGreater(st['sr'],gate)
        self.assertLess(st['sr'],gate*math.sqrt(252/21))
        self.assertEqual((0 or -99),-99)
        self.assertEqual(env['stats']([.1]*50)['maxdd'],0)

    def test_actual_ingest_equal_lengths_do_not_prove_aligned_sessions(self):
        path=source('justhodl-upside-radar');fn=functions(path)['ingest']
        env={'time':types.SimpleNamespace(time=lambda:0),'TIME_BUDGET':900,'RING':3,'DV_FLOOR':1,
             'sessions_to_fill':lambda st:['d1','d2','d3','d4'],
             'grouped':lambda d:{'SPY':(100+int(d[1:]),10),**({'A':(200+int(d[1:]),10)} if d!='d3' else {})}}
        exec(compile(ast.Module(body=[fn],type_ignores=[]),str(path),'exec'),env)
        st={'dv':{},'rings':{},'days_seen':0};env['ingest'](st,0)
        self.assertEqual(st['rings'],{'SPY':[102,103,104],'A':[201,202,204]})

    def test_actual_harness_publication_and_mode_b_outputs_preserved(self):
        old,_=harness_replay(True);new,env=harness_replay()
        a,b=old[env['OUT_KEY']],new[env['OUT_KEY']]
        self.assertEqual(a['live_signal_types'],b['live_signal_types'])
        self.assertEqual(b['live_signal_types'][0]['graded'],3)
        for key in (env['STATE_KEY'],'data/_backtest/graded.json.gz'):
            self.assertEqual(old[key],new[key])
        self.assertEqual(b['n_pass'],0);self.assertEqual(b['qualified_rules'],0);self.assertEqual(b['folds'],0)
        self.assertEqual(b['mode_a_qualification'],blocked_qualification())
        for r in b['rules']:
            self.assertIs(r['PASS'],False);self.assertIsNone(r['chosen']);self.assertEqual(r['configs_tried'],0)
            self.assertIsNone(r['deflated_gate_sr'])
            self.assertEqual(r['oos'],{'n':None,'sr':None,'hit':None,'avg':None,'maxdd':None,'curve':[]})
        for rings in ({},{'SPY':[]},{'SPY':[0]*256,'X':[0]*256}):
            result,_=harness_replay(rings=rings)
            self.assertEqual(result[env['OUT_KEY']]['rules'],b['rules'])

    def test_alpha_actual_handler_preserves_signal_health_and_history(self):
        packet={'rules':[{'rule':'fixture','family':'test','PASS':True,'deflated_gate_sr':99}],
                'live_signal_types':{'fixture':'bad'}}
        old,_=alpha_replay(True,packet)
        for value in (packet,None,[],False,0,'PASS',{'generated_at':'2999-01-01T00:00:00Z','n_pass':999}):
            new,reads=alpha_replay(packet=value)
            self.assertNotIn('data/backtest-harness.json',reads)
            self.assertEqual(old['data/alpha-decay-history.json'],new['data/alpha-decay-history.json'])
            a,b=copy.deepcopy(old['data/alpha-decay.json']),copy.deepcopy(new['data/alpha-decay.json'])
            self.assertTrue(a['backtest_vs_live']);self.assertEqual(b['backtest_vs_live'],[])
            self.assertEqual(b['stats']['backtest_pass_archetypes'],0)
            for v in (a,b):
                for k in ('version','thesis','backtest_vs_live','mode_a_qualification'):v.pop(k,None)
                v['stats'].pop('backtest_pass_archetypes')
            self.assertEqual(a,b)
            # Execute Quantum Desk's real extraction: signal-health output unchanged.
            q=scope(source('justhodl-quantum-desk'));left,right={},{}
            q['risk_extras']({'alpha_decay':old['data/alpha-decay.json']},left)
            q['risk_extras']({'alpha_decay':new['data/alpha-decay.json']},right)
            self.assertEqual(left,right);self.assertTrue(left['signal_health']['decayed'])

    def test_ask_desk_actual_reader_masks_direct_and_derived_legacy_claims(self):
        env=scope(source('justhodl-ask-desk'))
        fixtures=[None,[],False,0,'PASS',{},
                  {'n_pass':9,'rules':[{'PASS':True,'chosen':{'n':1},'oos':{'sr':99}}],
                   'methodology':'DEPLOYABLE','generated_at':'2999-01-01T00:00:00Z',
                   'mode_a_qualification':{'contract':'future.v999','status':'VALIDATED','decision_eligible':True}},
                  {'backtest_vs_live':[{'backtest_says':'DEPLOYABLE'}],
                   'stats':{'backtest_pass_archetypes':999},'thesis':'DEPLOYABLE'}]
        for key in ('data/backtest-harness.json','data/alpha-decay.json'):
            for packet in fixtures:
                original=copy.deepcopy(packet);env['S3']=MemoryS3({key:packet})
                raw=env['fetch_slim'](key);d=json.loads(raw)
                self.assertEqual(d['mode_a_status'],'BLOCKED');self.assertEqual(d['qualified_rules'],0)
                self.assertIs(d['mode_a_decision_eligible'],False);self.assertIs(d['validated_strategy'],False)
                self.assertNotIn('DEPLOYABLE',raw);self.assertNotIn('"PASS": true',raw)
                self.assertEqual(packet,original)
        live={'live_signal_types':[{'signal_type':'fixture','graded':0,'pending':1,'avg_excess_pct':0}]}
        self.assertEqual(harness_context(live)['live_signal_types'],live['live_signal_types'])
        env['S3']=MemoryS3({'data/meta-labeler.json':{'gates':[{'verdict':'TAKE'}]}})
        self.assertEqual(json.loads(env['fetch_slim']('data/meta-labeler.json')),env['slim'](meta_context({'gates':[{'verdict':'TAKE'}]})))

    def test_closed_contract_never_accepts_self_promoted_authority(self):
        for status in ('BLOCKED','VALIDATED','PASS',None,0,False):
            for value in (None,0,False,True,'0','999',999):
                d={'mode_a_qualification':dict(blocked_qualification(),status=status,decision_eligible=value),
                   'n_pass':value,'qualified_rules':value,'validated_strategy':value}
                for fn in (harness_context,alpha_context):
                    got=fn(d);self.assertEqual(got['mode_a_qualification'],blocked_qualification())
                    self.assertEqual(got['qualified_rules'],0);self.assertIs(got['validated_strategy'],False)


if __name__ == '__main__': unittest.main()
