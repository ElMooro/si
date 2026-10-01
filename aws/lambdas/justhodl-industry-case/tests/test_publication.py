"""Offline producer and governed narrative regressions; no real SDK/providers."""
import ast
from copy import deepcopy
from datetime import datetime, timezone
import importlib.util
import json
import os
from pathlib import Path
import runpy
import sys
import types
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[4]
SOURCE=ROOT/'aws/lambdas/justhodl-industry-case/source'
sys.path[:0]=[str(SOURCE),str(ROOT/'aws/shared')]
NOW=datetime(2026,10,1,13,0,tzinfo=timezone.utc)
class Frozen(datetime):
    @classmethod
    def now(cls,tz=None):return NOW


def load(legacy=False):
    path=SOURCE/'lambda_function.py' if not legacy else SOURCE.parent/'tests/legacy-before.py.txt'
    with patch.dict(sys.modules,{'boto3':types.SimpleNamespace(client=lambda *a,**k:object())}):
        ns=runpy.run_path(str(path))['build'].__globals__
    ns['datetime']=Frozen
    return ns


def fixture():
    stocks=[{'symbol':f'T{i:04}','name':'Name','industry':'Semis' if i<2 else 'Other',
             'sector':'Technology','market_cap':(800e9 if i==0 else 200e9 if i==1 else 1e9),'cap_bucket':'large'} for i in range(1000)]
    return {'data/universe.json':{'generated_at':'2026-08-18T03:00:00Z','stocks':stocks},
            'data/industry-boom.json':{'generated_at':'2026-08-17T22:00:00Z','league':[{'industry':'Semis','boom_score':0,'n':2,'comp':{'inst_net_bps':0,'insider_buys_30d':2}}]},
            'data/earnings.json':{'generated_at':'2026-08-18T01:00:00Z','as_of':'2026-08-18','beat_league':[{'t':'T0000','rank':1,'beat_score':55,'eps_surprise_pct':0}],'growth_calls':{'picks':[]}},
            'data/tape-truth.json':{'generated_at':'2026-08-18T02:00:00Z','symbols':{}},
            'spx-beaters/weekly-closes.json':{'dates':['2025-08-15']*52+['2026-08-14'],'closes':{'T0000':[100]*52+[150],'T0001':[100]*53}}}


def run(data=None,event=None,env=None,context=None):
    eng=load();writes=[];reads=[];data=fixture() if data is None else data
    eng['_g']=lambda k:(reads.append(k),deepcopy(data.get(k)))[1]
    eng['_put']=lambda k,v:writes.append((k,deepcopy(v)))
    with patch.dict(os.environ,env or {},clear=True):out=eng['build'](event,context)
    return out,writes,reads


class PublicationTests(unittest.TestCase):
    def test_old_populated_inputs_preserve_dates_not_freshness(self):
        out,writes,reads=run()
        self.assertEqual(out['generated_at'],NOW.isoformat());self.assertIsNone(out['as_of'])
        self.assertEqual(out['status'],'COMPUTED');self.assertEqual(out['qualification']['freshness'],'UNKNOWN')
        self.assertFalse(out['qualification']['current_eligible'])
        for key,value in fixture().items():
            self.assertEqual(out['sources'][key]['clocks']['generated_at']['value'],value.get('generated_at'))
        self.assertEqual(out['sources']['spx-beaters/weekly-closes.json']['clocks']['dates']['values'],fixture()['spx-beaters/weekly-closes.json']['dates'])
        self.assertEqual(len(reads),5);self.assertEqual([k for k,_ in writes],['data/industry-case.json'])

    def test_measured_arithmetic_matches_actual_predecessor(self):
        old=load(True);data=fixture();old['_g']=lambda k:deepcopy(data.get(k));old['_put']=lambda *a:None
        with patch.dict(os.environ,{},clear=True):before=old['build']({})
        after,_,_=run(data)
        self.assertEqual(before['industries'],after['industries'])
        for key,c in before['cases'].items():
            other=deepcopy(after['cases'][key]);c=deepcopy(c)
            for obj in (c,other):obj.pop('ai_mode',None);obj.pop('ai_case',None)
            self.assertEqual(c,other)
        self.assertEqual(after['industries']['Semis']['hhi'],6800)
        self.assertEqual(after['industries']['Semis']['wtd_ret_12m_pct'],40)

    def test_missing_invalid_future_clock_states(self):
        for value,status in [(None,'MISSING'),(False,'INVALID'),('bad','INVALID'),('2026-02-30T00:00:00Z','INVALID'),('2027-01-01T00:00:00Z','FUTURE')]:
            data=fixture();data['data/universe.json']['generated_at']=value
            out,_,_=run(data);c=out['sources']['data/universe.json']['clocks']['generated_at']
            self.assertEqual(c['value'],value);self.assertEqual(c['status'],status)
            self.assertEqual(out['qualification']['freshness'],'UNKNOWN');self.assertEqual(out['n_cases'],1000)

    def test_missing_primary_and_optional_data_never_live(self):
        for key in fixture():
            data=fixture();data.pop(key)
            out,_,_=run(data)
            self.assertNotEqual(out['status'],'LIVE')
            self.assertEqual(out['sources'][key]['availability'],'UNAVAILABLE')
            if key=='data/universe.json':self.assertEqual(out['status'],'MISSING')
            else:self.assertEqual(out['n_cases'],1000)

    def test_malformed_inputs_preserve_unaffected_measurements(self):
        data=fixture();data['data/industry-boom.json']['league'] += [None,False,{'industry':{}},{'industry':'Other','boom_score':{}}]
        data['data/earnings.json']['beat_league'] += [None,{'t':{},'rank':4}]
        data['data/earnings.json']['growth_calls']={'picks':True}
        data['spx-beaters/weekly-closes.json']['closes']['broken']=True
        data['spx-beaters/weekly-closes.json']['closes']['T0001']=[True]*53
        out,_,_=run(data)
        self.assertEqual(out['status'],'PARTIAL');self.assertEqual(out['cases']['T0000']['ret_12m_pct'],50)
        self.assertIsNone(out['cases']['T0001']['ret_12m_pct'])
        self.assertGreater(out['sources']['data/industry-boom.json']['discarded_rows'],0)
        for value in (True,[], 'bad', {'stocks':True}):
            data=fixture();data['data/universe.json']=value
            out,_,_=run(data);self.assertEqual(out['status'],'MISSING')
            self.assertEqual(out['sources']['data/universe.json']['availability'],'INVALID')

    def test_one_bad_universe_row_does_not_hide_the_valid_cohort(self):
        data=fixture();data['data/universe.json']['stocks'][-1]=None
        out,_,_=run(data)
        self.assertEqual(out['n_cases'],999);self.assertEqual(out['status'],'PARTIAL')
        self.assertEqual(out['industries']['Semis']['hhi'],6800)
        self.assertEqual(out['sources']['data/universe.json']['discarded_rows'],1)

    def test_nonfinite_clocks_and_overflow_do_not_emit_invalid_json(self):
        data=fixture();data['data/universe.json']['generated_at']=float('nan')
        data['spx-beaters/weekly-closes.json']['closes']['T0000']=[1e-300]*52+[1e300]
        out,_,_=run(data);json.dumps(out,allow_nan=False)
        self.assertIsNone(out['cases']['T0000']['ret_12m_pct'])
        c=out['sources']['data/universe.json']['clocks']['generated_at']
        self.assertEqual(c['status'],'INVALID');self.assertEqual(c['value'],{'invalid_number':'nan'})

    def test_unrepresentable_integer_and_share_overflow_preserve_other_cohorts(self):
        data=fixture();data['data/universe.json']['stocks'][-1]['market_cap']=10**1000
        out,_,_=run(data);self.assertEqual(out['n_cases'],999)
        self.assertEqual(out['industries']['Semis']['hhi'],6800);json.dumps(out,allow_nan=False)
        data=fixture();data['data/universe.json']['stocks'][0]['market_cap']=1e307
        out,_,_=run(data);self.assertNotIn('Semis',out['industries'])
        self.assertEqual(out['unavailable_industries'],['Semis']);self.assertEqual(out['n_cases'],998)
        self.assertEqual(out['industries']['Other']['n'],998);self.assertEqual(out['status'],'PARTIAL')
        json.dumps(out,allow_nan=False)

    def test_integer_aggregate_overflow_preserves_other_cohorts(self):
        data=fixture()
        for row in data['data/universe.json']['stocks'][:2]:
            row['market_cap']=10**308
        out,_,_=run(data)
        self.assertEqual(out['unavailable_industries'],['Semis'])
        self.assertEqual(out['industries']['Other']['n'],998)
        json.dumps(out,allow_nan=False)

    def test_mixed_integer_float_aggregate_overflow_preserves_other_cohorts(self):
        data=fixture()
        for row in data['data/universe.json']['stocks'][:2]:
            row['market_cap']=10**308
        data['data/universe.json']['stocks'][2].update(industry='Semis',market_cap=1.0)
        out,_,_=run(data)
        self.assertEqual(out['unavailable_industries'],['Semis'])
        self.assertEqual(out['industries']['Other']['n'],997)
        json.dumps(out,allow_nan=False)

    def test_duplicate_event_is_not_claimed_idempotent_and_cannot_enable_paid_calls(self):
        router=types.SimpleNamespace(complete=lambda *a,**k:(_ for _ in ()).throw(AssertionError('paid call')))
        with patch.dict(sys.modules,{'llm_router':router}):
            a,aw,_=run(event={'id':'duplicate','INDUSTRY_CASE_NARRATIVES':'governed'})
            b,bw,_=run(event={'id':'duplicate','narratives_enabled':True})
        self.assertEqual(a,b);self.assertEqual(len(aw)+len(bw),2)
        self.assertFalse(a['narrative_control']['enabled'])
        self.assertEqual(a['narrative_control']['duplicate_protection'],'BLOCKED_NO_DURABLE_IDEMPOTENCY')

    def llm(self,router=None,cost=None,remaining=100000,deadline=100):
        eng=load();calls=[]
        router=router or types.SimpleNamespace(complete=lambda *a,**k:(calls.append((a,k)) or 'governed narrative'))
        cost=cost or types.SimpleNamespace(mode=lambda:'normal',budget_ok=lambda:True,within_daily_cap=lambda:True)
        ctx=types.SimpleNamespace(get_remaining_time_in_millis=lambda:remaining)
        with patch.dict(sys.modules,{'llm_router':router,'llm_cost':cost}),patch.object(eng['time'],'monotonic',return_value=0):
            out=eng['llm_case'](True,'N'*10000,{'industry':'I'*10000,'rank':'1 of 2','share_pct':80,'boom':None},ctx,deadline)
        return out,calls

    def test_router_only_bounded_bulk_and_no_on_demand_escalation(self):
        (line,mode),calls=self.llm();self.assertEqual(line,'governed narrative')
        args,kw=calls[0];self.assertLessEqual(len(args[0]),1100)
        self.assertEqual(kw,{'tier':'bulk','max_tokens':160,'contains_proprietary':False,'on_demand':False})
        self.assertIn('governed_router',mode)

    def test_denial_errors_and_missing_router_fall_back_without_raw_exception(self):
        for mode in ('off','on_demand','unexpected'):
            cost=types.SimpleNamespace(mode=lambda:mode,budget_ok=lambda:True,within_daily_cap=lambda:True)
            (line,tag),calls=self.llm(cost=cost);self.assertEqual(calls,[]);self.assertIn('admission denied',tag)
        for field in ('budget_ok','within_daily_cap'):
            cost=types.SimpleNamespace(mode=lambda:'normal',budget_ok=lambda:True,within_daily_cap=lambda:True)
            setattr(cost,field,lambda:False)
            (_,tag),calls=self.llm(cost=cost);self.assertEqual(calls,[]);self.assertIn('denied',tag)
        for router in (types.SimpleNamespace(complete=lambda *a,**k:''),types.SimpleNamespace(complete=lambda *a,**k:(_ for _ in ()).throw(RuntimeError('SECRET')))):
            (line,tag),_=self.llm(router=router);self.assertIn('Recorded cohort',line);self.assertNotIn('SECRET',tag)
        eng=load()
        with patch.dict(sys.modules,{'llm_router':None}):
            line,tag=eng['llm_case'](True,'Name',{'industry':'Semis','rank':'1','share_pct':80},types.SimpleNamespace(get_remaining_time_in_millis=lambda:100000),eng['time'].monotonic()+10)
            self.assertIn('rules_only',tag)

    def test_time_budget_denies_optional_calls(self):
        for remaining,deadline in [(59999,100),(None,100),(True,100),(100000,0),(100000,None)]:
            (_,tag),calls=self.llm(remaining=remaining,deadline=deadline)
            self.assertEqual(calls,[]);self.assertIn('time budget',tag)

    def test_six_router_entries_bound_and_duplicate_protection_remains_blocked(self):
        calls=[]
        router=types.SimpleNamespace(complete=lambda *a,**k:(calls.append((a,k)) or 'governed text'))
        cost=types.SimpleNamespace(mode=lambda:'normal',budget_ok=lambda:True,within_daily_cap=lambda:True)
        context=types.SimpleNamespace(get_remaining_time_in_millis=lambda:100000)
        with patch.dict(sys.modules,{'llm_router':router,'llm_cost':cost}):
            a,_,_=run(event={'id':'duplicate'},env={'INDUSTRY_CASE_NARRATIVES':'governed'},context=context)
            self.assertEqual(len(calls),6)
            b,_,_=run(event={'id':'duplicate'},env={'INDUSTRY_CASE_NARRATIVES':'governed'},context=context)
            self.assertEqual(len(calls),12)
        self.assertEqual(a['narrative_control']['duplicate_protection'],'BLOCKED_NO_DURABLE_IDEMPOTENCY')
        self.assertTrue(all(k['max_tokens']==160 for _,k in calls))

    def test_budget_consumed_by_admission_prevents_router_entry(self):
        eng=load();remaining=iter([100000,59000]);calls=[]
        cost=types.SimpleNamespace(mode=lambda:'normal',budget_ok=lambda:True,within_daily_cap=lambda:True)
        router=types.SimpleNamespace(complete=lambda *a,**k:calls.append(a))
        with patch.dict(sys.modules,{'llm_router':router,'llm_cost':cost}):
            _,mode=eng['llm_case'](True,'Name',{'industry':'Semis','rank':'1','share_pct':80},
                types.SimpleNamespace(get_remaining_time_in_millis=lambda:next(remaining)),eng['time'].monotonic()+20)
        self.assertEqual(calls,[]);self.assertIn('time budget exhausted',mode)

    def test_no_provider_http_or_shared_policy_edits_required(self):
        src=(SOURCE/'lambda_function.py').read_text(encoding='utf-8')
        self.assertNotIn('api.anthropic.com',src);self.assertNotIn('urllib',src)
        self.assertNotIn('ANTHROPIC_KEY',src)
        tree=ast.parse(src)
        calls=[n for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute) and n.func.attr=='complete']
        self.assertEqual(len(calls),1)

if __name__=='__main__':
    if '--fixture' in sys.argv:print(json.dumps(run()[0]))
    else:unittest.main()
