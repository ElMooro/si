"""Actual consumer boundaries, with all external calls replaced by memory fixtures."""
import ast
import copy
from datetime import datetime, timedelta, timezone
import hashlib
import importlib.util
from io import BytesIO
import json
from pathlib import Path
import sys
import unittest

ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'tests')]
import cycle_model_context as cycle
import morning_free_research as morning
from calls_free_brief import build as calls_build

NOW=datetime.now(timezone.utc)


def fail(*args,**kwargs):raise AssertionError('External operation forbidden')


def source(engine):return ROOT/'aws/lambdas'/('justhodl-'+engine)/'source/lambda_function.py'


def functions(engine,names,scope=None):
    scope=dict(scope or {})
    nodes=[n for n in ast.parse(source(engine).read_text(encoding='utf-8')).body if isinstance(n,ast.FunctionDef) and n.name in names]
    assert len(nodes)==len(names)
    exec(compile(ast.Module(body=nodes,type_ignores=[]),str(source(engine)),'exec'),scope)
    return scope


def packet():
    return {'generated_at':NOW.isoformat(),'aggregate':{'global_phase':'EXPANSION','global_avg_cli':120},
            'by_country':{'USA':{'phase':'EXPANSION','cli_level':120}},'downturn_probability_6m':.9,
            'global_recession_prob_pct':99,'calls_eligible':True,'sizing_eligible':True}


class Memory:
    def __init__(self,raw):self.raw=raw;self.reads=[];self.bodies=[]
    def get_object(self,**kwargs):
        self.reads.append(kwargs['Key']);body=BytesIO(self.raw);self.bodies.append(body)
        return {'Body':body,'ContentLength':len(self.raw),'ETag':'whole-public-version'}


class CycleBoundaries(unittest.TestCase):
    def test_legacy_new_and_forged_flags_never_grant_votes_or_modify_inputs(self):
        for key in cycle.CONTRACTS:
            for p in (packet(),{**packet(),'contract':cycle.CONTRACTS[key],**dict.fromkeys(cycle.FLAGS,False)},None):
                before=copy.deepcopy(p);view=cycle.decision_view(key,p)
                self.assertEqual(p,before)
                self.assertTrue(all(view[f] is False for f in cycle.FLAGS))
                self.assertIsNone(view['global_recession_prob_pct'])
                self.assertEqual(view['aggregate'],{})
                self.assertEqual(view['research_context']['qualified_investment_votes'],0)
                self.assertEqual(cycle.guard(key,view),view)
                serialized=json.loads(json.dumps(view));serialized['research_context']['parsed_input_sha256']='forged'
                self.assertNotEqual(cycle.guard(key,serialized)['research_context']['parsed_input_sha256'],'forged')
        unrelated={'zero':0};self.assertIs(cycle.guard('data/unrelated.json',unrelated),unrelated)
        with self.assertRaises(ValueError):cycle.decision_view('private/account.json',{})

    def test_identity_covers_whole_parsed_input_without_asserting_original_bytes(self):
        p=packet();p['unselected']={'nested':[0,None,'original']}
        out=cycle.context(cycle.BUSINESS,p,NOW)
        self.assertEqual(out['parsed_input_sha256'],hashlib.sha256(json.dumps(p,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()).hexdigest())
        self.assertTrue(out['publication_within_26h'])
        for stamp in ('bad',None,'2026-01-01T00:00:00', (NOW+timedelta(seconds=1)).isoformat(),(NOW-timedelta(hours=27)).isoformat()):
            self.assertFalse(cycle.context(cycle.BUSINESS,{**p,'generated_at':stamp},NOW)['publication_within_26h'])

    def test_actual_katlin_has_no_cycle_legs_or_changes_to_other_authority(self):
        test_path=ROOT/'aws/lambdas/justhodl-katlin/tests/run_tests.py'
        spec=importlib.util.spec_from_file_location('cycle_katlin_fixture',test_path)
        tests=importlib.util.module_from_spec(spec);spec.loader.exec_module(tests)
        saved={k:sys.modules.get(k) for k in ('boto3','botocore','botocore.exceptions','botocore.config')}
        try:
            mod=tests._load()
            base={'risk_gate':tests._gate(),'khalid_risk':tests._auth(cap=50)}
            before=mod.war_room(base)
            for p in (packet(),{**packet(),**dict.fromkeys(cycle.FLAGS,False)}):
                feeds={**base,'gbc':p,'recession':p};original=copy.deepcopy(feeds)
                out=mod.war_room(feeds);self.assertEqual(feeds,original)
                self.assertEqual(out['legs'],before['legs'])
                self.assertEqual(out['exposure_cap_pct'],before['exposure_cap_pct'])
                self.assertTrue(all(out['cycle'][k] is None for k in ('phase','cli','downturn_prob_6m','recession_prob_pct')))
                self.assertEqual(set(out['cycle']['research_context']),{'gbc','recession'})
        finally:
            for k,v in saved.items():
                if v is None:sys.modules.pop(k,None)
                else:sys.modules[k]=v

    def test_actual_allocator_abstains_without_dropping_the_input(self):
        reads=[]
        scope=functions('allocator',{'rule_global_business_cycle'},{'fs3':lambda key:reads.append(key) or packet(),'add':fail})
        scores={'SPY':7,'TLT':-3};evidence={'SPY':['existing'],'TLT':[]}
        out=scope['rule_global_business_cycle'](scores,evidence)
        self.assertEqual(scores,{'SPY':7,'TLT':-3});self.assertEqual(evidence,{'SPY':['existing'],'TLT':[]})
        self.assertEqual(reads,[cycle.BUSINESS]);self.assertEqual(out['qualified_investment_votes'],0)
        # Execute the actual rule loop, not a copy of its accounting logic.
        tree=ast.parse(source('allocator').read_text(encoding='utf-8'))
        handler=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='lambda_handler')
        loop=next(n for n in handler.body if isinstance(n,ast.For))
        env={'RULES':[('global_business_cycle',scope['rule_global_business_cycle'])], 'scores':scores,'evidence':evidence,'rule_results':{}}
        exec(compile(ast.Module(body=[loop],type_ignores=[]),'<actual-allocator-loop>','exec'),env)
        result=env['rule_results']['global_business_cycle']
        self.assertFalse(result['applied']);self.assertTrue(result['abstained']);self.assertEqual(result['tilt_added'],0)

    def test_actual_morning_metrics_never_promote_cycle_labels(self):
        from tenor_research_model import public_summary
        from research_brief_model import narrative_context
        env={'datetime':datetime,'timezone':timezone,'timedelta':timedelta,'json':json,'_CALIBRATION_AVAILABLE':False,
             'tenor_research_summary':public_summary,'research_brief_context':narrative_context}
        fn=functions('morning-intelligence',{'extract_metrics'},env)['extract_metrics']
        for p in (packet(),cycle.decision_view(cycle.BUSINESS,packet())):
            result=fn({'global_cycle':p}, {})
            self.assertTrue(all(v is None for k,v in result.items() if k.startswith('gbc_') and k!='gbc_research'))
            self.assertEqual(result['gbc_research']['qualified_investment_votes'],0)

    def test_unrelated_functions_and_whole_legacy_calculations_are_preserved(self):
        # Later shipping changes are independently compared against the complete
        # pre-shipping predecessor in shipping_consumer_tests.py.
        changed={'katlin':{'war_room','load_feeds','catalyst_block','_run_handler'},'allocator':{'lambda_handler','rule_global_business_cycle'},
                 'morning-intelligence':{'ai','load_all','extract_metrics','build_brief','format_accuracy'}}
        renamed={'rule_global_business_cycle':'_legacy_rule_global_business_cycle','build_brief':'_legacy_build_brief','format_accuracy':'_legacy_format_accuracy'}
        for engine,allowed in changed.items():
            old=ast.parse((ROOT/'tests/fixtures'/('pre-cycle-qualification-'+engine+'.py.txt')).read_text(encoding='utf-8'))
            new=ast.parse(source(engine).read_text(encoding='utf-8'))
            def group(tree):
                result={}
                for n in tree.body:
                    if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef,ast.ClassDef)):result.setdefault(n.name,[]).append(n)
                return result
            previous,current=group(old),group(new)
            for name,nodes in previous.items():
                if engine=='katlin' and name in ('s3_json','s3_json_quiet'):
                    # The later price-source boundary has its own behavioural
                    # tests. Verify its exact narrow guard, then still compare
                    # every original reader statement instead of exempting it.
                    guarded=copy.deepcopy(current[name][0])
                    expected=ast.parse('if key == "data/volatility-squeeze.json":\n    return _volatility_research_abstention()').body[0]
                    self.assertEqual(ast.dump(guarded.body.pop(0)),ast.dump(expected))
                    self.assertEqual([ast.dump(n) for n in nodes],[ast.dump(guarded)],(engine,name))
                    continue
                if name not in allowed:self.assertEqual([ast.dump(n) for n in nodes],[ast.dump(n) for n in current[name]],(engine,name))
                elif name in renamed:
                    retained=copy.deepcopy(current[renamed[name]][0]);retained.name=name
                    self.assertEqual(ast.dump(nodes[0]),ast.dump(retained),(engine,name))


class MorningBoundaries(unittest.TestCase):
    def public(self):return calls_build(lambda key:{},NOW.isoformat())

    def test_complete_canonical_free_brief_and_clock_are_preserved(self):
        p=self.public();reads=[]
        out=morning.build(lambda k:reads.append(k) or p,NOW)
        self.assertEqual(reads,[morning.PUBLIC_KEY]);self.assertIn(p['brief_md'].rstrip(),out)
        self.assertIn(p['generated_at'],out);self.assertIn('**DECISIVE CALL: WAIT**',out)
        self.assertLessEqual(len(out.encode('utf-16-le'))//2,4096)

    def test_malformed_stale_future_paid_and_actionable_packets_abstain(self):
        base=self.public()
        changes=[{'generated_at':(NOW-timedelta(hours=9)).isoformat()},{'generated_at':(NOW+timedelta(seconds=1)).isoformat()},
                 {'generated_at':NOW.replace(tzinfo=None).isoformat()},{'paid_api_calls':True},{'paid_api_calls':1},
                 {'model':'paid'},{'decision_eligible':True},{'sizing_eligible':None},{'call_verb':'LONG'},
                 {'coverage':{'eligible_votes':False}},{'coverage':None},{'brief_md':'stub'},
                 {'brief_md':base['brief_md'].replace('DECISIVE CALL: WAIT','DECISIVE CALL: LONG')}]
        for change in changes:self.assertEqual(morning.build(lambda k:{**base,**change},NOW),morning.UNAVAILABLE,change)
        self.assertEqual(morning.build(fail,NOW),morning.UNAVAILABLE)

    def test_oversized_delivery_keeps_abstention_and_points_to_whole_brief(self):
        p=self.public();p['brief_md']=p['brief_md'].replace('## DATA TAPE','## DATA TAPE\n'+'x'*3000)
        out=morning.build(lambda k:p,NOW)
        self.assertIn('complete brief exceeds',out);self.assertIn('**DECISIVE CALL: WAIT**',out)
        self.assertLess(len(out),4096)

    def test_strict_whole_public_reader_closes_and_rejects_private_paths(self):
        raw=json.dumps(self.public()).encode();memory=Memory(raw)
        self.assertEqual(morning.read_public(memory,'bucket'),json.loads(raw));self.assertTrue(memory.bodies[-1].closed)
        with self.assertRaises(ValueError):morning.read_public(memory,'bucket','learning/private.json')
        self.assertEqual(memory.reads,[morning.PUBLIC_KEY])
        for bad in (b'{"x":1,"x":2}',b'{"x":NaN}',b'{"x":1e999}',b'\xff',b'{' ,b'x'*(2*1024*1024+1)):
            memory=Memory(bad)
            with self.assertRaises((ValueError,UnicodeError)):morning.read_public(memory,'bucket')
            self.assertTrue(memory.bodies[-1].closed)
        memory=Memory(raw);get=memory.get_object;memory.get_object=lambda **kw:{**get(**kw),'ContentLength':1}
        with self.assertRaises(ValueError):morning.read_public(memory,'bucket')

    def test_publication_clock_rejects_unbounded_or_control_character_forms(self):
        base=self.public()
        for stamp in (NOW.strftime('%Y-%m-%dT%H:%M:%S')+'.'+'0'*5000+'+00:00',
                      NOW.isoformat().replace('T','\n'),NOW.isoformat().replace('T','\u202e')):
            self.assertEqual(morning.build(lambda k:{**base,'generated_at':stamp},NOW),morning.UNAVAILABLE)
        for stamp in (NOW.isoformat(),NOW.isoformat().replace('+00:00','Z'),NOW.replace(microsecond=0).isoformat()):
            self.assertNotEqual(morning.build(lambda k:{**base,'generated_at':stamp},NOW),morning.UNAVAILABLE)

    def test_final_utf16_budget_includes_footer_and_preserves_whole_brief_or_link(self):
        base=self.public()
        original=morning.build(lambda k:base,NOW)
        room=4096-len(original.encode('utf-16-le'))//2
        self.assertGreater(room,0)
        for extra in ('x'*room,'x'*(room-2)+'\U0001f310'):
            packet={**base,'brief_md':base['brief_md'].replace('## DATA TAPE','## DATA TAPE'+extra,1)}
            exact=morning.build(lambda k:packet,NOW)
            self.assertEqual(len(exact.encode('utf-16-le'))//2,4096)
            self.assertIn(packet['brief_md'].rstrip(),exact)
            packet['brief_md']=packet['brief_md'].replace('## DATA TAPE','## DATA TAPE'+'x',1)
            linked=morning.build(lambda k:packet,NOW)
            self.assertIn('complete brief exceeds',linked)
            self.assertIn('**DECISIVE CALL: WAIT**',linked)
            self.assertIn(base['generated_at'],linked)
            self.assertLessEqual(len(linked.encode('utf-16-le'))//2,4096)

    def test_actual_active_builder_and_accuracy_never_call_models_or_private_logs(self):
        memory=Memory(json.dumps(self.public()).encode())
        env=functions('morning-intelligence',{'ai','build_brief','format_accuracy'},
                      {'s3':memory,'S3_BUCKET':'fixture','fs3':fail,'stg':fail,'gp':fail,'urllib':fail})
        self.assertIsNone(env['ai']('do not route'))
        out=env['build_brief']({}, {'picks':['UNQUALIFIED']},{'fake':{'accuracy':1}},None,{}, {})
        self.assertIn('**DECISIVE CALL: WAIT**',out);self.assertNotIn('UNQUALIFIED',out)
        self.assertEqual(memory.reads,[morning.PUBLIC_KEY])
        self.assertIn('Performance qualification: unavailable',env['format_accuracy']({'fake':{'accuracy':1}},{},{}))


def run():
    suite=unittest.TestSuite(unittest.defaultTestLoader.loadTestsFromTestCase(c) for c in (CycleBoundaries,MorningBoundaries))
    if not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful():raise SystemExit(1)


if __name__=='__main__':run()
