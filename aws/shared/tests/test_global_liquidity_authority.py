"""Actual downstream consumers must not score unqualified liquidity context."""
import ast,json,subprocess,textwrap,time,unittest
from datetime import datetime,timezone
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/shared'),str(Path(__file__).parent)]
from test_inflection_authority import functions


def source(engine):return (ROOT/f'aws/lambdas/justhodl-{engine}/source/lambda_function.py').read_text(encoding='utf-8')
def block(engine,start,end,scope):
    text=source(engine).split(start,1)[1].split(end,1)[0]
    exec(compile(textwrap.dedent(text),engine+' production block','exec'),scope)
    return scope


class Tests(unittest.TestCase):
    def test_allocator_preserves_existing_scores_when_unqualified(self):
        packet={'calls_eligible':False,'regime':'EXPANDING','global_impulse_13w_pct':99}
        def forbidden(*a):raise AssertionError('Unqualified research tried to score')
        scope=functions('allocator',{'rule_global_liquidity'},{'fs3':lambda key:packet if 'global-liquidity' in key else {},'add':forbidden})
        scores={'SPY':3};evidence=[];scope['rule_global_liquidity'](scores,evidence)
        self.assertEqual(scores,{'SPY':3});self.assertEqual(evidence,[])

    def test_crisis_and_quantum_abstain_before_defaults_or_recursive_number_search(self):
        for packet in ({},{'regime':'UNQUALIFIED'},{'calls_eligible':False,'regime':'EXPANDING','nested':{'change_13w':100}}):
            scope=functions('crisis-composite',{'comp_liquidity'},{})
            self.assertIsNone(scope['comp_liquidity'](packet))
            scope=functions('quantum-desk',{'leg_plumbing'},{})
            self.assertEqual(scope['leg_plumbing'](packet,'SPY'),(None,None))

    def test_risk_overlay_cannot_be_activated_by_unqualified_feed_alone(self):
        packet={'calls_eligible':False,'regime':'CONTRACTING'}
        scope={'_read':lambda key:packet if key=='data/global-liquidity.json' else {},'score':40,'posture':{'size_mult':1},'all_tells':[]}
        out=block('risk-regime','    # ── liquidity overlay (wires the liquidity cluster; core RORO driver, confirmation not core score) ──\n',
                  '    # ── capital-inflows overlay',scope)
        self.assertIsNone(out['liquidity']);self.assertNotIn('global',out['lr']);self.assertEqual(out['posture']['size_mult'],1)

    def test_cycle_direction_modifier_cannot_use_legacy_scalar(self):
        sys.path.insert(0,str(ROOT/'tests'))
        from cycle_native_test_support import synthesis_with
        out=synthesis_with('data/global-liquidity.json',{'calls_eligible':True,'score':99,'regime':'RISK-ON','global_impulse_13w_pct':-50})
        self.assertIsNone(out['risk']['squeeze_risk']);self.assertIsNone(out['synthesis']['score'])
        self.assertEqual(out['dependency_graph']['independent_investment_votes'],0)

    def test_cb_stock_context_is_abstention_not_a_neutral_or_directional_vote(self):
        for packet in ({'global_injection_impulse':99},{'calls_eligible':False,'global_injection_impulse':-99},
                       {'calls_eligible':True,'global_injection_impulse':float('nan')},
                       {'calls_eligible':True,'global_injection_impulse':True}):
            scope={'_read':lambda key:packet if key=='data/cb-injection.json' else {},'score':40,'posture':{'size_mult':1},'all_tells':[]}
            out=block('risk-regime','    # ── liquidity overlay (wires the liquidity cluster; core RORO driver, confirmation not core score) ──\n',
                      '    # ── capital-inflows overlay',scope)
            self.assertNotIn('cb_injection',out['lr']);self.assertIsNone(out['liquidity'])
            self.assertEqual(out['posture']['size_mult'],1)

    def test_watchlist_fusion_does_not_call_abstention_an_opposing_regime(self):
        saved={}
        class Storage:
            def put_object(self,**kw):saved[kw['Key']]=json.loads(kw['Body'])
        index={'engines':[{'state':'ACTIVE','theme':'LIQUIDITY','activation_pctile':90,'firing':False}]}
        scope=functions('wl-fusion',{'lambda_handler','dig'},{'time':time,'datetime':datetime,'timezone':timezone,'json':json,
            'S3':Storage(),'BUCKET':'fixture','OUT_KEY':'data/wl-fusion.json',
            'PLATFORM':{'LIQUIDITY':[('data/global-liquidity.json',('regime','global_impulse_13w_pct'))]},
            'gj':lambda key:index if key=='data/wl-engines.json' else {'calls_eligible':False,'regime':'UNQUALIFIED'}})
        scope['lambda_handler']({},None);self.assertEqual(saved['data/wl-fusion.json']['divergences'],[])

    def test_liquidity_agent_never_adds_china_money_supply_as_pboc_assets(self):
        # The native migration removed build_part4. Exercise the actual retained
        # context boundary in an isolated module environment, with synthetic S3.
        code='''
import sys,json
sys.path.insert(0,sys.argv[1])
from test_agent_native import setup,store
client,inputs=setup();read=store.reader(client,'fixture')
before=store.compile_output(inputs,read)
china={'china_m2_usd_bn':999999,'credit_impulse':99,'calls_eligible':True}
raw=json.dumps(china).encode();key='data/china-liquidity.json'
inputs['contexts'][key]['original']=store.private_bytes(client,'fixture',raw)
after=store.compile_output(inputs,read)
assert after['derived']==before['derived'] and after['series']==before['series']
assert after['contexts'][key]['independent_votes']==0
assert after['contexts'][key]['status']=='retained_unqualified_context'
assert client.objects[inputs['contexts'][key]['original']['key']]==raw
assert after['contexts'][key]['original']['sha256']==inputs['contexts'][key]['original']['sha256']
assert 'china_m2_usd_bn' not in json.dumps(after)
assert after['calls_eligible'] is False and after['sizing_eligible'] is False
'''
        subprocess.run([sys.executable,'-c',code,str(ROOT/'aws/lambdas/justhodl-liquidity-agent/tests')],check=True)


if __name__=='__main__':unittest.main()
