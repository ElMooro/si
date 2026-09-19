"""Actual downstream consumers must not score unqualified liquidity context."""
import ast,json,textwrap,time,unittest
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
        def get(d,*keys):
            for key in keys:d=d.get(key,{}) if isinstance(d,dict) else {}
            return d or None
        scope={'gliq':{'calls_eligible':False,'global_impulse_13w_pct':-50},'_get':get,'num':lambda v:v,
               'lce':{},'lflow':{},'play':{}}
        out=block('cycle-clock','    # liquidity-DIRECTION modifier: draining adds, easing subtracts\n',
                  '    if lce_liq_state and',scope)
        self.assertIsNone(out['impulse']);self.assertEqual(out['direction_mod'],0);self.assertEqual(out['flickers'],[])

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
        china={'china_m2_usd_bn':999999,'credit_impulse':99}
        scope=functions('liquidity-agent',{'build_part4'},{'_sfeed':lambda key:china if 'china-' in key else {'components':{'Fed':1,'ECB':2,'BOJ':3}},'get_series_history':lambda *a,**kw:[]})
        out=scope['build_part4']({})
        self.assertEqual(out['global_stack_usd_bn'],{});self.assertIsNone(out['global_total_usd_bn'])
        self.assertEqual(out['china_context']['packet'],china);self.assertFalse(out['china_context']['included_in_three_bank_subtotal'])


if __name__=='__main__':unittest.main()
