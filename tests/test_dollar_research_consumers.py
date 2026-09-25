from pathlib import Path
from copy import deepcopy
from unittest.mock import Mock
import ast,hashlib,json,sys,textwrap,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/ops/checks')]
import dollar_research_context as gate
from release_package_evidence import shared_imports
LEGACY={'regime':'DUMP','stance':'WEAK','score':99,'dollar_pressure':-100,'calls_eligible':True,
    'bbdxy':{'dxy_synth':{'chg_3m_pct':-10}},'technicals':{'double_top':True},
    'canaries':[{'lean':'DUMP','label':'invented'}],'risk_transmission':{'score':99,'verdict':'RISK_ON'}}
CHANGED=('signal-board','canary-warroom','master-allocator','cross-asset-flow-state','rotation-dashboard','katlin',
    'crypto-confluence','equity-confluence','calibration-fleet','regime-conditional-router','correlation-break-trade-router',
    'wl-fusion','market-interpreter','auction-interpreter','morning-intelligence','streaming-fanout')
def source(name):return (ROOT/f'aws/lambdas/justhodl-{name}/source/lambda_function.py').read_text(encoding='utf-8')
def funcs(name,names,scope=None):
    scope={} if scope is None else scope;nodes=[n for n in ast.parse(source(name)).body if isinstance(n,ast.FunctionDef) and n.name in names]
    assert len(nodes)==len(names);exec(compile(ast.Module(body=nodes,type_ignores=[]),name,'exec'),scope);return scope
def native():
    p={'contract':'dollar-original-research.v1','generated_at':'2020-01-01T00:00:00Z',**dict.fromkeys(gate.FLAGS,False),
        'call':None,'score':None,'regime':None,'dollar_pressure':None}
    p['replay']={'manifest_key':'data/dollar-research/runs/'+'a'*64+'.json',
        'output_sha256':hashlib.sha256(json.dumps(p,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()).hexdigest()}
    return p


class Tests(unittest.TestCase):
    def test_old_missing_and_self_promoted_packets_cannot_supply_votes(self):
        for value in (None,{},[],LEGACY,{**native(),'calls_eligible':True},{**native(),'score':99}):
            result=gate.decision_view(value);self.assertIsNone(result['score']);self.assertIsNone(result['regime'])
            self.assertEqual(result['canaries'],[]);self.assertEqual(result['bbdxy'],{});self.assertEqual(result['independent_investment_votes'],0)
            self.assertFalse(result['research_context']['native_reference_available']);self.assertIsNone(gate.qualified_score(value))
    def test_native_digest_is_bound_without_claiming_provider_replay_or_current_freshness(self):
        p=native();out=gate.context(p);self.assertTrue(out['native_reference_available']);self.assertTrue(out['output_digest_checked'])
        self.assertFalse(out['original_provider_replay_performed_by_consumer']);self.assertFalse(out['current_freshness_verified_by_consumer'])
        out['canonical']['replay']['output_sha256']='changed';self.assertNotEqual(p['replay']['output_sha256'],'changed')
        p['new_measurement']=999;self.assertFalse(gate.context(p)['native_reference_available'])
    def test_bad_clock_private_reference_or_numeric_identity_is_refused(self):
        for key,value in (('manifest_key','data/trade-tickets.json'),('manifest_key',7),('output_sha256',[])):
            p=native();p['replay'][key]=value;self.assertFalse(gate.context(p)['native_reference_available'])
        for stamp in ('9999-01-01T00:00:00Z','2020-01-01',None):
            p=native();p['generated_at']=stamp;self.assertFalse(gate.context(p)['native_reference_available'])
    def test_guard_closes_both_legacy_dollar_paths_without_mutating_other_inputs(self):
        for key in ('data/dollar.json','data/dollar-radar.json'):self.assertIsNone(gate.guard(key,LEGACY)['regime'])
        self.assertIs(gate.guard('data/other.json',LEGACY),LEGACY)
    def test_actual_signal_board_and_canary_normalizers_abstain(self):
        signal=funcs('signal-board',{'n_dollar_radar'});canary=funcs('canary-warroom',{'norm_dollar'})
        for packet in (LEGACY,native(),{}):
            self.assertIsNone(signal['n_dollar_radar'](packet)[0]);card,rows=canary['norm_dollar'](packet)
            self.assertEqual(rows,[]);self.assertIsNone(card['score']);self.assertEqual(card['band'],'ABSTAIN')
    def test_actual_allocator_cannot_size_from_dollar_pressure(self):
        scope=funcs('master-allocator',{'gather_signals','clamp'}, {'read_json':lambda key:LEGACY if key==gate.CURRENT else {}})
        self.assertNotIn('dollar_radar',scope['gather_signals']())
    def test_router_dollar_frameworks_abstain_with_legacy_extremes(self):
        names={'detect_dollar_shortage','detect_dollar_smile_left','detect_dollar_smile_right'};scope=funcs('regime-conditional-router',names)
        for name in names:
            result=scope[name](*([LEGACY]* (4 if name.endswith('right') else 3)))
            self.assertIsNone(result[0]);self.assertEqual(result[1]['status'],'ABSTAIN')
    def test_missing_dollar_cannot_trigger_reflation_carry(self):
        scope=funcs('correlation-break-trade-router',{'classify_correlation_regime','safe_get'},
            {'REGIME_TRADES':dict.fromkeys(('NORMAL','BOND_ROUT','DOLLAR_DEBASEMENT','STAGFLATION_PRICING','RISK_PARITY_UNWIND','DEFLATION_FEAR','REFLATION_CARRY'),{})})
        breaks={'pair_details':[{'pair':'BTC_QQQ','current_corr':.9,'z_delta':2}]}
        for dollar in ({},LEGACY,native()):
            for inputs in (breaks,None,[],{}):
                scores,evidence=scope['classify_correlation_regime'](inputs,dollar,{},{});self.assertIsNone(scores['REFLATION_CARRY'])
                self.assertEqual(evidence['REFLATION_CARRY']['status'],'ABSTAIN')
        line=next(l for l in source('correlation-break-trade-router').splitlines() if 'sorted_regimes = ' in l)
        scope={'scores':scores};exec(textwrap.dedent(line),scope);self.assertNotIn('REFLATION_CARRY',dict(scope['sorted_regimes']))
    def test_fleet_history_cannot_requalify_retired_dollar_forecast(self):
        s=source('calibration-fleet');start=s.index('    # ---- 5. per-engine');end=s.index('    # normalise weight',start)
        forbidden=Mock(side_effect=AssertionError('Unqualified historic score used'))
        scope={'REGISTRY':[{'name':'dollar_radar','direction':'stress','label':'Dollar','source_key':gate.CURRENT,'score_path':['dollar_pressure']}],
            'snaps':[{'date':'2026-09-01','scores':{'dollar_radar':99}}],'forward_dd':forbidden,'ddb_history_snapshots':forbidden}
        exec(textwrap.dedent(s[start:end]),scope);row=scope['engines_out'][0]
        self.assertEqual(row['quality_rating'],'UNQUALIFIED');self.assertIsNone(row['current_score']);self.assertEqual(scope['weight_props'],{});forbidden.assert_not_called()
        assignment=next(n for n in ast.walk(ast.parse(s)) if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='score' for t in n.targets) and isinstance(n.value,ast.IfExp))
        scope={'reg':{'name':'dollar_radar'},'d':LEGACY};exec(compile(ast.Module(body=[assignment],type_ignores=[]),'actual fleet score','exec'),scope);self.assertIsNone(scope['score'])
    def test_dollar_does_not_trigger_a_first_broadcast_or_categorical_flip(self):
        scope=funcs('streaming-fanout',{'_is_meaningful_delta'})
        for previous in (None,{'regime':'PUMP'}):self.assertFalse(scope['_is_meaningful_delta']({'name':'dollar_radar'},previous,LEGACY)[0])
    def test_actual_typed_read_expressions_cannot_copy_legacy_scores(self):
        for name,target in (('rotation-dashboard','dr'),('katlin','dr')):
            nodes=ast.walk(ast.parse(source(name)));assignment=next(n for n in nodes if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id==target for t in n.targets))
            scope={'rd':lambda key:LEGACY,'read_feed':lambda key:LEGACY,'F':{'dollar':LEGACY}}
            exec(compile(ast.Module(body=[assignment],type_ignores=[]),name,'exec'),scope);self.assertIsNone(scope[target]['regime']);self.assertEqual(scope[target]['bbdxy'],{})
        import flow_state_model
        read=Mock()
        with self.assertRaises(ValueError):flow_state_model.bind_parent('data/dollar-radar.json',LEGACY,read,'2026-09-24T23:00:00Z')
        read.assert_not_called()
    def test_narratives_receive_references_without_unsupported_causal_claims(self):
        for name in ('crypto-confluence','equity-confluence'):
            node=next(n for n in ast.walk(ast.parse(source(name))) if isinstance(n,ast.Assign) and any(isinstance(t,ast.Subscript) and isinstance(t.slice,ast.Constant) and t.slice.value=='dollar_context' for t in n.targets))
            scope={'out':{},'_read':lambda key:LEGACY};exec(compile(ast.Module(body=[node],type_ignores=[]),name,'exec'),scope)
            self.assertIsNone(scope['out']['dollar_context']['score'])
        self.assertNotIn('negative = DUMP (Fed QE/repo liquidity flood',source('morning-intelligence'))
    def test_every_changed_consumer_bundles_the_typed_boundary(self):
        for name in CHANGED:
            paths=list((ROOT/f'aws/lambdas/justhodl-{name}/source').glob('*.py'))
            required='flow_state_model.py' if name=='cross-asset-flow-state' else 'dollar_research_context.py'
            self.assertIn(required,[p.name for p in shared_imports(ROOT,paths)],name)


if __name__=='__main__':unittest.main(verbosity=2)
