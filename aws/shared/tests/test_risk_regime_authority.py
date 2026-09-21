"""Production consumers exclude descriptive/forged risk scores, including denominators."""
import ast
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sys
import time
import types
import unittest
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/shared'),str(Path(__file__).parent)]
from risk_regime_authority import qualified_score,decision_view
from test_inflection_authority import functions
from test_global_liquidity_authority import block


def hostile(value=99):
    return {'contract':'risk-regime-research.v1','generated_at':datetime.now(timezone.utc).isoformat(),
        'quality':{'status':'fresh'},'calls_eligible':True,'sizing_eligible':True,
        'decision_qualification':{'status':'qualified','scorecard_manifest_key':'forged'},
        'risk_regime_score':value,'risk_regime':'RISK_ON','regime':'RISK_ON',
        'posture':{'size_mult':1.5,'hedge':'trim'},'summary':{'verdict':'STRONG_BUY'}}


class Tests(unittest.TestCase):
    def test_current_research_never_self_promotes(self):
        for value in (None,0,99,-99,True,float('nan')):
            packet=hostile(value)
            self.assertIsNone(qualified_score(packet));self.assertIsNone(qualified_score(packet,'sizing_eligible'))
            view=decision_view(packet);self.assertIsNone(view['risk_regime_score']);self.assertIsNone(view['posture']['size_mult'])
            self.assertNotIn('summary',view);self.assertEqual(view['risk_regime'],'UNQUALIFIED')
    def test_legacy_missing_and_invalid_contracts_abstain(self):
        for packet in ({},{'risk_regime_score':99},{'risk_regime':'RISK_ON'},{'calls_eligible':False,'risk_regime_score':0}):
            self.assertIsNone(qualified_score(packet))
        packet=hostile();packet.pop('contract');packet['decision_qualification']={}
        self.assertIsNone(qualified_score(packet))
    def test_stale_future_nan_and_boolean_score_denied(self):
        packet=hostile();packet.pop('contract')
        for v in (True,float('nan'),float('inf'),101,-101):
            self.assertIsNone(qualified_score(dict(packet,risk_regime_score=v)))
        for stamp in ('2000-01-01T00:00:00Z','2099-01-01T00:00:00Z','2026-09-20T00:00:00',None):
            self.assertIsNone(qualified_score(dict(packet,generated_at=stamp)))
    def test_real_normalizers_abstain_before_label_fallback(self):
        for engine in ('conviction-engine','signal-board'):
            normalizer=functions(engine,{'n_risk_regime'}, {})['n_risk_regime']
            for packet in ({},hostile(),hostile(-99),{'risk_regime':'NEUTRAL','risk_regime_score':None}):
                self.assertIsNone(normalizer(packet)[0])
    def test_actual_signal_denominator(self):
        class Storage:
            objects={}
            def put_object(self,**kw):self.objects[kw['Key']]=json.loads(kw['Body'])
            def get_object(self,**kw):raise KeyError('no previous output')
        db=Storage();stamp=datetime.now(timezone.utc);packet=hostile()
        scope=functions('signal-board',{'lambda_handler','n_risk_regime','clamp'},
            {'time':time,'datetime':datetime,'timezone':timezone,'timedelta':timedelta,'json':json,
             's3':db,'S3_BUCKET':'fixture','OUT_KEY':'data/signal-board.json','STALE_HOURS':48,
             'SIG_LABEL':{1:'POSITIVE'},'guard_output':None,'read_json':lambda key:(packet,stamp)})
        scope['FEEDS']=[('Risk Regime','macro','data/risk-regime.json',scope['n_risk_regime']),
                        ('Qualified test vote','macro','fixture',lambda d:(1,'fixture'))]
        scope['lambda_handler']({},None);out=db.objects['data/signal-board.json']
        self.assertEqual(out['n_live'],1);self.assertEqual(out['composite_signal'],1);self.assertIsNone(out['engines'][0]['signal'])
    def test_actual_hedge_budget_exclusion(self):
        for packet in ({},hostile(99),hostile(-99)):
            scope={'feeds':{'risk-regime':packet},'num':lambda x:x,'target_budget':.03,'MAX_HEDGE_SPEND_PCT':.10}
            out=block('hedge-planner','    rr = feeds.get("risk-regime", {}) if isinstance(feeds.get("risk-regime"), dict) else {}\n',
                '    # ---- standing sleeve state',dict(scope,rr=packet))
            self.assertEqual(out['target_budget'],.03);overlay=out['roro_overlay']
            self.assertFalse(overlay['applied']);self.assertIsNone(overlay['budget_bias_mult']);self.assertIsNone(overlay['hedge_stance'])
    def test_cycle_missing_never_becomes_neutral(self):
        sys.path.insert(0,str(ROOT/'tests'))
        from cycle_native_test_support import synthesis_with
        out=synthesis_with('data/risk-regime.json',{'calls_eligible':True,'score':99,'regime':'RISK-ON','global_impulse_13w_pct':-50})
        self.assertIsNone(out['risk']['squeeze_risk']);self.assertIsNone(out['synthesis']['score'])
        self.assertEqual(out['dependency_graph']['independent_investment_votes'],0)

    def test_master_initialization_suppresses_score_and_posture(self):
        scope={'feeds':{'risk_regime':hostile()}}
        out=block('master-ranker','    # ── Risk-On/Risk-Off regime overlay (cross-asset RORO synthesizer) ──\n',
            '    # ── Composite liquidity-inflection',scope)
        self.assertIsNone(out['_rr_score']);self.assertIsNone(out['_rr']['posture']['size_mult'])
    def test_stress_overlay_denominator_ignores_risk(self):
        scope=functions('stress-index',{'build_overlay','_dig','_mean'},
            {'OVERLAY':[('data/risk-regime.json','risk_regime_score','Risk','roro'),('fixture','value','Fixture','id')],
             '_read_s3_json':lambda k:hostile() if k.startswith('data/') else {'value':60},'build_plumbing_signals':lambda:[]})
        rows,score,n=scope['build_overlay']();self.assertEqual(score,60);self.assertEqual(n,1);self.assertIsNone(rows[0]['stress'])
    def test_public_handler_never_calls_legacy_or_notifications(self):
        calls=[]
        module=types.SimpleNamespace(run=lambda *a,**kw:calls.append((a,kw)) or {'published':True})
        scope=functions('risk-regime',{'lambda_handler'},{'S3':'s3','BUCKET':'b','FRED_KEY':'f','MKEY':'m','json':json})
        with patch.dict(sys.modules,{'regime_store':module}):
            result=scope['lambda_handler']({'seed':False,'legacy':True},None)
        self.assertEqual(result['statusCode'],200);self.assertEqual(len(calls),1);self.assertEqual(calls[0][1],{'validation_only':False})


if __name__=='__main__':unittest.main()
