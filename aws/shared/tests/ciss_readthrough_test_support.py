"""Offline original-source fixtures and actual risk consumer handlers."""
from contextlib import ExitStack
from copy import deepcopy
from datetime import datetime, timedelta
import io
import hashlib
import json
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

sys.path[:0] = [str(Path(__file__).resolve().parent),str(Path(__file__).resolve().parents[1])]
from ciss_source_model import build, HEAD, digest
from ciss_readthrough import context, SERIES
from ciss_vintage_test_support import load, seal
from test_ciss_source_model import packet_inputs, NOW

CLOCK=datetime.fromisoformat(NOW)

class Frozen(datetime):
    @classmethod
    def now(cls,tz=None):return CLOCK if tz else CLOCK.replace(tzinfo=None)


def packet():
    discovery,histories=packet_inputs()
    for item in [*discovery.values(),*histories.values()]:
        sha=hashlib.sha256(item['raw']).hexdigest()
        item['evidence'].update(captured=True,provider='ecb',sha256=sha,
            key='data/evidence/ecb/'+('a'*64)+'/'+sha+'.bin.gz')
    return seal(build(discovery,histories,NOW))


class Tests(unittest.TestCase):
    def test_radar_nonfinite_and_boolean_inputs_are_unavailable(self):
        m=load('justhodl-risk-radar')
        for value in (True,False,float('inf'),float('-inf'),float('nan'),'Infinity'):
            self.assertIsNone(m.num(value))
        self.assertEqual(m.num('0'),0)

    def test_exact_source_identity_and_permission_are_preserved(self):
        p=packet();before=deepcopy(p)
        for key in [*SERIES.values(),'CISS.D.U2.Z0Z.4F.EC.SS_CON.CON']:
            out=context(p,CLOCK,key)
            self.assertEqual(out['status'],'fresh')
            self.assertEqual(out['source_replay'],p['replay'])
            self.assertEqual(out['independent_votes'],0)
            self.assertFalse(out['vote_eligible']);self.assertFalse(out['calls_eligible'])
            self.assertFalse(out['sizing_eligible']);self.assertIsNone(out['regime'])
        self.assertLess(out['value'],0)
        self.assertEqual(out['unit'],'dimensionless_contribution')
        self.assertEqual(p,before)

    def test_unbound_old_missing_and_future_measurements_cannot_become_calm(self):
        for p in (None,{}, {'ea_regime':'CALM','ea_composite':0},packet()):
            if isinstance(p,dict) and p.get('contract'):p['ea_composite']=.9
            out=context(p,CLOCK)
            self.assertIsNone(out['value']);self.assertIsNone(out['regime'])
            self.assertNotIn('calm',out['read'].lower())
        p=packet();out=context(p,CLOCK+timedelta(days=4))
        self.assertEqual(out['status'],'stale');self.assertIsNone(out['value'])
        self.assertEqual(context(p,CLOCK-timedelta(minutes=1))['status'],'invalid')
        p=packet();row=next(r for r in p['series'] if r['key']==HEAD)
        row.update(latest=None,latest_decimal=None)
        row['quality']['status']='missing'
        out=context(seal(p),CLOCK)
        self.assertEqual(out['status'],'missing');self.assertIsNone(out['value'])

    def test_old_acquisition_unit_and_duplicate_rows_are_rejected(self):
        for mutation in ('clock','unit','duplicate','reconciliation','decimal','evidence'):
            p=packet();row=next(r for r in p['series'] if r['key']==HEAD)
            if mutation=='clock':row['acquired_at']=(CLOCK-timedelta(days=4)).isoformat()
            elif mutation=='unit':row['unit']='percent'
            elif mutation=='duplicate':p['series'].append(deepcopy(row))
            elif mutation=='reconciliation':p['headline_reconciliation']['date']='2026-09-14'
            elif mutation=='evidence':row['evidence']['captured']=False
            else:row['latest_decimal']='0.9'
            out=context(seal(p),CLOCK)
            self.assertIsNone(out['value'],mutation)

    def test_actual_radar_missing_coverage_does_not_claim_health_or_short_backdrop(self):
        m=load('justhodl-risk-radar');writes=[]
        def get(**kw):return {'Body':io.BytesIO(b'{"ea_regime":null,"ea_composite":0.03}')}
        with patch.object(m,'datetime',Frozen),patch.object(m,'s3',types.SimpleNamespace(get_object=get,put_object=lambda **k:writes.append(k))), \
             patch.object(m,'load_universe',return_value=[{'symbol':'EMPTY'}]), \
             patch.object(m,'load_short_pressure',return_value={}),patch.object(m,'load_value_traps',return_value={}):
            m.lambda_handler({},None)
        out=json.loads(writes[-1]['Body'])
        self.assertEqual(out['assessment_coverage']['insufficient_axes'],1)
        self.assertIsNone(out['macro_stress']['value'])
        self.assertNotIn('headwind',out['macro_stress']['read'])
        self.assertIn('does not establish',out['headline'])
        self.assertFalse(out['calls_eligible'])

    def test_below_floor_assessment_counts_as_assessed_without_carrying_it(self):
        m=load('justhodl-risk-radar');writes=[]
        row={'symbol':'ZERO','name':'fixture','sector':'Technology'}
        with ExitStack() as stack:
            for name in ('axis_solvency','axis_earnings','axis_analyst','axis_valuation','axis_momentum'):
                stack.enter_context(patch.object(m,name,return_value=0))
            stack.enter_context(patch.object(m,'load_universe',return_value=[row]))
            stack.enter_context(patch.object(m,'load_short_pressure',return_value={}))
            stack.enter_context(patch.object(m,'load_value_traps',return_value={}))
            stack.enter_context(patch.object(m,'s3',types.SimpleNamespace(get_object=lambda **k: {'Body':io.BytesIO(b'{}')},put_object=lambda **k:writes.append(k))))
            m.lambda_handler({},None)
        out=json.loads(writes[-1]['Body'])
        self.assertEqual(out['assessment_coverage']['assessed'],1)
        self.assertEqual(out['n_carried'],0)

    def test_actual_global_handler_has_no_unvalidated_floor_or_notification_on_verification(self):
        m=load('justhodl-global-stress');writes=[];p=packet()
        p['ea_regime']='CRISIS';p=seal(p) # even a bound legacy label grants no authority
        get=lambda **k: {'Body':io.BytesIO(json.dumps(p).encode())}
        row={'asset_class':'equity','stress':20,'market':'fixture','tracks':'fixture','level':'CALM','key':'fixture'}
        alert=Mock()
        with ExitStack() as stack:
            patches={'datetime':Frozen,'FMP':'SYNTHETIC','s3':types.SimpleNamespace(get_object=get,put_object=lambda **k:writes.append(k))}
            for name,value in patches.items():stack.enter_context(patch.object(m,name,value))
            funcs={'scan':row,'load_weights':(None,'prior',0,None),'load_horizon_weights':{},
                   'update_history':[],'stress_momentum':None,'read_prev_output':{},
                   'build_stress_escalation':({'posture':'fixture','n_red':1,'n_amber':0,'newly_red':[]},[{'label':'fixture'}])}
            for name in ('implied_vol_panel','credit_spread_panel','rates_panel','sovereign_panel','funding_panel','contagion_index','stress_breadth','safe_haven_panel','write_dim_history'):
                funcs[name]=None
            for name,value in funcs.items():stack.enter_context(patch.object(m,name,return_value=value))
            stack.enter_context(patch.object(m,'send_telegram',alert))
            response=m.lambda_handler({'suppress_alerts':True},None)
        out=json.loads(next(w['Body'] for w in writes if w['Key']==m.OUT_KEY))
        self.assertEqual(response['statusCode'],200)
        self.assertEqual(out['global_stress_index'],20)
        self.assertIsNone(out['global_stress_index_ciss_adj'])
        self.assertEqual(out['ciss_systemic']['value'],.03)
        self.assertIsNone(out['ciss_systemic']['floor']);self.assertFalse(out['ciss_systemic']['applied'])
        alert.assert_not_called()


def run():
    result=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(Tests))
    if not result.wasSuccessful():raise SystemExit(1)


if __name__=='__main__':run()
