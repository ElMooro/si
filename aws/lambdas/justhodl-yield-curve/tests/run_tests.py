import importlib.util
import io
import json
import sys
import types
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[4]
sys.path.insert(0,str(ROOT/'aws/shared'))
with patch.dict(sys.modules,{'boto3':types.SimpleNamespace(client=lambda *a,**k:None),
                            'managed_secret':types.SimpleNamespace(managed_secret=lambda *a,**k:'test'),
                            '_fred_shim':types.SimpleNamespace()}):
    spec=importlib.util.spec_from_file_location('yield_curve_test',Path(__file__).resolve().parents[1]/'source/lambda_function.py')
    e=importlib.util.module_from_spec(spec);spec.loader.exec_module(e)
NOW=datetime.now(timezone.utc)
DATES=[(NOW-timedelta(days=i+2)).date().isoformat() for i in range(80)][::-1]


def run(missing=None,lag=None,stale=False,acm=None,gap=None):
    writes={};requested=[]
    def fred(sid,n=80):
        requested.append(sid)
        if sid==missing:return []
        dates=DATES[:-20] if stale else DATES[:-1] if sid==lag else DATES
        if sid==gap:dates=dates[:-3]+dates[-2:]
        return [{'date':d,'value':4+i/100+(1 if sid=='DGS10' else 0)} for i,d in enumerate(dates)]
    def read(**kw):return {'Body':io.BytesIO(json.dumps(acm or {}).encode())}
    with patch.object(e,'fred_obs',side_effect=fred),patch.object(e,'S3',types.SimpleNamespace(get_object=read,put_object=lambda **kw:writes.update({kw['Key']:json.loads(kw['Body'])}))):
        assert e.lambda_handler({'suppress_alerts':True})['statusCode']==200
    return writes[e.KEY],requested


class Integrity(unittest.TestCase):
    def test_current_full_curve_has_one_observation_date(self):
        out,_=run(lag='DGS5')
        self.assertEqual(out['quality']['status'],'fresh')
        self.assertEqual({p['date'] for p in out['curve_points']},{DATES[-2]})
        self.assertEqual({p['date'] for p in out['real_yields'].values()},{DATES[-2]})

    def test_missing_tenor_does_not_create_full_curve_average(self):
        out,_=run(missing='DGS10')
        self.assertEqual(out['regime'],'UNKNOWN')
        self.assertIsNone(out['spreads_bps']['2s10s'])
        self.assertIsNone(out['inversion_flags']['any_inversion'])
        self.assertIsNone(out['decomposition']['level_pct'])
        self.assertEqual(out['signals'],[])

    def test_stale_curve_does_not_give_current_regime(self):
        out,_=run(stale=True)
        self.assertEqual(out['quality']['status'],'incomplete')
        self.assertEqual(out['regime'],'UNKNOWN')
        self.assertFalse(out['curve_points'])

    def test_different_five_observation_endpoints_do_not_classify(self):
        out,_=run(gap='DGS2')
        self.assertEqual(out['regime'],'UNKNOWN')

    def test_monthly_policy_rate_replaced_by_daily_dff(self):
        out,requested=run()
        self.assertIn('DFF',requested)
        self.assertNotIn('FEDFUNDS',requested)
        self.assertEqual(out['fed_context']['FED_FUNDS_RATE_EFFECTIVE']['date'],out['as_of_date'])

    def test_residual_is_not_a_term_premium_fallback(self):
        out,_=run()
        self.assertIsNotNone(out['nominal_real_breakeven_residual_bps'])
        self.assertIsNone(out['term_premium_proxy_bps'])
        self.assertEqual(out['term_premium_source'],'unavailable')
        self.assertNotIn('term_premium_anomaly',[r['name'] for r in out['signals']])

    def test_only_fresh_finite_acm_model_estimate_is_used(self):
        doc={'generated_at':NOW.isoformat(),'latest':{'date':DATES[-1],'tp10':.7}}
        out,_=run(acm=doc)
        self.assertEqual(out['term_premium_proxy_bps'],70)
        for d,v in [('2999-01-01',.7),('2020-01-01',.7),(DATES[-1],float('nan'))]:
            doc['latest']={'date':d,'tp10':v}
            out,_=run(acm=doc)
            self.assertIsNone(out['term_premium_proxy_bps'])

    def test_flat_and_bull_flattening_taxonomy(self):
        self.assertEqual(e.classify_curve_regime(0,0,20)[0],'UNCHANGED')
        self.assertEqual(e.classify_curve_regime(5,5,20)[0],'PARALLEL_BEAR')
        reg,desc=e.classify_curve_regime(-5,-20,20)
        self.assertEqual(reg,'BULL_FLATTENER')
        self.assertIn('long rates falling faster',desc)


if __name__=='__main__':unittest.main()
