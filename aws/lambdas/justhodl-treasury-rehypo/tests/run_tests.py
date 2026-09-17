"""Execute the actual collateral handler with dated primary-source fixtures."""
import copy
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
                            'managed_secret':types.SimpleNamespace(managed_secret=lambda *a,**k:'test')}):
    spec=importlib.util.spec_from_file_location('collateral_engine_test',Path(__file__).resolve().parents[1]/'source/lambda_function.py')
    engine=importlib.util.module_from_spec(spec);spec.loader.exec_module(engine)
NOW=datetime.now(timezone.utc)
DAILY=[((NOW-timedelta(days=i+2)).date().isoformat(),4+i/1000) for i in range(350)][::-1]
WEEKLY=[(d,100+i%19) for i,(d,_) in enumerate(DAILY) if i%7==0]
FAILS={'generated_at':NOW.isoformat(),'classes':[{'key':'ust_ex_tips'}],'totals':{'scope':'all_asset'},
       'treasury':{'as_of':WEEKLY[-1][0],'unit':'USD_bn_par','quality':{'status':'fresh'},'complete':True,
                   'gross':WEEKLY,'gross_bn':WEEKLY[-1][1],'ftd_bn':50,'ftr_bn':WEEKLY[-1][1]-50}}


def run(fails=None, missing_funding=False, stale_funding=False, rrp_direction=1, old_history=True):
    docs={'data/settlement-fails.json':copy.deepcopy(FAILS if fails is None else fails),
          'data/treasury-rehypo-history.json':{'rows':[{'t':'2020-01-01','composite':99,'band':'SEIZING'}] if old_history else []}}
    writes={};calls=[]
    def fred(sid,n=200):
        if missing_funding and sid=='SOFR':return []
        if sid=='RRPONTSYD':return [(d,rrp_direction*i) for i,(d,_) in enumerate(DAILY)]
        rows=DAILY[:-40] if stale_funding else DAILY
        return [(d,v if sid=='SOFR' else 4) for d,v in rows]
    def ofr(m):
        calls.append(m)
        return [(d,v+(.02 if 'GCF' in m else 0)) for d,v in DAILY]
    def read(**kw):return {'Body':io.BytesIO(json.dumps(docs.get(kw['Key'],{})).encode())}
    with patch.object(engine,'s3',types.SimpleNamespace(get_object=read,put_object=lambda **kw:writes.update({kw['Key']:json.loads(kw['Body'])}))), \
         patch.object(engine,'fred_series',side_effect=fred),patch.object(engine,'ofr_series',side_effect=ofr), \
         patch.object(engine,'nyfed_catalog',side_effect=AssertionError('Independent overlapping fails discovery')):
        result=engine.lambda_handler({'suppress_alerts':True})
    assert result['ok']
    return writes[engine.OUT_KEY],writes,calls


class Integrity(unittest.TestCase):
    def test_exact_canonical_fails_and_units(self):
        out,_,_=run()
        self.assertEqual(out['treasury_fails'],json.loads(json.dumps(FAILS['treasury'])))
        self.assertEqual(out['legs']['fails']['latest'],FAILS['treasury']['gross_bn'])
        self.assertEqual(out['legs']['fails']['unit'],'USD_bn_par')

    def test_no_net_position_velocity_or_fake_specialness(self):
        out,_,_=run()
        self.assertIsNone(out['legs']['velocity']['latest'])
        self.assertIsNone(out['legs']['specialness']['latest_bps'])
        self.assertEqual(out['legs']['gc_venue_spread']['score_contribution'],0)

    def test_gc_comparison_has_matching_mnemonic_suffix(self):
        _,_,calls=run()
        self.assertEqual(calls[:2],['REPO-GCF_AR_OO-P','REPO-TRI_AR_OO-P'])

    def test_rrp_sign_does_not_change_review_index(self):
        a,_,_=run(rrp_direction=1);b,_,_=run(rrp_direction=-1)
        self.assertEqual(a['composite'],b['composite'])
        self.assertEqual(a['legs']['rrp_drain_4w']['score_contribution'],0)

    def test_calendar_delta_uses_calendar_days_not_twenty_observations(self):
        rows=[('2026-01-01',10),('2026-01-29',15),('2026-02-26',12)]
        self.assertEqual(engine.calendar_delta(rows),[('2026-01-29',5),('2026-02-26',-3)])
        self.assertEqual(engine.calendar_delta([('2026-01-01',10),('2026-02-10',15)]),[])

    def test_absent_or_stale_required_leg_expires_score(self):
        for args in ({'missing_funding':True},{'stale_funding':True},{'fails':{}}):
            out,_,_=run(**args)
            self.assertIsNone(out['composite'])
            self.assertEqual(out['band'],'UNAVAILABLE')
            self.assertEqual(out['quality']['status'],'incomplete')

    def test_old_composite_history_is_not_blended(self):
        out,writes,_=run()
        rows=writes['data/treasury-rehypo-history.json']['rows']
        self.assertEqual(len(rows),1)
        long=writes[engine.LONG_KEY]
        self.assertTrue(long['weekly'])
        self.assertTrue(all(set(r['z'])=={'fails','sofr_iorb'} and r['n']==2 for r in long['weekly']))
        self.assertEqual(list(writes)[-1],engine.OUT_KEY)

    def test_invalid_values_or_dates_are_not_current(self):
        self.assertEqual(engine.clean_series([('2026-01-01',float('nan')),('bad',1)]),[])
        self.assertEqual(engine.quality('2999-01-01')['status'],'invalid')
        self.assertIsNone(engine.measured_leg([('2999-01-01',1)],'test','USD')['latest'])

    def test_no_renormalization_on_one_available_leg(self):
        leg={'z':2,'quality':{'status':'fresh'}}
        self.assertEqual(engine.review_score({'fails':leg}),(None,'UNAVAILABLE'))
        self.assertEqual(engine.review_score({'fails':leg,'sofr_iorb':leg})[0],75)


if __name__=='__main__':unittest.main()
