"""D28/D29 actual-handler tests: no S3/provider network and no fabricated order terms."""
import contextlib
import importlib.util
import io
import json
import math
import sys
import types
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[2] / 'shared'))
NOW = datetime.now(timezone.utc)


class MemoryS3:
    def __init__(self, docs): self.docs, self.writes = docs, {}
    def get_object(self, Bucket, Key): return {'Body': io.BytesIO(json.dumps(self.docs[Key]).encode())}
    def put_object(self, Bucket, Key, Body, **kwargs):
        def invalid(value): raise AssertionError('Non-finite public JSON: ' + value)
        self.writes[Key] = json.loads(Body, parse_constant=invalid)


def load():
    fake = types.ModuleType('boto3'); fake.client = lambda *a, **kw: None
    secret = types.ModuleType('managed_secret'); secret.managed_secret = lambda *a, **kw: ''
    with patch.dict(sys.modules, {'boto3': fake, 'managed_secret': secret}):
        spec = importlib.util.spec_from_file_location('tickets_under_test', HERE.parent / 'source/lambda_function.py')
        mod = importlib.util.module_from_spec(spec); spec.loader.exec_module(mod)
    return mod


def short():
    return {'version':'1.1','measurement_contract':'short-positioning.v2',
            'measurement_units':{'short_interest':'shares','days_to_cover':'days','si_change_pct':'percent'},'generated_at':NOW.isoformat(),'by_ticker':{'GME':{
        'ticker':'GME','short_interest_source':'FINRA consolidated short interest',
        'short_interest_as_of':(NOW-timedelta(days=7)).date().isoformat(),
        'days_to_cover_source':'FINRA consolidated short interest',
        'days_to_cover_as_of':(NOW-timedelta(days=7)).date().isoformat(),'short_interest':10_000_000,'days_to_cover':2,'si_change_pct':0,
        'settlement_date':(NOW-timedelta(days=7)).date().isoformat(),'latest_short_pct':99}}}


def options():
    return {'engine':'justhodl-options-analytics','version':'1.0.0','ok':True,
            'generated_at':NOW.isoformat(),'board':[{'ticker':'GME','atm_iv_front':0.3,'hv20':0.28,
            'vrp':0.02,'iv_rank':30,'skew_25d':0.01,'term_slope':0.01,'term_structure':'FLAT',
            'gamma_regime':'POSITIVE','gamma_flip_strike':95,'call_wall':110,'put_wall':90,
            'expiries':[(NOW+timedelta(days=20)).date().isoformat()]}]}


class DonorHandlerTests(unittest.TestCase):
    def run_handler(self, sdoc=None, odoc=None, earnings=None):
        mod=load(); docs={}
        if sdoc is not None: docs['data/short-interest.json']=sdoc
        if odoc is not None: docs['data/options-analytics.json']=odoc
        s3=MemoryS3(docs)
        candidate={'ticker':'GME','tier':'ALERT_TIER','combined_score':70,
                   'theme_acceleration':70,'position_sizing':{'final_pct':5}}
        bars=[{'t':int(NOW.timestamp()*1000)+n*86400000,'c':100,'h':102,'l':98} for n in range(25)]
        with patch.object(mod,'s3',s3), \
             patch.object(mod,'get_active_cascade',return_value={'alert_tier':[candidate]}), \
             patch.object(mod,'get_horizon_attribution',return_value={}), \
             patch.object(mod,'get_tier_confidence',return_value={}), \
             patch.object(mod,'get_earnings_calendar',return_value=earnings or {}), \
             patch.object(mod,'fetch_polygon_ohlc',return_value=bars), \
             patch.object(mod.urllib.request,'urlopen',side_effect=AssertionError('network forbidden')), \
             contextlib.redirect_stdout(io.StringIO()):
            response=mod.lambda_handler({},None)
        self.assertEqual(response['statusCode'],200)
        out=s3.writes['data/trade-tickets.json']; self.assertEqual(out['n_tickets'],1)
        return out, out['tickets'][0]

    def test_valid_context_removes_missing_evidence_review_with_units(self):
        _, missing=self.run_handler(); out,row=self.run_handler(short(),options())
        self.assertEqual(missing['donor_analysis_status'],'REVIEW_REQUIRED')
        self.assertEqual(row['donor_analysis_status'],'CONTEXT_COMPLETE')
        self.assertEqual(row['options_context']['atm_iv_annualized_pct'],30)
        self.assertEqual(row['options_context']['premium_vs_realized_vol_pp'],2)
        expected=.3*math.sqrt(row['expected_horizon_days']/252)*100
        self.assertAlmostEqual(row['options_context']['model_one_sigma_horizon_move_pct'],expected,places=4)
        self.assertEqual(row['short_positioning']['health']['units']['short_interest'],'shares')
        self.assertEqual(out['donor_health']['data/options-analytics.json']['status'],'FRESH')
        self.assertEqual(row['shares'],missing['shares']); self.assertFalse(row['execution_eligible'])
        self.assertFalse(row['options_context']['executable_option_quote'])
        self.assertIsNone(row['short_positioning']['borrow_fee'])

    def test_crowded_inventory_changes_covering_review_without_inventing_borrow(self):
        sdoc=short(); sdoc['by_ticker']['GME'].update(days_to_cover=8)
        _,row=self.run_handler(sdoc,options())
        self.assertEqual(row['short_positioning']['covering_risk'],'ELEVATED')
        self.assertTrue(row['donor_review_required'])
        self.assertFalse(row['short_positioning']['locate_verified'])
        self.assertIsNone(row['short_positioning']['borrow_available'])

    def test_option_iv_cost_and_term_changes_affect_review_and_model_scale(self):
        _,base=self.run_handler(short(),options())
        odoc=options(); odoc['board'][0].update(atm_iv_front=.6,vrp=.32,iv_rank=90,term_structure='BACKWARDATION')
        _,row=self.run_handler(short(),odoc)
        self.assertAlmostEqual(row['options_context']['model_one_sigma_horizon_move_pct'],2*base['options_context']['model_one_sigma_horizon_move_pct'],places=3)
        self.assertTrue(row['options_context']['premium_cost_review'])
        self.assertTrue(row['options_context']['event_volatility_review'])
        self.assertEqual(row['entry'],base['entry']); self.assertEqual(row['shares'],base['shares'])

    def test_mixed_short_volume_percentage_does_not_change_ticket(self):
        _,base=self.run_handler(short(),options())
        sdoc=short(); sdoc['by_ticker']['GME']['latest_short_pct']=0
        _,row=self.run_handler(sdoc,options())
        self.assertEqual(row['donor_review_required'],base['donor_review_required'])
        self.assertEqual(row['short_positioning']['covering_risk'],base['short_positioning']['covering_risk'])
        self.assertFalse(row['short_positioning']['daily_short_volume_used'])

    def test_short_interest_missing_stale_undated_bad_schema_and_nonfinite(self):
        for label,modify in [
            ('stale',lambda d:d.update(generated_at=(NOW-timedelta(hours=73)).isoformat())),
            ('old settlement',lambda d:d['by_ticker']['GME'].update(settlement_date=(NOW-timedelta(days=36)).date().isoformat())),
            ('undated',lambda d:d['by_ticker']['GME'].pop('settlement_date')),
            ('nan',lambda d:d['by_ticker']['GME'].update(days_to_cover=float('nan'))),
            ('undated ratio fallback',lambda d:d['by_ticker']['GME'].update(days_to_cover_source='Finviz short ratio',days_to_cover_as_of=None)),
            ('ratio wrong date',lambda d:d['by_ticker']['GME'].update(days_to_cover_as_of='2020-01-01')),
            ('unit',lambda d:d['measurement_units'].update(short_interest='USD')),
            ('malformed units',lambda d:d.update(measurement_units=['shares'])),
            ('legacy contract',lambda d:d.pop('measurement_contract')),
            ('schema',lambda d:d.update(version='0')),
            ('boolean',lambda d:d['by_ticker']['GME'].update(short_interest=True)),
        ]:
            with self.subTest(label=label):
                d=short();modify(d);_,row=self.run_handler(d,options())
                self.assertFalse(row['short_positioning']['positioning_usable'])
                self.assertEqual(row['short_positioning']['covering_risk'],'UNKNOWN')
                self.assertTrue(row['donor_review_required'])

    def test_options_missing_stale_expired_invalid_and_wrong_schema_fail_closed(self):
        for label,modify in [
            ('stale',lambda d:d.update(generated_at=(NOW-timedelta(hours=31)).isoformat())),
            ('future',lambda d:d.update(generated_at=(NOW+timedelta(hours=1)).isoformat())),
            ('expired',lambda d:d['board'][0].update(expiries=[(NOW-timedelta(days=1)).date().isoformat()])),
            ('nan',lambda d:d['board'][0].update(atm_iv_front=float('nan'))),
            ('rank',lambda d:d['board'][0].update(iv_rank=101)),
            ('schema',lambda d:d.update(engine='other')),
            ('no expiry',lambda d:d['board'][0].update(expiries=[])),
        ]:
            with self.subTest(label=label):
                d=options();modify(d);_,row=self.run_handler(short(),d)
                self.assertFalse(row['options_context']['usable'])
                self.assertIsNone(row['options_context']['model_one_sigma_horizon_move_pct'])
                self.assertFalse(row['options_context']['premium_cost_review'])
                self.assertTrue(row['donor_review_required'])

    def test_zero_rank_and_zero_vrp_remain_valid(self):
        odoc=options();odoc['board'][0].update(iv_rank=0,vrp=0)
        _,row=self.run_handler(short(),odoc)
        self.assertTrue(row['options_context']['usable'])
        self.assertEqual(row['options_context']['premium_vs_realized_vol_pp'],0)


if __name__ == '__main__': unittest.main()
