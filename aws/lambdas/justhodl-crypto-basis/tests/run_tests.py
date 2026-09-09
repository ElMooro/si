"""D30 actual-handler basis/funding tests with provider and S3 fixtures."""
import contextlib
import importlib.util
import io
import json
import sys
import types
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[2] / 'shared'))
NOW = datetime.now(timezone.utc)
NOW_MS = int(NOW.timestamp()*1000)


class MemoryS3:
    def __init__(self, docs): self.docs, self.writes = docs, {}
    def get_object(self, Bucket, Key): return {'Body': io.BytesIO(json.dumps(self.docs[Key]).encode())}
    def put_object(self, Bucket, Key, Body, **kwargs):
        def invalid(value): raise AssertionError('Non-finite public JSON: ' + value)
        self.writes[Key] = json.loads(Body, parse_constant=invalid)


def load():
    fake=types.ModuleType('boto3');fake.client=lambda *a,**kw:None
    with patch.dict(sys.modules,{'boto3':fake}):
        spec=importlib.util.spec_from_file_location('basis_under_test',HERE.parent/'source/lambda_function.py')
        mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod)
    return mod


def funding(interval=8,rate=.0001):
    return {'version':'1.0.0','source':'OKX /api/v5/public + /api/v5/market',
            'generated_at':NOW.isoformat(),'market_composite':{'n_coins_analyzed':2},
            'by_coin':{c:{'instId':c+'-USDT-SWAP','current_funding_rate':rate,
                'funding_interval_hours':interval,'interval_source':'venue_schedule',
                'funding_rate_units':'fraction_per_observed_interval','funding_z_score':0,
                'funding_moments_units':'fraction_per_8h_equivalent',
                'annualized_pct':999,'oi_usd':1_000_000,'n_history_periods':30,
                'venue_observed_at':str(NOW_MS)}
                for c in ('BTC','ETH')}}


class DonorHandlerTests(unittest.TestCase):
    def run_handler(self, doc=None, native_funding=True, index_valid=True):
        mod=load();s3=MemoryS3({'data/crypto-funding.json':doc} if doc is not None else {})
        def provider(url, **kwargs):
            ccy='BTC' if 'BTC' in url else 'ETH';nm=ccy+'-FUTURE'
            if '/ticker?' in url:
                row={'index_price':100 if index_valid else None,'mark_price':100.2,'timestamp':NOW_MS,'current_funding':.99}
                if native_funding:row['funding_8h']=.0001
                return {'result':row}
            if 'get_instruments?' in url:
                return {'result':[{'instrument_name':nm,'settlement_period':'month',
                        'expiration_timestamp':NOW_MS+90*86400000}]}
            if 'get_book_summary_by_currency?' in url:
                return {'result':[{'instrument_name':nm,'mark_price':105,'open_interest':1000,
                        'volume_usd':10000,'timestamp':NOW_MS}]}
            raise AssertionError(url)
        with patch.object(mod,'s3',s3),patch.object(mod,'_get',side_effect=provider), \
             patch.object(mod.urllib.request,'urlopen',side_effect=AssertionError('network forbidden')), \
             contextlib.redirect_stdout(io.StringIO()):
            response=mod.lambda_handler({},None)
        self.assertEqual(response['statusCode'],200)
        self.assertIn(mod.HIST_KEY,s3.writes)
        return s3.writes[mod.OUT_KEY]

    def test_valid_funding_changes_comparison_from_missing_evidence(self):
        absent=self.run_handler()['btc'];out=self.run_handler(funding());row=out['btc']
        self.assertEqual(absent['funding_basis_comparison']['status'],'UNAVAILABLE')
        self.assertEqual(row['funding_basis_comparison']['status'],'CROSS_VENUE_RESEARCH_CONTEXT')
        self.assertAlmostEqual(row['perpetual_funding_context']['annualized_from_observed_interval_pct'],10.95)
        self.assertFalse(row['carry_review_required'])
        self.assertFalse(row['execution_eligible']);self.assertFalse(row['funding_basis_comparison']['locked_carry'])
        self.assertEqual(row['funding_basis_comparison']['dated_basis_venue'],'Deribit')
        self.assertIn('OKX',row['funding_basis_comparison']['perpetual_venue'])
        self.assertEqual(row['curve'][0]['mark_price'],105)
        self.assertIn('not executable net yield',out['interpretation'])

    def test_observed_four_hour_interval_doubles_funding_not_basis(self):
        eight=self.run_handler(funding(8))['btc'];four=self.run_handler(funding(4))['btc']
        self.assertAlmostEqual(four['perpetual_funding_context']['annualized_from_observed_interval_pct'],21.9)
        self.assertEqual(eight['cash_and_carry_yield_3m_pct'],four['cash_and_carry_yield_3m_pct'])
        self.assertAlmostEqual(eight['funding_basis_comparison']['basis_minus_current_funding_annualized_pp']-
                               four['funding_basis_comparison']['basis_minus_current_funding_annualized_pp'],10.95)

    def test_zero_funding_is_observed_zero_never_missing(self):
        row=self.run_handler(funding(rate=0))['btc']
        self.assertTrue(row['perpetual_funding_context']['usable'])
        self.assertEqual(row['perpetual_funding_context']['annualized_from_observed_interval_pct'],0)
        self.assertEqual(row['funding_basis_comparison']['basis_minus_current_funding_annualized_pp'],row['cash_and_carry_yield_3m_pct'])

    def test_reject_stale_future_unknown_interval_wrong_unit_and_invalid_values(self):
        for label,modify in [
            ('stale',lambda d:d.update(generated_at=(NOW-timedelta(hours=3)).isoformat())),
            ('future',lambda d:d.update(generated_at=(NOW+timedelta(hours=1)).isoformat())),
            ('old observation',lambda d:d['by_coin']['BTC'].update(venue_observed_at=str(NOW_MS-3*3600000))),
            ('missing observation',lambda d:d['by_coin']['BTC'].pop('venue_observed_at')),
            ('future observation',lambda d:d['by_coin']['BTC'].update(venue_observed_at=str(NOW_MS+3600000))),
            ('interval absent',lambda d:d['by_coin']['BTC'].pop('funding_interval_hours')),
            ('unknown source',lambda d:d['by_coin']['BTC'].update(interval_source='unknown')),
            ('unit',lambda d:d['by_coin']['BTC'].update(funding_rate_units='percent_per_8h')),
            ('schema',lambda d:d.update(version='0')),
            ('wrong instrument',lambda d:d['by_coin']['BTC'].update(instId='ETH-USDT-SWAP')),
            ('zero interval',lambda d:d['by_coin']['BTC'].update(funding_interval_hours=0)),
            ('nan',lambda d:d['by_coin']['BTC'].update(current_funding_rate=float('nan'))),
            ('negative OI',lambda d:d['by_coin']['BTC'].update(oi_usd=-1)),
            ('bool rate',lambda d:d['by_coin']['BTC'].update(current_funding_rate=True)),
        ]:
            with self.subTest(label=label):
                doc=funding();modify(doc);row=self.run_handler(doc)['btc']
                self.assertFalse(row['perpetual_funding_context']['usable'])
                self.assertIsNone(row['funding_basis_comparison']['basis_minus_current_funding_annualized_pp'])
                self.assertTrue(row['carry_review_required']);self.assertFalse(row['execution_eligible'])

    def test_opposite_direction_triggers_cross_venue_review(self):
        row=self.run_handler(funding(rate=-.0001))['btc']
        self.assertTrue(row['funding_basis_comparison']['opposite_direction_review'])
        self.assertTrue(row['carry_review_required'])

    def test_funding_crowding_changes_review(self):
        doc=funding();doc['by_coin']['BTC']['funding_z_score']=2.5
        row=self.run_handler(doc)['btc']
        self.assertTrue(row['perpetual_funding_context']['crowding_review'])
        self.assertTrue(row['carry_review_required'])

    def test_uncomparable_history_does_not_manufacture_crowding(self):
        doc=funding();doc['by_coin']['BTC'].update(funding_z_score=9,n_history_periods=2)
        row=self.run_handler(doc)['btc']
        self.assertFalse(row['perpetual_funding_context']['crowding_review'])
        self.assertEqual(row['perpetual_funding_context']['crowding_status'],'UNAVAILABLE')
        self.assertTrue(row['carry_review_required'])
        self.assertIsNotNone(row['perpetual_funding_context']['annualized_from_observed_interval_pct'])

    def test_missing_dated_basis_keeps_carry_review_open_even_with_valid_funding(self):
        row=self.run_handler(funding(),index_valid=False)['btc']
        self.assertTrue(row['carry_review_required'])
        self.assertEqual(row['funding_basis_comparison']['status'],'UNAVAILABLE')
        self.assertFalse(row['execution_eligible'])

    def test_deribit_current_funding_is_never_assumed_to_be_eight_hour_rate(self):
        row=self.run_handler(funding(),native_funding=False)['btc']
        self.assertIsNone(row['funding_8h_observed_rate'])
        self.assertIsNone(row['funding_interval_hours'])
        self.assertIsNone(row['funding_annualized_pct'])


if __name__ == '__main__': unittest.main()
