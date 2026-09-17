import importlib.util
import io
import json
import sys
import types
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch
with patch.dict(sys.modules,{'boto3':types.SimpleNamespace(client=lambda *a,**k:None),
                            'managed_secret':types.SimpleNamespace(managed_secret=lambda *a,**k:'test')}):
    spec=importlib.util.spec_from_file_location('sentinel_test',Path(__file__).resolve().parents[1]/'source/lambda_function.py')
    e=importlib.util.module_from_spec(spec);spec.loader.exec_module(e)


def business_dates(start,n):
    d=date.fromisoformat(start);out=[]
    while len(out)<n:
        if d.weekday()<5:out.append(d.isoformat())
        d+=timedelta(days=1)
    return out


DATES=business_dates('2010-01-01',1300)
YIELDS=[(d,5.1 if i in (250,550,850,1150) else 4) for i,d in enumerate(DATES)]
PRICES=[(d,100+i/10) for i,d in enumerate(DATES)]


class Integrity(unittest.TestCase):
    def test_daily_price_returns_and_true_even_sample_median(self):
        from statistics import median
        r=e.episode_study(YIELDS,PRICES)['cross_5.00']
        self.assertEqual(r['n_valid_3m'],4)
        values=[round((PRICES[i+63][1]/PRICES[i][1]-1)*100,2) for i in (250,550,850,1150)]
        self.assertEqual(r['median_spx_3m'],round(median(values),2))
        self.assertIn('price-only',r['return_basis'])

    def test_episode_before_price_coverage_cannot_jump_decades(self):
        r=e.episode_study(YIELDS,[(d,100+i) for i,d in enumerate(business_dates('2026-01-01',100))])['cross_5.00']
        self.assertEqual(r['n'],4)
        self.assertEqual(r['n_valid_3m'],0)
        self.assertIsNone(r['median_spx_3m'])
        self.assertEqual(r['episodes'][0]['quality']['3m'],'OUTSIDE_PRICE_COVERAGE')

    def test_monthly_or_gapped_rows_not_treated_as_daily_sessions(self):
        r=e.episode_study(YIELDS,PRICES[::21])['cross_5.00']
        self.assertIsNone(r['median_spx_3m'])
        self.assertEqual(r['n_valid_3m'],0)

    def test_incomplete_horizon_and_small_sample_not_a_hit_rate(self):
        r=e.episode_study(YIELDS[:551],PRICES[:570])['cross_5.00']
        self.assertEqual(r['n_valid_3m'],1)
        self.assertIsNone(r['median_spx_3m'])
        self.assertIsNone(r['neg_3m_hit_rate_pct'])
        self.assertEqual(r['episodes'][-1]['quality']['3m'],'HORIZON_NOT_COMPLETE')

    def test_625_percent_is_quarantined(self):
        prices=[(d,725.77 if i>=313 else 100) for i,d in enumerate(DATES)]
        r=e.episode_study(YIELDS,prices)['cross_5.00']
        self.assertIsNone(r['episodes'][0]['spx_3m'])
        self.assertEqual(r['episodes'][0]['quality']['3m'],'QUARANTINED_IMPLAUSIBLE_SPX_RETURN')

    def run_handler(self,missing=False,stale=False):
        now=datetime.now(timezone.utc).date()
        ds=business_dates((now-timedelta(days=1900)).isoformat(),1200)
        if not stale:
            offset=(now-date.fromisoformat(ds[-1])).days-1
            ds=[(date.fromisoformat(d)+timedelta(days=offset)).isoformat() for d in ds]
        docs={};calls=[]
        def fred(sid,**kw):
            calls.append(sid)
            return [] if missing else [(d,4.9 if sid=='DGS10' else 2 if sid=='DFII10' else 100+i/10) for i,d in enumerate(ds)]
        def read(**kw):return {'Body':io.BytesIO(json.dumps(docs.get(kw['Key'],{})).encode())}
        with patch.object(e,'S3',types.SimpleNamespace(get_object=read,put_object=lambda **kw:docs.update({kw['Key']:json.loads(kw['Body'])}))), \
             patch.object(e,'fred_series',side_effect=fred),patch.object(e,'telegram',side_effect=AssertionError('message sent')), \
             patch.object(e,'yahoo_daily',side_effect=AssertionError('unvalidated max-range provider used')):
            self.assertEqual(e.lambda_handler({'suppress_alerts':True})['statusCode'],200)
        return docs[e.OUT_KEY],calls

    def test_real_handler_uses_official_price_series_and_no_causal_claim(self):
        out,calls=self.run_handler()
        self.assertIn('SP500',calls)
        self.assertEqual(out['quality']['status'],'fresh')
        self.assertIsNone(out['yields_driving_stocks'])
        self.assertFalse(out['execution_eligible'])

    def test_missing_or_stale_yield_expires_live_fields(self):
        for kw in ({'missing':True},{'stale':True}):
            out,_=self.run_handler(**kw)
            self.assertIsNone(out['level'])
            self.assertEqual(out['tier'],'UNKNOWN')

    def test_nonfinite_zero_prices_are_excluded(self):
        self.assertEqual(e.clean_daily([('2026-01-01',float('nan')),('2026-01-02',0)],positive=True),[])


if __name__=='__main__':unittest.main()
