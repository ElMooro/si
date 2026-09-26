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

    def run_handler(self,missing=False,stale=False,count=1200,previous=None,event=None,conflict=False):
        now=datetime.now(timezone.utc).date()
        ds=business_dates((now-timedelta(days=1900)).isoformat(),1200)
        if not stale:
            offset=(now-date.fromisoformat(ds[-1])).days-1
            ds=[(date.fromisoformat(d)+timedelta(days=offset)).isoformat() for d in ds]
        ds=ds[-count:]
        docs={e.OUT_KEY:previous or {}};calls=[];writes=[]
        def fred(sid,**kw):
            calls.append(sid)
            return [] if missing else [(d,4.9 if sid=='DGS10' else 2 if sid=='DFII10' else 100+i/10) for i,d in enumerate(ds)]
        def read(**kw):return {'Body':io.BytesIO(json.dumps(docs.get(kw['Key'],{})).encode()),'ETag':'fixture'}
        def write(**kw):
            writes.append(kw)
            if conflict and 'IfMatch' in kw:raise RuntimeError('Concurrent current-head update')
            docs[kw['Key']]=json.loads(kw['Body'])
        with patch.object(e,'S3',types.SimpleNamespace(get_object=read,put_object=write)), \
             patch.object(e,'fred_series',side_effect=fred),patch.object(e,'telegram',side_effect=AssertionError('message sent')), \
             patch.object(e,'yahoo_daily',side_effect=AssertionError('unvalidated max-range provider used')):
            self.assertEqual(e.lambda_handler({'suppress_alerts':True} if event is None else event)['statusCode'],200)
        self.writes=writes
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
            if kw.get('stale'):
                trace=out['velocity']['comparisons']['60_observations']
                self.assertIsNotNone(trace['end_date']);self.assertFalse(trace['current_usable'])

    def test_nonfinite_zero_prices_are_excluded(self):
        self.assertEqual(e.clean_daily([('2026-01-01',float('nan')),('2026-01-02',0)],positive=True),[])

    def test_exact_window_boundary_and_zero_change_have_dated_endpoints(self):
        for n in (20,60):
            rows=[(d,4+i/100) for i,d in enumerate(DATES[:n+1])]
            trace=e.observed_change(rows,n)
            self.assertEqual(trace['value'],n)
            self.assertEqual(trace['start_date'],rows[0][0]);self.assertEqual(trace['end_date'],rows[-1][0])
            self.assertGreater(trace['elapsed_calendar_days'],n)
            self.assertIsNone(e.observed_change(rows[:-1],n)['value'])
        out,_=self.run_handler(count=61)
        self.assertEqual(out['velocity']['d60_bps'],0)
        self.assertEqual(out['velocity']['comparisons']['60_observations']['intervals'],60)

    def test_missing_change_is_not_a_zero_or_calendar_day_claim(self):
        out,_=self.run_handler(count=60)
        self.assertIsNone(out['velocity']['d60_bps'])
        self.assertIn('60-observation yield change: unavailable',out['tier_reason'])
        self.assertNotIn('Δ60d',out['tier_reason'])
        self.assertIsNone(out['negative_equity_yield_association'])

    def test_complete_episode_population_and_all_valid_returns_have_price_endpoints(self):
        dates=business_dates('2000-01-01',4300)
        yields=[(d,5.1 if i>=250 and (i-250)%300==0 else 4) for i,d in enumerate(dates)]
        prices=[(d,100+i/10) for i,d in enumerate(dates)]
        row=e.episode_study(yields,prices)['cross_5.00']
        self.assertGreater(row['n'],12);self.assertEqual(len(row['episodes']),row['n'])
        self.assertTrue(row['episodes_complete'])
        for episode in row['episodes']:
            for horizon,n in (('1w',5),('1m',21),('3m',63)):
                trace=episode['return_inputs'][horizon]
                self.assertEqual(trace['subsequent_observations'],n)
                self.assertEqual(episode['spx_'+horizon],round((trace['end_value']/trace['start_value']-1)*100,2))

    def test_unqualified_threshold_never_sends_an_alert_or_gains_authority(self):
        out,_=self.run_handler(previous={'tier':'BENIGN'},event={})
        self.assertEqual(out['tier'],'RED')
        for key in ('alert_sent','alert_eligible','calls_eligible','sizing_eligible','execution_eligible','forecast_qualified'):
            self.assertIs(out[key],False)
        self.assertEqual(out['bus_cross']['independent_votes'],0)
        self.assertFalse(out['bus_cross']['source_independence_verified'])
        self.assertEqual(self.writes[-1]['IfMatch'],'fixture')

    def test_failed_conditional_bus_overlay_cannot_overwrite_or_retry(self):
        out,_=self.run_handler(conflict=True)
        self.assertEqual(len(self.writes),2)
        self.assertNotIn('bus_cross',out)

    def test_complete_predecessor_is_preserved(self):
        import hashlib
        raw=(Path(__file__).parent/'legacy_before_observation_trace.py.txt').read_bytes()
        self.assertEqual(len(raw),14925)
        self.assertEqual(hashlib.sha256(raw).hexdigest(),'ef18f7fb8e74fe03829e74774817d9449e75bc13084afc9a9f20376eec6b7183')


if __name__=='__main__':unittest.main()
