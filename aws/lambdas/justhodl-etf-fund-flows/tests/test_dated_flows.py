"""Regression tests for the measurements consumed by the capital-flow radar."""
import sys
import unittest
from datetime import datetime,timezone,timedelta
from pathlib import Path
ROOT=Path(__file__).resolve().parents[4]
sys.path[:0]=[str(ROOT/'tests'),str(ROOT/'aws/shared')]
from provider_metrics_privacy_helpers import load
engine,_=load('justhodl-etf-fund-flows')
NOW=datetime.now(timezone.utc).date()


def snapshot(n=25,value=100):
    rows=[{'processed_date':(NOW-timedelta(days=i)).isoformat(),'effective_date':(NOW-timedelta(days=i)).isoformat(),'flow':value+i} for i in range(n)]
    return {'ticker':'SPY','processed_date':rows[0]['processed_date'],'effective_date':rows[0]['effective_date'],'aum_usd':10000,'history':rows}


class DatedFlow(unittest.TestCase):
    def test_short_window_not_labeled_five_or_twenty_one(self):
        out=engine.dated_flow_measurement(snapshot(3),NOW)
        self.assertEqual(out['daily_flow_usd'],100)
        self.assertIsNone(out['windows']['5']['sum_usd'])
        self.assertIsNone(out['windows']['21']['sum_usd'])

    def test_missing_latest_flow_never_substitutes_previous_value(self):
        s=snapshot();s['history'][0]['flow']=None
        out=engine.dated_flow_measurement(s,NOW)
        self.assertEqual(out['quality']['status'],'unavailable')
        self.assertIsNone(out['daily_flow_usd']);self.assertIsNone(out['windows']['5']['sum_usd'])

    def test_missing_inside_window_is_not_skipped(self):
        s=snapshot();s['history'][2]['flow']=None
        self.assertIsNone(engine.dated_flow_measurement(s,NOW)['windows']['5']['sum_usd'])

    def test_zero_is_preserved_and_breaks_persistence(self):
        s=snapshot(value=0);out=engine.dated_flow_measurement(s,NOW)
        self.assertEqual(out['daily_flow_usd'],0);self.assertEqual(out['persistence_observations'],0)
        self.assertEqual(out['windows']['5']['sum_usd'],10)

    def test_old_observation_not_refreshed_by_publication(self):
        out=engine.dated_flow_measurement(snapshot(),NOW+timedelta(days=8))
        self.assertEqual(out['quality']['status'],'stale');self.assertIsNone(out['aum_usd'])

    def test_conflicting_duplicate_date_blocks_measurement(self):
        s=snapshot();s['history'].append({**s['history'][0],'flow':999})
        self.assertEqual(engine.dated_flow_measurement(s,NOW)['quality']['status'],'invalid')

    def test_effective_date_is_separate_from_processing_date(self):
        s=snapshot()
        for row in s['history']:row['effective_date']=(datetime.strptime(row['processed_date'],'%Y-%m-%d').date()-timedelta(days=1)).isoformat()
        s['effective_date']=s['history'][0]['effective_date']
        out=engine.dated_flow_measurement(s,NOW)
        self.assertEqual(out['date_basis'],'effective_date');self.assertEqual(out['quality']['age_days'],1)

    def test_processing_date_alone_does_not_certify_freshness(self):
        s=snapshot()
        for row in s['history']:row.pop('effective_date')
        out=engine.dated_flow_measurement(s,NOW)
        self.assertEqual(out['quality']['status'],'unvalidated')
        self.assertIsNone(out['daily_flow_usd'])

    def test_sixty_prior_values_required_for_standardization(self):
        self.assertIsNone(engine.dated_flow_measurement(snapshot(30),NOW)['flow_zscore_60observations'])
        self.assertIsNotNone(engine.dated_flow_measurement(snapshot(61),NOW)['flow_zscore_60observations'])


if __name__=='__main__':unittest.main()
