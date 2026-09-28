from pathlib import Path
from datetime import date,timedelta
from copy import deepcopy
import sys,unittest
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'aws/lambdas/justhodl-velocity-acceleration/source'))
import volume_observations as m
ASOF=date(2026,9,28)


def rows(n=30):return [{'symbol':'SYNA','date':(ASOF-timedelta(days=n-i)).isoformat(),'volume':100,'open':100,'high':102,'low':98,'close':100} for i in range(n)]


def metrics(data):return {r['name']:r for r in m.calculate(data,'SYNA',ASOF)['measurements']}


class Tests(unittest.TestCase):
    def test_baseline_includes_real_zeros_and_exact_twenty_denominator(self):
        data=rows(27);data[0]['volume']=0;out=metrics(data)
        self.assertEqual(out['baseline_volume_mean_20']['value'],95)
        self.assertEqual(len(out['baseline_volume_mean_20']['operands']),20)
        self.assertEqual(out['baseline_volume_mean_20']['operands'][0]['value_exact'],'0')
    def test_zero_latest_ratio_is_measured_but_missing_never_borrows_previous(self):
        data=rows();data[-2]['volume']=500;data[-1]['volume']=0
        self.assertEqual(metrics(data)['last_relative_volume']['value'],0)
        data[-1]['volume']=None;out=metrics(data)
        self.assertIsNone(out['last_relative_volume']['value']);self.assertIsNone(out['relative_volume_slope_7']['value'])
    def test_first_recent_close_uses_the_actual_preceding_baseline_close(self):
        data=rows(27);data[19].update(open=50,high=52,low=48,close=50)
        out=m.calculate(data,'SYNA',ASOF)
        self.assertEqual(out['recent_observations'][0]['close_change_fraction'],1)
        self.assertEqual(out['recent_observations'][0]['previous_close_operand']['source_pointer'],'/19/close')
        self.assertAlmostEqual(metrics(data)['volume_weighted_close_direction_7']['value'],1/7)
    def test_linear_relative_volume_slope_has_independent_known_answer(self):
        data=rows(27)
        for i in range(7):data[20+i]['volume']=100*(i+1)
        out=metrics(data);self.assertEqual(out['relative_volume_slope_7']['value'],1)
        self.assertEqual(out['relative_volume_floor_change']['value'],3)
        self.assertEqual(len(out['relative_volume_slope_7']['operands']),27)
    def test_reported_baseline_preserves_fractional_calculation_denominator(self):
        data=rows(27);data[0]['volume']=101
        out=metrics(data);self.assertEqual(out['baseline_volume_mean_20']['value_exact'],'100.05')
        self.assertAlmostEqual(out['last_relative_volume']['value'],100/100.05)
    def test_zero_baseline_and_zero_early_floor_are_undefined_not_regularized(self):
        data=rows(27)
        for r in data[:20]:r['volume']=0
        out=metrics(data);self.assertEqual(out['baseline_volume_mean_20']['value'],0)
        self.assertIsNone(out['last_relative_volume']['value']);self.assertEqual(out['last_relative_volume']['reason'],'zero_baseline')
        data=rows(27);data[20]['volume']=0;out=metrics(data)
        self.assertIsNone(out['relative_volume_floor_change']['value']);self.assertEqual(out['relative_volume_floor_change']['reason'],'zero_early_floor')
    def test_missing_dated_member_cannot_be_dropped_or_backfilled(self):
        data=rows(40);data[-3]['volume']=None;out=m.calculate(data,'SYNA',ASOF)
        self.assertEqual(out['selected_source_indices'],list(range(13,40)))
        self.assertIsNone(next(r for r in out['measurements'] if r['name']=='relative_volume_slope_7')['value'])
        self.assertEqual(out['recent_observations'][-3]['ratio_status'],'missing_recent_volume')
    def test_boolean_price_and_volume_are_rejected_per_measurement(self):
        data=rows();data[-1]['close']=True;out=m.calculate(data,'SYNA',ASOF)
        self.assertIsNone(out['recent_observations'][-1]['close_change_fraction'])
        self.assertEqual(metrics(data)['last_relative_volume']['value'],1)
        data[-1]['volume']=True;self.assertIsNone(metrics(data)['last_relative_volume']['value'])
    def test_wrong_issuer_bad_date_and_duplicate_dates_block_the_window(self):
        for edit in ({'symbol':'OTHER'},{'date':'not a date'},{'date':rows()[1]['date']}):
            data=rows();data[0].update(edit);out=metrics(data)
            self.assertTrue(all(r['value'] is None for r in out.values()))
    def test_current_future_and_outside_query_dates_are_explicitly_excluded(self):
        data=rows(60);data.extend([dict(data[-1],date=ASOF.isoformat()),dict(data[-1],date=(ASOF+timedelta(days=1)).isoformat())])
        out=m.calculate(data,'SYNA',ASOF);self.assertEqual(out['selected_source_indices'][-1],59)
        self.assertEqual(len([r for r in out['row_issues'] if r['reason']=='outside_declared_completed_date_window']),5)
        self.assertEqual(out['observation_age_calendar_days'],1)
    def test_descending_source_order_and_short_windows_keep_original_coordinates(self):
        data=rows()[::-1];out=m.calculate(data,'SYNA',ASOF)
        self.assertEqual(out['recent_observations'][-1]['source_index'],0)
        short=metrics(rows(26));self.assertTrue(all(r['value'] is None for r in short.values()))
    def test_no_forecast_score_confirmation_or_independent_votes_from_transforms(self):
        out=m.calculate(rows(),'SYNA',ASOF);self.assertIsNone(out['composite_score']);self.assertIsNone(out['confirmation'])
        self.assertIsNone(out['independent_roots']);self.assertEqual(out['reported_source_family'],'issuer_ohlcv')
        for item in [out,*out['measurements'],*out['recent_observations']]:
            for flag in m.FLAGS:self.assertIs(item[flag],False)


if __name__=='__main__':unittest.main(verbosity=2)
