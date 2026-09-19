import importlib.util
import math
import sys
import types
import unittest
from datetime import datetime,timezone,timedelta
from pathlib import Path
from unittest.mock import patch
with patch.dict(sys.modules,{'boto3':types.SimpleNamespace(client=lambda *a,**k:None),'managed_secret':types.SimpleNamespace(managed_secret=lambda *a,**k:'test'),'_fred_shim':types.SimpleNamespace()}):
    spec=importlib.util.spec_from_file_location('yen_test',Path(__file__).resolve().parents[1]/'source/lambda_function.py')
    e=importlib.util.module_from_spec(spec);spec.loader.exec_module(e)
NOW=datetime.now(timezone.utc).date()
def daily(n=100):return [((NOW-timedelta(days=i)).isoformat(),100+math.sin(i/4)) for i in range(n)]
def cot(n):
    return [{'cftc_contract_market_code':'097741','report_date_as_yyyy_mm_dd':(NOW-timedelta(weeks=i)).isoformat(),
             'lev_money_positions_long':'100','lev_money_positions_short':str(101+i),'open_interest_all':'1000'} for i in range(n)]


class Yen(unittest.TestCase):
    def test_missing_history_does_not_become_calm_or_neutral(self):
        out=e.build_measurements({},e.cftc_measurement([],NOW),NOW)
        self.assertEqual(out['quality']['status'],'unavailable')
        self.assertIsNone(out['unwind_risk_score']);self.assertIsNone(out['boj_injection_score'])
        self.assertEqual(out['fx_detonator']['vol_regime'],'UNAVAILABLE')
        self.assertIsNone(out['call'])

    def test_change_requires_actual_old_endpoint(self):
        self.assertIsNone(e.daily_change(daily(10),30))
        self.assertIsNone(e.daily_change([daily(100)[0],daily(100)[90]],30))
        self.assertIsNotNone(e.daily_change(daily(100),30))

    def test_volatility_requires_full_daily_window(self):
        self.assertIsNone(e.daily_vol(daily(15),20))
        self.assertIsNone(e.daily_vol(daily(45)[::2]+[('2020-01-01',100)],60))
        self.assertIsNotNone(e.daily_vol(daily(30),20))

    def test_monthly_change_is_calendar_based(self):
        self.assertEqual(e.monthly_change([('2026-08-01',110),('2026-02-01',100)],6,True),10.000000000000009)
        self.assertIsNone(e.monthly_change([('2026-08-01',110),('2025-02-01',100)],6))

    def test_monthly_gap_uses_common_month_not_latest_daily_rate(self):
        today=datetime(2026,9,17).date()
        us=[(f'2026-07-{i:02}',4) for i in range(1,29)]+[('2026-09-16',20)]
        gap=e.matched_monthly_gap(us,[('2026-07-01',1)],today)
        self.assertEqual(gap['spread_pp'],3);self.assertEqual(gap['observation_month'],'2026-07')

    def test_cftc_scope_and_long_baseline(self):
        short=e.cftc_measurement(cot(8),NOW)
        self.assertIsNone(short['net_pct_oi_zscore'])
        long=e.cftc_measurement(cot(261),NOW)
        self.assertIsNotNone(long['net_pct_oi_zscore']);self.assertEqual(long['baseline_weeks'],260)
        self.assertIsNone(long['whole_carry_trade_size']);self.assertIsNone(long['crowded_short'])
        self.assertEqual(long['score_contribution'],0)

    def test_stale_cftc_current_is_not_published(self):
        out=e.cftc_measurement(cot(261),NOW+timedelta(days=20))
        self.assertIsNone(out['current']);self.assertEqual(out['quality']['status'],'stale')

    def test_interbank_not_relabelled_as_policy_and_assets_scaled(self):
        out=e.build_measurements({'boj_assets':[(NOW.isoformat(),6000000)],'jp_rate_3m':[(NOW.isoformat(),1)]},e.cftc_measurement([],NOW),NOW)
        self.assertEqual(out['boj_funding_leg']['boj_assets_jpy_trillion'],600)
        self.assertEqual(out['boj_funding_leg']['policy_direction'],'NOT_MEASURED')
        self.assertIsNone(out['carry_width']['executable_carry'])


if __name__=='__main__':
    from test_originals import Originals
    unittest.main()
