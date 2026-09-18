import copy
from datetime import date, timedelta
import unittest
from report_observations import measurement, liquidity, build, CONTRACT

NOW = '2026-09-18T20:00:00+00:00'


def inputs(sid='TEST', frequency='M', rows=None, unit='Index'):
    data = rows or [('2026-08-01', '110'), ('2026-07-01', '100'), ('2026-05-01', '90'), ('2025-08-01', '80')]
    evidence = {part: {'contract':'source-evidence.v1', 'captured':True, 'key':'data/evidence/'+part+'.bin.gz',
                       'sha256':'a'*64, 'source_url':'https://api.stlouisfed.org/fred/series'+('/observations' if part=='observations' else '')+'?series_id='+sid+('&units=lin' if part=='observations' else '')}
                for part in ('definition','observations')}
    return {'definition': {'seriess':[{'id':sid,'title':sid,'frequency_short':frequency,'units':unit,'seasonal_adjustment':'Not Seasonally Adjusted'}]},
            'observations': {'units':'lin','count':len(data),'limit':400,'offset':0,'observations':[{'date':d,'value':v} for d,v in data]},
            'evidence': evidence, 'acquired_at':NOW}


def compile(item=None, sid='TEST', stamp=NOW):
    x = item or inputs(sid)
    return measurement(sid, x['definition'], x['observations'], x['evidence'], stamp, x['acquired_at'])


class ReportObservationsTests(unittest.TestCase):
    def test_monthly_changes_use_calendar_periods_not_row_offsets(self):
        out = compile()
        self.assertIsNone(out['week_pct'])
        self.assertEqual(out['month_pct'], 10)
        self.assertEqual(out['changes']['month']['baseline_date'], '2026-07-01')
        self.assertEqual(out['year_pct'], 37.5)
        self.assertEqual(out['coverage']['returned'], 4)

    def test_missing_month_is_not_substituted_with_previous_valid_month(self):
        out = compile(inputs(rows=[('2026-08-01','110'),('2026-06-01','100')]))
        self.assertIsNone(out['month_pct'])
        out = compile(inputs(rows=[('2026-08-01','110'),('2026-07-01','.'),('2026-06-01','100')]))
        self.assertIsNone(out['month_pct']); self.assertIsNone(out['prev'])

    def test_missing_current_is_preserved_not_carried_forward(self):
        out = compile(inputs(rows=[('2026-08-01','.'),('2026-07-01','100')]))
        self.assertIsNone(out['current']); self.assertEqual(out['date'],'2026-08-01')
        self.assertEqual(out['quality']['status'],'unavailable')

    def test_daily_weekend_alignment_and_bounded_gap(self):
        out = compile(inputs(frequency='D', rows=[('2026-09-18','110'),('2026-08-17','100')]))
        self.assertEqual(out['month_pct'],10); self.assertIsNone(out['week_pct'])
        out = compile(inputs(frequency='D',rows=[('2026-09-18','110'),('2026-08-10','100')]))
        self.assertIsNone(out['month_pct'])

    def test_weekly_and_quarterly_horizons_are_not_trading_day_offsets(self):
        out = compile(inputs(frequency='W',rows=[('2026-09-12','196000'),('2026-09-05','195000')], unit='Number'))
        self.assertEqual(out['current'],196000)
        self.assertEqual(out['changes']['week']['change'],1000)
        out = compile(inputs(frequency='Q',rows=[('2026-07-01','101'),('2026-04-01','100')]))
        self.assertIsNone(out['month_pct']); self.assertEqual(out['quarter_pct'],1)

    def test_relative_percent_and_percentage_points_are_separate(self):
        out=compile(inputs(rows=[('2026-08-01','4.2'),('2026-07-01','4.0')],unit='Percent'))
        self.assertEqual(out['changes']['month']['change_decimal'],'0.2')
        self.assertEqual(out['changes']['month']['change_unit'],'percentage_points')
        self.assertEqual(out['month_pct'],5)

    def test_nonpositive_base_is_not_fake_zero_percent(self):
        for base in ('0','-2'):
            out=compile(inputs(rows=[('2026-08-01','2'),('2026-07-01',base)]))
            self.assertIsNone(out['month_pct']); self.assertIsNotNone(out['change'])

    def test_change_in_a_reported_growth_rate_is_percentage_points(self):
        out=compile(inputs(frequency='Q',rows=[('2026-04-01','1.5'),('2026-01-01','0.5')],unit='Percent Change from Preceding Period'))
        change=out['changes']['quarter']
        self.assertEqual(change['change_decimal'],'1.0')
        self.assertEqual(change['change_unit'],'percentage_points')
        self.assertEqual(change['source_unit'],'Percent Change from Preceding Period')

    def test_duplicates_future_dates_and_wrong_series_fail_closed(self):
        for rows in [[('2026-08-01','1'),('2026-08-01','2')],[('2027-01-01','1')]]:
            with self.assertRaises(ValueError):compile(inputs(rows=rows))
        x=inputs(); x['definition']['seriess'][0]['id']='OTHER'
        with self.assertRaises(ValueError):compile(x)
        x=inputs(); x['evidence']['observations']['source_url']=x['evidence']['observations']['source_url'].replace('TEST','OTHER')
        with self.assertRaises(ValueError):compile(x)

    def test_source_age_and_observation_age_are_separate(self):
        out=compile(inputs(rows=[('2025-01-01','1')]))
        self.assertEqual(out['quality']['status'],'stale')
        x=inputs();x['acquired_at']='2026-09-16T20:00:00+00:00'
        self.assertEqual(compile(x)['quality']['status'],'stale_source')

    def test_provider_page_count_and_offset_must_match_the_request(self):
        for patch in ({'count':5}, {'offset':1}, {'limit':3}):
            x=inputs();x['observations'].update(patch)
            with self.assertRaises(ValueError):compile(x)

    def test_preadvertised_policy_dates_are_retained_but_not_observations(self):
        out=compile(inputs('IORB','D',[('2026-09-21','3.9'),('2026-09-19','3.9'),('2026-09-18','3.9'),('2026-09-17','3.9')],'Percent'),'IORB')
        self.assertEqual(out['date'],'2026-09-18');self.assertEqual(out['current_row_index'],2)
        self.assertEqual(out['coverage']['returned'],4)
        self.assertEqual(out['coverage']['future_observations_excluded'],2)
        self.assertTrue(all(r['date']<='2026-09-18' for r in out['history']))
        self.assertEqual([r['date'] for r in out['future_dated_rows']],['2026-09-21','2026-09-19'])

    def test_exact_units_zero_rrp_and_mixed_dates(self):
        items={}
        for sid,unit,value,day in [('WALCL','Millions of U.S. Dollars','6746548','2026-09-16'),
                                 ('WTREGEN','Millions of U.S. Dollars','877028','2026-09-16'),
                                 ('RRPONTSYD','Billions of US Dollars','0','2026-09-18')]:
            items[sid]=compile(inputs(sid,'D',[(day,value)],unit),sid)
        result=liquidity(items)
        self.assertEqual(result['net_decimal'],'5869520');self.assertIsNone(result['direction'])
        items['RRPONTSYD']['current_decimal']='20000'
        self.assertEqual(liquidity(items)['net_decimal'],'-14130480')
        items['RRPONTSYD']['unit']='Millions of U.S. Dollars'
        self.assertIsNone(liquidity(items)['net'])

    def test_degraded_packet_names_all_missing_series_and_assigns_no_authority(self):
        out=build({'TEST':{},'MISSING':{}},{'TEST':inputs()},NOW)
        self.assertEqual(out['quality']['fresh_series'],1)
        self.assertEqual(out['quality']['status'],'degraded')
        self.assertEqual(out['errors']['MISSING'],'source_unavailable')
        self.assertFalse(out['sizing_eligible']);self.assertIsNone(out['call'])


if __name__=='__main__':unittest.main()
