from datetime import date,timedelta
from decimal import Decimal
import copy,json,unittest
from volatility_fixtures import model,originals,sources,STAMP,EVALUATION


class ModelCases(unittest.TestCase):
    def parsed(self,label='VIX_30D',values=None):
        raw=originals(label,values)
        if label in model.PUBLISHER:return model.parse_publisher(label,raw['observations'],EVALUATION)
        return model.parse_fred(label,raw['definition'],raw['observations'],EVALUATION)

    def test_native_precision_and_changes_do_not_round_to_a_display_scale(self):
        d,rows=self.parsed(values=[('2026-09-16','15.123456789001'),('2026-09-17','15.123456789002')])
        m=model.metric('VIX_30D',d,rows,STAMP)
        self.assertEqual(m['value'],15.123456789002)
        self.assertEqual(m['change_points'],1e-12)
        self.assertEqual(m['exact']['change_points'],'1E-12')

    def test_zero_is_visible_and_relative_change_from_zero_is_unavailable(self):
        d,rows=self.parsed(values=[('2026-09-16','0'),('2026-09-17','0')])
        m=model.metric('VIX_30D',d,rows,STAMP)
        self.assertEqual(m['value'],0);self.assertEqual(m['change_points'],0)
        self.assertIsNone(m['change_relative_pct'])

    def test_missing_latest_does_not_jump_to_last_numeric(self):
        d,rows=self.parsed(values=[('2026-09-16','15'),('2026-09-17','.')])
        m=model.metric('VIX_30D',d,rows,STAMP)
        self.assertIsNone(m['value']);self.assertIsNone(m['change_points'])
        self.assertEqual(m['latest_numeric_context']['observation_date'],'2026-09-16')

    def test_missing_previous_does_not_become_multi_day_change(self):
        d,rows=self.parsed(values=[('2026-09-15','12'),('2026-09-16','.'),('2026-09-17','15')])
        m=model.metric('VIX_30D',d,rows,STAMP)
        self.assertIsNone(m['change_points']);self.assertEqual(m['previous_observation_date'],'2026-09-16')

    def test_calendar_gaps_and_historical_weekend_rows_remain_explicit(self):
        d,rows=self.parsed(values=[('2026-09-13','12'),('2026-09-17','15')])
        m=model.metric('VIX_30D',d,rows,STAMP)
        self.assertEqual(m['history_coverage']['weekend_numeric_rows'],1)
        self.assertEqual(m['comparison_gap_days'],4)

    def test_latest_numeric_window_not_oldest_newest_first_slice(self):
        vals=[(str(date(2025,1,1)+timedelta(days=i)),str(i)) for i in range(300)]
        _,rows=self.parsed(values=vals)
        s=model.statistics(rows,Decimal(299),252)
        self.assertEqual(s['mean_index_points'],173.5)
        self.assertEqual(s['first_date'],str(date(2025,1,1)+timedelta(days=48)))

    def test_constant_window_has_no_zscore(self):
        _,rows=self.parsed(values=[(str(date(2026,7,1)+timedelta(days=i)),'20') for i in range(60)])
        s=model.statistics(rows,Decimal(20),60)
        self.assertEqual(s['sample_stddev_points'],0);self.assertIsNone(s['z_score'])

    def test_percentile_requires_real_span_and_ties_get_half_weight(self):
        rows=[{'date':str(date(2025,1,1)+timedelta(days=i)),'value':Decimal(20)} for i in range(400)]
        self.assertEqual(model.percentile(rows,Decimal(20),'2025-01-01')['percentile_pct'],50)
        self.assertIsNone(model.percentile(rows,Decimal(20),'1990-01-01')['percentile_pct'])

    def test_index_definition_mismatch_rejected(self):
        raw=originals('VIX_30D')
        for field,value in (('id','VXVCLS'),('units','Percent'),('frequency_short','M'),('seasonal_adjustment_short','SA'),('title','Unrelated index')):
            with self.subTest(field=field):
                d=json.loads(raw['definition']);d['seriess'][0][field]=value
                with self.assertRaises(ValueError):model.parse_fred('VIX_30D',model.encoded(d),raw['observations'],EVALUATION)

    def test_partial_transformed_mixed_vintage_duplicate_future_responses_rejected(self):
        raw=originals('VIX_30D',[('2026-09-16','15'),('2026-09-17','16')])
        mutations=[lambda d:d.update(count=3),lambda d:d.update(units='pch'),lambda d:d.update(offset=True),
            lambda d:d['observations'][1].update(date='2026-09-16'),lambda d:d['observations'][1].update(date='2026-09-21'),
            lambda d:d['observations'][1].update(realtime_start='2026-09-19')]
        for mutate in mutations:
            d=json.loads(raw['observations']);mutate(d)
            with self.assertRaises(ValueError):model.parse_fred('VIX_30D',raw['definition'],model.encoded(d),EVALUATION)

    def test_publisher_identity_duplicate_and_future_rows_rejected(self):
        for raw in (b'DATE,VVIX\n09/17/2026,87\n',b'DATE,SKEW\n09/17/2026,140\n09/17/2026,141\n',
                    b'DATE,SKEW\n09/21/2026,140\n',b'DATE,SKEW\n09/17/2026,NaN\n',b'DATE,SKEW\n09/17/2026,-1\n'):
            with self.assertRaises(ValueError):model.parse_publisher('SKEW',raw,EVALUATION)

    def test_publisher_null_is_not_zero(self):
        d,rows=self.parsed('VVIX',[('2026-09-17','87'),('2026-09-18','.')])
        m=model.metric('VVIX',d,rows,STAMP)
        self.assertIsNone(m['value']);self.assertEqual(m['original_row_index'],2)

    def test_missing_input_keeps_inventory_and_abstains(self):
        src=sources();src['VIX_30D']={'error':'provider_unavailable'}
        out=model.compute(src,STAMP)
        self.assertEqual(len(out['measurements']),16);self.assertEqual(out['quality']['status'],'partial')
        self.assertIsNone(out['measurements']['VIX_30D']['value']);self.assertIsNone(out['composite_stress_score'])
        self.assertEqual(out['portfolio_action'],'WAIT')

    def test_cross_source_dates_do_not_get_combined_into_a_current_comparison(self):
        src=sources()
        src['VIX_30D']={k:{'raw':v} for k,v in originals('VIX_30D',[('2026-09-16','15'),('2026-09-17','16')]).items()}
        src['VIX_3M']={k:{'raw':v} for k,v in originals('VIX_3M',[('2026-09-16','18')]).items()}
        out=model.compute(src,STAMP);t=out['tenor_comparison']
        self.assertEqual(t['difference_points'],-3);self.assertEqual(t['observation_date'],'2026-09-16')
        self.assertFalse(t['current_comparison_available']);self.assertIsNone(out['term_structure']['ratio_30d_3m'])

    def test_dispersion_requires_all_five_same_date_indices(self):
        src=sources();src['GS_VOL']={'error':'unavailable'}
        out=model.compute(src,STAMP)
        self.assertFalse(out['single_name_dispersion']['current_comparison_available'])
        self.assertIsNone(out['single_name_dispersion']['population_stddev_points'])

    def test_collection_time_does_not_refresh_old_observation(self):
        d,rows=self.parsed(values=[('2026-09-01','15')]);m=model.metric('VIX_30D',d,rows,STAMP)
        self.assertEqual(m['quality']['status'],'stale')
        self.assertEqual(m['source_valid_until'],'2026-09-07T00:00:00+00:00')

    def test_complete_replay_has_no_unsupported_investment_authority(self):
        src=sources();out=model.compute(src,STAMP)
        self.assertEqual(out['quality']['within_age_ceiling'],16)
        for key in model.PERMISSIONS:self.assertIs(out[key],False)
        self.assertIsNone(out['regime']);self.assertIsNone(out['skew']['tail_mult'])
        self.assertIsNone(out['term_structure']['inverted']);self.assertEqual(out['alerts'],[])
        self.assertEqual(model.encoded(out),model.encoded(model.compute(copy.deepcopy(src),STAMP)))


if __name__=='__main__':unittest.main(verbosity=2)
