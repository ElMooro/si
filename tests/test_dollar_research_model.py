from pathlib import Path
from copy import deepcopy
from decimal import Decimal
import sys,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'tests')]
from dollar_fixture import fixture,canonical,STAMP
import dollar_research_model as model


class Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):cls.data=fixture()
    def build(self,originals=None,packet=None,generated=STAMP):
        p,o,c,l,_=self.data
        return model.build(packet or (canonical(originals) if originals is not None else p),o if originals is None else originals,generated,c,l)
    def test_all_source_scopes_and_legacy_references_survive_without_votes(self):
        p=self.build();self.assertEqual(len(p['series']),32);self.assertEqual(len(p['bilaterals']),14);self.assertEqual(len(p['indices']),4)
        self.assertEqual(p['retained_predecessors'],self.data[3]);self.assertEqual(set(p['retained_contexts']),set(model.CONTEXT_KEYS))
        self.assertEqual(p['quality']['within_age_ceilings'],32);self.assertEqual(p['dependency_graph']['independent_votes'],0)
        self.assertTrue(all(p[k] is False for k in model.PERMISSIONS));self.assertIsNone(p['dollar_pressure']);self.assertIsNone(p['risk_transmission']['score'])
        self.assertFalse(p['benchmark_identity']['benchmark_replication_qualified'])
    def test_reciprocal_change_is_not_negated_original_percent(self):
        a={'date':'2026-09-18','value':Decimal('1.2'),'original_row_index':0};b={'date':'2026-08-18','value':Decimal('1.1'),'original_row_index':1}
        raw=model.change(a,b,'USD per EUR');inverse=model.change(a,b,'EUR per USD',True)
        self.assertAlmostEqual(raw['relative_percent']['value'],100/11,places=8)
        self.assertAlmostEqual(inverse['relative_percent']['value'],-100/12,places=8)
        self.assertNotEqual(inverse['relative_percent']['value'],-raw['relative_percent']['value'])
    def test_actual_bilateral_direction_and_cny_identity(self):
        p=self.build()['series'];eur=p['DEXUSEU']['quote'];yen=p['DEXJPUS']['quote'];cny=p['DEXCHUS']['quote']
        self.assertEqual((eur['numerator'],eur['denominator']),('USD','EUR'))
        self.assertEqual((yen['numerator'],yen['denominator']),('JPY','USD'))
        self.assertAlmostEqual(eur['usd_per_foreign_unit']['value']*eur['foreign_units_per_usd']['value'],1,places=8)
        self.assertEqual(cny['currency'],'CNY');self.assertFalse(cny['cnh_substitution_performed'])
    def test_missing_calendar_baseline_is_not_replaced_by_prior_numeric_day(self):
        rows=[{'date':d,'value':v,'original_row_index':i} for i,(d,v) in enumerate([('2026-08-17',Decimal('1')),('2026-08-18',None),('2026-09-18',Decimal('2'))])]
        result=model.comparisons(rows,'DEXUSEU')['month']
        self.assertEqual(result['baseline']['date'],'2026-08-18');self.assertFalse(result['available']);self.assertIsNone(result['dollar_strength']['relative_percent']['value'])
    def test_daily_calendar_lag_bound(self):
        rows=[{'date':d,'value':Decimal(v),'original_row_index':i} for i,(d,v) in enumerate([('2026-08-13','1'),('2026-09-18','2')])]
        self.assertFalse(model.comparisons(rows,'DEXJPUS')['month']['available'])
        rows[0]['date']='2026-08-14';r=model.comparisons(rows,'DEXJPUS')['month']
        self.assertTrue(r['available']);self.assertEqual(r['baseline_lag_days'],4)
    def test_monthly_is_monthly_and_cannot_generate_weekly_change(self):
        row=self.build()['series']['RTWEXBGS'];self.assertEqual(row['frequency'],'M')
        self.assertFalse(row['comparisons']['week']['available']);self.assertEqual(row['comparisons']['month']['baseline']['date'],'2026-07-01')
    def test_missing_current_remains_missing_and_history_is_retained(self):
        original=deepcopy(self.data[1]);original['DEXUSEU']['observations']['observations'][0]['value']='.'
        row=self.build(originals=original)['series']['DEXUSEU']
        self.assertIsNone(row['current_value']);self.assertIsNone(row['latest_observation']['value']);self.assertTrue(row['history'])
        self.assertIsNone(row['quote']['usd_per_foreign_unit']['value'])
    def test_stale_source_preserves_observation_and_cannot_renew_derived_liquidity(self):
        p=self.build(generated='2026-09-23T21:50:00+00:00')
        self.assertEqual(p['quality']['within_age_ceilings'],0);self.assertIsNone(p['series']['DEXUSEU']['current_value'])
        self.assertIsNotNone(p['series']['DEXUSEU']['latest_observation']['value']);self.assertIsNone(p['derived']['net_liquidity']['net'])
    def test_tampered_canonical_value_or_quote_definition_refused(self):
        p=deepcopy(self.data[0]);p['measurements']['DEXUSEU']['current']=9
        with self.assertRaises(ValueError):self.build(packet=p)
        original=deepcopy(self.data[1]);original['DEXUSEU']['definition']['seriess'][0]['units']='EUR per USD'
        with self.assertRaisesRegex(ValueError,'definition differs'):self.build(originals=original)
    def test_missing_source_is_explicit_and_not_zero(self):
        original=deepcopy(self.data[1]);del original['DEXTAUS'];p=self.build(originals=original)
        self.assertEqual(p['quality']['available_original_histories'],31);self.assertEqual(p['quality']['status'],'partial_descriptive')
        self.assertIsNone(p['series']['DEXTAUS']['latest_observation']);self.assertIsNone(p['series']['DEXTAUS']['current_value'])
    def test_us_germany_uses_matching_closed_months_and_all_returned_us_members(self):
        p=self.build()['derived']['us_germany_monthly'];last=p['latest']
        self.assertEqual(last['reference_month'],'2026-08-01');self.assertEqual(last['german']['date'],'2026-08-01')
        self.assertTrue(last['available']);self.assertTrue(all(x['date'].startswith('2026-08') for x in last['us_daily_members']))
        sample=[Decimal(x['exact_value']) for x in last['us_daily_members'] if x['exact_value'] is not None]
        expected=100*(sum(sample)/len(sample)-Decimal(last['german']['exact_value']))
        self.assertAlmostEqual(last['difference_bps']['value'],float(expected),places=9)
        self.assertFalse(last['calendar_completeness_verified'])
    def test_monthly_mean_requires_coverage_and_minimum_numeric_rows(self):
        histories={'DGS10':[{'date':'2026-08-31','value':Decimal('4'),'original_row_index':0}],
            'IRLTLT01DEM156N':[{'date':'2026-08-01','value':Decimal('3'),'original_row_index':0}]}
        out=model.monthly_rate_comparison(histories,STAMP)['latest'];self.assertFalse(out['available']);self.assertIsNone(out['difference_bps']['value'])
    def test_mixed_liquidity_units_dates_and_zero_are_explicit(self):
        original=deepcopy(self.data[1]);original['RRPONTSYD']['observations']['observations'][0]['value']='0'
        p=self.build(originals=original);out=p['derived']['net_liquidity'];self.assertEqual(out['components']['RRPONTSYD']['value_decimal'],'0')
        self.assertEqual(out['components']['WALCL']['date'],'2026-09-16');self.assertEqual(out['components']['RRPONTSYD']['date'],'2026-09-18')
        self.assertEqual(out['net'],0);self.assertIn('weekly average',out['basis'])
    def test_curve_must_match_actual_observation_dates(self):
        p=self.build();rows=p['series'];rows['DGS2']['latest_observation']['date']='2026-09-17'
        self.assertFalse(model.matched_curve(rows)['available'])
    def test_duplicate_negative_fx_and_future_provider_rows(self):
        original=deepcopy(self.data[1]['DEXJPUS']);row=deepcopy(original['observations']['observations'][0]);original['observations']['observations'].append(row)
        with self.assertRaisesRegex(ValueError,'Duplicate'):model.history(original,'DEXJPUS','2026-09-21')
        original['observations']['observations'].pop();original['observations']['observations'][0]['value']='-1'
        with self.assertRaisesRegex(ValueError,'Nonpositive'):model.history(original,'DEXJPUS','2026-09-21')
        original['observations']['observations'][0].update(date='2026-09-22',value='1')
        self.assertTrue(all(x['date']<='2026-09-21' for x in model.history(original,'DEXJPUS','2026-09-21')))
    def test_future_rows_do_not_become_observed_when_replayed_later(self):
        original=deepcopy(self.data[1]);original['DEXJPUS']['observations']['observations'][0].update(date='2026-09-22',value='999')
        early=self.build(originals=original)['series']['DEXJPUS']
        later=self.build(originals=original,generated='2026-09-23T21:50:00+00:00')['series']['DEXJPUS']
        self.assertEqual(later['history'],early['history']);self.assertEqual(later['latest_observation'],early['latest_observation'])
        self.assertEqual(later['history_scope']['source_coverage']['future_observations_excluded'],1)
        self.assertTrue(all(p['date']<'2026-09-22' for p in later['history']))

if __name__=='__main__':unittest.main(verbosity=2)
