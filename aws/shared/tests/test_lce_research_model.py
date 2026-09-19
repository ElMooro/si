from copy import deepcopy
from datetime import datetime,timedelta
import unittest

import lce_research_model as model
from lce_research_catalog import extend_catalog,SERIES
from report_observations import build as macro_build,digest
from test_report_observations import inputs,NOW


def fixture():
    original={
        'RRPONTSYD':inputs('RRPONTSYD','D',[('2026-09-18','.576'),('2026-08-18','.5'),('2025-09-18','1')],'Billions of US Dollars'),
        'WALCL':inputs('WALCL','W',[('2026-09-16','6746548'),('2026-09-09','6737000')],'Millions of U.S. Dollars'),
        'TOTRESNS':inputs('TOTRESNS','M',[('2026-07-01','3051.5'),('2026-06-01','3000')],'Billions of Dollars'),
        'DRTSCILM':inputs('DRTSCILM','Q',[('2026-07-01','0'),('2026-04-01','-2')],'Percent')}
    catalog={sid:SERIES[sid] for sid in original}
    source=macro_build(catalog,original,NOW)
    source['replay']={'manifest_key':'data/report-research/runs/'+('a'*64)+'.json','output_sha256':digest(source)}
    return source,original


class Tests(unittest.TestCase):
    def test_narrative_context_preserves_native_units_dates_and_expiry(self):
        source,original=fixture();packet=model.build(source,None,None,original,NOW)
        packet['replay']={'manifest_key':'data/lce-research/runs/'+('a'*64)+'.json','output_sha256':digest(packet)}
        context=model.narrative_context(packet,datetime.fromisoformat(NOW))
        row=next(r for r in context['measurements'] if r['series_id']=='WALCL')
        self.assertEqual(row['value'],'6746548');self.assertEqual(row['unit'],'Millions of U.S. Dollars')
        self.assertEqual(row['observation_date'],'2026-09-16');self.assertFalse(context['sizing_eligible'])
        self.assertEqual(model.narrative_context(packet,datetime.fromisoformat(NOW)+timedelta(days=2))['measurements'],[])
        packet['series']['WALCL']['latest_value_decimal']='999'
        self.assertEqual(model.narrative_context(packet,datetime.fromisoformat(NOW))['status'],'unavailable')

    def test_native_units_and_calendar_changes_survive_without_threshold_authority(self):
        source,original=fixture();before=deepcopy((source,original))
        out=model.build(source,None,None,original,NOW)
        rows=out['series'];self.assertEqual(rows['RRPONTSYD']['latest_value'],.576)
        self.assertEqual(rows['TOTRESNS']['latest_value'],3051.5)
        self.assertEqual(rows['WALCL']['latest_value'],6746548)
        self.assertEqual(rows['WALCL']['_units'],'Millions of U.S. Dollars')
        self.assertIsNone(rows['TOTRESNS']['wow_pct']);self.assertIsNone(rows['DRTSCILM']['mom_pct'])
        self.assertEqual(rows['DRTSCILM']['calendar_comparisons']['quarter']['change_decimal'],'2')
        self.assertEqual(rows['DRTSCILM']['calendar_comparisons']['quarter']['change_unit'],'percentage_points')
        self.assertIsNone(rows['DRTSCILM']['qoq_pct'])
        self.assertEqual(out['quality']['expected_series'],51);self.assertEqual(out['quality']['fresh_series'],4)
        self.assertEqual(out['decision']['meaning'],'abstain');self.assertIsNone(out['composite']['score'])
        self.assertEqual(out['interpretation']['target_allocation'],[])
        self.assertTrue(all(row['signal'] is None and row['call'] is None for row in rows.values()))
        self.assertEqual(before,(source,original))

    def test_expired_source_clears_current_values_and_changes_but_retains_history(self):
        source,original=fixture()
        later=(datetime.fromisoformat(NOW)+timedelta(days=2)).isoformat()
        out=model.build(source,None,None,original,later);row=out['series']['RRPONTSYD']
        self.assertFalse(row['available']);self.assertIsNone(row['latest_value']);self.assertIsNone(row['mom_pct'])
        self.assertEqual(row['calendar_comparisons'],{});self.assertTrue(row['history'])
        self.assertEqual(row['last_observed_value'],'0.576');self.assertEqual(out['quality']['fresh_series'],0)

    def test_tampered_original_packet_and_missing_originals_fail_closed(self):
        for mutation in ('packet','original','missing'):
            source,original=fixture()
            if mutation=='packet':source['measurements']['RRPONTSYD']['current']=900
            elif mutation=='original':original['RRPONTSYD']['observations']['observations'][0]['value']='999'
            else:del original['RRPONTSYD']
            with self.assertRaises(ValueError):model.build(source,None,None,original,NOW)

    def test_missing_current_cannot_fall_back_and_short_window_cannot_claim_five_year_z(self):
        source,original=fixture()
        out=model.build(source,None,None,original,NOW)
        self.assertIsNone(out['series']['RRPONTSYD']['z_5y'])
        self.assertEqual(out['series']['RRPONTSYD']['statistics']['5y']['status'],'insufficient_history')
        self.assertEqual(out['series']['RRPONTSYD']['statistics']['1y']['numeric_observations'],3)
        item=original['RRPONTSYD'];item['observations']['observations'][0]['value']='.'
        source=macro_build({'RRPONTSYD':SERIES['RRPONTSYD']},{'RRPONTSYD':item},NOW)
        source['replay']={'manifest_key':'data/report-research/runs/'+('a'*64)+'.json','output_sha256':digest(source)}
        row=model.build(source,None,None,{'RRPONTSYD':item},NOW)['series']['RRPONTSYD']
        self.assertFalse(row['available']);self.assertIsNone(row['latest_value']);self.assertIsNone(row['z_1y'])

    def test_extending_collector_catalog_keeps_existing_labels_and_all_requested_ids(self):
        original={'WALCL':{'category':'custom','display_name':'existing'},'OTHER':{'category':'other'}}
        out=extend_catalog(original)
        self.assertTrue(set(SERIES)<=set(out));self.assertEqual(out['WALCL'],original['WALCL'])
        self.assertIn('OTHER',out);self.assertEqual(len(original),2)


if __name__=='__main__':unittest.main()
