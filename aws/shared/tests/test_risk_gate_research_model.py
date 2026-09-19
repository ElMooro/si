from copy import deepcopy
from datetime import datetime,timedelta
import unittest

import risk_gate_research_model as model
from risk_gate_research_catalog import SERIES,extend_catalog
from report_observations import build as macro_build,digest
from test_report_observations import inputs,NOW


def fixture():
    original={
        'RRPONTSYD':inputs('RRPONTSYD','D',[('2026-09-18','.576'),('2026-08-18','.5')],'Billions of US Dollars'),
        'RIFSPPNA2P2D90NB':inputs('RIFSPPNA2P2D90NB','D',[('2026-09-17','4.29'),('2026-09-16','4.27')],'Percent'),
        'DCPN3M':inputs('DCPN3M','D',[('2026-09-17','4.06'),('2026-09-16','4.02')],'Percent'),
        'TRUCKD11':inputs('TRUCKD11','M',[('2026-06-01','113.3'),('2025-06-01','110')],'Index, 2015 = 100')}
    source=macro_build(extend_catalog({}),original,NOW)
    source['replay']={'manifest_key':'data/report-research/runs/'+'a'*64+'.json','output_sha256':digest(source)}
    return source,original


def recompile(original):
    source=macro_build(extend_catalog({}),original,NOW)
    source['replay']={'manifest_key':'data/report-research/runs/'+'a'*64+'.json','output_sha256':digest(source)}
    return source


class Tests(unittest.TestCase):
    def test_exact_cp_rate_difference_has_no_threshold_or_portfolio_authority(self):
        source,original=fixture();before=deepcopy((source,original))
        fleet={'data/example.json':{'score':99,'posture':'RISK_ON','sizing_multiplier':1}}
        out=model.build(source,None,fleet,original,NOW)
        diff=out['derived']['cp_a2p2_minus_aa_90d']
        self.assertEqual(diff['value_decimal'],'23.00');self.assertEqual(diff['value'],23)
        self.assertEqual(diff['observation_date'],'2026-09-17');self.assertEqual(diff['unit'],'basis_points')
        self.assertEqual(diff['inputs'][0]['latest_value_decimal'],'4.29')
        self.assertEqual(out['series']['RRPONTSYD']['latest_value'],.576)
        self.assertFalse(out['series']['TRUCKD11']['available']);self.assertEqual(out['series']['TRUCKD11']['last_observed_value'],'113.3')
        self.assertEqual(set(out['series']),set(SERIES));self.assertEqual(before,(source,original))
        self.assertIsNone(out['composite']);self.assertIsNone(out['sizing_multiplier'])
        self.assertEqual(out['posture'],'UNAVAILABLE');self.assertEqual(out['decision']['verb'],'WAIT')
        self.assertFalse(out['portfolio_consequences']['allows_new_entries']);self.assertFalse(out['portfolio_consequences']['forced_liquidation'])
        self.assertEqual(out['fleet_context']['inputs']['data/example.json']['content_sha256'],digest(fleet['data/example.json']))
        self.assertTrue(all(d['independent_votes']==0 and not d['sizing_eligible'] for d in out['derived'].values()))

    def test_missing_trade_mismatched_date_and_wrong_units_never_produce_cp_spread(self):
        for variant in ('missing_trade','date','units','old_source'):
            with self.subTest(variant=variant):
                source,original=fixture();item=original['RIFSPPNA2P2D90NB'];stamp=NOW
                if variant=='missing_trade':item['observations']['observations'][0]['value']='.'
                elif variant=='date':item['observations']['observations'][0]['date']='2026-09-18'
                elif variant=='units':item['definition']['seriess'][0]['units']='Basis Points'
                elif variant=='old_source':stamp=(datetime.fromisoformat(NOW)+timedelta(days=2)).isoformat()
                source=recompile(original);out=model.build(source,None,{},original,stamp)
                diff=out['derived']['cp_a2p2_minus_aa_90d'];self.assertIsNone(diff['value']);self.assertEqual(diff['status'],'unavailable')

    def test_zero_and_negative_matched_differences_are_preserved(self):
        for value,expected in [('4.06','0.00'),('4.00','-6.00')]:
            source,original=fixture();original['RIFSPPNA2P2D90NB']['observations']['observations'][0]['value']=value
            out=model.build(recompile(original),None,{},original,NOW)
            self.assertEqual(out['derived']['cp_a2p2_minus_aa_90d']['value_decimal'],expected)

    def test_tampered_source_or_original_and_missing_original_fail_before_publication(self):
        for variant in ('packet','original','missing'):
            source,original=fixture()
            if variant=='packet':source['measurements']['DCPN3M']['current']=99
            elif variant=='original':original['DCPN3M']['observations']['observations'][0]['value']='99'
            else:del original['DCPN3M']
            with self.assertRaises(ValueError):model.build(source,None,{},original,NOW)

    def test_expiry_removes_current_calculations_but_preserves_historical_context(self):
        source,original=fixture();later=(datetime.fromisoformat(NOW)+timedelta(days=2)).isoformat()
        out=model.build(source,None,{},original,later)
        self.assertEqual(out['quality']['fresh_series'],0)
        self.assertEqual(out['series']['RRPONTSYD']['calendar_comparisons'],{})
        self.assertTrue(out['series']['RRPONTSYD']['history'])
        self.assertTrue(all(x['value'] is None for x in out['derived'].values()))


if __name__=='__main__':unittest.main()
