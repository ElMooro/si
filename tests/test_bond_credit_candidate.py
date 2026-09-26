from pathlib import Path
from fractions import Fraction
import copy,json,sys,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'aws/ops/checks'))
import bond_credit_candidate as model
AT='2026-09-26T19:00:00Z'


def fixture():
    p={'contract':'credit-native-research.v1','generated_at':'2026-09-26T15:00:00Z','freshness':{'valid_until':'2026-09-28T03:00:00Z'},'measurements':{},'comparisons':{},'source_evidence':[],**{k:False for k in model.FLAGS}}
    for sid in {s for pair in model.PAIRS.values() for s in pair}:
        p['measurements'][sid]={'series_id':sid,'kind':'oas','source_unit':'Percent','unit':'percent','observation_date':'2026-09-25','original_row_index':10,'exact':{'value_pct':'1.25'},'value_pct':1.25,'value_bps':125,'collected_at':p['generated_at'],'source_valid_until':'2026-09-30T00:00:00Z','quality':{'status':'within_age_ceiling'},**{k:False for k in model.FLAGS}}
        for kind in ('definition','observations'):p['source_evidence'].append({'series_id':sid,'kind':kind,'sha256':'a'*64,'key':'audit-private/20260909-originals/credit-research/'+'a'*64+'.bin','bytes':100,'acquired_at':'2026-09-26T14:59:00Z'})
    for name,(left,right) in model.PAIRS.items():p['comparisons'][name]={'id':name,'left_series_id':left,'right_series_id':right,'unit':'basis_points','left_original_row_index':10,'right_original_row_index':10,'observation_date':'2026-09-25','left_latest_date':'2026-09-25','right_latest_date':'2026-09-25','both_latest_dates_match':True,'current_comparison_available':True,'value_bps':0,'value_pp':0,**{k:False for k in model.FLAGS}}
    return p


class Tests(unittest.TestCase):
    def test_exact_same_date_zero_and_four_named_comparisons(self):
        out=model.project(model.encode(fixture()),AT);self.assertEqual(set(out['comparisons']),set(model.PAIRS))
        for row in out['comparisons'].values():self.assertEqual(row['value_bps'],0);self.assertEqual(row['value_decimal'],'0.00');self.assertEqual(len(row['components']),2)
        self.assertEqual(out['independent_votes'],0);self.assertIsNone(out['default_probability'])

    def test_large_percent_is_always_scaled_not_guessed_from_magnitude(self):
        p=fixture();sid=model.PAIRS['ccc_minus_bb'][0];p['measurements'][sid].update(value_pct=51,value_bps=5100,exact={'value_pct':'51.00'})
        p['comparisons']['ccc_minus_bb'].update(value_bps=4975,value_pp=49.75)
        row=model.project(model.encode(p),AT)['comparisons']['ccc_minus_bb'];self.assertEqual(row['value_bps'],4975)
        self.assertEqual(Fraction(row['value_decimal']),100*(Fraction('51.00')-Fraction('1.25')))

    def test_units_identity_dates_original_indices_and_arithmetic_cannot_self_certify(self):
        for mutate in (lambda p:p['comparisons']['ccc_minus_bb'].update(unit='percent'),lambda p:p['comparisons']['ccc_minus_bb'].update(value_bps=1),lambda p:p['comparisons']['ccc_minus_bb'].update(value_bps=False),lambda p:p['measurements']['BAMLH0A3HYC'].update(original_row_index=False),lambda p:p['measurements']['BAMLH0A3HYC'].update(observation_date='2026-09-24'),lambda p:p['measurements']['BAMLH0A3HYC'].update(value_bps=1.25),lambda p:p['measurements']['BAMLH0A3HYC'].update(source_unit='Basis points')):
            p=fixture();mutate(p);row=model.project(model.encode(p),AT)['comparisons']['ccc_minus_bb'];self.assertIsNone(row['value_bps']);self.assertEqual(row['status'],'unavailable')

    def test_missing_expired_future_and_duplicate_source_are_explicit(self):
        for at in ('2026-09-28T04:00:00Z','2026-09-26T14:00:00Z'):
            out=model.project(model.encode(fixture()),at);self.assertTrue(all(r['value_bps'] is None for r in out['comparisons'].values()))
        p=fixture();del p['measurements']['BAMLH0A3HYC'];self.assertIsNone(model.project(model.encode(p),AT)['legacy_fields']['ccc_minus_bb_bps'])
        p=fixture();p['source_evidence'].append(copy.deepcopy(p['source_evidence'][0]))
        with self.assertRaises(ValueError):model.project(model.encode(p),AT)
        p=fixture();p['calls_eligible']=True
        with self.assertRaises(ValueError):model.project(model.encode(p),AT)
        with self.assertRaises(ValueError):model.project(b'{"x":1,"x":2}',AT)

    def test_old_acquisition_cannot_borrow_a_recent_packet_clock(self):
        p=fixture()
        for ref in p['source_evidence']:
            if ref['series_id']=='BAMLH0A3HYC':ref['acquired_at']='2025-09-26T14:59:00Z'
        out=model.project(model.encode(p),AT)
        self.assertEqual(out['comparisons']['ccc_minus_bb']['reason'],'protected_original_reference_differs')
        self.assertIsNone(out['legacy_fields']['ccc_minus_bb_bps'])

if __name__=='__main__':unittest.main()
