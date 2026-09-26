"""Original conservation, matched endpoints, independent arithmetic and bad data."""
from pathlib import Path
from copy import deepcopy
from datetime import date,timedelta
from decimal import localcontext,ROUND_UP
import ast,hashlib,json,sys,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/p) for p in ('aws/shared','aws/shared/tests','aws/ops/checks','scripts')]
import yield_curve_candidate as model
import verify_yield_curve_arithmetic as independent
from test_report_observations import inputs,NOW


def ref(raw=b'whole preserved predecessor'):
    digest=hashlib.sha256(raw).hexdigest()
    return {'key':model.PRIVATE+digest+'.bin','sha256':digest,'bytes':len(raw)}


def context():
    return {'source_key':'data/term-premium.json','status':'retained_unqualified_context',
        'original':ref(b'whole ACM packet'),'captured_at':NOW,'independent_votes':0}


def source(originals):
    available={s:o for s,o in originals.items() if o is not None}
    with localcontext() as arithmetic:
        arithmetic.prec=28
        out=model.observations.build({s:{} for s in available},available,NOW)
    out['replay']={'manifest_key':'data/report-research/runs/'+'a'*64+'.json','output_sha256':model.observations.digest(out)}
    return out


def fixture():
    originals={}
    for j,(sid,spec) in enumerate(model.SPECS.items()):
        rows=[(str(date(2026,9,17)-timedelta(days=i*2)),str((400+j*5-i)/100)) for i in range(80)]
        item=inputs(sid,'D',rows,'Percent');item['definition']['seriess'][0]['frequency']=spec['reviewed_definition'][2]
        originals[sid]=item
    return originals


def build(originals,stamp=NOW):return model.build(source(originals),originals,stamp,context(),ref())


class Tests(unittest.TestCase):
    def test_complete_predecessor_inventory_and_exact_tenors(self):
        tree=ast.parse((ROOT/'tests/fixtures/legacy-yield-curve-before-native.py.txt').read_text(encoding='utf8'))
        names={n.targets[0].id:n.value for n in tree.body if isinstance(n,ast.Assign) and len(n.targets)==1 and isinstance(n.targets[0],ast.Name)}
        series=tuple(ast.literal_eval(row.elts[0]) for name in ('NOMINAL_TENORS','REAL_TENORS','BREAKEVENS','EXTRAS') for row in names[name].elts)
        self.assertEqual(model.SERIES,series);self.assertEqual(len(series),23)
        self.assertEqual(model.SPECS['DGS1MO']['tenor_months'],1)
        self.assertEqual(model.SPECS['DFII20']['tenor_months'],240)

    def test_complete_rows_exact_math_and_no_input_mutation(self):
        originals=fixture();before=deepcopy(originals);out=build(originals)
        proof=independent.verify(json.loads(model.observations.encoded(out)),source(originals),originals)
        self.assertEqual(proof['original_rows'],1840);self.assertEqual(proof['matched_observation_comparisons'],140)
        self.assertEqual(proof['current_series'],23);self.assertEqual(before,originals)
        butterfly=out['derived']['butterfly_2_5_10']['current']['measurement']['value']
        self.assertEqual(out['derived']['curvature_2_5_10']['current']['measurement']['value'],2*butterfly)

    def test_observation_lags_disclose_actual_calendar_span_and_bps(self):
        out=build(fixture());comparison=out['series']['DGS2']['current_observation_comparisons']['5']
        self.assertEqual(comparison['elapsed_calendar_days'],10)
        self.assertAlmostEqual(comparison['change']['value'],5)
        self.assertEqual(comparison['change_unit'],'basis_points')

    def test_missing_latest_is_preserved_and_never_backfilled_as_current(self):
        originals=fixture();originals['DGS2']['observations']['observations'][0]['value']='.'
        out=build(originals);independent.verify(out,source(originals),originals)
        self.assertIsNone(out['derived']['2s10s']['current']);self.assertIsNone(out['shape']['label'])
        self.assertFalse(out['curves']['nominal']['complete']);self.assertTrue(out['curves']['real']['complete'])
        self.assertEqual(out['series']['DGS2']['history'][0]['native_value'],'.')

    def test_missing_tenor_never_changes_full_curve_denominator(self):
        originals=fixture();originals['DGS20']=None;out=build(originals)
        independent.verify(out,source(originals),originals)
        self.assertIsNone(out['derived']['nominal_mean']['current'])
        self.assertEqual(out['derived']['nominal_mean']['divisor'],11)
        self.assertEqual(out['quality']['missing_series'],['DGS20'])

    def test_every_spread_uses_the_same_dated_legs_even_with_gaps(self):
        originals=fixture();doc=originals['DGS2']['observations'];doc['observations'][3]['value']='.'
        out=build(originals);independent.verify(out,source(originals),originals)
        comparison=out['derived']['2s10s']['current_comparisons']['5']
        self.assertEqual(comparison['elapsed_calendar_days'],12)
        self.assertEqual(comparison['unmatched_or_missing_dates'],1)
        self.assertEqual(out['shape']['baseline_date'],comparison['baseline']['observation_date'])

    def test_lagging_available_tenor_aligns_without_forward_fill(self):
        originals=fixture();doc=originals['DGS20']['observations'];doc['observations'].pop(0);doc['count']-=1
        out=build(originals);independent.verify(out,source(originals),originals)
        self.assertEqual(out['curves']['nominal']['observation_date'],'2026-09-15')
        self.assertEqual(out['derived']['nominal_mean']['current']['legs']['DGS10']['original_row'],1)

    def test_future_rows_retained_and_excluded(self):
        originals=fixture();doc=originals['DGS2']['observations'];doc['observations'].insert(0,{'date':'2099-01-01','value':'99'});doc['count']+=1
        out=build(originals);independent.verify(out,source(originals),originals)
        self.assertEqual(len(out['series']['DGS2']['history']),81)
        self.assertEqual(out['derived']['2s10s']['current']['legs']['DGS2']['original_row'],1)

    def test_stale_source_and_observation_have_separate_clocks(self):
        originals=fixture();out=build(originals,'2026-09-20T20:00:00+00:00')
        independent.verify(out,source(originals),originals)
        self.assertEqual(out['series']['DGS2']['quality']['status'],'stale_source')
        self.assertIsNone(out['derived']['2s10s']['current'])
        self.assertIsNotNone(out['derived']['2s10s']['last_matched'])
        out=build(originals,'2026-10-01T20:00:00+00:00')
        self.assertEqual(out['series']['DGS2']['quality']['status'],'stale_observation')

    def test_definition_drift_cannot_gain_curve_authority(self):
        for key,value in (('units','Index'),('frequency','Daily, 7-Day'),('seasonal_adjustment','Seasonally Adjusted')):
            originals=fixture();originals['DGS2']['definition']['seriess'][0][key]=value
            out=build(originals);independent.verify(out,source(originals),originals)
            self.assertEqual(out['series']['DGS2']['quality']['status'],'definition_mismatch')
            self.assertIsNone(out['derived']['2s10s']['last_matched']['measurement']['value'])

    def test_zero_negative_and_equal_opposite_changes_are_valid(self):
        originals=fixture()
        for sid in ('DGS2','DGS10'):
            for i,row in enumerate(originals[sid]['observations']['observations']):row['value']=str(i/100 if sid=='DGS2' else -i/100)
        out=build(originals);independent.verify(out,source(originals),originals)
        self.assertEqual(out['series']['DGS2']['current']['value'],0)
        self.assertEqual(out['shape']['label'],'UNCHANGED_MEAN_STEEPENING')
        self.assertIsNone(out['qualified_term_premium_bps']);self.assertEqual(out['signals'],[])

    def test_invalid_duplicate_or_precision_records_fail(self):
        for value in ('NaN','Infinity','abc',True,'1e-30'):
            originals=fixture();originals['DGS2']['observations']['observations'][0]['value']=value
            with self.assertRaises(ValueError):build(originals)
        originals=fixture();doc=originals['DGS2']['observations'];doc['observations'][1]['date']=doc['observations'][0]['date']
        with self.assertRaises(ValueError):build(originals)

    def test_source_and_retained_result_tampering_detected(self):
        originals=fixture();packet=source(originals);packet['measurements']['DGS2']['current']=999
        with self.assertRaises(ValueError):model.build(packet,originals,NOW,context(),ref())
        out=build(originals);out['derived']['2s10s']['current']['measurement']['exact_decimal']='999'
        with self.assertRaises(AssertionError):independent.verify(out,source(originals),originals)

    def test_original_provider_float_expansions_survive_exactly(self):
        originals=fixture();raw='4.3573699999999995'
        originals['SOFR30DAYAVG']['observations']['observations'][0]['value']=raw
        out=build(originals);independent.verify(out,source(originals),originals)
        self.assertEqual(out['series']['SOFR30DAYAVG']['current']['exact_decimal'],raw)
        self.assertEqual(out['series']['SOFR30DAYAVG']['history'][0]['native_value'],raw)

    def test_missing_acm_remains_named_and_never_uses_residual(self):
        originals=fixture();ctx={'source_key':'data/term-premium.json','status':'missing','original':None,'independent_votes':0}
        out=model.build(source(originals),originals,NOW,ctx,ref());independent.verify(out,source(originals),originals)
        self.assertIsNone(out['qualified_term_premium_bps']);self.assertEqual(out['term_premium_context'],ctx)

    def test_fixed_decimal_context_and_future_clock_guards(self):
        originals=fixture();expected=build(originals)
        with localcontext() as arithmetic:
            arithmetic.prec=7;arithmetic.rounding=ROUND_UP
            actual=model.build(source(originals),originals,NOW,context(),ref())
        self.assertEqual(expected,actual)
        with self.assertRaises(ValueError):build(originals,'2026-09-17T00:00:00+00:00')

if __name__=='__main__':unittest.main(verbosity=2)
