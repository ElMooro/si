"""Whole-source conservation and independent arithmetic, including boundary cases."""
from pathlib import Path
from copy import deepcopy
from datetime import date, timedelta
from decimal import localcontext, ROUND_UP
import hashlib, json, sys, unittest
ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/p) for p in ('aws/ops/checks','aws/shared','aws/shared/tests','scripts',
    'aws/lambdas/justhodl-liquidity-flow/source','aws/lambdas/justhodl-crisis-composite/tests')]
import liquidity_agent_candidate as model
import verify_liquidity_agent_arithmetic as independent
from liquidity_agent_input_inventory import inventory
from test_report_observations import inputs, NOW
from test_native_research import fixtures


def ref(raw=b'whole retained predecessor'):
    digest = hashlib.sha256(raw).hexdigest()
    return {'key':model.PRIVATE+digest+'.bin','sha256':digest,'bytes':len(raw)}


def contexts():
    return {key:{'status':'retained_unqualified_context','original':ref(key.encode()),
        'captured_at':NOW,'independent_votes':0,'same_snapshot_as_baseline':False}
        for key in model.CONTEXT_KEYS}


def recompile(originals):
    source = model.observations.build({s:{} for s in originals},originals,NOW)
    source['replay'] = {'manifest_key':'data/report-research/runs/'+'a'*64+'.json',
        'output_sha256':model.observations.digest(source)}
    return source


def synthetic():
    originals = {}
    for sid,spec in model.SPECS.items():
        if not spec['reviewed_definition']: continue
        unit,freq,basis,season = spec['reviewed_definition']
        rows = [('2026-09-16','6.5'),('2026-09-09','5.0'),('2026-08-14','4.5'),('2026-06-16','4'),('2025-09-16','3')]
        if freq == 'M': rows = [('2026-08-01','6.5'),('2026-07-01','5'),('2026-05-01','4.5'),('2025-08-01','3')]
        if freq == 'Q': rows = [('2026-07-01','6.5'),('2026-04-01','5'),('2025-07-01','3')]
        if sid == 'SP500':
            # Gaps between observations make 60 calendar days unlike 60 rows.
            rows = [(str(date(2026,9,16)-timedelta(days=i*2)),str(100+i)) for i in range(70)]
            rows[10] = (rows[10][0],'200')
        item = inputs(sid,freq,rows,unit)
        item['definition']['seriess'][0].update(frequency=basis,seasonal_adjustment=season,
            seasonal_adjustment_short='NSA' if season == 'Not Seasonally Adjusted' else 'SA')
        originals[sid] = item
    return recompile(originals),originals


def build(source, originals, stamp=NOW):
    return model.build(source,originals,stamp,contexts(),ref())


class Tests(unittest.TestCase):
    def test_entire_catalog_matches_real_inventory_including_hidden_input(self):
        original=(ROOT/'aws/lambdas/justhodl-liquidity-agent/source/lambda_function.py').read_bytes()
        self.assertEqual(list(model.SERIES),inventory(original)['all_series'])
        self.assertEqual(sum(s['reviewed_definition'] is not None for s in model.SPECS.values()),61)
        self.assertEqual(model.SPECS['WORAL']['group'],'fed_repo_assets')
        self.assertEqual(model.SPECS['THREEFYTP10']['reviewed_definition'][1],'D')
        self.assertIn('Kim-Wright',model.DEFINITION_NOTES['THREEFYTP10'])

    def test_real_retained_originals_are_not_replaced_by_synthetic_inputs(self):
        source, originals=fixtures()
        originals={s:o for s,o in originals.items() if s in model.SERIES}
        ctx=contexts()
        for item in ctx.values():item['captured_at']=source['generated_at']
        output=model.build(source,originals,source['generated_at'],ctx,ref())
        proof=independent.verify(output,source,originals)
        self.assertGreater(proof['original_rows'],5000)
        self.assertGreater(proof['missing_series'],0)
        self.assertEqual(proof['flow_verification']['calendar_dates_checked'],180)

    def test_all_available_identities_complete_rows_and_independent_math(self):
        source, originals=synthetic(); before=deepcopy((source,originals))
        out=build(source,originals); proof=independent.verify(out,source,originals)
        self.assertEqual(proof['reconstructed_series'],61);self.assertEqual(proof['missing_series'],12)
        self.assertEqual(proof['current_series'],61);self.assertEqual(proof['calendar_comparisons'],244)
        self.assertEqual(proof['original_rows'],sum(len(o['observations']['observations']) for o in originals.values()))
        self.assertEqual(before,(source,originals))
        self.assertEqual(out['series']['TOTRESNS']['usd_billions']['multiplier'],'1')
        self.assertEqual(out['series']['WORAL']['usd_billions']['multiplier'],'0.001')
        self.assertIsNone(out['series']['DPCREDIT']['usd_billions']['multiplier'])
        independent.verify(json.loads(model.observations.encoded(out)),source,originals)

    def test_calendar_return_and_window_high_are_different_and_not_causal(self):
        source,originals=synthetic();out=build(source,originals);independent.verify(out,source,originals)
        sp=out['derived']['sp500_60_calendar_days']
        self.assertEqual(sp['baseline']['original_row'],30)
        self.assertEqual(sp['window_high']['original_row'],10)
        self.assertEqual(sp['numeric_window_rows'],31)
        self.assertAlmostEqual(sp['historical_comparison']['price_return_pct']['value'],-23.07692307692308)
        self.assertEqual(sp['historical_comparison']['distance_from_window_high_pct']['value'],-50)
        self.assertFalse(sp['total_return']);self.assertFalse(sp['causal_sequence_verified'])

    def test_latest_and_baseline_missing_rows_are_not_backfilled(self):
        _,originals=synthetic()
        originals['WALCL']['observations']['observations'][0]['value']='.'
        originals['SP500']['observations']['observations'][30]['value']='.'
        source=recompile(originals);out=build(source,originals);independent.verify(out,source,originals)
        self.assertIsNone(out['series']['WALCL']['current']['value'])
        self.assertIsNone(out['derived']['net_liquidity']['current'])
        sp=out['derived']['sp500_60_calendar_days']
        self.assertEqual(sp['baseline']['original_row'],30)
        self.assertIsNone(sp['historical_comparison']['price_return_pct']['value'])
        self.assertEqual(sp['historical_comparison']['distance_from_window_high_pct']['value'],-50)

    def test_future_source_records_are_preserved_without_becoming_current(self):
        _,originals=synthetic();sp=originals['SP500']['observations']
        sp['observations'].insert(0,{'date':'2099-01-01','value':'999999'});sp['count']+=1
        source=recompile(originals);out=build(source,originals);independent.verify(out,source,originals)
        self.assertEqual(out['series']['SP500']['history'][0]['observation_date'],'2099-01-01')
        self.assertEqual(out['series']['SP500']['original_row'],1)
        self.assertEqual(out['derived']['sp500_60_calendar_days']['window_high']['native_value'],'200')

    def test_stale_acquisition_keeps_history_and_withholds_current_claims(self):
        source,originals=synthetic()
        stamp=(model.clock(NOW)+timedelta(hours=27)).isoformat()
        out=build(source,originals,stamp);proof=independent.verify(out,source,originals)
        self.assertEqual(proof['current_series'],0)
        self.assertIsNone(out['derived']['net_liquidity']['current'])
        self.assertIsNone(out['derived']['sp500_60_calendar_days']['current_comparison'])
        self.assertTrue(out['series']['SP500']['historical_calendar_comparisons'])

    def test_definition_drift_never_acquires_current_authority(self):
        for sid,field,value in (('WALCL','units','Billions of Dollars'),('WORAL','frequency','Daily'),
                               ('M2SL','seasonal_adjustment','Not Seasonally Adjusted'),('SP500','units','Percent')):
            with self.subTest(sid=sid):
                _,originals=synthetic();originals[sid]['definition']['seriess'][0][field]=value
                source=recompile(originals);out=build(source,originals);independent.verify(out,source,originals)
                self.assertEqual(out['series'][sid]['quality']['status'],'definition_mismatch')
                self.assertIsNone(out['series'][sid]['current']['value']);self.assertTrue(out['series'][sid]['history'])

    def test_newly_available_unreviewed_series_remains_named_and_ineligible(self):
        _,originals=synthetic();originals['OFRFSI']=inputs('OFRFSI','D',[('2026-09-16','-0.2')])
        source=recompile(originals);out=build(source,originals);proof=independent.verify(out,source,originals)
        self.assertEqual(proof['reconstructed_series'],62)
        self.assertEqual(out['series']['OFRFSI']['quality']['status'],'definition_unreviewed')
        self.assertIsNone(out['series']['OFRFSI']['current']['value'])

    def test_all_missing_does_not_fabricate_a_neutral_score(self):
        source=recompile({});out=build(source,{});proof=independent.verify(out,source,{})
        self.assertEqual(proof['original_rows'],0);self.assertEqual(proof['missing_series'],73)
        self.assertEqual(out['quality']['status'],'unavailable')
        self.assertIsNone(out['uncalibrated_legacy_metrics']['composite_score'])

    def test_bad_original_and_rehashed_fabrication_rejected(self):
        source,originals=synthetic();originals['SP500']['observations']['observations'][0]['value']='900'
        with self.assertRaisesRegex(ValueError,'Original reconstruction'):build(source,originals)
        source,originals=synthetic();source['measurements']['SP500']['current']=900
        source['replay']['output_sha256']=model.observations.digest({k:v for k,v in source.items() if k!='replay'})
        with self.assertRaisesRegex(ValueError,'Original reconstruction'):build(source,originals)

    def test_nonfinite_input_is_invalid_not_a_missing_observation(self):
        _,originals=synthetic();originals['SP500']['observations']['observations'][4]['value']='NaN'
        source=recompile(originals)
        with self.assertRaisesRegex(ValueError,'Invalid original'):build(source,originals)

    def test_verifier_catches_row_loss_wrong_unit_and_return_tampering(self):
        source,originals=synthetic();clean=build(source,originals)
        for mutate in (lambda o:o['series']['SP500']['history'].pop(),
            lambda o:o['series']['WALCL']['usd_billions'].update(exact_decimal='6500'),
            lambda o:o['derived']['sp500_60_calendar_days']['baseline'].update(original_row=60),
            lambda o:o.update(calls_eligible=True)):
            out=deepcopy(clean);mutate(out)
            with self.assertRaises(AssertionError):independent.verify(out,source,originals)

    def test_ambient_decimal_settings_and_future_clocks_cannot_change_result(self):
        source,originals=synthetic();expected=build(source,originals)
        with localcontext() as ctx:
            ctx.prec=4;ctx.rounding=ROUND_UP
            self.assertEqual(build(source,originals),expected)
        with self.assertRaises(ValueError):build(source,originals,'2025-01-01T00:00:00Z')

    def test_context_inventory_and_complete_private_references_required(self):
        source,originals=synthetic();ctx=contexts();ctx.pop('data/fred-cache.json')
        with self.assertRaises(ValueError):model.build(source,originals,NOW,ctx,ref())
        ctx=contexts();ctx['data/china-liquidity.json']['original']['key']='data/portfolio.json'
        with self.assertRaises(ValueError):model.build(source,originals,NOW,ctx,ref())


if __name__=='__main__':unittest.main(verbosity=2)
