"""Source arithmetic, retained replay and conditional publication regression cases."""
from pathlib import Path
from datetime import timedelta
from decimal import Decimal,localcontext
from copy import deepcopy
import importlib.util,json,sys,types,unittest
from unittest.mock import patch
from nowcast_fixture import fixture,model,store,STAMP,AT

class Native(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client,cls.inputs,cls.macro,cls.originals=fixture()
        cls.out=store.compile_output(cls.inputs,store.reader(cls.client,'b'))

    def test_common_month_and_explicit_missing_real_retail(self):
        out=self.out;idx=out['research_index']
        self.assertEqual(idx['current']['reference_month'],'2026-07-01')
        self.assertEqual(idx['latest_closed_reference']['reference_month'],'2026-08-01')
        self.assertIsNone(idx['latest_closed_reference']['value'])
        self.assertEqual(idx['latest_closed_reference']['complete_components'],6)
        self.assertTrue(all(c['reference_month']=='2026-07-01' for c in idx['current']['components'].values()))
        self.assertIsNone(out['measurements']['RRSFS']['value'])
        self.assertEqual(out['measurements']['HOUST']['seasonal_adjustment'],'SAAR')
        self.assertTrue(all(r['series']['RRSFS']['yoy_percent'] is None for r in out['nominal_real_retail']['rows']))

    def test_aggregation_preserves_daily_mean_rows_and_closes_month(self):
        _,hist=model.observed(self.macro,self.originals,STAMP)
        months=model.monthly_inputs(hist['T10Y2Y'],'T10Y2Y',STAMP);row=months['2026-08-01']
        values=[Decimal(r['exact_value']) for r in row['members'] if r['exact_value'] is not None]
        self.assertEqual(row['value'],sum(values)/len(values));self.assertGreater(len(values),15)
        self.assertNotIn('2026-09-01',months);self.assertFalse(months['2011-05-01']['span_covered'])
        self.assertIsNone(months['2011-05-01']['value'])
        partial=[r for r in hist['T10Y2Y'] if r['date']<'2026-08-14']
        self.assertIsNone(model.monthly_inputs(partial,'T10Y2Y',STAMP)['2026-08-01']['value'])

    def test_missing_exact_year_does_not_shift_and_target_excluded_from_baseline(self):
        _,hist=model.observed(self.macro,self.originals,STAMP)
        months=model.monthly_inputs(hist['PAYEMS'],'PAYEMS',STAMP);months.pop('2025-08-01')
        self.assertIsNone(model.transformed(months,'PAYEMS')['2026-08-01']['value'])
        comp=self.out['research_index']['current']['components']['PAYEMS']
        self.assertEqual(comp['window_start'],'2021-07-01');self.assertEqual(comp['window_end'],'2026-06-01')
        self.assertNotIn('2026-07-01',[p['reference_month'] for p in comp['sample_inputs']])
        values=[Decimal(str(r['transformed_value'])) for r in comp['sample_inputs']]
        self.assertAlmostEqual(float(sum(values)/len(values)),comp['mean'],places=7)

    def test_no_missing_weight_renormalization_or_rounding_before_sum(self):
        current=self.out['research_index']['current']
        self.assertEqual(Decimal(current['exact_value']),sum(Decimal(c['exact_contribution']) for c in current['components'].values()))
        bad=deepcopy(self.originals);bad['UNRATE']=None;macro=deepcopy(self.macro);macro['measurements'].pop('UNRATE')
        out=model.build(macro,bad,STAMP)
        self.assertIsNone(out['research_index']['current']);self.assertTrue(all(r['value'] is None for r in out['research_index']['trail']))

    def test_constant_and_sparse_calendar_windows_do_not_create_zero_z(self):
        seq={model.month('2000-01-01',i):{'value':Decimal('4'),'reference_month':model.month('2000-01-01',i),'current':None,'baseline':None} for i in range(100)}
        c=model.component(seq,'UNRATE','2008-04-01');self.assertIsNone(c['z_score']);self.assertEqual(c['status'],'constant_window')
        for i in range(45,80):seq.pop(model.month('2000-01-01',i))
        c=model.component(seq,'UNRATE','2008-04-01');self.assertIsNone(c['z_score']);self.assertLess(c['numeric_months'],48)

    def test_price_endpoints_exact_calendar_months_and_overlap(self):
        outcomes=self.out['price_outcomes'];row=next(r for r in outcomes['rows'] if r['reference_month']=='2024-01-01' and r['horizon_calendar_months']==3)
        self.assertEqual(row['target_reference_month'],'2024-04-01');self.assertEqual(row['end']['endpoint']['date'][:7],'2024-04')
        a=Decimal(row['start']['exact_value']);b=Decimal(row['end']['exact_value'])
        self.assertAlmostEqual(row['price_return_percent'],float(100*(b/a-1)),places=7)
        self.assertGreater(len(outcomes['overlapping_pairs']['12']),0);self.assertIsNone(outcomes['hit_rate'])
        _,hist=model.observed(self.macro,self.originals,STAMP);months=model.monthly_inputs(hist['SP500'],'SP500',STAMP);months.pop('2024-04-01')
        changed=model.price_outcomes(months,[{'reference_month':'2024-01-01','value':1}])
        self.assertIsNone(next(r for r in changed['rows'] if r['horizon_calendar_months']==3)['price_return_percent'])

    def test_source_definitions_duplicates_bad_numbers_and_future_clock_fail(self):
        for kind in ('definition','duplicate','nonfinite','negative'):
            macro=deepcopy(self.macro);originals=deepcopy(self.originals)
            if kind=='definition':macro['measurements']['HOUST']['definition']['seasonal_adjustment_short']='SA'
            if kind=='duplicate':originals['PAYEMS']['observations']['observations'].append(originals['PAYEMS']['observations']['observations'][0])
            if kind=='nonfinite':originals['PAYEMS']['observations']['observations'][0]['value']='NaN'
            if kind=='negative':originals['PAYEMS']['observations']['observations'][0]['value']='-1'
            with self.subTest(kind=kind),self.assertRaises(ValueError):model.build(macro,originals,STAMP)
        with self.assertRaises(ValueError):model.build(self.macro,self.originals,(AT-timedelta(seconds=1)).isoformat())

    def test_authority_expires_and_history_remains_explicitly_revised(self):
        out=model.build(self.macro,self.originals,(AT+timedelta(hours=27)).isoformat())
        self.assertIsNone(out['research_index']['current']);self.assertTrue(out['research_index']['trail'])
        self.assertFalse(out['research_index']['historical_first_availability_verified'])
        for k in model.PERMISSIONS:self.assertIs(out[k],False)
        for k in ('score','regime','normalized_score','raw_score','call','confidence'):self.assertIsNone(out[k])
        self.assertEqual(out['portfolio_action'],'WAIT')

    def test_decimal_context_is_local(self):
        with localcontext() as c:
            c.prec=9;out=model.build(self.macro,self.originals,STAMP)
        self.assertEqual(out,self.out)

    def test_retained_replay_detects_tampering(self):
        s,i,_,_=fixture();out=store.compile_output(i,store.reader(s,'b'));ref=store.retain(s,'b',i,out)
        self.assertEqual(store.replay(ref,store.reader(s,'b')),out)
        key=next(k for k in s.objects if k.startswith('data/evidence/fred/'));s.objects[key]=b'broken'
        with self.assertRaises(Exception):store.replay(ref,store.reader(s,'b'))

    def test_publish_preserves_predecessor_and_rejects_older_vintage(self):
        s,i,_,_=fixture();old=s.objects[store.CURRENT];out=deepcopy(self.out)
        self.assertTrue(store.publish(s,'b',out));self.assertEqual(s.objects[model.PRIVATE+model.sha(old)+'.bin'],old)
        stale=deepcopy(out);stale['source_generated_at']=(AT-timedelta(hours=1)).isoformat();stale['generated_at']=(AT+timedelta(minutes=1)).isoformat()
        self.assertFalse(store.publish(s,'b',stale));self.assertEqual(json.loads(s.objects[store.CURRENT]),out)

    def test_idempotence_failure_and_allowlist(self):
        s,i,_,_=fixture();old=s.objects[store.CURRENT]
        with patch.object(store,'now',return_value=STAMP),patch.object(store,'compile_output',side_effect=ValueError('bad original')):
            with self.assertRaises(RuntimeError):store.run(s,'b','fail-once','id1')
        self.assertEqual(s.objects[store.CURRENT],old);self.assertEqual(store.run(s,'b','fail-once','id2')['status'],'failed')
        with patch.object(store,'now',return_value=STAMP):first=store.run(s,'b','one','id3')
        writes=len(s.writes);self.assertEqual(store.run(s,'b','one','id4'),first);self.assertEqual(len(s.writes),writes)
        for path in ('accounts/example.json','data/thesis-state-v2.json.gz','data/other.json'):
            with self.assertRaises(ValueError):store.reader(s,'b')(path)

    def test_handler_validate_and_current_state_never_invoke_producer(self):
        spec=importlib.util.spec_from_file_location('native_nowcast_handler',Path(__file__).resolve().parents[1]/'source/lambda_function.py')
        with patch.dict(sys.modules,{'boto3':types.SimpleNamespace(client=lambda *a,**kw:self.client),'botocore.config':types.SimpleNamespace(Config=lambda **kw:None)}):
            handler=importlib.util.module_from_spec(spec);spec.loader.exec_module(handler)
            with patch.object(handler,'run',side_effect=AssertionError('producer invoked')):
                self.assertEqual(handler.lambda_handler({'validate_only':True})['statusCode'],200)
                with patch.object(handler,'reader',return_value=lambda key:model.encoded(self.out)):
                    self.assertEqual(handler.lambda_handler({'action':'current_state'})['statusCode'],200)

if __name__=='__main__':unittest.main(verbosity=2)
