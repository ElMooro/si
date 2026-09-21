"""Calendar periods, provenance, gaps and write-boundary regression checks."""
from pathlib import Path
from copy import deepcopy
from datetime import timedelta
from decimal import Decimal,localcontext
import importlib.util,json,sys,types,unittest
from unittest.mock import patch
from activity_fixture import fixture,model,store,STAMP,AT

class Native(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client,cls.inputs,cls.macro,cls.originals=fixture()
        cls.out=store.compile_output(cls.inputs,store.reader(cls.client,'b'))

    def test_common_week_preserves_distinct_friday_and_saturday_dates(self):
        common=self.out['weekly_context']['current']
        self.assertEqual(common['reference_week_ending'],'2026-09-05')
        self.assertEqual(common['components']['NFCI']['observation']['date'],'2026-09-04')
        self.assertEqual(common['components']['CCSA']['observation']['date'],'2026-09-05')
        self.assertEqual(self.out['weekly_context']['latest_closed_week']['reference_week_ending'],'2026-09-19')
        self.assertEqual(self.out['weekly_context']['latest_closed_week']['available_components'],1)
        self.assertEqual(self.out['latest_sources']['ICSA']['observation']['date'],'2026-09-12')

    def test_weekly_calendar_gap_never_shifts_to_prior_numeric_row(self):
        _,hist=model.observed(self.macro,self.originals,STAMP)
        rows=hist['ICSA'];current=rows[-1];target='2026-08-15'
        missing=[r for r in rows if r['date']!=target]
        change=model.changes(missing,'ICSA',current)['4']
        self.assertEqual(change['requested_baseline_date'],target)
        self.assertIsNone(change['baseline']);self.assertIsNone(change['value_change'])
        altered=deepcopy(rows);next(r for r in altered if r['date']==target)['value']=None
        change=model.changes(altered,'ICSA',current)['4']
        self.assertEqual(change['baseline']['date'],target);self.assertFalse(change['available'])

    def test_daily_reference_keeps_actual_endpoint_and_missing_row(self):
        rows=[{'date':'2026-09-16','value':Decimal('1.5'),'original_row_index':1},
              {'date':'2026-09-17','value':None,'original_row_index':0}]
        endpoint=model.endpoint(rows,'2026-09-19',4)
        self.assertEqual(endpoint['date'],'2026-09-17');self.assertIsNone(endpoint['value'])
        self.assertIsNone(model.endpoint(rows,'2026-09-22',4))
        self.assertFalse(model.weekly_row(rows,'BAA10Y','2026-09-19')['value'])

    def test_standardization_excludes_target_and_discloses_calendar_window(self):
        _,hist=model.observed(self.macro,self.originals,STAMP);rows=hist['ICSA'];row=rows[-1]
        stats=model.standardize(rows,'ICSA',row,True)
        self.assertEqual(stats['numeric_observations'],156);self.assertFalse(stats['current_in_window'])
        self.assertEqual(stats['window_end_exclusive'],row['date'])
        self.assertNotIn(row['date'],[r['date'] for r in stats['members']])
        values=[Decimal(r['exact_value']) for r in stats['members']]
        average=sum(values)/len(values);sd=(sum((v-average)**2 for v in values)/(len(values)-1)).sqrt()
        self.assertAlmostEqual(stats['value'],float((row['value']-average)/sd),places=7)

    def test_zero_variance_sparse_and_short_spans_never_create_zero_z(self):
        _,hist=model.observed(self.macro,self.originals,STAMP);rows=deepcopy(hist['WEI'])
        for row in rows:row['value']=Decimal('2')
        self.assertEqual(model.standardize(rows,'WEI',rows[-1])['status'],'zero_variance')
        self.assertIsNone(model.standardize(rows,'WEI',rows[-1])['value'])
        self.assertFalse(model.standardize(rows[-100:],'WEI',rows[-1])['full_returned_span'])
        sparse=[r for i,r in enumerate(rows) if i%2]
        self.assertLess(model.standardize(sparse,'WEI',sparse[-1])['numeric_observations'],125)
        self.assertIsNone(model.standardize(sparse,'WEI',sparse[-1])['value'])

    def test_spread_units_and_source_model_identity(self):
        c=self.out['latest_sources']['BAA10Y']['changes']['4']
        self.assertAlmostEqual(c['basis_point_change'],c['value_change']*100,places=7)
        g=self.out['regional_context']['GDPNOW']
        self.assertEqual(g['measurement']['frequency'],'Q');self.assertEqual(g['measurement']['seasonal_adjustment'],'SAAR')
        self.assertFalse(g['forecast_release_time_verified']);self.assertFalse(g['vintage_path_available'])
        self.assertIsNone(self.out['regional_context']['T10Y3M']['cleveland_recession_probability'])
        graph=self.out['dependency_graph'];self.assertEqual(graph['known_overlap'][0]['contains'],['ICSA','CCSA'])
        self.assertEqual(graph['independent_votes'],0);self.assertEqual(len(graph['retained_contexts']),5)

    def test_bad_dates_definitions_values_and_future_clock_rejected(self):
        for kind in ('definition','duplicate','weekday','quarter','nonfinite','negative'):
            m=deepcopy(self.macro);o=deepcopy(self.originals)
            if kind=='definition':m['measurements']['ICSA']['unit']='Percent'
            if kind=='duplicate':o['ICSA']['observations']['observations'].append(o['ICSA']['observations']['observations'][0])
            if kind=='weekday':o['ICSA']['observations']['observations'][0]['date']='2026-09-13'
            if kind=='quarter':o['GDPNOW']['observations']['observations'][0]['date']='2026-08-01'
            if kind=='nonfinite':o['ICSA']['observations']['observations'][0]['value']='NaN'
            if kind=='negative':o['ICSA']['observations']['observations'][0]['value']='-1'
            with self.subTest(kind=kind),self.assertRaises(ValueError):model.build(m,o,STAMP)
        with self.assertRaises(ValueError):model.build(self.macro,self.originals,(AT-timedelta(seconds=1)).isoformat())

    def test_missing_series_and_expiry_keep_history_without_current_composite(self):
        s,i,_,_=fixture(missing=('WEI',));out=store.compile_output(i,store.reader(s,'b'))
        self.assertIsNone(out['weekly_context']['current']);self.assertEqual(out['quality']['available_histories'],7)
        stale=model.build(self.macro,self.originals,(AT+timedelta(hours=27)).isoformat())
        self.assertIsNone(stale['weekly_context']['current']);self.assertEqual(len(stale['weekly_context']['trail']),156)
        self.assertTrue(all(stale[k] is False for k in model.PERMISSIONS))
        for k in ('activity_index','activity_z','regime','momentum','call'):self.assertIsNone(stale[k])
        self.assertFalse(stale['divergence']['available']);self.assertIsNone(stale['divergence']['gap'])

    def test_decimal_context_local(self):
        with localcontext() as c:
            c.prec=9;out=store.compile_output(self.inputs,store.reader(self.client,'b'))
        self.assertEqual(out,self.out)

    def test_retained_replay_detects_original_and_compiler_tampering(self):
        for kind in ('original','compiler'):
            s,i,_,_=fixture();out=store.compile_output(i,store.reader(s,'b'));ref=store.retain(s,'b',i,out)
            self.assertEqual(store.replay(ref,store.reader(s,'b')),out)
            key=next(k for k in s.objects if k.startswith('data/evidence/fred/' if kind=='original' else model.PREFIX+'compilers/'))
            s.objects[key]=b'broken'
            with self.subTest(kind=kind),self.assertRaises(Exception):store.replay(ref,store.reader(s,'b'))

    def test_conditional_publication_preserves_original_and_refuses_source_rollback(self):
        s,_,_,_=fixture();old=s.objects[store.CURRENT];out=deepcopy(self.out)
        self.assertTrue(store.publish(s,'b',out));self.assertEqual(s.objects[model.PRIVATE+model.sha(old)+'.bin'],old)
        stale=deepcopy(out);stale['source_generated_at']=(AT-timedelta(hours=1)).isoformat();stale['generated_at']=(AT+timedelta(minutes=1)).isoformat()
        self.assertFalse(store.publish(s,'b',stale));self.assertEqual(json.loads(s.objects[store.CURRENT]),out)

    def test_idempotent_failure_and_success_and_read_allowlist(self):
        s,_,_,_=fixture();old=s.objects[store.CURRENT]
        with patch.object(store,'now',return_value=STAMP),patch.object(store,'compile_output',side_effect=ValueError('bad original')):
            with self.assertRaises(RuntimeError):store.run(s,'b','failed-request','1')
        self.assertEqual(s.objects[store.CURRENT],old);self.assertEqual(store.run(s,'b','failed-request','2')['status'],'failed')
        with patch.object(store,'now',return_value=STAMP):first=store.run(s,'b','success-request','3')
        writes=len(s.writes);self.assertEqual(store.run(s,'b','success-request','4'),first);self.assertEqual(len(s.writes),writes)
        for key in ('accounts/example.json','data/thesis-state-v2.json.gz','data/unreviewed.json'):
            with self.assertRaises(ValueError):store.reader(s,'b')(key)

    def test_http_and_validation_are_read_only(self):
        spec=importlib.util.spec_from_file_location('native_activity_handler',Path(__file__).resolve().parents[1]/'source/lambda_function.py')
        with patch.dict(sys.modules,{'boto3':types.SimpleNamespace(client=lambda *a,**kw:self.client),'botocore.config':types.SimpleNamespace(Config=lambda **kw:None)}):
            h=importlib.util.module_from_spec(spec);spec.loader.exec_module(h)
            with patch.object(h,'run',side_effect=AssertionError('producer invoked')):
                self.assertEqual(h.lambda_handler({'validate_only':True})['statusCode'],200)
                with patch.object(h,'reader',return_value=lambda key:model.encoded(self.out)):
                    self.assertEqual(h.lambda_handler({'action':'current_state'})['statusCode'],200)
                    self.assertEqual(h.lambda_handler({'httpMethod':'GET'})['statusCode'],200)

if __name__=='__main__':unittest.main(verbosity=2)
