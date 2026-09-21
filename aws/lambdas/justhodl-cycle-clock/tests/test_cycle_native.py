from copy import deepcopy
from datetime import timedelta
from decimal import Decimal,localcontext,ROUND_UP
from unittest.mock import patch
import json,sys,unittest
from cycle_fixture import model,store,fixture,packet,Storage,STAMP,AT,ROOT

class Native(unittest.TestCase):
    @classmethod
    def setUpClass(cls):cls.client,cls.inputs,cls.macro,cls.originals=fixture();cls.output=store.compile_output(cls.inputs,store.reader(cls.client,'b'))

    def test_source_originals_full_inventory_and_no_authority(self):
        out=self.output
        self.assertEqual(len(out['measurements']),14);self.assertEqual(out['quality']['available_histories'],13)
        self.assertEqual(out['dependency_graph']['retained_inputs'],76)
        self.assertNotIn('SAHMCURRENT',out['dependency_graph']['verified_core_roots'])
        self.assertEqual(out['dependency_graph']['root_overlap'][0]['input_count'],76)
        self.assertIsNone(out['cycle']['phase']);self.assertIsNone(out['cycle']['recession_prob_pct'])
        self.assertIsNone(out['synthesis']['score']);self.assertEqual(out['portfolio_action'],'WAIT')
        self.assertTrue(all(out[k] is False for k in model.PERMISSIONS))
        self.assertEqual(out['track_record']['qualified_samples'],0)

    def test_exact_yoy_missing_month_never_shifts_by_row_count(self):
        rows=[{'date':model.month('2024-01-01',i),'value':Decimal(100+i),'original_row_index':i} for i in range(25)]
        rows.pop(12);y=model.monthly_yoy(rows)
        self.assertIsNone(y['2026-01-01']['value'])
        self.assertEqual(y['2025-02-01']['baseline']['date'],'2024-02-01')

    def test_coordinate_window_gaps_and_staggered_reference_months(self):
        c=self.output['coordinates'];self.assertEqual(c['current']['reference_month'],'2026-08-01')
        self.assertFalse(c['trail'][-1]['available']);self.assertEqual(c['trail'][-1]['reference_month'],'2026-09-01')
        component=c['current']['components']['CPIAUCSL'];self.assertIn('2025-10-01',component['excluded_months'])
        self.assertEqual(component['requested_calendar_months'],120);self.assertEqual(component['numeric_months'],119)
        self.assertEqual(len(component['sample_inputs']),119);self.assertNotIn('sample_inputs',c['trail'][-2]['components']['CPIAUCSL'])

    def test_constant_and_insufficient_windows_do_not_fabricate_zero_z(self):
        rows=[{'date':model.month('2000-01-01',i),'value':Decimal(100),'original_row_index':i} for i in range(180)]
        self.assertIsNone(model.coordinate_component(model.monthly_yoy(rows),'2014-12-01')['z_score'])
        rows=rows[-25:];c=model.coordinate_component(model.monthly_yoy(rows),'2014-12-01')
        self.assertFalse(c['full_requested_span_returned']);self.assertIsNone(c['z_score'])

    def test_sahm_uses_prior_three_month_means_including_negative_difference(self):
        days=[model.month('2025-06-01',i) for i in range(15)]
        vals=[Decimal(5)]*12+[Decimal(4)]*3
        h={'UNRATE':[{'date':d,'value':v,'original_row_index':i} for i,(d,v) in enumerate(zip(days,vals))]}
        m=deepcopy(self.output['measurements']);m['UNRATE'].update(observation_date=days[-1],value=4)
        s=model.sahm(m,h);self.assertAlmostEqual(s['value'],-1/3,7)
        self.assertFalse(s['threshold_met']);self.assertEqual(len(s['windows']),13)
        self.assertEqual(s['minimum_previous_twelve_three_month_means_percent'],4.33333333)
        h['UNRATE'].pop(4);self.assertFalse(model.sahm(m,h)['available'])

    def test_liquidity_joins_never_reach_forward_or_zero_fill(self):
        h={s:[{'date':'2026-09-16','value':Decimal(v),'original_row_index':0}] for s,v in [('WALCL',6000000),('WTREGEN',800000),('RRPONTSYD',100)]}
        p=model.liquidity_point(h,'2026-09-16');self.assertEqual(p['value'],5100)
        h['RRPONTSYD'][0]['date']='2026-09-17';self.assertFalse(model.liquidity_point(h,'2026-09-16')['available'])
        h['RRPONTSYD'][0]['date']='2026-09-16';h['RRPONTSYD'][0]['value']=Decimal(0)
        self.assertEqual(model.liquidity_point(h,'2026-09-16')['value'],5200)
        h['WTREGEN'][0]['date']='2026-09-09';self.assertFalse(model.liquidity_point(h,'2026-09-16')['available'])

    def test_liquidity_changed_legs_have_correct_signs_and_exact_dates(self):
        h={s:[] for s in ('WALCL','WTREGEN','RRPONTSYD')}
        for d,values in [('2026-06-17',(6000000,800000,100)),('2026-09-16',(6100000,900000,50))]:
            for s,v in zip(h,values):h[s].append({'date':d,'value':Decimal(v),'original_row_index':len(h[s])})
        m={s:{'value':1} for s in h};out=model.liquidity(m,h)
        self.assertEqual(out['change_13_weeks_usd_bn'],50);self.assertEqual(out['signed_leg_changes_usd_bn'],{'WALCL':100,'WTREGEN':-100,'RRPONTSYD':50})
        h['WALCL'][0]['date']='2026-06-16';self.assertIsNone(model.liquidity(m,h)['change_13_weeks_usd_bn'])

    def test_current_vintage_freshness_does_not_refresh_old_sources(self):
        i=deepcopy(self.inputs);i['generated_at']=(AT+timedelta(hours=27)).isoformat()
        out=store.compile_output(i,store.reader(self.client,'b'))
        self.assertTrue(all(m['value'] is None for m in out['measurements'].values()))
        self.assertIsNone(out['coordinates']['current']);self.assertIsNone(out['net_liquidity_proxy']['current'])
        self.assertIsNotNone(out['coordinates']['last_complete_historical'])

    def test_calendar_changes_do_not_relabel_months_as_weeks_or_percent_as_points(self):
        out=self.output
        self.assertEqual(out['central_bank_changes']['JPNASSETS']['requested_calendar_months'],3)
        self.assertIsNone(out['central_bank_changes']['JPNASSETS']['requested_calendar_days'])
        self.assertEqual(out['central_bank_changes']['WALCL']['requested_calendar_days'],91)
        self.assertEqual(out['capacity_change_12_months']['unit'],'percentage_points')

    def test_invalid_number_duplicate_month_and_definition_rejected(self):
        for change in ('number','duplicate','definition'):
            with self.subTest(change=change):
                m=deepcopy(self.macro);o=deepcopy(self.originals)
                if change=='number':o['INDPRO']['observations']['observations'][0]['value']='NaN'
                if change=='duplicate':o['INDPRO']['observations']['observations'].append(o['INDPRO']['observations']['observations'][0])
                if change=='definition':m['measurements']['INDPRO']['unit']='Percent'
                with self.assertRaises(ValueError):model.observed(m,o,STAMP)

    def test_future_and_unstamped_feed_clocks_never_turn_into_fresh_votes(self):
        packets={k:{'generated_at':(AT+timedelta(days=1)).isoformat(),'calls_eligible':True} for k in model.DEPENDENCIES}
        graph=model.dependencies(packets,{k:None for k in packets},self.macro,STAMP)
        self.assertTrue(all(n['clock_status']=='future_invalid' and n['additional_independent_votes']==0 for n in graph['inputs']))

    def test_original_replay_compiler_and_source_tampering_fail_closed(self):
        s=Storage(self.client.objects);ref=store.retain(s,'b',self.inputs,self.output)
        self.assertEqual(store.replay(ref,store.reader(s,'b')),self.output)
        s.objects[self.inputs['macro']['key']]+=b' '
        with self.assertRaises(ValueError):store.replay(ref,store.reader(s,'b'))
        s=Storage(self.client.objects);ref=store.retain(s,'b',self.inputs,self.output)
        run=json.loads(s.objects[ref['manifest_key']]);s.objects[run['compilers']['cycle_research_model']['key']]=b'changed'
        with self.assertRaises(ValueError):store.replay(ref,store.reader(s,'b'))

    def test_complete_history_retained_and_no_account_or_history_writes(self):
        s=Storage(self.client.objects);old=s.objects[store.CURRENT];history=s.objects['data/cycle-clock-history.json']
        with patch.object(store,'now',return_value=STAMP):r=store.run(s,'b','cycle-test','execution-1')
        self.assertTrue(r['published']);self.assertEqual(s.objects[model.PRIVATE+model.sha(old)+'.bin'],old)
        self.assertEqual(s.objects['data/cycle-clock-history.json'],history)
        writes=len(s.writes)
        with patch.object(store,'now',return_value=STAMP):again=store.run(s,'b','cycle-test','execution-2')
        self.assertEqual(again['execution_id'],'execution-1');self.assertEqual(len(s.writes),writes)
        self.assertTrue(all(w['Key'].startswith((model.PREFIX,model.PRIVATE)) or w['Key']==store.CURRENT for w in s.writes))
        self.assertFalse(store.allowed('data/accounts/anything.json'))

    def test_failure_leaves_current_and_no_canonical_or_calendar_reversal(self):
        s=Storage(self.client.objects);old=s.objects[store.CURRENT];s.objects[store.SOURCES[0]]=b'{}'
        with patch.object(store,'now',return_value=STAMP),self.assertRaises(RuntimeError):store.run(s,'b','failure','e')
        self.assertEqual(s.objects[store.CURRENT],old)
        s=Storage(self.client.objects);p=deepcopy(self.output);p['source_generated_at']=(AT+timedelta(hours=1)).isoformat();s.objects[store.CURRENT]=model.encoded(p)
        new=deepcopy(self.output);new['generated_at']=(AT+timedelta(hours=2)).isoformat()
        self.assertFalse(store.publish(s,'b',new))

    def test_decimal_context_and_http_validation_do_not_change_authority(self):
        with localcontext() as c:
            c.prec=9;c.rounding=ROUND_UP;out=store.compile_output(self.inputs,store.reader(self.client,'b'))
        self.assertEqual(out,self.output)
        import lambda_function as handler
        with patch.object(handler.boto3,'client',side_effect=AssertionError('No AWS for validation')):
            result=handler.lambda_handler({'validate_only':True},None)
        self.assertFalse(json.loads(result['body'])['published'])

if __name__=='__main__':unittest.main(verbosity=2)
