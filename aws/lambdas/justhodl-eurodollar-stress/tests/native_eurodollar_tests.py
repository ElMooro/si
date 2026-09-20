"""Native USD-context reconstruction and publication regression, no network."""
from pathlib import Path
from copy import deepcopy
from datetime import datetime,timedelta,timezone
from decimal import Decimal,localcontext,ROUND_UP
import gzip,json,sys,unittest
from unittest.mock import patch
HERE=Path(__file__).resolve().parent;ROOT=HERE.parents[3]
sys.path[:0]=[str(HERE.parent/'source'),str(ROOT/'aws/shared'),str(ROOT/'aws/lambdas/justhodl-crisis-composite/tests')]
from test_native_research import fixtures,store_fixture,Storage
import eurodollar_research_model as model
import eurodollar_research_store as store
STAMP='2026-09-20T09:30:00+00:00'

class Native(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.packet,cls.originals=fixtures();cls.output=model.build(cls.packet,cls.originals,{},STAMP)
    def test_all_nine_sources_and_no_synthetic_authority(self):
        o=self.output;self.assertEqual(o['quality']['within_age_ceiling'],9)
        self.assertEqual(set(o['measurements']),set(model.SERIES));self.assertEqual(len(o['signals']),9)
        for k in model.PERMISSIONS:self.assertIs(o[k],False)
        for k in ('composite_score','severity','regime','call'):self.assertIsNone(o[k])
        self.assertEqual(o['portfolio_action'],'WAIT');self.assertEqual(o['n_signals_used'],0)
        self.assertEqual(o['n_failures'],0);self.assertEqual(o['failures'],[]);self.assertIsNone(o['duration_s'])
        self.assertTrue(all(v is None for v in o['thresholds'].values()))
    def test_native_percent_to_bps_and_exact_source_precision(self):
        m=self.output['measurements'];self.assertEqual(m['BAMLH0A0HYM2']['value_bps'],270)
        self.assertEqual(m['BAMLC0A0CM']['value_bps'],78)
        self.assertEqual(m['DTWEXBGS']['value'],118.2126)
        self.assertEqual(m['DTWEXBGS']['observation_date'],'2026-09-11')
        self.assertEqual(m['DTWEXBGS']['quality']['observation_age_days'],9)
        self.assertIsNone(m['VIXCLS']['value_bps'])
    def test_repo_matches_date_not_array_position(self):
        o=self.output['repo_comparison'];self.assertTrue(o['current_comparison_available'])
        self.assertEqual(o['observation_date'],'2026-09-17')
        a=self.output['measurements']['SOFR'];b=self.output['measurements']['DFF']
        self.assertEqual(o['difference_bps'],float((Decimal(a['exact_value'])-Decimal(b['exact_value']))*100))
    def test_repo_does_not_bridge_missing_or_mismatched_latest(self):
        rows,h=model.measurements(self.packet,self.originals,STAMP)
        h['SOFR'][-1]['value']=None
        self.assertIsNone(model.repo_spread(rows,h)['difference_bps'])
        rows['SOFR']['observation_date']='2026-09-18'
        self.assertIsNone(model.repo_spread(rows,h)['difference_bps'])
    def test_rate_dispersion_uses_60_actual_adjacent_changes(self):
        d=self.output['yield_change_dispersion'];self.assertEqual(d['valid_differences'],60)
        self.assertTrue(d['current_comparison_available']);self.assertEqual(d['last_to_date'],'2026-09-17')
        self.assertGreater(d['excluded_missing_pairs_in_returned_history'],0)
        self.assertIn('convention',d['annualization_assumption'])
    def test_constant_change_is_zero_and_missing_pairs_not_bridged(self):
        start=datetime(2026,1,1).date()
        h=[{'date':(start+timedelta(days=i)).isoformat(),'value':Decimal(i)} for i in range(64)]
        row={'observation_date':h[-1]['date'],'quality':{'status':'within_age_ceiling'}}
        self.assertEqual(model.yield_change_dispersion(row,h)['sample_stddev_bps'],0)
        h[2]['value']=None;d=model.yield_change_dispersion(row,h)
        self.assertEqual(d['excluded_missing_pairs_in_returned_history'],2);self.assertEqual(d['valid_differences'],60)
        h[-1]['value']=None;self.assertFalse(model.yield_change_dispersion(row,h)['current_comparison_available'])
        self.assertFalse(model.yield_change_dispersion(row,h[:60])['current_comparison_available'])
    def test_stale_clock_withholds_values_but_retains_evidence(self):
        o=model.build(self.packet,self.originals,{},'2026-09-22T09:30:00Z')
        self.assertEqual(o['quality']['within_age_ceiling'],0)
        self.assertTrue(all(m['value'] is None for m in o['measurements'].values()))
        self.assertTrue(o['measurements']['SOFR']['evidence']);self.assertIsNone(o['repo_comparison']['difference_bps'])
    def test_future_source_refused_and_missing_series_is_null(self):
        with self.assertRaises(ValueError):model.build(self.packet,self.originals,{},'2026-09-19T09:30:00Z')
        p=deepcopy(self.packet);del p['measurements']['SOFR'];orig=deepcopy(self.originals);orig.pop('SOFR')
        o=model.build(p,orig,{},STAMP);self.assertEqual(o['quality']['within_age_ceiling'],8)
        self.assertIsNone(o['measurements']['SOFR']['value'])
    def test_units_drift_is_not_silently_converted(self):
        p=deepcopy(self.packet);p['measurements']['BAMLH0A0HYM2']['unit']='Basis Points'
        o=model.build(p,self.originals,{},STAMP);m=o['measurements']['BAMLH0A0HYM2']
        self.assertEqual(m['quality']['status'],'definition_changed');self.assertIsNone(m['value_bps'])
    def test_original_provider_missing_and_future_rows_remain_distinct(self):
        o={'observations':{'observations':[{'date':'2026-09-20','value':'4.0'},{'date':'2026-09-19','value':False},{'date':'2026-09-21','value':'9'}]}}
        rows=model.records(o,'2026-09-20');self.assertEqual(len(rows),2)
        self.assertIsNone(rows[0]['value']);self.assertEqual(rows[1]['original_row_index'],0)
    def test_ambient_decimal_context_does_not_change_output(self):
        with localcontext() as ctx:
            ctx.prec=7;ctx.rounding=ROUND_UP
            self.assertEqual(model.build(self.packet,self.originals,{},STAMP),self.output)
    def test_fx_context_cannot_promote_a_vote_and_zero_survives(self):
        for value in (0,1.5,False,float('nan'),None):
            o=model.build(self.packet,self.originals,{'calls_eligible':True,'regime_metrics':{'usd_synthetic_20d_pct':value}},STAMP)
            self.assertIs(o['fx_context']['calls_eligible'],False);self.assertIsNone(o['signals'][-1]['value'])
            if type(value) in (int,float) and value==value:self.assertEqual(o['fx_context']['reported_usd_synthetic_20d_pct'],value)
            else:self.assertIsNone(o['fx_context']['reported_usd_synthetic_20d_pct'])
        self.assertIsNone(model.build(self.packet,self.originals,{'regime_metrics':[]},STAMP)['fx_context']['reported_usd_synthetic_20d_pct'])
    def test_lag_deadline_precedes_source_refresh_deadline(self):
        m=self.output['measurements']['DTWEXBGS']
        self.assertLessEqual(model.clock(m['source_valid_until']),model.clock(self.output['freshness']['pipeline_check_due_at']))
        self.assertFalse(m['quality']['release_calendar_verified'])

class Store(unittest.TestCase):
    def setUp(self):
        objects,i=store_fixture();self.client=Storage(objects)
        self.client.objects[store.SOURCES[0]]=model.encoded(i['macro'])
        self.client.objects[store.SOURCES[1]]=b'{"regime_metrics":{"usd_synthetic_20d_pct":0}}'
        self.inputs={'contract':'eurodollar-native-inputs.v1','generated_at':STAMP,
            'macro':store.snapshot(self.client,'b',store.SOURCES[0]),'fx':store.snapshot(self.client,'b',store.SOURCES[1])}
    def test_full_retained_replay_reconstructs_originals_and_compilers(self):
        read=store.reader(self.client,'b');out=store.compile_output(self.inputs,read)
        ref=store.retain(self.client,'b',self.inputs,out);self.assertEqual(store.replay(ref,read),out)
        self.assertEqual(len([k for k in self.client.reads if k.endswith('.gz')]),54)
        key=next(k for k in self.client.objects if k.endswith('.gz'))
        # Target one of this model's sources, not a different macro series.
        packet=json.loads(self.client.objects[store.SOURCES[0]])
        key=packet['measurements']['SOFR']['evidence']['observations']['key'];self.client.objects[key]=gzip.compress(b'{}')
        with self.assertRaises(ValueError):store.replay(ref,read)
    def test_compiler_bytes_cannot_change_under_same_digest(self):
        read=store.reader(self.client,'b');out=store.compile_output(self.inputs,read);ref=store.retain(self.client,'b',self.inputs,out)
        run=json.loads(read(ref['manifest_key']));key=run['compilers']['eurodollar_research_model']['key'];self.client.objects[key]+=b'\n'
        with self.assertRaisesRegex(ValueError,'compiler'):store.replay(ref,read)
    def test_path_allowlist_protects_accounts_and_upstream_writes(self):
        for key in ('portfolio/state.json','data/brain.json','audit-private/other.bin','data/report-research/inputs/'+'a'*64+'.json'):
            with self.assertRaises(ValueError):store.reader(self.client,'b')(key)
        with self.assertRaises(ValueError):store.immutable(self.client,'b',store.SOURCES[0],b'{}')
    def test_publish_archives_legacy_and_refuses_source_regression(self):
        old=b'{"as_of":"2026-09-19T21:16:24Z","score":99}'
        self.client.objects[store.CURRENT]=old
        out=store.compile_output(self.inputs,store.reader(self.client,'b'))
        self.assertTrue(store.publish(self.client,'b',out));self.assertEqual(self.client.objects[store.PRIVATE+model.sha(old)+'.bin'],old)
        back=deepcopy(out);back['generated_at']='2026-09-20T09:31:00Z';back['source_generated_at']='2026-09-19T09:00:00Z'
        self.assertFalse(store.publish(self.client,'b',back))
        changed=deepcopy(out);changed['version']='wrong'
        with self.assertRaisesRegex(ValueError,'same-clock'):store.publish(self.client,'b',changed)
    def test_request_is_idempotent_and_never_reads_accounts(self):
        with patch.object(store,'now',return_value=STAMP):
            a=store.run(self.client,'b','native-test','execution-test');n=len(self.client.writes)
            b=store.run(self.client,'b','native-test','execution-test-2')
        self.assertEqual(a,b);self.assertEqual(n,len(self.client.writes));self.assertEqual(a['status'],'complete')
        self.assertEqual(a['provider_requests'],0)
        self.assertTrue(all(not k.startswith('portfolio/') for k in self.client.reads))
    def test_corrupt_source_marks_failure_without_overwriting_current(self):
        old=b'{"as_of":"2026-09-19T21:16:24Z"}';self.client.objects[store.CURRENT]=old
        macro=json.loads(self.client.objects[store.SOURCES[0]]);macro['measurements']['SOFR']['current']=99
        self.client.objects[store.SOURCES[0]]=model.encoded(macro)
        with patch.object(store,'now',return_value=STAMP):
            with self.assertRaises(RuntimeError):store.run(self.client,'b','failed-test','execution-test')
        self.assertEqual(self.client.objects[store.CURRENT],old)
        status=json.loads(self.client.objects[store.request_key('failed-test')]);self.assertEqual(status['status'],'failed')

if __name__=='__main__':unittest.main()
