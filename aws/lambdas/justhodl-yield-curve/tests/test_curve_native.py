from pathlib import Path
from copy import deepcopy
from datetime import datetime,timezone
from unittest.mock import patch
import hashlib,json,sys,unittest
ROOT=Path(__file__).resolve().parents[4];DRAFT=Path(__file__).resolve().parents[1]/'source'
sys.path[:0]=[str(DRAFT),str(ROOT/'aws/shared'),str(ROOT/'aws/lambdas/justhodl-crisis-composite/tests')]
import yield_curve_model as model
import yield_curve_store as store
from test_native_research import Storage,store_fixture,fixtures,STAMP


def setup():
    objects,_=store_fixture();client=Storage(objects)
    legacy=b'{"generated_at":"2026-09-17T00:00:00Z","legacy":"complete old snapshot"}'
    client.objects[model.CURRENT]=legacy
    raw=b'{"context":"complete unqualified ACM packet"}'
    client.objects['data/term-premium.json']=raw
    context={'source_key':'data/term-premium.json','status':'retained_unqualified_context',
        'original':store.private_bytes(client,'b',raw),'captured_at':STAMP,'independent_votes':0}
    return client,{'contract':'yield-curve-inputs.v1','generated_at':STAMP,
        'macro':store.retain_bytes(client,'b',client.objects[store.SOURCE],'snapshots'),
        'context':context,'predecessor':store.private_bytes(client,'b',legacy),
        'previous_watermarks':{sid:None for sid in store.catalog.SERIES}}



class Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        client,cls.inputs=setup();cls.objects=client.objects
        cls.output=store.compile_output(cls.inputs,store.reader(client,'b'))
    def setUp(self):self.client=Storage(self.objects);self.read=store.reader(self.client,'b')
    def test_complete_native_output_has_compact_view_and_every_history_retained(self):
        ref,view=store.retain(self.client,'b',self.inputs,self.output)
        full,reproduced=store.replay(ref,self.read)
        self.assertEqual(full,self.output);self.assertEqual(view,reproduced)
        self.assertEqual(len(view['series']),23)
        self.assertLess(len(model.encoded(view)),len(model.encoded(full)))
        for sid,row in full['series'].items():
            self.assertNotIn('history',view['series'][sid])
            artifact=view['series'][sid]['complete_history_artifact']
            self.assertEqual(store.checked(artifact,'series',self.read),row)
            self.assertEqual(view['series'][sid]['retained_original_rows'],len(row['history']))
        self.assertFalse(view['calls_eligible']);self.assertFalse(view['sizing_eligible'])
    def test_any_missing_or_tampered_shard_cannot_pass_full_replay(self):
        ref,view=store.retain(self.client,'b',self.inputs,self.output)
        target=view['series']['DGS2']['complete_history_artifact']['key']
        self.client.objects[target]=b'{}'
        with self.assertRaises(ValueError):store.replay(ref,self.read)
    def test_unauthorized_paths_fail_before_read(self):
        for key in ('data/portfolio.json','audit-private/x','data/yield-curve-research/series/../../x'):
            with self.assertRaises(ValueError):self.read(key)
        self.assertEqual(self.client.reads,[])
    def test_original_closure_and_view_corruption_is_detected(self):
        ref,view=store.retain(self.client,'b',self.inputs,self.output)
        manifest=json.loads(self.read(ref['manifest_key']))
        for key in (manifest['compilers']['yield_curve_model']['key'],manifest['view']['key'],self.inputs['macro']['key']):
            old=self.client.objects[key];self.client.objects[key]=b'{}'
            with self.assertRaises(ValueError):store.replay(ref,self.read)
            self.client.objects[key]=old
    def test_whole_legacy_head_retained_and_newer_packet_not_overwritten(self):
        raw=self.client.objects[model.CURRENT];ref,marks=store.previous_state(self.client,'b')
        self.assertEqual(self.client.objects[ref['key']],raw);self.assertTrue(all(v is None for v in marks.values()))
        replay,view=store.retain(self.client,'b',self.inputs,self.output);packet={**view,'replay':replay}
        self.assertTrue(store.publish(self.client,'b',packet))
        future=deepcopy(packet);future['generated_at']='2026-09-23T00:00:00Z'
        self.client.objects[model.CURRENT]=model.encoded(future)
        self.assertFalse(store.publish(self.client,'b',packet))
    def test_source_regression_withholds_current_without_losing_acquisition_watermark(self):
        source,originals=fixtures();originals={s:originals.get(s) for s in store.catalog.SERIES}
        previous={s:None for s in store.catalog.SERIES};previous['DGS2']='2026-09-20T09:00:00Z'
        output=model.build(source,originals,STAMP,self.inputs['context'],self.inputs['predecessor'],previous)
        self.assertEqual(output['series']['DGS2']['quality']['status'],'source_regression')
        self.assertIsNone(output['series']['DGS2']['current']['value']);self.assertIsNone(output['derived']['2s10s']['current'])
        self.assertEqual(output['source_acquisition_watermarks']['DGS2'],previous['DGS2'])
        self.assertTrue(output['series']['DGS2']['history'])
    def test_same_clock_or_cas_conflict_preserves_newer_work(self):
        replay,view=store.retain(self.client,'b',self.inputs,self.output);packet={**view,'replay':replay}
        self.assertTrue(store.publish(self.client,'b',packet));self.assertTrue(store.publish(self.client,'b',packet))
        changed=deepcopy(packet);changed['decision']['verb']='LONG'
        with self.assertRaises(ValueError):store.publish(self.client,'b',changed)
        self.client.objects[model.CURRENT]=b'{"generated_at":"2026-09-17T00:00:00Z"}'
        future=deepcopy(packet);future['generated_at']='2026-09-23T00:00:00Z';original=self.client.put_object
        def race(**request):
            if request['Key']==model.CURRENT:self.client.objects[model.CURRENT]=model.encoded(future)
            return original(**request)
        with patch.object(self.client,'put_object',side_effect=race):self.assertFalse(store.publish(self.client,'b',packet))
    def test_complete_scheduled_path_publishes_and_replays_without_provider_acquisition(self):
        class Clock:
            @staticmethod
            def now(tz):return datetime.fromisoformat(STAMP)
        with patch.object(store,'datetime',Clock):result=store.run(self.client,'b')
        self.assertTrue(result['published']);self.assertEqual(result['provider_requests'],0)
        packet=json.loads(self.client.objects[model.CURRENT]);full,view=store.replay(packet['replay'],self.read)
        self.assertEqual(view,{k:v for k,v in packet.items() if k!='replay'})
        self.assertEqual(packet['view']['complete_original_rows'],sum(len(r['history']) for r in full['series'].values()))

    def test_missing_recovery_keeps_high_watermark_and_does_not_revive_old_observation(self):
        source,originals=fixtures();originals={s:originals.get(s) for s in store.catalog.SERIES}
        high='2026-09-20T10:00:00Z';previous=deepcopy(self.inputs['previous_watermarks']);previous['DGS2']=high
        missing=deepcopy(source);missing['measurements'].pop('DGS2')
        missing['replay']['output_sha256']=model.digest({k:v for k,v in missing.items() if k!='replay'})
        subset={**originals,'DGS2':None}
        packet=model.build(missing,subset,'2026-09-20T11:00:00Z',self.inputs['context'],self.inputs['predecessor'],previous)
        self.assertIsNone(packet['series']['DGS2']['current']['value']);self.assertEqual(packet['source_acquisition_watermarks']['DGS2'],high)
        recovery=model.build(source,originals,'2026-09-20T12:00:00Z',self.inputs['context'],self.inputs['predecessor'],model.watermarks(packet))
        self.assertEqual(recovery['series']['DGS2']['quality']['status'],'source_regression');self.assertIsNone(recovery['derived']['2s10s']['current'])
        self.assertTrue(recovery['series']['DGS2']['history'])

    def test_publication_boundary_refuses_invented_authority_before_storage_access(self):
        replay,view=store.retain(self.client,'b',self.inputs,self.output);packet={**view,'replay':replay}
        for change in (lambda p:p.update(sizing_eligible=True),lambda p:p['decision'].update(verb='LONG'),
                       lambda p:p['series']['DGS2'].update(calls_eligible=True),lambda p:p.pop('replay'),lambda p:p['derived']['2s10s'].update(sizing_eligible=True),
                       lambda p:p['shape'].update(forecast_qualified=True),lambda p:p.update(signals=['LONG']),
                       lambda p:p['portfolio_consequences'].update(target_weights={'SPY':1})):
            bad=deepcopy(packet);change(bad)
            with patch.object(self.client,'get_object',side_effect=AssertionError('Storage must not be read')):
                with self.assertRaises(ValueError):store.publish(self.client,'b',bad)

    def test_http_validation_and_missing_execution_context_cannot_publish(self):
        import importlib.util
        spec=importlib.util.spec_from_file_location('native_curve_handler',DRAFT/'lambda_function.py')
        handler=importlib.util.module_from_spec(spec);spec.loader.exec_module(handler)
        with patch.object(handler.boto3,'client',side_effect=AssertionError('AWS access forbidden')):
            self.assertEqual(handler.lambda_handler({'httpMethod':'GET'},None)['statusCode'],307)
            self.assertEqual(handler.lambda_handler({'requestContext':{'http':{'method':'GET'}}},None)['statusCode'],307)
            self.assertFalse(json.loads(handler.lambda_handler({'validate_only':True},None)['body'])['published'])
            with self.assertRaises(ValueError):handler.lambda_handler({},None)

    def test_complete_predecessors_and_exact_reviewed_arithmetic_and_browser_definitions(self):
        fixture=json.loads((ROOT/'tests/fixtures/yield-curve-native-migration.json').read_text())
        for row in fixture['complete_predecessors']:
            raw=(ROOT/row['predecessor']).read_bytes()
            self.assertEqual(len(raw),row['bytes']);self.assertEqual(hashlib.sha256(raw).hexdigest(),row['sha256'])
        store.qualified_arithmetic()
        text=(ROOT/'jh-yield-curve-definitions.js').read_text()
        definitions=json.loads(text.split('const specs=',1)[1].split(';if(typeof module',1)[0])
        expected={sid:{'group':s['group'],'tenor_months':s['tenor_months'],'definition':list(s['reviewed_definition'])} for sid,s in store.catalog.SPECS.items()}
        self.assertEqual(definitions,expected)
        config=json.loads((DRAFT.parent/'config.json').read_text())
        self.assertEqual(config['architectures'],['x86_64']);self.assertEqual(config['memory_mb'],512);self.assertEqual(config['timeout_s'],600)
        self.assertNotIn('eventbridge_scheduler',config);self.assertNotIn('schedule',config)

    def test_legacy_allocation_and_alert_consumers_abstain_without_compatibility_signals(self):
        import ast
        for engine,fn in [('justhodl-allocator','rule_yield_curve'),('justhodl-alert-router','check_yield_curve')]:
            path=ROOT/('aws/lambdas/'+engine+'/source/lambda_function.py')
            tree=ast.parse(path.read_text(encoding='utf8'))
            candidates=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name==fn]
            if not candidates and engine=='justhodl-alert-router':
                candidates=[n for n in tree.body if isinstance(n,ast.FunctionDef) and 'yield_curve' in n.name]
            self.assertEqual(len(candidates),1)
            scope={'fs3':lambda *a:self.output,'load_json':lambda *a,**k:self.output}
            def forbidden(*a,**k):raise AssertionError('Research cannot create allocation or alert')
            scope.update(add=forbidden,emit=forbidden,send=forbidden)
            exec(compile(ast.Module(body=candidates,type_ignores=[]),str(path),'exec'),scope)
            fn=scope[candidates[0].name]
            if engine=='justhodl-allocator':fn({},[])
            else:
                alerts=[];fn(alerts);self.assertEqual(alerts,[])

    def test_unmodified_replay_evidence_cannot_authorize_a_changed_value(self):
        ref,view=store.retain(self.client,'b',self.inputs,self.output)
        packet={**view,'replay':ref};packet['series']['DGS2']['current']={'value':999,'exact_decimal':'999'}
        with self.assertRaisesRegex(ValueError,'Publication differs'):store.publish(self.client,'b',packet)
        self.assertFalse(any(w['Key']==model.CURRENT for w in self.client.writes))

    def test_fixed_arithmetic_and_current_leg_regression_preserve_history(self):
        from decimal import localcontext,ROUND_UP
        source,originals=fixtures();originals={s:originals.get(s) for s in store.catalog.SERIES}
        previous={s:None for s in store.catalog.SERIES}
        expected=model.build(source,originals,STAMP,self.inputs['context'],self.inputs['predecessor'],previous)
        with localcontext() as precision:
            precision.prec=7;precision.rounding=ROUND_UP
            self.assertEqual(model.build(source,originals,STAMP,self.inputs['context'],self.inputs['predecessor'],previous),expected)
        previous['DGS2']='2026-09-20T09:00:00Z'
        out=model.build(source,originals,STAMP,self.inputs['context'],self.inputs['predecessor'],previous)
        self.assertFalse(out['curves']['nominal']['complete']);self.assertIsNone(out['shape']['label'])
        self.assertEqual(out['series']['DGS2']['history'],expected['series']['DGS2']['history'])


if __name__=='__main__':unittest.main(verbosity=2)
