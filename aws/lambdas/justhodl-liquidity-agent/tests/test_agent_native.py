from pathlib import Path
from copy import deepcopy
from datetime import datetime,timezone
from unittest.mock import patch
import hashlib,json,sys,unittest
ROOT=Path(__file__).resolve().parents[4];DRAFT=Path(__file__).resolve().parents[1]/'source'
sys.path[:0]=[str(DRAFT),str(ROOT/'aws/shared'),str(ROOT/'aws/lambdas/justhodl-crisis-composite/tests')]
import liquidity_agent_model as model
import liquidity_agent_store as store
from test_native_research import Storage,store_fixture,fixtures,STAMP


def setup():
    objects,_=store_fixture();client=Storage(objects)
    legacy=b'{"generated_at":"2026-09-17T00:00:00Z","legacy":"complete old snapshot"}'
    client.objects[model.CURRENT]=legacy
    context={}
    for key in store.catalog.CONTEXT_KEYS:
        raw=b'{"context":"retained separately"}'
        client.objects[key]=raw
        context[key]={'status':'retained_unqualified_context','original':store.private_bytes(client,'b',raw),
            'captured_at':STAMP,'independent_votes':0}
    return client,{'contract':'liquidity-agent-inputs.v1','generated_at':STAMP,
        'macro':store.retain_bytes(client,'b',client.objects[store.SOURCE],'snapshots'),
        'contexts':context,'predecessor':store.private_bytes(client,'b',legacy),
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
        self.assertEqual(len(view['series']),73)
        self.assertLess(len(model.encoded(view)),len(model.encoded(full)))
        for sid,row in full['series'].items():
            self.assertNotIn('history',view['series'][sid])
            artifact=view['series'][sid]['complete_history_artifact']
            self.assertEqual(store.checked(artifact,'series',self.read),row)
            self.assertEqual(view['series'][sid]['retained_original_rows'],len(row['history']))
        self.assertFalse(view['calls_eligible']);self.assertFalse(view['sizing_eligible'])
    def test_any_missing_or_tampered_shard_cannot_pass_full_replay(self):
        ref,view=store.retain(self.client,'b',self.inputs,self.output)
        target=view['series']['WALCL']['complete_history_artifact']['key']
        self.client.objects[target]=b'{}'
        with self.assertRaises(ValueError):store.replay(ref,self.read)
    def test_unauthorized_paths_fail_before_read(self):
        for key in ('data/portfolio.json','audit-private/x','data/liquidity-agent-research/series/../../x'):
            with self.assertRaises(ValueError):self.read(key)
        self.assertEqual(self.client.reads,[])
    def test_original_closure_and_view_corruption_is_detected(self):
        ref,view=store.retain(self.client,'b',self.inputs,self.output)
        manifest=json.loads(self.read(ref['manifest_key']))
        for key in (manifest['compilers']['liquidity_agent_model']['key'],manifest['view']['key'],self.inputs['macro']['key']):
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
        source,originals=fixtures();originals={s:o for s,o in originals.items() if s in store.catalog.SERIES}
        previous={s:None for s in store.catalog.SERIES};previous['WALCL']='2026-09-20T09:00:00Z'
        output=model.build(source,originals,STAMP,self.inputs['contexts'],self.inputs['predecessor'],previous)
        self.assertEqual(output['series']['WALCL']['quality']['status'],'source_regression')
        self.assertIsNone(output['series']['WALCL']['current']['value']);self.assertIsNone(output['derived']['net_liquidity']['current'])
        self.assertEqual(output['source_acquisition_watermarks']['WALCL'],previous['WALCL'])
        self.assertTrue(output['series']['WALCL']['history'])
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
        source,originals=fixtures();originals={s:o for s,o in originals.items() if s in store.catalog.SERIES}
        high='2026-09-20T10:00:00Z';previous=deepcopy(self.inputs['previous_watermarks']);previous['WALCL']=high
        missing=deepcopy(source);missing['measurements'].pop('WALCL')
        missing['replay']['output_sha256']=model.digest({k:v for k,v in missing.items() if k!='replay'})
        subset={s:o for s,o in originals.items() if s!='WALCL'}
        packet=model.build(missing,subset,'2026-09-20T11:00:00Z',self.inputs['contexts'],self.inputs['predecessor'],previous)
        self.assertIsNone(packet['series']['WALCL']['current']['value']);self.assertEqual(packet['source_acquisition_watermarks']['WALCL'],high)
        recovery=model.build(source,originals,'2026-09-20T12:00:00Z',self.inputs['contexts'],self.inputs['predecessor'],model.watermarks(packet))
        self.assertEqual(recovery['series']['WALCL']['quality']['status'],'source_regression');self.assertIsNone(recovery['derived']['net_liquidity']['current'])
        self.assertTrue(recovery['series']['WALCL']['history'])

    def test_publication_boundary_refuses_invented_authority_before_storage_access(self):
        replay,view=store.retain(self.client,'b',self.inputs,self.output);packet={**view,'replay':replay}
        for change in (lambda p:p.update(sizing_eligible=True),lambda p:p['decision'].update(verb='LONG'),
                       lambda p:p['series']['WALCL'].update(calls_eligible=True),lambda p:p.pop('replay')):
            bad=deepcopy(packet);change(bad)
            with patch.object(self.client,'get_object',side_effect=AssertionError('Storage must not be read')):
                with self.assertRaises(ValueError):store.publish(self.client,'b',bad)

    def test_changed_value_or_json_type_cannot_borrow_existing_replay(self):
        ref,view=store.retain(self.client,'b',self.inputs,self.output);packet={**view,'replay':ref}
        before=self.client.objects[model.CURRENT]
        for change in (lambda p:p['series']['WALCL']['current'].update(value=999),
                       lambda p:p['series']['WALCL'].update(retained_original_rows=float(p['series']['WALCL']['retained_original_rows'])),
                       lambda p:p['quality'].update(release_calendar_verified=0)):
            bad=deepcopy(packet);change(bad)
            with self.assertRaisesRegex(ValueError,'retained original-source view'):store.publish(self.client,'b',bad)
            self.assertEqual(self.client.objects[model.CURRENT],before)
        self.assertFalse(any(row['Key']==model.CURRENT for row in self.client.writes))

    def test_same_clock_alteration_and_bad_watermarks_or_readback_cannot_pass(self):
        ref,view=store.retain(self.client,'b',self.inputs,self.output);packet={**view,'replay':ref}
        bad=deepcopy(packet);bad['series']['WALCL']['retained_original_rows']=float(bad['series']['WALCL']['retained_original_rows'])
        self.client.objects[model.CURRENT]=model.encoded(bad)
        with self.assertRaisesRegex(ValueError,'same-clock'):store.publish(self.client,'b',packet)
        with self.assertRaisesRegex(ValueError,'retained original-source view'):store.previous_state(self.client,'b')
        self.client.objects[model.CURRENT]=b'{"generated_at":"2026-09-17T00:00:00Z"}';original=self.client.put_object
        def alter(**request):
            original(**request)
            if request['Key']==model.CURRENT:self.client.objects[model.CURRENT]=model.encoded(bad)
        with patch.object(self.client,'put_object',side_effect=alter),self.assertRaisesRegex(ValueError,'readback'):
            store.publish(self.client,'b',packet)

    def test_public_replay_command_is_type_exact(self):
        sys.path.insert(0,str(ROOT/'scripts'))
        from replay_liquidity_agent_research import verify
        ref,view=store.retain(self.client,'b',self.inputs,self.output);packet={**view,'replay':ref}
        self.assertTrue(verify(packet,self.read)['replayed']);packet['calls_eligible']=0
        with self.assertRaisesRegex(ValueError,'Published view differs'):verify(packet,self.read)

    def test_only_exact_reviewed_storage_predecessor_replays_without_executing_it(self):
        ref,view=store.retain(self.client,'b',self.inputs,self.output);manifest=json.loads(self.read(ref['manifest_key']))
        raw=(DRAFT.parent/'tests/legacy_store_before_typed_binding.py.txt').read_bytes()
        digest=store.sha(raw);self.assertEqual(len(raw),12994);self.assertIn(digest,store.REVIEWED_STORAGE_REVISIONS)
        key=model.PREFIX+'compilers/'+digest+'.py';self.client.objects[key]=raw
        manifest['compilers']['liquidity_agent_store']={'key':key,'sha256':digest}
        def retained_ref(value):
            r=store.retain_bytes(self.client,'b',model.encoded(value),'runs')
            return {'manifest_key':r['key'],'output_sha256':value['output_sha256']}
        prior=retained_ref(manifest)
        full,restored=store.replay(prior,self.read);self.assertEqual(full,self.output);self.assertEqual(restored,view)
        self.client.objects[key]=raw+b'\n'
        with self.assertRaisesRegex(ValueError,'predecessor storage bytes'):store.replay(prior,self.read)
        unknown=b'raise AssertionError("archived code must never execute")'
        unknown_key=model.PREFIX+'compilers/'+store.sha(unknown)+'.py';self.client.objects[unknown_key]=unknown
        manifest['compilers']['liquidity_agent_store']={'key':unknown_key,'sha256':store.sha(unknown)}
        with self.assertRaisesRegex(ValueError,'reviewed compiler'):store.replay(retained_ref(manifest),self.read)

    def test_ambiguous_or_nonfinite_json_cannot_bind_to_originals(self):
        for raw in (b'{"x":1,"x":1}',b'{"x":NaN}',b'{"x":1e999}'):
            with self.assertRaises(ValueError):store.strict(raw)
        ref,view=store.retain(self.client,'b',self.inputs,self.output)
        raw=self.client.objects[ref['manifest_key']];raw=b'{"contract":"liquidity-agent-replay.v1",'+raw[1:]
        key=model.PREFIX+'runs/'+store.sha(raw)+'.json';self.client.objects[key]=raw
        with self.assertRaisesRegex(ValueError,'Duplicate'):store.replay({**ref,'manifest_key':key},self.read)

    def test_http_validation_and_missing_execution_context_cannot_publish(self):
        import importlib.util
        spec=importlib.util.spec_from_file_location('native_liquidity_handler',DRAFT/'lambda_function.py')
        handler=importlib.util.module_from_spec(spec);spec.loader.exec_module(handler)
        with patch.object(handler.boto3,'client',side_effect=AssertionError('AWS access forbidden')):
            self.assertEqual(handler.lambda_handler({'httpMethod':'GET'},None)['statusCode'],307)
            self.assertEqual(handler.lambda_handler({'requestContext':{'http':{'method':'GET'}}},None)['statusCode'],307)
            self.assertFalse(json.loads(handler.lambda_handler({'validate_only':True},None)['body'])['published'])
            with self.assertRaises(ValueError):handler.lambda_handler({},None)

    def test_complete_predecessors_and_exact_reviewed_arithmetic_and_browser_definitions(self):
        fixture=json.loads((ROOT/'tests/fixtures/liquidity-agent-native-migration.json').read_text())
        for row in fixture['complete_predecessors']:
            raw=(ROOT/row['predecessor']).read_bytes()
            self.assertEqual(len(raw),row['bytes']);self.assertEqual(hashlib.sha256(raw).hexdigest(),row['sha256'])
        store.qualified_arithmetic()
        text=(ROOT/'jh-liquidity-agent-definitions.js').read_text()
        definitions=json.loads(text.split('const specs=',1)[1].split(';if(typeof module',1)[0])
        expected={sid:{'group':s['group'],'definition':list(s['reviewed_definition']) if s['reviewed_definition'] else None} for sid,s in store.catalog.SPECS.items()}
        self.assertEqual(definitions,expected)
        config=json.loads((DRAFT.parent/'config.json').read_text())
        self.assertEqual(config['architectures'],['arm64']);self.assertEqual(config['memory_mb'],512);self.assertEqual(config['timeout_s'],300)
        self.assertEqual(config['preserved_schedule_reference']['cron'],'cron(30 12 * * ? *)')
        sys.path.insert(0,str(ROOT/'scripts'));from normalize_lambda_config import normalize_config
        normalized=normalize_config(config)
        self.assertNotIn('eventbridge_scheduler',normalized);self.assertNotIn('schedule',normalized)
        self.assertEqual(normalized['release_schedule_note']['binding_action'],'PRESERVE_EXISTING')

    def test_legacy_verdict_consumer_treats_research_as_unavailable(self):
        import ast
        path=ROOT/'aws/lambdas/justhodl-self-critique/source/lambda_function.py'
        node=next(n for n in ast.parse(path.read_text()).body if isinstance(n,ast.FunctionDef) and n.name=='_pluck')
        scope={};exec(compile(ast.Module(body=[node],type_ignores=[]),str(path),'exec'),scope)
        self.assertIsNone(scope['_pluck'](self.output,['part4.credit_first_sequence.verdict']))


if __name__=='__main__':unittest.main(verbosity=2)
