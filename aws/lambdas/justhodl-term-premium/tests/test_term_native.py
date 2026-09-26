"""Native publication, replay, missingness and actual consumer authority checks."""
from pathlib import Path
from copy import deepcopy
from unittest.mock import patch,Mock
import ast,hashlib,io,json,sys,unittest
ROOT=Path(__file__).resolve().parents[4];SOURCE=Path(__file__).resolve().parents[1]/'source'
sys.path[:0]=[str(SOURCE),str(ROOT/'tests')]
import term_premium_model as model
import term_premium_store as store
from test_term_premium_candidate import Book,RAW,SOURCE as RECEIPT,NOW

class Conflict(Exception):
    response={'Error':{'Code':'PreconditionFailed'}}
class Storage:
    def __init__(self):
        self.objects={model.CURRENT:b'{"generated_at":"2026-09-25T00:00:00Z","old":"whole"}',
            'data/history/acm-term-premium.json':b'[{"old_archive":"whole"}]'};self.reads=[];self.writes=[]
    def get_object(self,**kw):
        self.reads.append(kw['Key']);raw=self.objects[kw['Key']]
        return {'Body':io.BytesIO(raw),'ETag':hashlib.sha256(raw).hexdigest()}
    def put_object(self,**kw):
        key=kw['Key'];old=self.objects.get(key)
        if kw.get('IfNoneMatch')=='*' and old is not None:raise Conflict()
        if 'IfMatch' in kw and (old is None or kw['IfMatch']!=hashlib.sha256(old).hexdigest()):raise Conflict()
        self.objects[key]=kw['Body'];self.writes.append(kw)


class Tests(unittest.TestCase):
    def setUp(self):
        self.client=Storage();self.book=Book();self.patch=patch.object(model.arithmetic.xlrd,'open_workbook',return_value=self.book);self.patch.start();self.addCleanup(self.patch.stop)
        self.read=store.reader(self.client,'b');pre,previous,_=store.previous_state(self.client,'b')
        self.inputs={'contract':'term-premium-inputs.v1','generated_at':NOW,'predecessors':pre,'previous_watermarks':previous,'acquisition':{'status':'acquired'},
            'source':store.retain_bytes(self.client,'b',model.encoded(RECEIPT),'sources'),'workbook':store.retain_bytes(self.client,'b',RAW,'originals','xls')}
        self.output=store.compile_output(self.inputs,self.read)
    def packet(self):return store.retain(self.client,'b',self.inputs,self.output)
    def test_complete_compact_view_and_all_original_cells_replay(self):
        packet=self.packet();self.assertEqual(store.replay(packet,self.read),self.output)
        self.assertEqual(packet['view']['complete_original_data_rows'],320)
        self.assertEqual(len(packet['series']),60);self.assertFalse(packet['calls_eligible'])
        for name,table in self.output['tables'].items():
            self.assertNotIn('rows',packet['tables'][name])
            self.assertEqual(store.checked(packet['tables'][name]['complete_table_artifact'],'tables',self.read),table)
    def test_compiler_workbook_view_and_table_tampering_fail(self):
        packet=self.packet();manifest=store.binding(packet,self.read)
        for ref in (manifest['compilers']['xlrd/book.py'],self.inputs['workbook'],manifest['view'],manifest['tables']['ACM Daily']):
            old=self.client.objects[ref['key']];self.client.objects[ref['key']]=b'tampered'
            with self.assertRaises(ValueError):store.replay(packet,self.read)
            self.client.objects[ref['key']]=old
    def test_published_view_cannot_borrow_another_runs_evidence(self):
        packet=self.packet();bad=deepcopy(packet);bad['series']['D:ACMTP10']['current']['value']=99
        with self.assertRaises(ValueError):store.publish(self.client,'b',bad)
        self.assertTrue(store.publish(self.client,'b',packet));self.assertTrue(store.publish(self.client,'b',packet))
    def test_type_substitutions_fail_binding_replay_and_publication(self):
        packet=self.packet();before=self.client.objects[model.CURRENT]
        for change in (lambda p:p.update(calls_eligible=0),
                       lambda p:p['view'].update(complete_original_data_rows=320.0),
                       lambda p:p['dependency_graph'].update(independent_votes=False)):
            bad=deepcopy(packet);change(bad)
            with self.assertRaises(ValueError):store.binding(bad,self.read)
            with self.assertRaises(ValueError):store.replay(bad,self.read)
            with self.assertRaises(ValueError):store.publish(self.client,'b',bad)
            self.assertEqual(self.client.objects[model.CURRENT],before)
    def test_same_clock_type_conflict_and_changed_readback_do_not_pass(self):
        packet=self.packet();changed=deepcopy(packet);changed['view']['complete_original_data_rows']=320.0
        self.client.objects[model.CURRENT]=model.encoded(changed)
        with self.assertRaisesRegex(ValueError,'same-clock'):store.publish(self.client,'b',packet)
        self.client.objects[model.CURRENT]=b'{"generated_at":"2026-09-25T00:00:00Z"}'
        original=self.client.put_object
        def altered(**kw):
            original(**kw)
            if kw['Key']==model.CURRENT:self.client.objects[model.CURRENT]=model.encoded(changed)
        with patch.object(self.client,'put_object',side_effect=altered),self.assertRaisesRegex(ValueError,'readback'):
            store.publish(self.client,'b',packet)
    def test_ambiguous_json_cannot_receive_immutable_binding(self):
        for raw in (b'{"x":1,"x":1}',b'{"x":NaN}',b'{"x":Infinity}',b'{"x":1e999}'):
            with self.assertRaises(ValueError):store.strict(raw)
        packet=self.packet();manifest=store.binding(packet,self.read)
        raw=model.encoded(manifest);raw=b'{"contract":"term-premium-replay.v1",'+raw[1:]
        key=model.PREFIX+'runs/'+store.sha(raw)+'.json';self.client.objects[key]=raw
        packet['replay']['manifest_key']=key
        with self.assertRaisesRegex(ValueError,'Duplicate'):store.binding(packet,self.read)
    def test_authority_is_checked_at_final_publication_boundary(self):
        packet=self.packet()
        for change in (lambda p:p.update(sizing_eligible=True),lambda p:p['series']['D:ACMTP10'].update(calls_eligible=True),
            lambda p:p['decision'].update(verb='LONG'),lambda p:p['portfolio_consequences'].update(target_weights={'bonds':1})):
            bad=deepcopy(packet);change(bad)
            with self.assertRaises(ValueError):store.publish(self.client,'b',bad)
    def test_source_failure_preserves_history_without_refreshing_current(self):
        out=model.build(RAW,RECEIPT,NOW,None,self.inputs['predecessors'],{'status':'failed','error_type':'TimeoutError'})
        self.assertEqual(out['quality']['current_series'],0);self.assertEqual(out['tables'],self.output['tables'])
        self.assertTrue(all(row['current'] is None and row['current_comparisons'] is None for row in out['series'].values()))
        self.assertEqual(out['source']['acquired_at'],RECEIPT['acquired_at'])
    def test_older_workbook_with_newer_acquisition_cannot_become_current(self):
        previous=model.continuity(self.output);previous['observations']['ACM Daily']='2026-09-25'
        out=model.build(RAW,RECEIPT,NOW,previous,self.inputs['predecessors'],{'status':'acquired'})
        self.assertEqual(out['quality']['current_series'],30)
        self.assertEqual(out['source_watermarks']['observations']['ACM Daily'],'2026-09-25')
        self.assertEqual(out['series']['D:ACMTP10']['quality']['status'],'source_regression')
        self.assertIsNone(out['series']['D:ACMTP10']['current'])
    def test_missing_latest_keeps_observation_high_watermark(self):
        previous=model.continuity(self.output);self.book.sheets['ACM Daily'].rows[-1][20]=None
        out=model.build(RAW,RECEIPT,NOW,previous,self.inputs['predecessors'],{'status':'acquired'})
        self.assertIsNone(out['series']['D:ACMTP10']['current']);self.assertEqual(out['source_watermarks'],previous)
    def test_newer_or_conflicting_current_head_is_not_overwritten(self):
        packet=self.packet();future=deepcopy(packet);future['generated_at']='2026-09-27T00:00:00Z'
        self.client.objects[model.CURRENT]=model.encoded(future);self.assertFalse(store.publish(self.client,'b',packet))
        future['generated_at']=NOW;future['extra']='conflict';self.client.objects[model.CURRENT]=model.encoded(future)
        with self.assertRaises(ValueError):store.publish(self.client,'b',packet)
    def test_compare_and_swap_race_does_not_overwrite_newer_source(self):
        packet=self.packet();original=self.client.put_object;future=deepcopy(packet);future['generated_at']='2026-09-27T00:00:00Z'
        def race(**kw):
            if kw['Key']==model.CURRENT:self.client.objects[model.CURRENT]=model.encoded(future)
            return original(**kw)
        with patch.object(self.client,'put_object',side_effect=race):self.assertFalse(store.publish(self.client,'b',packet))
    def test_normal_path_and_failed_next_acquisition_are_replayable(self):
        with patch.object(store,'now',return_value=NOW),patch.object(store,'acquire',return_value=(RAW,RECEIPT)) as acquire:
            result=store.run(self.client,'b','normal-event');self.assertTrue(result['published']);self.assertEqual(acquire.call_count,1)
            with self.assertRaises(Conflict):store.run(self.client,'b','normal-event')
            self.assertEqual(acquire.call_count,1)
        with patch.object(store,'now',return_value='2026-09-26T05:00:00Z'),patch.object(store,'acquire',side_effect=TimeoutError):
            result=store.run(self.client,'b','next-normal-event');self.assertTrue(result['published'])
        packet=json.loads(self.client.objects[model.CURRENT]);self.assertEqual(packet['quality']['current_series'],0)
        self.assertEqual(store.replay(packet,self.read)['source']['acquired_at'],NOW)
    def test_first_migration_failure_does_not_fabricate_source_from_parsed_archive(self):
        old=self.client.objects[model.CURRENT]
        with patch.object(store,'now',return_value=NOW),patch.object(store,'acquire',side_effect=TimeoutError),self.assertRaises(TimeoutError):
            store.run(self.client,'b','failed-first-event')
        self.assertEqual(self.client.objects[model.CURRENT],old)
    def test_public_reader_never_requests_private_account_or_arbitrary_paths(self):
        self.client.reads=[]
        for key in ('audit-private/x','data/account.json','data/term-premium-research/originals/../secret'):
            with self.assertRaises(ValueError):self.read(key)
        self.assertEqual(self.client.reads,[])
    def test_public_http_and_validation_never_acquire_or_publish(self):
        import lambda_function as handler
        with patch.object(handler.boto3,'client') as client:
            self.assertEqual(handler.lambda_handler({'httpMethod':'GET'})['statusCode'],307)
            self.assertEqual(handler.lambda_handler({'validate_only':True})['statusCode'],200)
            with self.assertRaises(ValueError):handler.lambda_handler({})
            client.assert_not_called()
    def test_runtime_and_conserved_predecessor_remain_exact(self):
        store.qualified_arithmetic();config=json.loads((SOURCE.parent/'config.json').read_bytes())
        self.assertEqual((config['memory'],config['timeout'],config['architectures']),(512,120,['x86_64']))
        sys.path.insert(0,str(ROOT/'scripts'));from normalize_lambda_config import normalize_config
        normalized=normalize_config(config)
        self.assertNotIn('eventbridge_scheduler',normalized);self.assertNotIn('schedule',normalized)
        self.assertEqual(normalized['release_schedule_note']['binding_action'],'PRESERVE_EXISTING')
        self.assertEqual(config['schedule'],'cron(45 13 ? * MON-FRI *)')
        self.assertEqual(config['preserved_schedule_reference'],json.loads((Path(__file__).parent/'legacy_config.json.txt').read_bytes())['schedule'])
        old=(Path(__file__).parent/'legacy_lambda_function.py.txt').read_bytes()
        self.assertEqual(len(old),7511)
        self.assertIn(b'data/history/acm-term-premium.json',old)
        for name,digest in {'legacy_lambda_function.py.txt':'28cf7619d859d2157ab553b9c749fd220a770ced25ca1dfff2d6a71cbf63dd62',
            'legacy_term_premium.html.txt':'18f81c1c9c632dec8b723b47445cc66804ce78b768d87f26fd4e6e63a0f2d988',
            'legacy_config.json.txt':'2f8ae64c4626a44d373bbe34b39489f5af70652ced0d0a74c3313761193157ab'}.items():
            self.assertEqual(hashlib.sha256((Path(__file__).parent/name).read_bytes()).hexdigest(),digest)
    def test_actual_signal_board_and_bond_desk_abstain(self):
        from signal_board_native_test_support import assert_abstention
        assert_abstention('data/term-premium.json',(self.output,{'score':99,'calls_eligible':True},{},None))
        path=ROOT/'aws/lambdas/justhodl-bond-desk/source/lambda_function.py';tree=ast.parse(path.read_text(encoding='utf8'))
        node=next(n for n in ast.walk(tree) if isinstance(n,ast.If) and ast.unparse(n.test)=="tp.get('calls_eligible') is False")
        namespace={'tp':self.output,'US':{'score':50,'fresh':True}}
        exec(compile(ast.Module(body=[node],type_ignores=[]),str(path),'exec'),namespace)
        self.assertIsNone(namespace['US']['score']);self.assertFalse(namespace['US']['fresh'])


if __name__=='__main__':unittest.main(verbosity=2)
