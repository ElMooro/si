"""Whole restored handler and note-limit regressions without AWS or Brain writes."""
from pathlib import Path
from unittest.mock import patch
import ast,hashlib,importlib.util,io,json,sys,types,unittest
SOURCE=Path(__file__).resolve().parents[1]/'source/lambda_function.py'
with patch.dict(sys.modules,{'boto3':types.SimpleNamespace(client=lambda *a,**kw:None)}):
    spec=importlib.util.spec_from_file_location('tv_notes_restored',SOURCE)
    engine=importlib.util.module_from_spec(spec);spec.loader.exec_module(engine)


class Tests(unittest.TestCase):
    def test_complete_predecessor_retained_and_every_handler_survives(self):
        raw=(Path(__file__).parent/'fixtures/pre-save-repair.py.txt').read_bytes()
        self.assertEqual(raw.count(b'MAX_TEXT = 32000'),1)
        original=raw.replace(b'MAX_TEXT = 32000',b'MAX_TEXT = 8000')
        self.assertEqual(len(original),22973)
        self.assertEqual(hashlib.sha256(original).hexdigest(),'27c008f20ab921355b5cf5a0622140575572af231399d4c97eca874e200f8e37')
        before={n.name for n in ast.parse(raw).body if isinstance(n,ast.FunctionDef)}
        after={n.name for n in ast.parse(SOURCE.read_bytes()).body if isinstance(n,ast.FunctionDef)}
        self.assertLessEqual(before,after)
    def test_note_limit_keeps_long_complete_notes_and_caps_bounded_text(self):
        for n in (9000,31990,31991,40000):
            note={'symbol':'SPY','text':'A'*n,'created':1720000000000}
            result=engine._norm(note)
            self.assertEqual(len(result['text']),min(n+9,32000))
            self.assertEqual(result['text'].endswith('…'),n+9>32000)
            self.assertEqual(result['id'],engine._norm(note)['id'])
    def test_unicode_limit_is_characters_and_rejection_remains(self):
        self.assertEqual(len(engine._norm({'symbol':'SPY','text':'界'*40000})['text']),32000)
        self.assertIsNone(engine._norm({'text':'x'}));self.assertIsNone(engine._norm(None))
    def test_actual_preflight_handler_needs_no_account_or_provider_access(self):
        with patch.object(engine,'_mirror_read',side_effect=AssertionError('account read')), \
             patch.object(engine,'_brain_put',side_effect=AssertionError('Brain write')):
            result=engine.lambda_handler({'requestContext':{'http':{'method':'OPTIONS'}}},None)
        self.assertEqual(result['statusCode'],204)

    def test_description_save_uses_real_client_and_retains_existing_metadata(self):
        s3=FakeS3({'descriptions':{'OLD':'retained'},'extra':{'retained':True}})
        with patch.object(engine,'S3',s3):
            self.assertEqual(engine._save_descs({'NEW':'new name','OLD':'must not replace'}),1)
            self.assertEqual(engine._save_descs({'NEW':'duplicate'}),0)
        self.assertEqual(s3.document['descriptions'],{'OLD':'retained','NEW':'new name'})
        self.assertEqual(s3.document['extra'],{'retained':True})
        self.assertEqual(len(s3.writes),1);self.assertEqual(s3.writes[0]['IfMatch'],'"v0"')

    def test_missing_description_object_uses_conditional_creation(self):
        s3=FakeS3(None)
        with patch.object(engine,'S3',s3):self.assertEqual(engine._save_descs({'NEW':'名字'}),1)
        self.assertEqual(s3.writes[0]['IfNoneMatch'],'*')
        self.assertEqual(s3.document['descriptions']['NEW'],'名字')

    def test_unreadable_malformed_or_unversioned_description_store_cannot_be_overwritten(self):
        for failure in [StoreError('AccessDenied'),StoreError('SlowDown'),ValueError('bad response')]:
            s3=FakeS3({});s3.read_failure=failure
            with patch.object(engine,'S3',s3),self.assertRaises(Exception):engine._save_descs({'NEW':'new'})
            self.assertEqual(s3.writes,[])
        for original in [[],{'descriptions':[]},{'other':'data'}]:
            s3=FakeS3(original)
            with patch.object(engine,'S3',s3),self.assertRaises(ValueError):engine._save_descs({'NEW':'new'})
            self.assertEqual(s3.writes,[])
        s3=FakeS3({'descriptions':{}});s3.etag=None
        with patch.object(engine,'S3',s3),self.assertRaises(ValueError):engine._save_descs({'NEW':'new'})
        self.assertEqual(s3.writes,[])

    def test_ambiguous_stored_json_cannot_erase_records(self):
        for raw in (b'{"descriptions":{"OLD":"one","OLD":"two"}}',b'{"descriptions":{},"extra":NaN}'):
            s3=FakeS3({})
            with patch.object(engine,'S3',s3),patch.object(s3,'get_object',return_value={'Body':io.BytesIO(raw),'ETag':'"v0"'}),self.assertRaises(ValueError):
                engine._save_descs({'NEW':'new'})
            self.assertEqual(s3.writes,[])

    def test_conflicting_write_reloads_and_preserves_concurrent_insert(self):
        s3=FakeS3({'descriptions':{'OLD':'old'}});s3.conflicts=1
        with patch.object(engine,'S3',s3):self.assertEqual(engine._save_descs({'NEW':'new'}),1)
        self.assertEqual(s3.document['descriptions'],{'OLD':'old','CONCURRENT':'another batch','NEW':'new'})
        self.assertEqual(s3.writes[-1]['IfMatch'],'"v1"')
        s3=FakeS3({'descriptions':{'OLD':'old'}});s3.conflicts=10
        with patch.object(engine,'S3',s3),self.assertRaisesRegex(RuntimeError,'conflict'):engine._save_descs({'NEW':'new'})
        self.assertEqual(len(s3.writes),3);self.assertNotIn('NEW',s3.document['descriptions'])
        self.assertTrue(all('IfMatch' in row for row in s3.writes))

    def test_create_race_reloads_the_winner_and_write_denial_does_not_retry_unconditionally(self):
        s3=FakeS3(None);s3.conflicts=1
        with patch.object(engine,'S3',s3):self.assertEqual(engine._save_descs({'NEW':'new'}),1)
        self.assertEqual(s3.writes[0]['IfNoneMatch'],'*');self.assertEqual(s3.writes[1]['IfMatch'],'"v1"')
        self.assertEqual(s3.document['descriptions'],{'CONCURRENT':'another batch','NEW':'new'})
        s3=FakeS3({'descriptions':{'OLD':'old'}})
        with patch.object(engine,'S3',s3),patch.object(s3,'put_object',side_effect=StoreError('AccessDenied')) as put,self.assertRaises(StoreError):
            engine._save_descs({'NEW':'new'})
        self.assertEqual(put.call_count,1);self.assertEqual(s3.document['descriptions'],{'OLD':'old'})

    def test_selftest_precedes_every_storage_and_mutation_path(self):
        data={'token':'fixture','selftest':True,'notes':[{'text':'synthetic note','created':1720000000000}],
              'watchlists':[{'name':'test'}],'sources':[{'symbol':'TEST','source':'test'}],
              'descs':{'TEST':'test'},'harvest_diag':{'test':True}}
        names=['_save_descs','_save_sources','_save_watchlists','_save_bars','_save_series','_mirror_read','_mirror_write','_brain_put']
        from contextlib import ExitStack
        with ExitStack() as stack:
            for name in names:stack.enter_context(patch.object(engine,name,side_effect=AssertionError('unexpected storage or mutation')))
            ssm=stack.enter_context(patch.object(engine,'_ssm',side_effect=lambda name,key:'fixture' if key=='INGEST_TOKEN' else self.fail('personal UID lookup')))
            result=engine.lambda_handler({'body':json.dumps(data)},None)
            body=json.loads(result['body']);self.assertEqual(result['statusCode'],200)
            self.assertTrue(body['dryrun']);self.assertEqual(body['would_ingest'],1)
            self.assertEqual(set(body['ignored_sections']),{'watchlists','sources','descs','harvest_diag'})
            for extra in ({'kind':'bars','bars':[]},{'kind':'series','series':[]},{'delete_ids':['fixture']}):
                reply=engine.lambda_handler({'body':json.dumps({**data,**extra})},None)
                self.assertEqual(reply['statusCode'],400);self.assertTrue(json.loads(reply['body'])['dryrun'])

    def test_oversized_notes_are_rejected_before_any_auxiliary_mutation_or_account_lookup(self):
        with patch.object(engine,'_ssm',side_effect=lambda name,key:'fixture' if key=='INGEST_TOKEN' else self.fail('UID lookup')), \
             patch.object(engine,'_save_descs',side_effect=AssertionError('save')):
            result=engine.lambda_handler({'body':json.dumps({'token':'fixture','notes':[{}]*2001,'descs':{'TEST':'test'}})},None)
        self.assertEqual(result['statusCode'],413)
        for payload in ([],None,True):
            self.assertEqual(engine.lambda_handler({'body':json.dumps(payload)},None)['statusCode'],400)

    def test_description_failure_is_returned_and_source_failure_does_not_block_it(self):
        with patch.object(engine,'_ssm',return_value='fixture'),patch.object(engine,'_save_watchlists',return_value=0), \
             patch.object(engine,'_save_sources',side_effect=ValueError('source failure')), \
             patch.object(engine,'_save_descs',return_value=1) as descriptions:
            result=engine.lambda_handler({'body':json.dumps({'token':'fixture','descs':{'TEST':'test'}})},None)
            descriptions.assert_called_once();body=json.loads(result['body'])
            self.assertEqual(result['statusCode'],200);self.assertEqual(body['descriptions_saved'],1)
            self.assertEqual(body['save_errors'],['sources_save_failed'])
        with patch.object(engine,'_ssm',return_value='fixture'),patch.object(engine,'_save_descs',side_effect=ValueError('private message')):
            result=engine.lambda_handler({'body':json.dumps({'token':'fixture','descs':{'TEST':'test'}})},None)
        self.assertEqual(result['statusCode'],502);self.assertNotIn('private message',result['body'])
        self.assertEqual(json.loads(result['body'])['save_errors'],['descriptions_save_failed'])

    def test_description_only_idempotent_retry_returns_success(self):
        with patch.object(engine,'_ssm',return_value='fixture'),patch.object(engine,'S3',FakeS3({'descriptions':{'TEST':'existing'}})):
            result=engine.lambda_handler({'body':json.dumps({'token':'fixture','descs':{'TEST':'test'}})},None)
        self.assertEqual(result['statusCode'],200);self.assertEqual(json.loads(result['body'])['descriptions_saved'],0)


class StoreError(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}


class FakeS3:
    def __init__(self,document):
        self.document=document;self.etag='"v0"';self.writes=[];self.read_failure=None;self.conflicts=0;self.version=0
    def get_object(self,**kwargs):
        if self.read_failure:raise self.read_failure
        if self.document is None:raise StoreError('NoSuchKey')
        return {'Body':io.BytesIO(json.dumps(self.document).encode()),'ETag':self.etag}
    def put_object(self,**kwargs):
        self.writes.append(kwargs)
        if self.conflicts:
            self.conflicts-=1;self.version+=1;self.etag='"v%d"'%self.version
            if self.document is None:self.document={'descriptions':{}}
            self.document['descriptions']['CONCURRENT']='another batch'
            raise StoreError('PreconditionFailed')
        if self.document is None:
            assert kwargs['IfNoneMatch']=='*'
        else:assert kwargs['IfMatch']==self.etag
        self.document=json.loads(kwargs['Body']);self.version+=1;self.etag='"v%d"'%self.version


if __name__=='__main__':unittest.main(verbosity=2)
