from pathlib import Path
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import patch
import base64,gzip,hashlib,importlib.util,io,json,sqlite3,sys,tempfile,unittest
R=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(R/'aws/lambdas/justhodl-symdir/source'))
import warehouse_cache as m
fixture=json.loads((R/'tests/fixtures/symbol-directory/warehouse-cache-reproduction.json').read_bytes())
ORIGINAL=base64.b64decode(fixture['whole_invented_database_base64'])


class Store:
    def __init__(self,database=ORIGINAL,clock='2000-01-01T00:00:00+00:00'):
        self.database=database;self.body=gzip.compress(database);digest=hashlib.sha256(self.body).hexdigest()
        self.key='data/search/index/provider-search-20000101T000000Z-'+digest[:12]+'.sqlite.gz'
        self.head={'schema_version':1,'generated_at':clock,'index':{'key':self.key,'bytes':len(self.body),
            'uncompressed_bytes':len(database),'sha256':digest,'format':'sqlite-fts5+gzip'}}
        self.calls=[];self.streams=[]

    def get_object(self,**kw):
        assert kw['Bucket']=='invented-bucket';key=kw['Key'];self.calls.append(key)
        assert key in (m.MANIFEST_KEY,self.key)
        raw=json.dumps(self.head).encode() if key==m.MANIFEST_KEY else self.body
        stream=io.BytesIO(raw);self.streams.append(stream)
        return {'Body':stream,'ContentLength':len(raw)}


class Tests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.active=self.root/'active.sqlite';self.state={}

    def materialize(self,store,force=False):
        return m.materialize(self.state,store,'invented-bucket',self.active,'2000-01-04T00:00:00+00:00',force)

    def install(self):
        store=Store();self.materialize(store);return store

    def changed(self):
        path=self.root/'changed.sqlite';path.write_bytes(ORIGINAL)
        con=sqlite3.connect(path);con.execute("UPDATE docs SET title='Invented replacement title'");con.commit();con.close()
        raw=path.read_bytes();path.unlink();return Store(raw)

    def reject_preserving(self,store):
        state=deepcopy(self.state);before=self.active.read_bytes()
        with self.assertRaises(Exception):self.materialize(store,True)
        self.assertEqual(self.state,state);self.assertEqual(self.active.read_bytes(),before)
        self.assertEqual(list(self.root.iterdir()),[self.active]);self.assertTrue(all(x.closed for x in store.streams))

    def tight_space(self,store):
        capacity=self.active.stat().st_size+len(store.body)+8192
        def usage(path):
            used=sum(p.stat().st_size for p in self.root.rglob('*') if p.is_file())
            return SimpleNamespace(free=m.RESERVE+capacity-used)
        return patch.object(m.shutil,'disk_usage',side_effect=usage)

    def test_whole_database_matches_source_and_same_identity_skips_repeated_download(self):
        store=self.install();self.assertEqual(self.active.read_bytes(),ORIGINAL)
        self.assertEqual(self.materialize(store),str(self.active))
        self.assertEqual(store.calls,[m.MANIFEST_KEY,store.key,m.MANIFEST_KEY])
        self.assertEqual(self.state['expanded_sha256'],hashlib.sha256(ORIGINAL).hexdigest())
        self.assertIs(self.state['investment_authority'],False);self.assertTrue(all(s.closed for s in store.streams))

    def test_same_clock_replacement_uses_artifact_identity(self):
        self.install();new=self.changed();self.materialize(new)
        self.assertEqual(self.active.read_bytes(),new.database)
        self.assertEqual(new.calls,[m.MANIFEST_KEY,new.key])
        self.assertEqual(self.state['identity']['sha256'],new.head['index']['sha256'])

    def test_failed_acquisition_and_bad_digest_leave_previous_file_and_state_intact(self):
        self.install()
        for mutate in (lambda s:setattr(s,'body',s.body[:-1]),lambda s:setattr(s,'body',b'X'+s.body[1:]),
                       lambda s:s.head['index'].__setitem__('sha256','a'*64)):
            store=self.changed();mutate(store);self.reject_preserving(store)
        store=self.changed()
        with patch.object(store,'get_object',side_effect=OSError('Invented acquisition outage')):
            self.reject_preserving(store)

    def test_invalid_or_regressing_metadata_never_fetches_an_arbitrary_object(self):
        self.install()
        for mutate in (lambda s:s.head.__setitem__('index',None),lambda s:s.head['index'].__setitem__('key','private/account.json'),
                       lambda s:s.head['index'].__setitem__('bytes',True),lambda s:s.head['index'].__setitem__('uncompressed_bytes',0),
                       lambda s:s.head['index'].__setitem__('uncompressed_bytes',m.MAX_DATABASE_BYTES+1),
                       lambda s:s.head['index'].__setitem__('sha256',None),lambda s:s.head['index'].__setitem__('format','unknown'),
                       lambda s:s.head.__setitem__('generated_at','1999-01-01T00:00:00+00:00'),
                       lambda s:s.head.__setitem__('generated_at','2001-01-01T00:00:00+00:00')):
            store=self.changed();mutate(store);self.reject_preserving(store);self.assertEqual(store.calls,[m.MANIFEST_KEY])

    def test_bad_database_structure_does_not_replace_existing_file(self):
        self.install();self.reject_preserving(Store(b'Invented non-SQLite bytes'))
        path=self.root/'wrong-schema.sqlite';con=sqlite3.connect(path);con.execute('CREATE TABLE docs(id TEXT)');con.commit();con.close()
        bad=path.read_bytes();path.unlink();self.reject_preserving(Store(bad))

    def test_overlong_expansion_is_rejected_before_install(self):
        self.install();store=self.changed();store.head['index']['uncompressed_bytes']-=1;self.reject_preserving(store)

    def test_verified_checkpoint_allows_refresh_without_two_full_databases(self):
        self.install();store=self.changed();original=m.checkpoint
        with self.tight_space(store),patch.object(m,'checkpoint',wraps=original) as checkpoint:
            self.materialize(store,True);self.assertEqual(checkpoint.call_count,1)
        self.assertEqual(self.active.read_bytes(),store.database);self.assertEqual(list(self.root.iterdir()),[self.active])

    def test_after_releasing_old_file_failed_inspection_restores_every_original_byte(self):
        self.install();store=self.changed()
        with self.tight_space(store),patch.object(m,'inspect_database',side_effect=m.WarehouseCacheError('Invented structural failure')):
            self.reject_preserving(store)

    def test_failed_checkpoint_or_insufficient_space_never_releases_original_file(self):
        self.install();store=self.changed()
        with self.tight_space(store),patch.object(m,'checkpoint',side_effect=OSError('Invented checkpoint failure')):
            self.reject_preserving(store)
        with patch.object(m.shutil,'disk_usage',return_value=SimpleNamespace(free=0)):
            self.reject_preserving(self.changed())

    def test_corrupt_gzip_trailer_restores_old_file_under_disk_pressure(self):
        self.install();store=self.changed();store.body=store.body[:-1]
        store.head['index']['bytes']=len(store.body);store.head['index']['sha256']=hashlib.sha256(store.body).hexdigest()
        with self.tight_space(store):self.reject_preserving(store)

    def test_manifest_transport_and_duplicate_fields_are_not_absence(self):
        self.install()
        for raw,size in ((b'{"index":null,"index":{}}',None),(b'{}',True),(b'{}',3),(b'[]',None)):
            store=Store()
            def fetch(**kw):
                stream=io.BytesIO(raw);store.streams.append(stream)
                return {'Body':stream,**({'ContentLength':size} if size is not None else {})}
            with patch.object(store,'get_object',side_effect=fetch):self.reject_preserving(store)

    def test_cold_bad_database_never_publishes_path_or_ready_state(self):
        with self.assertRaises(Exception):self.materialize(Store(b'Invented malformed database'))
        self.assertEqual(self.state,{});self.assertFalse(self.active.exists());self.assertEqual(list(self.root.iterdir()),[])

    def test_evidence_separates_current_availability_from_last_verified_cache(self):
        self.install();before=deepcopy(self.state)
        value=m.evidence(self.state,'unavailable')
        self.assertEqual(value['status'],'unavailable')
        self.assertEqual(value['artifact_identity'],self.state['identity'])
        self.assertIs(value['source_replay_verified'],False);self.assertIs(value['investment_authority'],False)
        self.assertNotIn(str(self.active),json.dumps(value))
        value['artifact_identity']['sha256']='changed-by-caller'
        self.assertEqual(self.state,before)
        with self.assertRaises(ValueError):m.evidence(self.state,'fresh')

    def native(self):
        import test_directory_native
        module,_,indexed,_,_=test_directory_native.build()
        module._IDX.update({'docs':indexed['docs'],'pop':indexed['pop'],
                            **module.index_for_docs(indexed['docs']),
                            'built_at':indexed['built_at'],'loaded_at':indexed['built_at']})
        module.s3=Store()
        module.warehouse_materialize=lambda state,client,bucket,path,now,force=False:m.materialize(
            state,client,bucket,self.active,now,force)
        return module

    def test_whole_native_search_queries_actual_fixture_database_and_reports_failed_source(self):
        module=self.native();result=module.warehouse_search('invented',10)
        self.assertEqual([r['id'] for r in result['rows']],['invented:old'])
        self.assertEqual(result['integrity']['status'],'available')
        self.assertEqual(result['integrity']['artifact_identity']['sha256'],module.s3.head['index']['sha256'])
        prior_state=deepcopy(module._WIDX);prior_file=self.active.read_bytes()
        module.s3.get_object=lambda **kw:(_ for _ in ()).throw(OSError('Invented manifest outage'))
        result=module.lambda_handler({'mode':'search','q':'ZZTEST'},None)
        self.assertEqual(result['statusCode'],200);body=json.loads(result['body'])
        self.assertTrue(any(row['id']=='ZZTEST' for row in body['rows']))
        self.assertEqual(body['warehouse_integrity']['status'],'unavailable')
        self.assertIn('Invented manifest outage',body['warehouse_error'])
        self.assertEqual(module._WIDX,prior_state);self.assertEqual(self.active.read_bytes(),prior_file)

    def test_whole_native_warm_cannot_call_failed_warehouse_manifest_ready(self):
        module=self.native();module.warehouse_search('invented',10)
        before=self.active.read_bytes();module.load_index=lambda force=False:module._IDX
        module.s3.get_object=lambda **kw:(_ for _ in ()).throw(OSError('Invented manifest outage'))
        result=module.lambda_handler({'mode':'warm','queryStringParameters':{'force':'1'}},None)
        self.assertFalse(result['warehouse_ready']);self.assertEqual(result['warehouse_integrity']['status'],'unavailable')
        self.assertIn('Invented manifest outage',result['warehouse_error']);self.assertEqual(self.active.read_bytes(),before)


if __name__=='__main__':unittest.main(verbosity=2)
