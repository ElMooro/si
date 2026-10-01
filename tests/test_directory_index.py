from pathlib import Path
from array import array
from copy import deepcopy
from unittest.mock import patch
import gzip,hashlib,importlib.util,io,json,pickle,tempfile,unittest

HERE=Path(__file__).parent
spec=importlib.util.spec_from_file_location('directory_index',HERE.parent/'aws/lambdas/justhodl-symdir/source/directory_index.py')
ix=importlib.util.module_from_spec(spec);spec.loader.exec_module(ix)
PREFIX='data/symdir/'


def population(ident='INVENTED',stamp='2000-01-01T00:00:00+00:00'):
    return {'version':'invented-v1','built_at':stamp,
            'docs':[[ident,'invented','Invented title','instrument',.5,None,None,None,None,None,None,{}]],
            'pop':array('f',[.5])}


def derived(docs):
    return {'index':{'invented':array('I',range(len(docs)))},'toklist':['invented'],
            'ids':sorted((d[0].upper(),i) for i,d in enumerate(docs)),
            'bare':sorted((d[0].rsplit(':',1)[-1].upper(),i) for i,d in enumerate(docs) if ':' in d[0])}


class Store:
    def __init__(self,a=None,b=None):
        a=population() if a is None else a
        self.docs=a
        raw=gzip.compress(pickle.dumps(a,protocol=5))
        b={**derived(a['docs']),'version':a['version'],'built_at':a['built_at'],'docs_sha256':hashlib.sha256(raw).hexdigest()} if b is None else b
        self.index=b
        iraw=gzip.compress(pickle.dumps(b,protocol=5))
        self.descriptor=ix.descriptor(raw,iraw,PREFIX)
        self.head={'built_at':a['built_at'],'index_generation':self.descriptor}
        self.objects={self.descriptor['files']['docs']['key']:raw,self.descriptor['files']['index']['key']:iraw}
        self.calls=[];self.closed=[]

    def get_object(self,**kw):
        assert kw['Bucket']=='invented-bucket' and set(kw)=={'Bucket','Key'}
        key=kw['Key'];self.calls.append(key)
        raw=json.dumps(self.head).encode() if key==PREFIX+'manifest.json' else self.objects[key]
        if isinstance(raw,Exception):raise raw
        body=io.BytesIO(raw);self.closed.append(body)
        return {'Body':body,'ContentLength':len(raw)}


class Tests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.cache={k:None for k in ix.FIELDS}

    def refresh(self,store,rebuild=derived):
        return ix.refresh(self.cache,store,'invented-bucket',PREFIX,rebuild,lambda:'2000-01-04T00:00:00+00:00',self.tmp.name)

    def assert_rollback(self,store):
        before=pickle.dumps(self.cache)
        with self.assertRaises(Exception):self.refresh(store)
        self.assertEqual(pickle.dumps(self.cache),before)
        self.assertFalse(list(Path(self.tmp.name).iterdir()))
        self.assertTrue(all(body.closed for body in store.closed))

    def test_whole_generation_uses_only_pinned_files_and_separate_integrity_metadata(self):
        store=Store();result=self.refresh(store)
        self.assertIs(result,self.cache);self.assertEqual(result['docs'],store.docs['docs'])
        self.assertEqual(result['index_integrity']['status'],'hash_bound_generation')
        self.assertIs(result['index_integrity']['investment_authority'],False)
        self.assertEqual(store.calls,[PREFIX+'manifest.json',store.descriptor['files']['docs']['key'],store.descriptor['files']['index']['key']])
        self.assertTrue(all(body.closed for body in store.closed));self.assertFalse(list(Path(self.tmp.name).iterdir()))

    def test_legacy_rebuild_ignores_the_entire_unbound_index_head(self):
        fixture=HERE/'fixtures/symbol-directory/index-reproductions.json'
        import base64
        original=json.loads(fixture.read_bytes());store=Store();store.head={}
        store.objects={k:base64.b64decode(v) for k,v in original['whole_inputs'].items()}
        result=self.refresh(store)
        self.assertEqual(result['ids'],[('OLDTEST',0)])
        self.assertEqual(result['index_integrity']['status'],'legacy_reconstructed_from_single_docs_object')
        self.assertNotIn(PREFIX+'index.pkl.gz',store.calls)

    def test_transport_hash_and_declared_length_failures_preserve_original_cache(self):
        self.refresh(Store())
        for kind in ('failed','truncated','overlong','same-size-corrupt'):
            store=Store(population('NEW'))
            key=store.descriptor['files']['docs']['key'];body=store.objects[key]
            store.objects[key]={'failed':OSError('Invented read failure'),'truncated':body[:-1],
                                'overlong':body+b'x','same-size-corrupt':b'X'+body[1:]}[kind]
            self.assert_rollback(store)

    def test_complete_but_inconsistent_index_restores_every_old_cache_field(self):
        self.refresh(Store());self.cache['invented_extra_metadata']={'retained':True}
        for field,value in [('ids',[('WRONG',0)]),('index',{'invented':array('I',[1])}),
                            ('docs_sha256','0'*64),('built_at','2001-01-01'),('version','other'),('toklist',[])]:
            store=Store();index=deepcopy(store.index);index[field]=value
            self.assert_rollback(Store(store.docs,index))

    def test_invalid_document_or_popularity_restores_old_cache(self):
        self.refresh(Store())
        for mutate in (lambda d:d['pop'].append(.5),lambda d:d['docs'][0].__setitem__(4,True),
                       lambda d:d['docs'][0].__setitem__(11,[]),lambda d:d['docs'][0].pop(),
                       lambda d:d.__setitem__('built_at',None)):
            d=population('NEW');mutate(d);self.assert_rollback(Store(d))

    def test_descriptor_cannot_escape_scope_or_ignore_an_incomplete_pair(self):
        self.refresh(Store())
        for mutate in (lambda d:d['files']['docs'].__setitem__('key','private/account.json'),
                       lambda d:d['files'].pop('index'),lambda d:d.__setitem__('schema_version',True),
                       lambda d:d['files']['docs'].__setitem__('bytes',True),
                       lambda d:d.__setitem__('generation','f'*64)):
            store=Store();mutate(store.descriptor);self.assert_rollback(store)
            self.assertEqual(store.calls,[PREFIX+'manifest.json'])

    def test_newer_generation_replaces_all_fields_but_older_cannot_regress_it(self):
        self.refresh(Store());new=Store(population('NEXT','2000-01-03T00:00:00+00:00'))
        self.refresh(new);self.assertEqual(self.cache['ids'],[('NEXT',0)])
        self.assert_rollback(Store())

    def test_insufficient_staging_or_checkpoint_space_preserves_original_graph(self):
        self.refresh(Store());old_docs=self.cache['docs']
        with patch.object(ix.shutil,'disk_usage',return_value=type('Disk',(),{'free':0})()):
            self.assert_rollback(Store())
        self.assertIs(self.cache['docs'],old_docs)
        original=ix.SpaceCheckedWriter.write
        with patch.object(ix.SpaceCheckedWriter,'write',side_effect=OSError('Invented checkpoint failure')):
            self.assert_rollback(Store())
        self.assertIs(self.cache['docs'],old_docs)

    def test_rebuild_failure_cold_does_not_leave_partial_population(self):
        store=Store();store.head={};store.objects[PREFIX+'docs.pkl.gz']=next(iter(store.objects.values()))
        def fail(docs):raise MemoryError('Invented reconstruction failure')
        with self.assertRaises(ix.IndexIntegrityError):self.refresh(store,fail)
        self.assertTrue(all(self.cache[k] is None for k in ix.FIELDS))
        self.assertFalse(list(Path(self.tmp.name).iterdir()))

    def test_arbitrary_pickle_classes_trailing_bytes_and_bad_crc_are_rejected(self):
        path=Path(self.tmp.name)/'invented.gz'
        for body in (pickle.dumps(Path('invented')),pickle.dumps({})+b'extra'):
            path.write_bytes(gzip.compress(body))
            with self.assertRaises(ix.IndexIntegrityError):ix.unpack(path)
        raw=gzip.compress(pickle.dumps({}));path.write_bytes(raw[:-1])
        with self.assertRaises(EOFError):ix.unpack(path)

    def test_partial_candidate_graph_is_freed_before_old_graph_is_restored(self):
        import weakref
        self.refresh(Store());original=ix.unpack;candidate_ref=[]
        def unpack(path):
            if path.name=='rollback.gz':
                self.assertIsNone(candidate_ref[0](), 'Failed generation retained through traceback')
            result=original(path)
            if path.name=='docs.gz':candidate_ref.append(weakref.ref(result['pop']))
            return result
        store=Store();bad=deepcopy(store.index);bad['ids']=[('WRONG',0)]
        with patch.object(ix,'unpack',side_effect=unpack):self.assert_rollback(Store(store.docs,bad))

    def test_malformed_clocks_and_real_utc_regression_are_rejected(self):
        self.refresh(Store())
        for stamp in ('not-a-clock','2000-01-02',True,None,'2000-01-01T00:30:00+01:00','2001-01-01T00:00:00+00:00'):
            self.assert_rollback(Store(population('NEW',stamp)))

    def test_changed_manifest_during_download_cannot_mix_pinned_generations(self):
        store=Store();next_store=Store(population('NEXT','2000-01-03T00:00:00+00:00'))
        getter=store.get_object
        def changing(**kw):
            response=getter(**kw)
            if kw['Key']==PREFIX+'manifest.json':
                store.head=next_store.head;store.objects.update(next_store.objects)
            return response
        with patch.object(store,'get_object',side_effect=changing):
            self.refresh(store)
        self.assertEqual(self.cache['ids'],[('INVENTED',0)])

    def test_manifest_duplicate_fields_and_false_transport_lengths_are_rejected(self):
        self.refresh(Store())
        for raw,size in [(b'{"index_generation":null,"index_generation":{}}',None),
                         (b'{}',True),(b'{}',3),(b'{"score":NaN}',None)]:
            store=Store()
            def get(**kw):
                body=io.BytesIO(raw);store.closed.append(body)
                return {'Body':body,**({'ContentLength':size} if size is not None else {})}
            with patch.object(store,'get_object',side_effect=get):self.assert_rollback(store)

    def test_whole_native_publication_load_and_search_ignore_mixed_mutable_aliases(self):
        import test_directory_native
        module,report,indexed,puts,_=test_directory_native.build()
        stored={key:value['Body'] for key,value in puts.items()}
        metadata=report['index_generation'];ix.validate_descriptor(metadata,PREFIX)
        for name in ('docs','index'):
            raw=stored[metadata['files'][name]['key']]
            self.assertEqual(hashlib.sha256(raw).hexdigest(),metadata['files'][name]['sha256'])
            self.assertEqual(raw,stored[PREFIX+name+'.pkl.gz'])
        keys=list(puts)
        self.assertLess(keys.index(metadata['files']['index']['key']),keys.index(PREFIX+'docs.pkl.gz'))
        self.assertEqual(keys[-1],PREFIX+'manifest.json')
        stored[PREFIX+'docs.pkl.gz']=b'Invented mutable generation mismatch'
        stored[PREFIX+'index.pkl.gz']=b'Invented old postings'
        reads=[]
        class Storage:
            def get_object(self,**kw):
                self_outer.assertEqual(kw['Bucket'],'invented-bucket');reads.append(kw['Key'])
                raw=stored[kw['Key']];return {'Body':io.BytesIO(raw),'ContentLength':len(raw)}
        self_outer=self;module.s3=Storage()
        module.refresh_index=lambda *args:ix.refresh(*args,temp_root=self.tmp.name)
        result=module.lambda_handler({'mode':'search','q':'ZZTEST'},None)
        self.assertEqual(result['statusCode'],200)
        public=json.loads(result['body'])
        self.assertTrue(any(row['id']=='ZZTEST' for row in public['rows']))
        self.assertEqual(public['index_integrity']['status'],'hash_bound_generation')
        self.assertNotIn(PREFIX+'docs.pkl.gz',reads);self.assertNotIn(PREFIX+'index.pkl.gz',reads)
        before=pickle.dumps(module._IDX)
        module.s3.get_object=lambda **kw:(_ for _ in ()).throw(OSError('Invented refresh outage'))
        with self.assertRaises(OSError):module.load_index(force=True)
        self.assertEqual(pickle.dumps(module._IDX),before)
        self.assertEqual(module.lambda_handler({'mode':'search','q':'ZZTEST'},None)['statusCode'],200)

    def test_failed_immutable_publication_never_changes_mutable_directory_heads(self):
        import test_directory_native
        for suffix in ('/docs.pkl.gz','/index.pkl.gz'):
            writes=[]
            def observe(value):
                key=value['Key'];writes.append(key)
                if '/generations/' in key and key.endswith(suffix):
                    raise OSError('Invented immutable-object write failure')
            with self.assertRaises(OSError):test_directory_native.build(write_observer=observe)
            for key in ('docs.pkl.gz','index.pkl.gz','instruments.json.gz','manifest.json'):
                self.assertNotIn(PREFIX+key,writes)


    def test_generation_identity_changes_even_when_publication_clocks_are_equal(self):
        original=Store();self.refresh(original)
        self.assertFalse(ix.refresh_needed(original.head,self.cache,PREFIX))
        other=Store(population('OTHER'))
        self.assertTrue(ix.refresh_needed(other.head,self.cache,PREFIX))
        for head in ({'built_at':original.head['built_at']},
                     {**other.head,'built_at':'1999-01-01T00:00:00+00:00'},
                     {**other.head,'built_at':False}):
            with self.assertRaises(ix.IndexIntegrityError):ix.refresh_needed(head,self.cache,PREFIX)

    def test_native_warm_reloads_same_clock_generation_and_surfaces_failed_head_checks(self):
        import test_directory_native
        module,report,indexed,puts,_=test_directory_native.build()
        first=Store();module.s3=first
        module.refresh_index=lambda *args:ix.refresh(*args,temp_root=self.tmp.name)
        module.load_index()
        next_store=Store(population('SAME_CLOCK_NEXT'));module.s3=next_store
        result=module.lambda_handler({'mode':'warm'},None)
        self.assertTrue(result['ok']);self.assertTrue(result['reloaded'])
        self.assertEqual(module._IDX['ids'],[('SAME_CLOCK_NEXT',0)])
        before=pickle.dumps(module._IDX)
        module.s3.get_object=lambda **kw:(_ for _ in ()).throw(OSError('Invented head-check failure'))
        with self.assertRaises(OSError):module.lambda_handler({'mode':'warm'},None)
        self.assertEqual(pickle.dumps(module._IDX),before)
        self.assertIsNotNone(module.load_index()['docs'])


if __name__=='__main__':unittest.main(verbosity=2)
