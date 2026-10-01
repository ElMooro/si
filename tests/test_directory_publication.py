from pathlib import Path
from copy import deepcopy
from array import array
import base64,gzip,hashlib,importlib.util,io,json,pickle,sys,unittest

R=Path(__file__).resolve().parent.parent
sys.path.insert(0,str(R/'aws/lambdas/justhodl-symdir/source'))
import directory_index as ix
import directory_publication as p
PREFIX='data/symdir/';HEAD=PREFIX+'manifest.json';NOW='2000-02-01T00:00:00+00:00'


class Error(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}


class Store:
    def __init__(self):self.objects={};self.writes=[];self.reads=[];self.on_put=None;self.bodies=[]
    def put_object(self,**kw):
        assert kw['Bucket']=='invented-bucket';key=kw['Key']
        if self.on_put:self.on_put(kw)
        old=self.objects.get(key)
        if kw.get('IfNoneMatch')=='*' and old is not None:raise Error('PreconditionFailed')
        if 'IfMatch' in kw and (old is None or old['ETag']!=kw['IfMatch']):raise Error('ConditionalRequestConflict')
        self.objects[key]={'raw':kw['Body'],'ETag':'"'+hashlib.sha256(kw['Body']).hexdigest()+'"','Metadata':deepcopy(kw.get('Metadata',{}))}
        self.writes.append(deepcopy(kw));return {'ETag':self.objects[key]['ETag']}
    def get_object(self,**kw):
        assert kw['Bucket']=='invented-bucket';key=kw['Key'];self.reads.append(key)
        if key not in self.objects:raise Error('NoSuchKey')
        row=self.objects[key];body=io.BytesIO(row['raw']);self.bodies.append(body)
        return {'Body':body,'ContentLength':len(row['raw']),'ETag':row['ETag']}
    def head_object(self,**kw):
        assert kw['Bucket']=='invented-bucket';key=kw['Key'];self.reads.append(key)
        if key not in self.objects:raise Error('404')
        return {k:v for k,v in self.objects[key].items() if k!='raw'}
    def current(self):return json.loads(self.objects[HEAD]['raw'])


def generation(name='INVENTED',day=2,started=None):
    stamp=f'2000-01-{day:02d}T00:00:00+00:00'
    docs=[[name,'invented','Invented title','instrument',.5,None,None,None,None,None,None,{}]]
    db=gzip.compress(pickle.dumps({'version':'invented-v1','built_at':stamp,'docs':docs,'pop':array('f',[.5])},protocol=5),mtime=0)
    derived={'index':{'invented':array('I',[0])},'toklist':['invented'],'ids':[(name,0)],'bare':[]}
    ib=gzip.compress(pickle.dumps({'version':'invented-v1','built_at':stamp,'docs_sha256':hashlib.sha256(db).hexdigest(),**derived},protocol=5),mtime=0)
    cb=gzip.compress(json.dumps({'built_at':stamp,'n':1,'cols':['symbol','name','exchange','type','market','pop'],'rows':[[name,'Invented title','TEST','CS','stocks',.5]]}).encode(),mtime=0)
    return {'version':'invented-v1','built_at':stamp,'build_started_at':started or stamp,'finished_at':stamp,'index_generation':ix.descriptor(db,ib,PREFIX),'docs':1,'tokens':1,'instruments':1,'retained_optional':{'invented':True}}, {'docs':db,'index':ib,'instruments':cb}


def publish(store,doc=None,blobs=None):
    if doc is None:doc,blobs=generation()
    return p.publish(store,'invented-bucket',PREFIX,doc,blobs,lambda:NOW)


class Tests(unittest.TestCase):
    def test_complete_immutable_inputs_precede_head_and_aliases_follow(self):
        store=Store();doc,blobs=generation();original=deepcopy(doc);out=publish(store,doc,blobs)
        self.assertEqual(doc,original);head=store.current();self.assertTrue(out['publication']['aliases_complete'])
        self.assertEqual(head['retained_optional'],original['retained_optional']);self.assertNotIn('publication',head)
        keys=[r['Key'] for r in store.writes];self.assertEqual(keys[3],HEAD)
        self.assertEqual(set(keys[:3]),{r['key'] for r in head['index_generation']['files'].values()}|{head['instrument_generation']['key']})
        for name,ref in [('docs',head['index_generation']['files']['docs']),('index',head['index_generation']['files']['index']),('instruments',head['instrument_generation'])]:
            self.assertEqual(store.objects[ref['key']]['raw'],blobs[name]);self.assertEqual(hashlib.sha256(blobs[name]).hexdigest(),ref['sha256'])
        self.assertTrue(all(s.closed for s in store.bodies))

    def test_identical_retry_verifies_immutable_bytes_and_repairs_aliases(self):
        store=Store();doc,blobs=generation();publish(store,doc,blobs)
        del store.objects[PREFIX+'index.pkl.gz'];n=len([r for r in store.writes if r['Key']==HEAD])
        result=publish(store,doc,blobs)
        self.assertEqual(result['publication']['status'],'already_current');self.assertTrue(result['publication']['aliases_complete'])
        self.assertEqual(len([r for r in store.writes if r['Key']==HEAD]),n)

    def test_stale_start_even_with_later_finish_cannot_replace_a_newer_build(self):
        store=Store();publish(store,*generation(day=5));before=deepcopy(store.objects);writes=len(store.writes)
        result=publish(store,*generation('LATE_OLD',7,'2000-01-03T00:00:00+00:00'))
        self.assertEqual(result['publication']['status'],'superseded');self.assertFalse(result['publication']['published'])
        self.assertEqual(store.objects,before);self.assertEqual(len(store.writes),writes)

    def test_overlapping_head_write_cannot_overwrite_a_newer_winner(self):
        store=Store();publish(store,*generation(day=2));winner=[]
        def overlap(kw):
            if kw['Key']==HEAD:
                store.on_put=None;winner.append(publish(store,*generation('NEWER',5)))
        store.on_put=overlap;loser=publish(store,*generation('OLDER',4))
        self.assertEqual(loser['publication']['status'],'superseded');self.assertTrue(winner[0]['publication']['published'])
        self.assertEqual(store.current()['built_at'],'2000-01-05T00:00:00+00:00')
        self.assertEqual(json.loads(gzip.decompress(store.objects[PREFIX+'instruments.json.gz']['raw']))['rows'][0][0],'NEWER')

    def test_same_start_different_generation_is_a_conflict_not_a_coin_flip(self):
        store=Store();publish(store);before=deepcopy(store.objects)
        with self.assertRaises(p.PublicationError):publish(store,*generation('DIFFERENT'))
        self.assertEqual(store.objects,before)

    def test_failed_immutable_upload_never_changes_head_or_legacy_aliases(self):
        for ending in ('/docs.pkl.gz','/index.pkl.gz','/instruments.json.gz'):
            store=Store();publish(store);before=deepcopy(store.objects)
            def fail(kw):
                if ('/generations/' in kw['Key'] or '/artifacts/' in kw['Key']) and kw['Key'].endswith(ending):raise OSError('Invented upload failure')
            store.on_put=fail
            with self.assertRaises(OSError):publish(store,*generation('NEW',3))
            for key in (HEAD,PREFIX+'docs.pkl.gz',PREFIX+'index.pkl.gz',PREFIX+'instruments.json.gz'):self.assertEqual(store.objects[key],before[key])

    def test_corrupt_existing_immutable_cannot_be_trusted_by_path_or_metadata(self):
        store=Store();doc,blobs=generation();ref=doc['index_generation']['files']['docs']
        store.put_object(Bucket='invented-bucket',Key=ref['key'],Body=b'Corrupt invented bytes',Metadata={'sha256':ref['sha256']})
        with self.assertRaises(p.PublicationError):publish(store,doc,blobs)
        self.assertNotIn(HEAD,store.objects)

    def test_alias_failure_is_explicit_and_cannot_invalidate_committed_immutable_pair(self):
        store=Store()
        def fail(kw):
            if kw['Key']==PREFIX+'index.pkl.gz':raise OSError('Invented alias error')
        store.on_put=fail
        with self.assertRaisesRegex(p.PublicationError,'generation committed; compatibility alias updates failed: index.pkl.gz'):
            publish(store)
        self.assertEqual(store.objects[store.current()['index_generation']['files']['index']['key']]['raw'],generation()[1]['index'])
        self.assertIn(PREFIX+'instruments.json.gz',store.objects)
        store.on_put=None;self.assertTrue(publish(store)['publication']['aliases_complete'])

    def test_alias_race_rechecks_head_and_never_overwrites_newer_alias(self):
        store=Store()
        def overlap(kw):
            if kw['Key']==PREFIX+'docs.pkl.gz':
                store.on_put=None;publish(store,*generation('NEXT',3))
        store.on_put=overlap;out=publish(store)
        self.assertFalse(out['publication']['aliases_complete'])
        self.assertTrue(all(r['status']=='superseded' for r in out['publication']['aliases'].values()))
        self.assertEqual(pickle.loads(gzip.decompress(store.objects[PREFIX+'docs.pkl.gz']['raw']))['docs'][0][0],'NEXT')

    def test_malformed_unavailable_future_or_oversized_heads_never_become_missing(self):
        for raw in (b'[]',b'{"built_at":null}',b'{"built_at":"2000-01-02","built_at":null}',b'x'*1048577):
            store=Store();store.put_object(Bucket='invented-bucket',Key=HEAD,Body=raw);before=deepcopy(store.objects)
            with self.assertRaises(Exception):publish(store)
            self.assertEqual(store.objects,before)
        store=Store();store.get_object=lambda **kw:(_ for _ in ()).throw(OSError('Invented outage'))
        with self.assertRaises(OSError):publish(store)
        self.assertEqual(store.writes,[])

    def test_complete_legacy_head_migrates_conservatively_without_dropping_aliases(self):
        store=Store();fixture=json.loads((R/'tests/fixtures/symbol-directory/build-reproduction.json').read_bytes())
        for key,row in fixture['whole_writes'].items():
            store.put_object(Bucket='invented-bucket',Key=key,Body=base64.b64decode(row['body_base64']))
        result=publish(store);self.assertTrue(result['publication']['aliases_complete'])
        self.assertEqual(store.current()['publication_contract']['legacy_aliases'],'separate_compatibility_copies_not_atomic')

    def test_exhausted_conflicts_preserve_prior_head_and_surface_failure(self):
        store=Store();publish(store);before=deepcopy(store.objects);attempts=[]
        def conflict(kw):
            if kw['Key']==HEAD:attempts.append(kw);raise Error('ConditionalRequestConflict')
        store.on_put=conflict
        with self.assertRaises(p.PublicationError):publish(store,*generation('NEW',3))
        self.assertEqual(len(attempts),5)
        for key in (HEAD,PREFIX+'docs.pkl.gz',PREFIX+'index.pkl.gz',PREFIX+'instruments.json.gz'):self.assertEqual(store.objects[key],before[key])

    def test_bad_candidate_clocks_sizes_and_manifest_bounds_produce_no_writes(self):
        for field,value in [('build_started_at',None),('build_started_at','2000-01-03T00:00:00+00:00'),('finished_at','2000-03-01T00:00:00+00:00'),('finished_at','2000-01-01T00:00:00+00:00'),('extra','x'*1048577)]:
            store=Store();doc,blobs=generation();doc[field]=value
            with self.assertRaises(Exception):publish(store,doc,blobs)
            self.assertEqual(store.writes,[])
        store=Store();doc,blobs=generation();blobs['docs']+=b'bad'
        with self.assertRaises(p.PublicationError):publish(store,doc,blobs)
        self.assertEqual(store.writes,[])


class Integration(unittest.TestCase):
    def test_whole_native_alias_failure_raises_instead_of_returning_scheduler_success(self):
        import test_directory_native as native
        store=Store()
        def fail(row):
            if row['Key']==PREFIX+'instruments.json.gz':raise OSError('Invented catalog copy failure')
        store.on_put=fail
        with self.assertRaisesRegex(p.PublicationError,'compatibility alias updates failed: instruments.json.gz'):
            native.build(client_override=store)
        head=store.current()
        self.assertIn(head['instrument_generation']['key'],store.objects)
        self.assertIn(head['index_generation']['files']['docs']['key'],store.objects)

    def test_whole_native_overlapping_build_preserves_newer_manifest_and_catalog(self):
        import test_directory_native as native
        store=Store();newer=[];timeline=['2000-01-01T00:00:00+00:00']
        def overlap(row):
            if row['Key']==HEAD:
                store.on_put=None;timeline[0]='2000-01-02T00:00:00+00:00';newer.append(native.build(stamp=timeline[0],client_override=store))
        store.on_put=overlap
        older=native.build(stamp=lambda:timeline[0],client_override=store)
        self.assertEqual(older[1]['publication']['status'],'superseded')
        self.assertFalse(older[1]['publication']['published'])
        self.assertTrue(newer[0][1]['publication']['aliases_complete'])
        self.assertEqual(store.current()['built_at'],'2000-01-02T00:00:00+00:00')
        self.assertEqual(store.current()['index_generation'],newer[0][1]['index_generation'])
        self.assertEqual(set(older[4]['invented_inputs']),set(newer[0][4]['invented_inputs']))
    def test_full_native_build_retains_all_existing_manifest_fields_and_whole_catalog(self):
        import test_directory_native as native
        _,result,indexed,writes,_=native.build()
        self.assertTrue(result['publication']['aliases_complete'])
        baseline=json.loads((R/'tests/fixtures/symbol-directory/publication-reproduction.json').read_bytes())
        import base64,gzip
        old=json.loads(base64.b64decode(baseline['older_whole_writes'][HEAD]['body_base64']))
        self.assertLessEqual(set(old),set(result))
        self.assertEqual(result['docs'],len(indexed['docs']))
        bound=result['instrument_generation'];whole=writes[bound['key']]['Body']
        self.assertEqual(whole,writes[PREFIX+'instruments.json.gz']['Body'])
        self.assertEqual(json.loads(gzip.decompress(whole))['n'],result['instruments'])


if __name__=='__main__':unittest.main(verbosity=2)
