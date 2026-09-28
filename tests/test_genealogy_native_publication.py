"""Complete original replay, immutable publication and interrupted-run boundaries."""
from copy import deepcopy
from datetime import timedelta
import hashlib
import io
import json
from pathlib import Path
import shutil
import sys
import tempfile
import unittest

ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops/checks/genealogy_native_candidate',
    'aws/ops/checks','aws/shared','tests')]
import genealogy_native_publication as publication
import genealogy_cache_checkpoint as checkpoint
import genealogy_revision_cache as revision
import genealogy_streamed_pipeline as pipeline
import genealogy_public_archive as archive
from test_genealogy_public_archive import fixture, NOW
from test_genealogy_revision_cache import Client
from test_genealogy_cache_checkpoint import StorageError


class DiskStore:
    """Isolated conditional artifact store; no network or original access."""
    def __init__(self,root):
        self.root=Path(root);self.root.mkdir(exist_ok=True)
        self.reads=[];self.writes=[];self.streams=[];self.before_put=lambda kw:None
    def path(self,key):
        assert publication.allowed(key) or key.startswith(checkpoint.PREFIX)
        return self.root/hashlib.sha256(key.encode()).hexdigest()
    def get_object(self,**kw):
        assert kw['Bucket']==archive.BUCKET
        key=kw['Key'];self.reads.append(key);path=self.path(key)
        if not path.exists():raise StorageError('NoSuchKey')
        stream=path.open('rb');self.streams.append(stream)
        h=hashlib.sha256()
        for raw in pipeline.file_chunks(path):h.update(raw)
        return {'Body':stream,'ContentLength':path.stat().st_size,'ETag':'"'+h.hexdigest()+'"'}
    def put_object(self,**kw):
        assert kw['Bucket']==archive.BUCKET
        self.before_put(kw);path=self.path(kw['Key'])
        if kw.get('IfNoneMatch')=='*' and path.exists():raise StorageError('PreconditionFailed')
        if 'IfMatch' in kw and (not path.exists() or '"'+publication.sha(path.read_bytes())+'"'!=kw['IfMatch']):
            raise StorageError('PreconditionFailed')
        assert ('IfMatch' in kw)!=('IfNoneMatch' in kw)
        with path.open('wb') as target:
            body=kw['Body']
            if isinstance(body,bytes):target.write(body)
            else:shutil.copyfileobj(body,target,65536)
        self.writes.append(kw['Key'])


class Publications(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name)
        originals,_,_=fixture();self.originals=originals;self.source=Client(originals)
        self.inventory=originals.inventories();self.rows=revision.revisions(self.source,self.inventory)
        self.cache=revision.OriginalCache(self.root/'cache.sqlite');self.bound=self.cache.bind(self.source,self.rows)
        self.proof=pipeline.collect(self.bound,self.inventory,self.root/'calculated')
        self.store=DiskStore(self.root/'objects')
        cutoff=next(iter(self.inventory.values()))['cutoff']
        self.original_ref=checkpoint.snapshot(self.store,archive.BUCKET,self.bound,cutoff)
        self.ref=publication.retain(self.store,self.root/'calculated',self.proof,self.inventory,self.original_ref)
    def tearDown(self):
        self.cache.close();self.tmp.cleanup()
    def replay(self,name='replay',ref=None):
        return publication.replay(self.store,self.ref if ref is None else ref,self.root/name)
    def manifest(self):return archive.strict_json(self.store.path(self.ref['key']).read_bytes())
    def rewrite(self,change):
        doc=self.manifest();change(doc)
        return publication.retain_bytes(self.store,archive.canonical(doc),'runs')

    def test_retained_originals_rebuild_complete_result_after_live_originals_are_removed(self):
        expected=archive.strict_json((self.root/'calculated/head.json').read_bytes())
        self.originals.objects.clear();self.source.reads.clear()
        packet=self.replay()
        self.assertEqual(packet,{**expected,'replay':self.ref})
        self.assertEqual(self.source.reads,[])
        result=publication.publish(self.store,self.ref,self.root/'publish')
        self.assertTrue(result['published'])
        self.assertEqual(archive.strict_json(self.store.path(publication.CURRENT).read_bytes()),packet)
        self.assertTrue(all(s.closed for s in self.store.streams))
        self.assertFalse(packet['authority']['calls_eligible'])

    def test_every_retained_file_and_compiler_is_checked_before_publication(self):
        doc=self.manifest()
        refs=[*doc['files'].values(),*doc['compilers'].values()]
        for n,ref in enumerate(refs):
            path=self.store.path(ref['key']);raw=path.read_bytes();path.write_bytes(raw+b' ')
            with self.assertRaises(ValueError):
                publication.publish(self.store,self.ref,self.root/('corrupt'+str(n)))
            self.assertFalse(self.store.path(publication.CURRENT).exists());path.write_bytes(raw)

    def test_internally_rehashed_forged_output_cannot_borrow_real_input_evidence(self):
        doc=self.manifest();output=archive.strict_json(self.store.path(doc['files']['output']['key']).read_bytes())
        output['membership']['possible_comparisons']+=1
        forged=publication.retain_bytes(self.store,archive.canonical(output),'artifacts')
        ref=self.rewrite(lambda d:d['files'].update(output=forged))
        with self.assertRaisesRegex(ValueError,'calculation_differs'):
            publication.publish(self.store,ref,self.root/'forged')
        self.assertFalse(self.store.path(publication.CURRENT).exists())

    def test_changed_summary_and_missing_compiler_cannot_publish(self):
        doc=self.manifest();head=archive.strict_json(self.store.path(doc['files']['head']['key']).read_bytes())
        head['coverage']['retained_records']+=1
        bad=publication.retain_bytes(self.store,archive.canonical(head),'artifacts')
        ref=self.rewrite(lambda d:d['files'].update(head=bad))
        with self.assertRaises(ValueError):self.replay('head',ref)
        ref=self.rewrite(lambda d:d['compilers'].pop('genealogy_source_fold'))
        with self.assertRaisesRegex(ValueError,'compiler_closure'):self.replay('compiler',ref)

    def test_partial_disposable_cache_is_not_a_complete_retained_original_archive(self):
        self.cache.db.execute('DELETE FROM originals WHERE key=?',(self.rows[0]['key'],));self.cache.db.commit()
        ref=checkpoint.snapshot(self.store,archive.BUCKET,self.bound,next(iter(self.inventory.values()))['cutoff'])
        before=list(self.store.writes)
        with self.assertRaisesRegex(ValueError,'complete_original_archive'):
            publication.retain(self.store,self.root/'calculated',self.proof,self.inventory,ref)
        self.assertEqual(self.store.writes,before)

    def test_missing_original_or_changed_revision_membership_fails_before_head(self):
        doc=checkpoint.validate(self.store,archive.BUCKET,self.original_ref)
        path=self.store.path(doc['shards'][-1]['artifact']['key']);raw=path.read_bytes();path.unlink()
        with self.assertRaises(StorageError):self.replay('missing')
        self.assertFalse(self.store.path(publication.CURRENT).exists());path.write_bytes(raw)
        ref=self.rewrite(lambda d:d.update(revision_sha256='0'*64))
        with self.assertRaisesRegex(ValueError,'complete_original_archive'):self.replay('changed',ref)

    def test_interrupted_retention_cannot_replace_existing_complete_head(self):
        publication.publish(self.store,self.ref,self.root/'initial')
        old=self.store.path(publication.CURRENT).read_bytes()
        def fail(kw):
            if '/runs/' in kw['Key']:raise RuntimeError('manifest persistence failed')
        self.store.before_put=fail
        with self.assertRaisesRegex(RuntimeError,'manifest persistence failed'):
            publication.retain(self.store,self.root/'calculated',self.proof,self.inventory,self.original_ref)
        self.assertEqual(self.store.path(publication.CURRENT).read_bytes(),old)

    def test_older_or_conflicting_head_and_cas_race_are_handled(self):
        packet=self.replay('get-head');newer=deepcopy(packet)
        newer['generated_at']=(NOW+timedelta(hours=2)).isoformat()
        self.store.path(publication.CURRENT).write_bytes(archive.canonical(newer))
        result=publication.publish(self.store,self.ref,self.root/'older')
        self.assertFalse(result['published'])
        conflict=deepcopy(packet);conflict['coverage']['captures']+=1
        self.store.path(publication.CURRENT).write_bytes(archive.canonical(conflict))
        with self.assertRaisesRegex(ValueError,'same_clock'):
            publication.publish(self.store,self.ref,self.root/'same-clock')
        self.store.path(publication.CURRENT).unlink()
        def race(kw):
            if kw['Key']==publication.CURRENT:
                self.store.before_put=lambda kw:None
                self.store.path(publication.CURRENT).write_bytes(archive.canonical(newer))
                raise StorageError('PreconditionFailed')
        self.store.before_put=race
        self.assertFalse(publication.publish(self.store,self.ref,self.root/'race')['published'])
        self.assertEqual(self.store.path(publication.CURRENT).read_bytes(),archive.canonical(newer))

    def test_unreviewed_refs_and_existing_scratch_rejected(self):
        for bad in ({**self.ref,'key':'data/prospective-outcomes.json'},{**self.ref,'bytes':True}):
            with self.assertRaisesRegex(ValueError,'typed_publication'):
                publication.read_ref(self.store,bad,'runs')
        with self.assertRaisesRegex(ValueError,'fresh_replay'):
            publication.replay(self.store,self.ref,self.root)
        self.assertFalse(self.store.path(publication.CURRENT).exists())


if __name__=='__main__':unittest.main()
