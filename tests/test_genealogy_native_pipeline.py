from pathlib import Path
from copy import deepcopy
from datetime import timedelta
import hashlib,io,json,sys,unittest
R=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(R/'aws/ops/checks/genealogy_native_candidate'),str(R/'aws/shared'),str(R/'tests')]
import genealogy_public_archive as archive
import genealogy_research_model as model
import genealogy_research_store as store
import test_genealogy_public_archive as fixtures


class Conflict(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}


class Memory(fixtures.Store):
    def __init__(self):super().__init__();self.writes=[];self.race=None
    def get_object(self,**kw):
        if kw['Key'] not in self.objects:raise Conflict('NoSuchKey')
        result=super().get_object(**kw)
        result['ETag']='"'+hashlib.sha256(self.objects[kw['Key']][0]).hexdigest()+'"'
        return result
    def put_object(self,**kw):
        key=kw['Key'];self.writes.append(key)
        assert store.allowed(key)
        if key==store.CURRENT and self.race:
            self.race();self.race=None;raise Conflict('PreconditionFailed')
        if kw.get('IfNoneMatch')=='*' and key in self.objects:raise Conflict('PreconditionFailed')
        if 'IfMatch' in kw and (key not in self.objects or '"'+store.sha(self.objects[key][0])+'"'!=kw['IfMatch']):raise Conflict('PreconditionFailed')
        self.objects[key]=(kw['Body'],fixtures.NOW)
    def get_paginator(self,name):
        assert name=='list_objects_v2'
        owner=self
        class Paginator:
            def paginate(self,**kw):
                objects=[{'Key':k,'Size':len(raw),'LastModified':at} for k,(raw,at) in sorted(owner.objects.items()) if k.startswith(kw['Prefix'])]
                for start in range(0,len(objects),1):yield {'Contents':objects[start:start+1]}
        return Paginator()


def original():
    f,c,r=fixtures.fixture();s=Memory();s.objects=f.objects
    return s,c,r


def packet(s,cutoff=None):
    inv=s.inventories()
    if cutoff:
        for row in inv.values():row['cutoff']=cutoff
    inputs,output=model.collect(s,inv)
    ref=store.retain(s,archive.BUCKET,inputs,output)
    return {**model.summary(output),'replay':ref},inputs,output


class NativeCandidate(unittest.TestCase):
    def test_whole_public_inputs_reconcile_compile_retain_and_replay(self):
        s,_,_=original();head,inputs,out=packet(s)
        self.assertEqual(out,store.replay(head['replay'],store.reader(s,archive.BUCKET)))
        self.assertTrue(store.publish(s,archive.BUCKET,head))
        self.assertEqual(json.loads(s.objects[store.CURRENT][0]),head)
        self.assertTrue(all(stream.closed for stream in s.streams))
        self.assertTrue(all(key.startswith((archive.PREFIX,store.PREFIX)) for key in s.reads))
        self.assertTrue(all(key.startswith(store.PREFIX) for key in s.writes))
        self.assertFalse(head['authority']['forecast_qualified'])

    def test_original_failure_cannot_become_a_public_head(self):
        s,_,_=original();inv=s.inventories();key=next(k for k in s.objects if '/records/' in k)
        raw,at=s.objects[key];s.objects[key]=(raw[:-1],at)
        with self.assertRaises(ValueError):model.collect(s,inv)
        self.assertEqual(s.writes,[])

    def test_population_or_capture_evidence_cannot_be_removed_from_frozen_input(self):
        s,_,_=original();inputs,_=model.collect(s,s.inventories())
        for mutation in (lambda x:x['contexts'].clear(),
                         lambda x:x['contexts'][0].update(capture_sha256='0'*64),
                         lambda x:x['contexts'][0]['sources'].clear(),
                         lambda x:x['audit'].update(record_body_and_storage_checks_complete=False)):
            changed=deepcopy(inputs);mutation(changed)
            with self.assertRaises(ValueError):model.compile_frozen(changed)

    def test_retained_input_output_and_whole_compiler_closure_are_checked(self):
        for kind in ('inputs','outputs','compilers'):
            s,_,_=original();head,_,_=packet(s)
            key=next(k for k in s.objects if k.startswith(store.PREFIX+kind+'/'))
            raw,at=s.objects[key];s.objects[key]=(raw+b' ',at)
            with self.assertRaises(ValueError):store.replay(head['replay'],store.reader(s,archive.BUCKET))

    def test_borrowed_valid_replay_cannot_authorize_changed_public_counts(self):
        s,_,_=original();head,_,_=packet(s);head['coverage']['retained_records']+=1
        before=list(s.writes)
        with self.assertRaises(ValueError):store.publish(s,archive.BUCKET,head)
        self.assertEqual(s.writes,before)

    def test_older_or_same_clock_conflicting_publications_do_not_overwrite(self):
        s,_,_=original();early,_,_=packet(s);later,_,_=packet(s,(fixtures.NOW+timedelta(hours=2)).isoformat())
        self.assertTrue(store.publish(s,archive.BUCKET,later));before=s.objects[store.CURRENT][0]
        self.assertFalse(store.publish(s,archive.BUCKET,early));self.assertEqual(s.objects[store.CURRENT][0],before)
        different=deepcopy(later);different['coverage']['retained_records']+=1
        s.objects[store.CURRENT]=(archive.canonical(different),fixtures.NOW)
        with self.assertRaisesRegex(ValueError,'same_clock'):store.publish(s,archive.BUCKET,later)

    def test_publication_race_rechecks_newer_head(self):
        s,_,_=original();early,_,_=packet(s);later,_,_=packet(s,(fixtures.NOW+timedelta(hours=2)).isoformat())
        s.race=lambda:s.objects.update({store.CURRENT:(archive.canonical(later),fixtures.NOW)})
        self.assertFalse(store.publish(s,archive.BUCKET,early))
        self.assertEqual(json.loads(s.objects[store.CURRENT][0]),later)

    def test_inventory_is_complete_paginated_and_excludes_only_after_cutoff(self):
        s,_,_=original();prefix=archive.PREFIX+'records/'
        listed=store.inventory(s,prefix,(fixtures.NOW+timedelta(hours=1)).isoformat())
        self.assertEqual(listed['listing_pages'],2);self.assertEqual(listed['objects_at_cutoff'],2)
        listed=store.inventory(s,prefix,(fixtures.NOW-timedelta(seconds=1)).isoformat())
        self.assertEqual(listed['objects_at_cutoff'],0);self.assertEqual(listed['objects_after_cutoff'],2)
        with self.assertRaises(ValueError):store.inventory(s,'data/private/',fixtures.NOW.isoformat())

    def test_unreviewed_paths_fail_before_reads_and_bool_lengths_are_rejected(self):
        s,_,_=original();read=store.reader(s,archive.BUCKET)
        for key in ('data/signal-genealogy.json','data/prospective-outcomes.json','portfolio/snapshot.json'):
            with self.assertRaises(ValueError):read(key)
        self.assertEqual(s.reads,[])
        with self.assertRaises(ValueError):store.checked({'key':'x','sha256':'a'*64,'bytes':True},'inputs',read)


if __name__=='__main__':unittest.main()
