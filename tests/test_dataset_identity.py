from pathlib import Path
from datetime import datetime,timezone
from unittest.mock import patch
import hashlib,io,json,types,unittest
R=Path(__file__).resolve().parents[1];F=R/'tests/fixtures/no-paid-adapters'
STAMP=datetime(2020,1,1,tzinfo=timezone.utc)
class Clock:
    @staticmethod
    def now(tz=None):return STAMP
class Store:
    def __init__(self):
        self.objects={('public','data/brain.json'):json.dumps({'notes':[{'id':'n1','cat':'rule','text':'Entirely invented note with adequate length for a regression case'}]}).encode()};self.writes=[];self.fail=None
    def get_object(self,Bucket,Key):return {'Body':io.BytesIO(self.objects[(Bucket,Key)])}
    def put_object(self,**kw):
        k=(kw['Bucket'],kw['Key'])
        if self.fail and self.fail(kw):raise RuntimeError('invented write denial')
        if kw.get('IfNoneMatch')=='*' and k in self.objects:raise RuntimeError('invented precondition conflict')
        self.objects[k]=kw['Body'];self.writes.append(kw)
def load(old=False):
    p=F/'brain-dataset.before.txt' if old else R/'aws/lambdas/justhodl-ai/source/brain_dataset.py'
    m=types.ModuleType('dataset_under_test');exec(compile(p.read_bytes(),str(p),'exec'),m.__dict__);m.datetime=Clock;return m
class DatasetIdentity(unittest.TestCase):
    def test_predecessor_same_clock_overwrote_first_dataset(self):
        m=load(True);db=Store();a=m.build_brain_dataset(db,'public','private',min_class_rows=1);b=m.build_brain_dataset(db,'public','private',min_class_rows=20)
        self.assertEqual(a['dataset_id'],b['dataset_id']);self.assertEqual(m.load_rows(db,'private',a['dataset_id'])[0]['split'],'excluded')
    def test_identical_clocks_preserve_both_datasets_and_split_rules(self):
        m=load();db=Store();a=m.build_brain_dataset(db,'public','private',min_class_rows=1);b=m.build_brain_dataset(db,'public','private',min_class_rows=20)
        self.assertNotEqual(a['dataset_id'],b['dataset_id']);first=m.load_rows(db,'private',a['dataset_id'])[0]
        self.assertEqual(first['split'],m._split(first['id']));self.assertEqual(m.load_rows(db,'private',b['dataset_id'])[0]['split'],'excluded')
        self.assertEqual(m.latest_dataset(db,'private')['dataset_id'],b['dataset_id'])
        for w in db.writes:
            if w['Key'].endswith(('rows.jsonl.gz','manifest.json')):self.assertEqual(w['IfNoneMatch'],'*');self.assertEqual(w['Bucket'],'private');self.assertEqual(w['ServerSideEncryption'],'AES256')
    def test_forced_identity_collision_cannot_overwrite_rows_or_latest_pointer(self):
        m=load();db=Store()
        with patch.object(m.uuid,'uuid4',return_value=types.SimpleNamespace(hex='a'*32)):
            m.build_brain_dataset(db,'public','private',min_class_rows=1);before=dict(db.objects)
            with self.assertRaisesRegex(RuntimeError,'precondition'):m.build_brain_dataset(db,'public','private',min_class_rows=20)
        self.assertEqual(db.objects,before)
    def test_rows_or_manifest_failure_cannot_publish_latest(self):
        for suffix in ('rows.jsonl.gz','manifest.json'):
            m=load();db=Store();old=b'{"previous":"invented"}';db.objects[('private','ai/datasets/brain/latest.json')]=old;db.fail=lambda kw:kw['Key'].endswith(suffix)
            with self.assertRaisesRegex(RuntimeError,'write denial'):m.build_brain_dataset(db,'public','private',min_class_rows=1)
            self.assertEqual(db.objects[('private','ai/datasets/brain/latest.json')],old)
    def test_existing_embedding_manifest_update_remains_possible(self):
        m=load();db=Store();m._put_json(db,'private','invented/manifest.json',{'v':1},create_only=True);m._put_json(db,'private','invented/manifest.json',{'v':2})
        self.assertEqual(json.loads(db.objects[('private','invented/manifest.json')]),{'v':2});self.assertNotIn('IfNoneMatch',db.writes[-1])
    def test_exact_source_and_test_predecessors_remain_reconstructable(self):
        for manifest,archive in [('brain-dataset-edit.json','brain-dataset.before.txt'),('global-flow-test-edit.json','global-flow-consumer-tests.before.txt')]:
            edit=json.loads((F/manifest).read_bytes());raw=(F/archive).read_bytes();self.assertEqual(hashlib.sha256(raw).hexdigest(),edit['predecessor_sha256']);s=raw.decode()
            for a,b in edit['edits']:self.assertEqual(s.count(a),1);s=s.replace(a,b)
            self.assertEqual(s,(R/edit['target']).read_text(encoding='utf-8'))
if __name__=='__main__':unittest.main(verbosity=2)
