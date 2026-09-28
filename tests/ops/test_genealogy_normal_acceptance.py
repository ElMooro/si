from pathlib import Path
from datetime import datetime,timedelta,timezone
from copy import deepcopy
import ast,importlib.util,json,sys,tempfile,unittest
ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/'aws/lambdas/justhodl-signal-genealogy/source'),str(ROOT/'aws/shared'),str(ROOT/'tests')]
path=ROOT/'aws/ops/staged/ops_6322_genealogy_normal_publication_acceptance.py'
spec=importlib.util.spec_from_file_location('normal_genealogy_acceptance',path);probe=importlib.util.module_from_spec(spec);spec.loader.exec_module(probe)
import genealogy_native_publication as publication
import genealogy_native_runtime as runtime
import genealogy_public_archive as archive
import genealogy_cache_checkpoint as checkpoint
from test_genealogy_public_archive import fixture
from test_genealogy_revision_cache import Client
from test_genealogy_native_publication import DiskStore


class Tests(unittest.TestCase):
    def test_only_exact_new_public_retained_paths_are_readable(self):
        class Reader:
            def __init__(self):self.calls=[]
            def get_object(self,**kw):self.calls.append(kw);return {}
        underlying=Reader();client=probe.PublicRetainedOnly(underlying)
        allowed=[publication.CURRENT,publication.PREFIX+'artifacts/'+'a'*64+'.json',publication.PREFIX+'runs/'+'b'*64+'.json',publication.PREFIX+'compilers/'+'c'*64+'.py',checkpoint.PREFIX+'chunks/'+'d'*64+'.json',checkpoint.PREFIX+'snapshots/'+'e'*64+'.json']
        for key in allowed:client.get_object(Bucket=archive.BUCKET,Key=key)
        before=len(underlying.calls)
        for key in (None,'data/prospective-outcomes.json','data/signal-genealogy.json','data/research-forecasts/records/'+'a'*64+'.json','audit-private/anything',checkpoint.CURRENT,checkpoint.PREFIX+'chunks/../private.json',publication.PREFIX+'runs/short.json'):
            with self.subTest(key=key),self.assertRaises(ValueError):client.get_object(Bucket=archive.BUCKET,Key=key)
        with self.assertRaises(ValueError):client.get_object(Bucket='other',Key=publication.CURRENT)
        self.assertEqual(len(underlying.calls),before)
        self.assertFalse(hasattr(client,'put_object'));self.assertFalse(hasattr(client,'invoke'))

    def test_complete_native_candidate_replays_through_read_only_guard(self):
        original,_,_=fixture();at='2026-09-29T06:40:12+00:00';source=Client(original)
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);store=DiskStore(root/'objects');created=runtime.run(source,store,at,root/'run')
            writes_before=len(store.writes)
            client=probe.PublicRetainedOnly(store)
            replayed=publication.replay(client,created['packet']['replay'],root/'acceptance')
            self.assertEqual(publication.canonical(replayed),publication.canonical(created['packet']))
            self.assertTrue(all(key.startswith(publication.PREFIX) for key in client.keys))
            now=datetime(2026,9,29,6,45,tzinfo=timezone.utc)
            clocks=probe.validate_head(replayed,now-timedelta(seconds=10),now)
            self.assertEqual(clocks['publication_elapsed_upper_bound_s'],278)
            self.assertEqual(writes_before,len(store.writes))
            # All retained objects still get whole hash checks through the guard.
            ref=created['packet']['replay'];store.path(ref['key']).write_bytes(b'{}')
            with self.assertRaises(ValueError):publication.replay(client,ref,root/'corrupted')
            self.assertEqual(writes_before,len(store.writes))

    def test_publication_clocks_and_authority_must_remain_exact(self):
        now=datetime(2026,9,29,6,50,tzinfo=timezone.utc)
        head={'contract':'genealogy-research-head.v1','version':'2.0.0','generated_at':'2026-09-29T06:40:10+00:00',
            'authority':{'calls_eligible':False,'execution_eligible':False,'forecast_qualified':False,'sizing_eligible':False},
            'independent_evidence_count':None,'original_engine_replay_verified':False,'replay':publication.reference(b'{}','runs')}
        probe.validate_head(head,now,now)
        for field,value in (('contract','legacy'),('version','1.1.0'),('generated_at','2026-09-28T06:40:10+00:00'),('generated_at','2026-09-29T07:40:10+00:00'),('independent_evidence_count',1),('original_engine_replay_verified',True)):
            bad=deepcopy(head);bad[field]=value
            with self.subTest(field=field,value=value),self.assertRaises(ValueError):probe.validate_head(bad,now,now)
        for modified in (None,now.replace(tzinfo=None),now+timedelta(seconds=1),now-timedelta(minutes=20)):
            with self.subTest(modified=modified),self.assertRaises(ValueError):probe.validate_head(head,modified,now)
        with self.assertRaises(ValueError):probe.validate_head(head,now+timedelta(seconds=11),now+timedelta(seconds=11))
        with self.assertRaises(ValueError):probe.validate_head(head,now,now+timedelta(hours=27))
        bad=deepcopy(head);bad['authority']['calls_eligible']=True
        with self.assertRaises(ValueError):probe.validate_head(bad,now,now)

    def test_probe_has_no_native_mutation_or_private_output_reader(self):
        source=path.read_text(encoding='utf-8');tree=ast.parse(source)
        calls={n.func.attr for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute)}
        self.assertFalse(calls&{'invoke','put_object','update_schedule','put_rule','update_function_configuration','scan','query','get_secret_value'})
        self.assertIn('sys.exit(1)',source);self.assertIn('publication.replay',source)


if __name__=='__main__':unittest.main()
