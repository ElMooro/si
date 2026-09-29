"""Actual Calls compiler/auditor with complete synthetic ECB originals."""
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime, timedelta
import gzip
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'tests'),str(ROOT/'scripts')]
import calls_research_replay as calls
import calls_ciss_binding as binding
import calls_original_reader as transport
from calls_contract import make_snapshot
from test_calls_ciss_lineage import snapshot, fixture, storage


class CissBinding(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.raw,read,_,_=snapshot();cls.read=staticmethod(read);cls.packet=json.loads(cls.raw);cls.at=fixture.NOW
        cls.reader=transport.ImmutableReader(read)
        cls.bundle=calls.prepare(lambda key:deepcopy(cls.packet) if key==binding.KEY else {},cls.at,cls.reader)

    def test_actual_compiler_binds_all_original_rows_and_seven_current_legs(self):
        out=self.bundle['payload']['output'];proof=out['original_source_lineage']['ciss']
        self.assertEqual(proof['status'],'verified');self.assertEqual(proof['coverage']['original_rows'],14)
        row=next(r for r in out['evidence'] if r['source']==binding.KEY)
        self.assertTrue(row['original_source_evidence_verified']);self.assertTrue(row['current_original_research_eligible'])
        self.assertEqual(row['quality_status'],'fresh');self.assertEqual(row['max_source_age_seconds'],72*3600)
        self.assertEqual(len(row['original_observation_ids']),7)
        self.assertTrue(all(identity in proof['observations'] for identity in row['original_observation_ids']))
        self.assertEqual(out['call_verb'],'WAIT');self.assertFalse(out['decision_eligible']);self.assertFalse(out['sizing_eligible'])
        self.assertEqual(calls.replay(self.bundle,self.reader)['status'],'reproduced')
        with self.assertRaisesRegex(ValueError,'artifact reader required'):calls.replay(self.bundle)

    def test_native_replays_are_cached_once_per_reader_but_freshness_is_never_cached(self):
        reader=transport.ImmutableReader(self.read)
        with patch.object(binding.lineage.replay,'replay',wraps=binding.lineage.replay.replay) as native:
            bundle=calls.prepare(lambda key:self.packet if key==binding.KEY else {},self.at,reader)
            self.assertEqual(native.call_count,2)  # Full reconstruction plus complete candidate replay.
            calls.replay(bundle,reader)
            before=reader.bytes
            for hours,eligible in ((27,True),(72,True),(73,False)):
                at=(datetime.fromisoformat(self.at)+timedelta(hours=hours)).isoformat()
                out=calls.compile_frozen(bundle['payload']['inputs'],at,reader)
                row=next(r for r in out['evidence'] if r['source']==binding.KEY)
                self.assertEqual(row['current_original_research_eligible'],eligible)
                self.assertEqual(row['quality_status'],'fresh' if eligible else 'stale')
                self.assertEqual(row['source_clock_status'],'within_age_ceiling' if eligible else 'stale')
                self.assertEqual(row['value'],.03)
                permission=next(p for p in out['evidence_inventory']['permission_mask'] if p['series_id']==row['series_id'])
                self.assertEqual(permission['may_inform_research'],eligible);self.assertFalse(permission['may_vote'])
            self.assertEqual(native.call_count,2);self.assertEqual(reader.bytes,before)
            self.assertEqual(len(reader._verified),1)
            calls.replay(bundle,transport.ImmutableReader(self.read));self.assertEqual(native.call_count,4)

    def test_new_decision_time_does_not_mutate_snapshot_or_old_frozen_run(self):
        before=calls.canonical(self.bundle)
        late=(datetime.fromisoformat(self.at)+timedelta(hours=73)).isoformat()
        out=calls.compile_frozen(self.bundle['payload']['inputs'],late,self.reader)
        original=self.bundle['payload']['output']['original_source_lineage']['ciss']
        self.assertEqual(out['original_source_lineage']['ciss']['observations'],original['observations'])
        self.assertFalse(out['original_source_lineage']['ciss']['current_headline_research_eligible'])
        self.assertEqual(before,calls.canonical(self.bundle));self.assertEqual(calls.replay(self.bundle,self.reader)['status'],'reproduced')

    def test_complete_source_tamper_and_rehashed_projection_cannot_borrow_verification(self):
        changed={**self.packet,'private_canary':'DO-NOT-PUBLISH'}
        self.assertEqual(binding.capture(changed,self.reader,self.at),binding.unavailable('original_replay_failed'))
        run=deepcopy(self.bundle);entry=run['payload']['inputs'][binding.KEY]
        entry['projection']['ea_composite']=.8;entry['sha256']=calls.digest(entry['projection'])
        run['payload_sha256']=calls.digest(run['payload']);run['run_id']='calls-research-'+run['payload_sha256']
        with self.assertRaisesRegex(ValueError,'frozen projection differ'):calls.replay(run,self.reader)
        entry=deepcopy(self.bundle['payload']['inputs'][binding.KEY]['original_binding']);entry['canonical_packet_sha256']='a'*64
        with self.assertRaisesRegex(ValueError,'frozen CISS packet differs'):binding.resolve(entry,self.reader,self.at)

    def test_frozen_failure_cannot_recover_retroactively_and_exception_text_never_leaks(self):
        def fail(key):raise ValueError('PRIVATE-FAILURE-CANARY')
        run=calls.prepare(lambda key:self.packet if key==binding.KEY else {},self.at,fail)
        self.assertEqual(run['payload']['inputs'][binding.KEY]['original_binding'],binding.unavailable('original_replay_failed'))
        self.assertNotIn('PRIVATE-FAILURE-CANARY',calls.canonical(run).decode())
        seen=[];self.assertEqual(calls.replay(run,lambda key:seen.append(key))['status'],'reproduced');self.assertEqual(seen,[])
        row=next(r for r in run['payload']['output']['evidence'] if r['source']==binding.KEY)
        self.assertEqual(row['quality_status'],'unverified');self.assertFalse(row['current_original_research_eligible'])

    def test_private_current_and_invalid_reference_paths_rejected_before_transport(self):
        seen=[];read=transport.ImmutableReader(lambda key:seen.append(key))
        for key in ('data/ciss-stress.json','data/ciss-ai.json','data/prospective-outcomes.json','private/account.json','data/ciss-research/cache/a.json',None):
            with self.assertRaises(ValueError):read(key)
        for ref in (None,{**self.packet['replay'],'manifest_key':'data/ciss-stress.json'},
                    {**self.packet['replay'],'extra':'CANARY'}, {**self.packet['replay'],'compiler':{'key':'private/code.py','sha256':'a'*64}}):
            self.assertEqual(binding.capture({**self.packet,'replay':ref},read,self.at),binding.unavailable('invalid_original_reference'))
        self.assertEqual(seen,[])

    def test_unknown_valid_metadata_does_not_enter_public_run(self):
        raw,read,_,_=snapshot(decorate=True);packet=json.loads(raw)
        run=calls.prepare(lambda key:packet if key==binding.KEY else {},self.at,transport.ImmutableReader(read))
        self.assertEqual(run['payload']['output']['original_source_lineage']['ciss']['status'],'verified')
        self.assertNotIn('DESCRIPTOR_SECRET',calls.canonical(run).decode());self.assertNotIn('EVIDENCE_SECRET',calls.canonical(run).decode())

    def client(self):
        client=storage.MemoryS3()
        for key,raw in self.reader.cache.items():client.objects[key]=gzip.compress(raw,mtime=0) if key.endswith('.gz') else raw
        return client

    def test_actual_persistence_and_auditor_reconstruct_with_fresh_reader(self):
        client=self.client();reader=transport.reader(client,'fixture')
        ref=calls.persist(client,'fixture',self.bundle,reader)
        public={**deepcopy(self.bundle['payload']['output']),'research_replay':ref}
        row=make_snapshot({'as_of':self.at},public);public['snapshot_id']=row['snapshot_id']
        client.objects['data/ai-brief-public.json']=calls.canonical(public)
        client.objects['data/decisive-call-history.json']=calls.canonical({'snapshots':[row]})
        path=ROOT/'aws/lambdas/justhodl-calls-research-audit/source/lambda_function.py'
        with patch.dict(sys.modules,{'boto3':types.SimpleNamespace(client=lambda *args,**kwargs:client)}):
            spec=importlib.util.spec_from_file_location('ciss_calls_audit',path);audit=importlib.util.module_from_spec(spec);spec.loader.exec_module(audit)
        self.assertEqual(audit.verify_current(client,'fixture',datetime.fromisoformat(self.at))['status'],'reproduced')
        client.objects[self.packet['replay']['manifest_key']]=b'{}'
        with self.assertRaises(ValueError):audit.verify_current(client,'fixture')

    def test_offline_mirror_and_invocation_cache_corruption_fail_closed(self):
        from replay_calls_research import original_reader
        client=self.client()
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)
            for key,raw in client.objects.items():
                p=root/key;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(raw)
            self.assertEqual(calls.replay(self.bundle,original_reader(root))['status'],'reproduced')
            (root/self.packet['replay']['manifest_key']).write_bytes(b'{}')
            with self.assertRaises(ValueError):calls.replay(self.bundle,original_reader(root))
        read=transport.ImmutableReader(self.read);binding.snapshot(self.packet['replay'],read)
        identity=next(iter(read._verified));sha,raw=read._verified[identity];read._verified[identity]=(sha,b'{}')
        with self.assertRaisesRegex(ValueError,'snapshot differs'):binding.snapshot(self.packet['replay'],read)

    def test_concurrent_original_reads_share_one_complete_result_and_budget(self):
        key=self.packet['replay']['manifest_key'];seen=[]
        def read(k):seen.append(k);return self.read(k)
        reader=transport.ImmutableReader(read)
        with ThreadPoolExecutor(max_workers=8) as pool:results=list(pool.map(reader,[key]*32))
        self.assertEqual(seen,[key]);self.assertEqual(reader.bytes,len(results[0]));self.assertTrue(all(r==results[0] for r in results))
        with self.assertRaisesRegex(ValueError,'snapshot exceeds bound'):reader.ciss_snapshot('a'*64,lambda:b'')

    def test_compiler_closure_and_independent_checker_copy_are_exact(self):
        files=calls.compiler_identity()['files'];self.assertEqual(len(files),25)
        self.assertTrue({'calls_ciss_binding.py','calls_ciss_originals.py','ciss_original_replay.py','ciss_source_model.py','verify_ciss_arithmetic.py'}<=set(files))
        self.assertEqual((ROOT/'scripts/verify_ciss_arithmetic.py').read_bytes(),(ROOT/'aws/shared/verify_ciss_arithmetic.py').read_bytes())


if __name__=='__main__':
    if sys.argv[1:]==['--fixture']:
        CissBinding.setUpClass();print(calls.canonical(CissBinding.bundle['payload']['output']).decode())
    else:unittest.main(verbosity=2)
