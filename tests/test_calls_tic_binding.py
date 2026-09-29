"""Actual Calls persistence, auditor and offline replay with complete TIC originals."""
from copy import deepcopy
from datetime import datetime,timedelta
import gzip,hashlib,importlib.util,json
from pathlib import Path
import sys,tempfile,types,unittest
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'tests'),str(ROOT/'scripts')]
import calls_research_replay as calls
import calls_tic_binding as binding
import calls_original_reader as transport
from calls_contract import make_snapshot
from test_calls_tic_lineage import snapshot,fixture


class TicBinding(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.raw,read,_,_=snapshot();cls.packet=json.loads(cls.raw);cls.read=staticmethod(read);cls.at=fixture.AT
        cls.reader=transport.ImmutableReader(read)
        cls.bundle=calls.prepare(lambda key:deepcopy(cls.packet) if key==binding.KEY else {},cls.at,cls.reader)

    def test_actual_calls_binds_all_native_rows_and_twelve_original_months(self):
        out=self.bundle['payload']['output'];proof=out['original_source_lineage']['tic']
        self.assertEqual(proof['status'],'verified');self.assertEqual(proof['coverage']['original_core_rows'],324)
        row=next(r for r in out['evidence'] if r['source']==binding.KEY)
        self.assertTrue(row['original_source_evidence_verified']);self.assertTrue(row['current_original_research_eligible'])
        self.assertEqual(row['value'],48);self.assertEqual(row['quality_status'],'fresh')
        self.assertEqual(row['max_source_age_seconds'],26*3600);self.assertIsNone(row['max_observation_age_days'])
        self.assertEqual(len(row['original_observation_ids']),12)
        self.assertTrue(all(identity in proof['observations'] for identity in row['original_observation_ids']))
        self.assertEqual(proof['net_cross_border_definition']['value'],18)
        self.assertEqual(out['call_verb'],'WAIT');self.assertFalse(out['sizing_eligible']);self.assertFalse(out['decision_eligible'])
        self.assertEqual(calls.replay(self.bundle,self.reader)['status'],'reproduced')
        with self.assertRaisesRegex(ValueError,'artifact reader required'):calls.replay(self.bundle)

    def test_invocation_reuse_does_not_cache_expired_permission_or_change_old_run(self):
        reader=transport.ImmutableReader(self.read)
        with patch.object(binding.lineage.store,'replay',wraps=binding.lineage.store.replay) as native:
            bundle=calls.prepare(lambda key:self.packet if key==binding.KEY else {},self.at,reader)
            self.assertEqual(native.call_count,2);before=calls.canonical(bundle)
            for seconds,eligible in ((26*3600,True),(26*3600+1,False),(27*3600,False)):
                at=(datetime.fromisoformat(self.at)+timedelta(seconds=seconds)).isoformat()
                out=calls.compile_frozen(bundle['payload']['inputs'],at,reader)
                row=next(r for r in out['evidence'] if r['source']==binding.KEY)
                self.assertEqual(row['current_original_research_eligible'],eligible)
                self.assertEqual(row['quality_status'],'fresh' if eligible else 'stale')
                self.assertEqual(row['value'],48)
                permission=next(p for p in out['evidence_inventory']['permission_mask'] if p['series_id']==row['series_id'])
                self.assertEqual(permission['may_inform_research'],eligible);self.assertFalse(permission['may_vote'])
                self.assertEqual(out['original_source_lineage']['tic']['observations'],bundle['payload']['output']['original_source_lineage']['tic']['observations'])
            self.assertEqual(calls.canonical(bundle),before);calls.replay(bundle,reader);self.assertEqual(native.call_count,2)
            calls.replay(bundle,transport.ImmutableReader(self.read));self.assertEqual(native.call_count,4)

    def test_fresh_wrapper_cannot_renew_old_acquisitions_and_missing_months_stay_missing(self):
        later='2026-09-20T18:00:00+00:00';raw,read,_,_=snapshot(generated=later)
        run=calls.prepare(lambda key:json.loads(raw) if key==binding.KEY else {},later,transport.ImmutableReader(read))
        proof=run['payload']['output']['original_source_lineage']['tic']
        self.assertFalse(proof['current_use']['eligible']);self.assertNotIn('native_packet:publication_age',proof['current_use']['issues'])
        self.assertTrue(any(v.endswith(':acquisition_age') for v in proof['current_use']['issues']))
        def missing(doc):
            row=next(v for v in doc['series'] if v['source_id']=='for_lt_total_net_99996');del row['observations'][4]
        raw,read,_,_=snapshot(missing)
        run=calls.prepare(lambda key:json.loads(raw) if key==binding.KEY else {},self.at,transport.ImmutableReader(read))
        row=next(r for r in run['payload']['output']['evidence'] if r['source']==binding.KEY)
        self.assertIsNone(row['value']);self.assertFalse(row['current_original_research_eligible'])
        self.assertEqual(run['payload']['output']['original_source_lineage']['tic']['windows']['total']['missing_months'],['2026-03-01'])

    def test_rehashed_projection_and_altered_whole_source_cannot_borrow_proof(self):
        self.assertEqual(binding.capture({**self.packet,'private_canary':'NO'},self.reader,self.at),binding.unavailable('original_replay_failed'))
        run=deepcopy(self.bundle);entry=run['payload']['inputs'][binding.KEY]
        entry['projection']['headline']['foreign_net_into_us_lt_12mo_b']=999;entry['sha256']=calls.digest(entry['projection'])
        run['payload_sha256']=calls.digest(run['payload']);run['run_id']='calls-research-'+run['payload_sha256']
        with self.assertRaisesRegex(ValueError,'frozen projection differ'):calls.replay(run,self.reader)
        identity=deepcopy(self.bundle['payload']['inputs'][binding.KEY]['original_binding']);identity['canonical_packet_sha256']='a'*64
        with self.assertRaisesRegex(ValueError,'frozen TIC packet differs'):binding.resolve(identity,self.reader,self.at)

    def test_failed_capture_stays_frozen_and_exception_text_is_private(self):
        def fail(key):raise ValueError('PRIVATE_FAILURE_CANARY')
        run=calls.prepare(lambda key:self.packet if key==binding.KEY else {},self.at,fail)
        self.assertEqual(run['payload']['inputs'][binding.KEY]['original_binding'],binding.unavailable('original_replay_failed'))
        self.assertNotIn(b'PRIVATE_FAILURE_CANARY',calls.canonical(run))
        seen=[];self.assertEqual(calls.replay(run,lambda key:seen.append(key))['status'],'reproduced');self.assertEqual(seen,[])
        row=next(r for r in run['payload']['output']['evidence'] if r['source']==binding.KEY)
        self.assertEqual(row['quality_status'],'unverified');self.assertFalse(row['current_original_research_eligible'])

    def test_unknown_metadata_and_originals_stay_outside_public_run(self):
        raw,read,_,_=snapshot(decorate=True)
        run=calls.prepare(lambda key:json.loads(raw) if key==binding.KEY else {},self.at,transport.ImmutableReader(read))
        self.assertEqual(run['payload']['output']['original_source_lineage']['tic']['status'],'verified')
        for text in (b'DESCRIPTOR_CANARY',b'EVIDENCE_CANARY',b'EXTRA_ORIGINAL_CANARY'):
            self.assertNotIn(text,calls.canonical(run))

    def test_invalid_and_private_references_fail_before_transport(self):
        seen=[];read=transport.ImmutableReader(lambda key:seen.append(key))
        for key in ('data/capital-inflows.json','data/prospective-outcomes.json','private/account.json','data/tic-research/current.json',None):
            with self.assertRaises(ValueError):read(key)
        for ref in (None,{**self.packet['replay'],'extra':'CANARY'},{**self.packet['replay'],'manifest_key':'data/capital-inflows.json'}):
            self.assertEqual(binding.capture({**self.packet,'replay':ref},read,self.at),binding.unavailable('invalid_original_reference'))
        self.assertEqual(seen,[])

    def client(self):
        client=fixture.Storage()
        for key,raw in self.reader.cache.items():client.objects[key]=gzip.compress(raw,mtime=0) if key.endswith('.gz') else raw
        return client

    def test_actual_persistence_and_native_auditor_use_new_whole_original_replay(self):
        client=self.client();ref=calls.persist(client,'fixture',self.bundle,transport.reader(client,'fixture'))
        public={**deepcopy(self.bundle['payload']['output']),'research_replay':ref}
        row=make_snapshot({'as_of':self.at},public);public['snapshot_id']=row['snapshot_id']
        client.objects['data/ai-brief-public.json']=calls.canonical(public)
        client.objects['data/decisive-call-history.json']=calls.canonical({'snapshots':[row]})
        path=ROOT/'aws/lambdas/justhodl-calls-research-audit/source/lambda_function.py'
        with patch.dict(sys.modules,{'boto3':types.SimpleNamespace(client=lambda *args,**kwargs:client)}):
            spec=importlib.util.spec_from_file_location('tic_calls_audit',path);audit=importlib.util.module_from_spec(spec);spec.loader.exec_module(audit)
        self.assertEqual(audit.verify_current(client,'fixture',datetime.fromisoformat(self.at))['status'],'reproduced')
        client.objects[self.packet['replay']['manifest_key']]=b'{}'
        with self.assertRaises(ValueError):audit.verify_current(client,'fixture')

    def test_complete_offline_mirror_and_snapshot_corruption_fail_closed(self):
        from replay_calls_research import original_reader
        client=self.client()
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)
            for key,raw in client.objects.items():
                p=root/key;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(raw)
            self.assertEqual(calls.replay(self.bundle,original_reader(root))['status'],'reproduced')
            (root/self.packet['replay']['manifest_key']).write_bytes(b'{}')
            with self.assertRaises(ValueError):calls.replay(self.bundle,original_reader(root))
        reader=transport.ImmutableReader(self.read);binding.snapshot(self.packet['replay'],reader)
        identity=next(iter(reader._verified));sha,raw=reader._verified[identity];reader._verified[identity]=(sha,b'{}')
        with self.assertRaisesRegex(ValueError,'snapshot differs'):binding.snapshot(self.packet['replay'],reader)

    def test_all_native_compiler_copies_and_expiry_projection_are_exact(self):
        self.assertEqual(len(calls.compiler_identity()['files']),31)
        for name in ('tic_original.py','tic_research.py'):
            self.assertEqual((ROOT/'aws/shared'/name).read_bytes(),(ROOT/'aws/lambdas/justhodl-capital-inflows/source'/name).read_bytes())
        self.assertEqual((ROOT/'scripts/verify_tic_arithmetic.py').read_bytes(),(ROOT/'aws/shared/verify_tic_arithmetic.py').read_bytes())
        import calls_tic_lineage
        at=(datetime.fromisoformat(self.at)+timedelta(hours=27)).isoformat()
        fresh=binding.at_time(binding.snapshot(self.packet['replay'],self.reader),at)
        candidate=calls_tic_lineage.inspect(self.raw,self.read,at)
        self.assertEqual(fresh['current_use'],candidate['current_use'])
        self.assertEqual(fresh['windows'],candidate['windows'])


if __name__=='__main__':
    if sys.argv[1:]==['--fixture']:
        TicBinding.setUpClass();print(calls.canonical(TicBinding.bundle['payload']['output']).decode())
    else:unittest.main(verbosity=2)
