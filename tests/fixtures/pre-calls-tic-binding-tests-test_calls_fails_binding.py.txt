"""Actual Calls compiler and auditor with complete synthetic FR2004 archives."""
from copy import deepcopy
from datetime import datetime, timedelta
from pathlib import Path
import importlib.util
import json
import sys
import tempfile
import types
import unittest
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'tests'),str(ROOT/'scripts')]
import calls_research_replay as calls
import calls_fails_binding as binding
import calls_original_reader as transport
from calls_contract import make_snapshot
from test_calls_fails_lineage import snapshot, fixture


class FailsBinding(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.raw,cls.client,cls.definitions=snapshot();cls.packet=json.loads(cls.raw);cls.at=fixture.AT
        cls.reader=transport.reader(cls.client,'fixture')
        with patch.dict(binding.lineage.store.native.DEFINITIONS,cls.definitions,clear=True):
            cls.bundle=calls.prepare(lambda key:deepcopy(cls.packet) if key==binding.KEY else {},cls.at,cls.reader)

    def test_actual_compiler_freezes_complete_originals_with_six_linked_scope_fields(self):
        out=self.bundle['payload']['output'];proof=out['original_source_lineage']['settlement_fails']
        self.assertEqual(proof['status'],'verified');self.assertEqual(proof['coverage']['original_rows'],1272)
        self.assertEqual(proof['coverage']['scope_history_rows'],848)
        self.assertEqual(len(proof['observations']),4);self.assertNotIn('history',proof['scopes']['ust_ex_tips'])
        self.assertEqual(proof['complete_source_output_key'],'data/fails-research/outputs/'+self.packet['replay']['output_sha256']+'.json')
        rows=[r for r in out['evidence'] if r['source']==binding.KEY]
        self.assertEqual(len(rows),6)
        for row in rows:
            self.assertTrue(row['original_source_evidence_verified']);self.assertTrue(row['current_original_research_eligible'])
            self.assertEqual(row['quality_status'],'fresh');self.assertIsNone(row['max_observation_age_days'])
            self.assertEqual(row['max_source_age_seconds'],36*3600)
            self.assertTrue(row['original_observation_ids'])
            self.assertTrue(all(identity in proof['observations'] for identity in row['original_observation_ids']))
        self.assertFalse(out['decision_eligible']);self.assertFalse(out['sizing_eligible']);self.assertEqual(out['call_verb'],'WAIT')
        with patch.dict(binding.lineage.store.native.DEFINITIONS,self.definitions,clear=True):
            self.assertEqual(calls.replay(self.bundle,self.reader)['status'],'reproduced')
        with self.assertRaisesRegex(ValueError,'artifact reader required'):calls.replay(self.bundle)

    def test_each_field_references_only_its_actual_side_and_shared_rows_reuse_identity(self):
        rows={r['series_id'].split('#')[1]:r for r in self.bundle['payload']['output']['evidence'] if r['source']==binding.KEY}
        self.assertEqual(len(rows['headline.ftd_bn']['original_observation_ids']),1)
        self.assertEqual(len(rows['treasury.ftd_bn']['original_observation_ids']),2)
        self.assertTrue(set(rows['headline.ftd_bn']['original_observation_ids'])<set(rows['treasury.ftd_bn']['original_observation_ids']))
        self.assertTrue(set(rows['headline.combined_bn']['original_observation_ids'])<set(rows['treasury.gross_bn']['original_observation_ids']))
        self.assertFalse(set(rows['treasury.ftd_bn']['original_observation_ids']) & set(rows['treasury.ftr_bn']['original_observation_ids']))

    def test_36_hour_original_policy_is_consistent_and_expiry_retains_reported_amounts(self):
        for hours,eligible in ((27,True),(37,False)):
            at=(datetime.fromisoformat(self.at)+timedelta(hours=hours)).isoformat()
            with patch.dict(binding.lineage.store.native.DEFINITIONS,self.definitions,clear=True):
                out=calls.compile_frozen(self.bundle['payload']['inputs'],at,self.reader)
            rows=[r for r in out['evidence'] if r['source']==binding.KEY]
            for row in rows:
                self.assertEqual(row['current_original_research_eligible'],eligible)
                self.assertEqual(row['quality_status'],'fresh' if eligible else 'stale')
                self.assertEqual(row['source_clock_status'],'within_age_ceiling' if eligible else 'stale')
                self.assertIsNotNone(row['value'])
                permission=next(p for p in out['evidence_inventory']['permission_mask'] if p['series_id']==row['series_id'])
                self.assertEqual(permission['may_inform_research'],eligible);self.assertFalse(permission['may_vote'])

    def test_capture_failure_is_frozen_and_later_recovery_cannot_qualify_it(self):
        def fail(key):raise ValueError('PRIVATE-FAILURE-CANARY')
        with patch.dict(binding.lineage.store.native.DEFINITIONS,self.definitions,clear=True):
            run=calls.prepare(lambda key:self.packet if key==binding.KEY else {},self.at,fail)
        self.assertEqual(run['payload']['inputs'][binding.KEY]['original_binding'],binding.unavailable('original_replay_failed'))
        self.assertNotIn('PRIVATE-FAILURE-CANARY',calls.canonical(run).decode())
        seen=[];self.assertEqual(calls.replay(run,lambda key:seen.append(key))['status'],'reproduced');self.assertEqual(seen,[])
        rows=[r for r in run['payload']['output']['evidence'] if r['source']==binding.KEY]
        self.assertTrue(all(r['quality_status']=='unverified' and not r['current_original_research_eligible'] for r in rows))

    def test_full_packet_and_rehashed_outer_projection_mutations_are_rejected(self):
        changed={**self.packet,'account_canary':'NO-PUBLIC-ACCOUNT'}
        with patch.dict(binding.lineage.store.native.DEFINITIONS,self.definitions,clear=True):
            self.assertEqual(binding.capture(changed,self.reader,self.at),binding.unavailable('original_replay_failed'))
        run=deepcopy(self.bundle);row=run['payload']['inputs'][binding.KEY]
        row['projection']['headline']['ftd_bn']+=1;row['sha256']=calls.digest(row['projection'])
        run['payload_sha256']=calls.digest(run['payload']);run['run_id']='calls-research-'+run['payload_sha256']
        with patch.dict(binding.lineage.store.native.DEFINITIONS,self.definitions,clear=True),self.assertRaisesRegex(ValueError,'frozen projection differ'):
            calls.replay(run,self.reader)

    def test_bad_references_and_unreviewed_paths_never_reach_transport(self):
        seen=[];read=transport.ImmutableReader(lambda key:seen.append(key))
        for key in ('data/settlement-fails.json','data/liquidity-flow.json','data/report-measurements.json',
                    'data/prospective-outcomes.json','data/fails-research/migration.json','private/account.json',None):
            with self.assertRaises(ValueError):read(key)
        self.assertEqual(seen,[])
        for ref in (None,{'manifest_key':'data/settlement-fails.json','output_sha256':'a'*64},
                    {**self.packet['replay'],'private_note':'NO'}):
            self.assertEqual(binding.capture({**self.packet,'replay':ref},read,self.at),binding.unavailable('invalid_original_reference'))
        self.assertEqual(seen,[])

    def test_valid_original_archives_cannot_copy_unselected_source_metadata_into_calls(self):
        def decorate(inputs):
            inputs['sources']['observations']['unselected_note']='PRIVATE-METADATA-CANARY'
            inputs['sources']['catalog']['evidence']['extra']={'positions':'PRIVATE-METADATA-CANARY'}
        raw,client,defs=snapshot(decorate=decorate);packet=json.loads(raw)
        self.assertIn('PRIVATE-METADATA-CANARY',raw.decode())
        with patch.dict(binding.lineage.store.native.DEFINITIONS,defs,clear=True):
            run=calls.prepare(lambda key:packet if key==binding.KEY else {},self.at,transport.reader(client,'fixture'))
        self.assertEqual(run['payload']['output']['original_source_lineage']['settlement_fails']['status'],'verified')
        self.assertNotIn('PRIVATE-METADATA-CANARY',calls.canonical(run).decode())

    def test_actual_native_auditor_uses_combined_reader_and_detects_missing_archive(self):
        client=fixture.Storage();client.objects=deepcopy(self.client.objects)
        reader=transport.reader(client,'fixture')
        with patch.dict(binding.lineage.store.native.DEFINITIONS,self.definitions,clear=True):
            ref=calls.persist(client,'fixture',self.bundle,reader)
            public={**deepcopy(self.bundle['payload']['output']),'research_replay':ref}
            row=make_snapshot({'as_of':self.at},public);public['snapshot_id']=row['snapshot_id']
            client.objects['data/ai-brief-public.json']=calls.canonical(public)
            client.objects['data/decisive-call-history.json']=calls.canonical({'snapshots':[row]})
            path=ROOT/'aws/lambdas/justhodl-calls-research-audit/source/lambda_function.py'
            with patch.dict(sys.modules,{'boto3':types.SimpleNamespace(client=lambda *args,**kwargs:client)}):
                spec=importlib.util.spec_from_file_location('fails_calls_audit',path);audit=importlib.util.module_from_spec(spec);spec.loader.exec_module(audit)
            self.assertEqual(audit.verify_current(client,'fixture',datetime.fromisoformat(self.at))['status'],'reproduced')
            client.objects[self.packet['replay']['manifest_key']]=b'{}'
            with self.assertRaises(ValueError):audit.verify_current(client,'fixture')

    def test_complete_offline_mirror_replays_without_current_source_fallback(self):
        from replay_calls_research import original_reader
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)
            for key,raw in self.client.objects.items():
                path=root/key;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(raw)
            with patch.dict(binding.lineage.store.native.DEFINITIONS,self.definitions,clear=True):
                self.assertEqual(calls.replay(self.bundle,original_reader(root))['status'],'reproduced')
                (root/self.packet['replay']['manifest_key']).write_bytes(b'{}')
                with self.assertRaises(ValueError):calls.replay(self.bundle,original_reader(root))

    def test_twenty_five_compiler_files_and_native_copies_are_exact(self):
        for name in ('fails_native.py','fails_research.py','fails_store.py'):
            self.assertEqual((ROOT/'aws/shared'/name).read_bytes(),(ROOT/'aws/lambdas/justhodl-settlement-fails/source'/name).read_bytes())
        self.assertEqual((ROOT/'aws/shared/verify_fails_arithmetic.py').read_bytes(),(ROOT/'scripts/verify_fails_arithmetic.py').read_bytes())
        files=calls.compiler_identity()['files'];self.assertEqual(len(files),25)
        self.assertTrue({'calls_fails_binding.py','calls_fails_originals.py','calls_original_reader.py','fails_native.py',
            'fails_research.py','fails_store.py','verify_fails_arithmetic.py'} <= set(files))


if __name__=='__main__':
    if sys.argv[1:]==['--fixture']:
        FailsBinding.setUpClass();print(calls.canonical(FailsBinding.bundle['payload']['output']).decode())
    else:unittest.main(verbosity=2)
