"""Actual frozen Calls compilation and native audit against complete retained originals."""
from copy import deepcopy
from datetime import datetime,timedelta
from pathlib import Path
import hashlib,importlib.util,json,sys,types,unittest,subprocess,tempfile
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'tests')]
import calls_liquidity_binding as binding
import calls_research_replay as calls
from calls_contract import make_snapshot
from test_calls_liquidity_lineage import packet_at,native


class OriginalBinding(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.raw,cls.objects=packet_at();cls.packet=json.loads(cls.raw);cls.at=cls.packet['generated_at']
        cls.originals=binding.reader(native.Storage(cls.objects),'fixture')
        cls.bundle=calls.prepare(lambda key:deepcopy(cls.packet) if key==binding.KEY else {},cls.at,cls.originals)

    def test_actual_prepare_freezes_originals_and_matches_the_existing_qualified_arithmetic(self):
        out=self.bundle['payload']['output'];row=out['evidence'][0];proof=out['original_source_lineage']['liquidity_flow']
        self.assertTrue(row['original_source_evidence_verified']);self.assertTrue(row['current_original_research_eligible'])
        self.assertEqual(row['value'],self.packet['current']['net_liquidity_b']);self.assertEqual(row['quality_status'],'fresh')
        self.assertEqual(proof['coverage']['original_rows'],7364);self.assertEqual(proof['coverage']['calendar_dates'],180)
        self.assertEqual(proof['coverage']['comparison_windows'],4);self.assertEqual(proof['independent_arithmetic']['exact_rational_comparisons'],776)
        self.assertNotIn('packet_sha256',proof);self.assertIn('not transport bytes',proof['hash_basis'])
        self.assertEqual(row['original_calculation_id'],proof['latest_reconstructed']['calculation_id'])
        self.assertFalse(out['decision_eligible']);self.assertFalse(out['sizing_eligible']);self.assertEqual(out['call_verb'],'WAIT')
        self.assertEqual(calls.replay(self.bundle,self.originals)['status'],'reproduced')
        with self.assertRaisesRegex(ValueError,'artifact reader required'):calls.replay(self.bundle)

    def test_later_cutoff_retains_every_calculation_but_cannot_renew_old_sources(self):
        later=(datetime.fromisoformat(self.at.replace('Z','+00:00'))+timedelta(days=3)).isoformat()
        out=calls.compile_frozen(self.bundle['payload']['inputs'],later,self.originals)
        first=self.bundle['payload']['output']['original_source_lineage']['liquidity_flow'];line=out['original_source_lineage']['liquidity_flow']
        for key in ('observations','latest_reconstructed','calendar_history_180d','comparisons'):self.assertEqual(line[key],first[key])
        self.assertFalse(out['evidence'][0]['current_original_research_eligible']);self.assertEqual(out['evidence'][0]['quality_status'],'stale')
        self.assertIsNone(line['current_research_value']);self.assertFalse(out['evidence_inventory']['permission_mask'][0]['may_inform_research'])

    def test_failed_capture_stays_unavailable_when_the_archive_recovers(self):
        def fail(key):raise ValueError('PRIVATE-CANARY transport failure')
        run=calls.prepare(lambda key:self.packet if key==binding.KEY else {},self.at,fail)
        record=run['payload']['inputs'][binding.KEY]['original_binding']
        self.assertEqual(record,binding.unavailable('original_replay_failed'))
        self.assertNotIn('PRIVATE-CANARY',calls.canonical(run).decode())
        self.assertFalse(run['payload']['output']['evidence_inventory']['permission_mask'][0]['may_inform_research'])
        reads=[]
        self.assertEqual(calls.replay(run,lambda key:reads.append(key) or self.originals(key))['status'],'reproduced')
        self.assertEqual(reads,[])

    def test_mutable_private_bad_reference_and_whole_packet_mutation_are_rejected(self):
        for value in (None,{'manifest_key':'private/account.json','output_sha256':'a'*64},
            {'manifest_key':'data/liquidity-flow.json','output_sha256':'a'*64},
            {**self.packet['replay'],'note':'PRIVATE-CANARY'}):
            p=deepcopy(self.packet);p['replay']=value;seen=[]
            result=binding.capture(p,lambda key:seen.append(key),self.at)
            self.assertEqual(result,binding.unavailable('invalid_original_reference'));self.assertEqual(seen,[])
        for change in ({'account':{'positions':['PRIVATE-CANARY']}},{'current':{'net_liquidity_b':999999}}):
            self.assertEqual(binding.capture({**self.packet,**change},self.originals,self.at),binding.unavailable('original_replay_failed'))

    def test_rehashed_calls_projection_cannot_disagree_with_frozen_originals(self):
        run=deepcopy(self.bundle);row=run['payload']['inputs'][binding.KEY]
        row['projection']['current']['net_liquidity_b']=123;row['sha256']=calls.digest(row['projection'])
        run['payload_sha256']=calls.digest(run['payload']);run['run_id']='calls-research-'+run['payload_sha256']
        with self.assertRaisesRegex(ValueError,'frozen projection differ'):calls.replay(run,self.originals)

    def test_full_native_auditor_replays_originals_and_refuses_missing_or_tampered_archive(self):
        store=native.Storage(self.objects);originals=binding.reader(store,'fixture')
        ref=calls.persist(store,'fixture',self.bundle,originals)
        public={**deepcopy(self.bundle['payload']['output']),'research_replay':ref}
        row=make_snapshot({'as_of':self.at},public);public['snapshot_id']=row['snapshot_id']
        store.objects['data/ai-brief-public.json']=calls.canonical(public)
        store.objects['data/decisive-call-history.json']=calls.canonical({'snapshots':[row]})
        path=ROOT/'aws/lambdas/justhodl-calls-research-audit/source/lambda_function.py'
        with patch.dict(sys.modules,{'boto3':types.SimpleNamespace(client=lambda *a,**k:store)}):
            spec=importlib.util.spec_from_file_location('original_bound_calls_audit',path)
            auditor=importlib.util.module_from_spec(spec);spec.loader.exec_module(auditor)
        self.assertEqual(auditor.verify_current(store,'fixture',datetime.fromisoformat(self.at.replace('Z','+00:00')))['status'],'reproduced')
        key=self.packet['replay']['manifest_key'];store.objects[key]=b'{}'
        with self.assertRaises(ValueError):auditor.verify_current(store,'fixture')

    def test_read_boundary_cache_is_complete_and_only_immutable(self):
        class Transport:
            def __init__(self):self.reads=[]
            def get_object(self,**kw):self.reads.append(kw);raise AssertionError('Unapproved read')
        client=Transport();read=binding.reader(client,'fixture')
        for key in ('data/liquidity-flow.json','data/report-measurements.json','data/settlement-fails.json',
                    'private/account.json','data/prospective-outcomes.json'):
            with self.assertRaises(ValueError):read(key)
        self.assertEqual(client.reads,[])
        self.assertTrue(all('/'+hashlib.sha256(raw).hexdigest()+'.' in key for key,raw in self.originals.cache.items()))

    def test_shared_compilers_preserve_exact_reviewed_source_and_complete_closure(self):
        for name in ('liquidity_flow_store.py','liquidity_flow_model.py','liquidity_flow_arithmetic.py'):
            self.assertEqual((ROOT/'aws/shared'/name).read_bytes(),(ROOT/'aws/lambdas/justhodl-liquidity-flow/source'/name).read_bytes())
        self.assertEqual((ROOT/'aws/shared/verify_liquidity_arithmetic.py').read_bytes(),(ROOT/'scripts/verify_liquidity_arithmetic.py').read_bytes())
        files=calls.compiler_identity()['files'];self.assertEqual(len(files),13)
        for name in ('calls_liquidity_binding.py','calls_liquidity_originals.py','canonical_fred_replay.py','liquidity_flow_store.py'):
            self.assertIn(name,files)

    def test_command_line_replay_uses_complete_offline_mirror_without_current_source_fallback(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);bundle=root/'run.json';bundle.write_bytes(calls.canonical(self.bundle))
            for key,raw in self.objects.items():
                path=root/key;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(raw)
            result=subprocess.run([sys.executable,str(ROOT/'scripts/replay_calls_research.py'),'--file',str(bundle),
                '--originals-dir',str(root)],capture_output=True,text=True,timeout=60)
            self.assertEqual(result.returncode,0,result.stderr)
            self.assertEqual(json.loads(result.stdout)['status'],'reproduced')
            (root/self.packet['replay']['manifest_key']).write_bytes(b'{}')
            result=subprocess.run([sys.executable,str(ROOT/'scripts/replay_calls_research.py'),'--file',str(bundle),
                '--originals-dir',str(root)],capture_output=True,text=True,timeout=60)
            self.assertNotEqual(result.returncode,0)


if __name__=='__main__':
    if sys.argv[1:]==['--fixture']:
        OriginalBinding.setUpClass();print(calls.canonical(OriginalBinding.bundle['payload']['output']).decode())
    else:unittest.main(verbosity=2)
