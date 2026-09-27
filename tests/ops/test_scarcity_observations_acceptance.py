from pathlib import Path
from copy import deepcopy
import importlib.util,json,sys,unittest
ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops/staged','aws/ops','aws/ops/checks','aws/shared')]
import ops_6245_scarcity_observations_acceptance as op
spec=importlib.util.spec_from_file_location('isolated_scarcity_native_tests',ROOT/'aws/lambdas/justhodl-scarcity-radar/tests/run_tests.py');native=importlib.util.module_from_spec(spec);spec.loader.exec_module(native)


class Tests(unittest.TestCase):
    def test_actual_native_whole_originals_replay_without_mutations(self):
        _,memory=native.compiled();before=list(memory.writes);result,protected=op.publication(memory,memory.data[native.KEY])
        self.assertEqual(result['status'],'complete_native_donor_originals_replayed');self.assertEqual(result['source_count'],7);self.assertEqual(len(protected),7);self.assertEqual(memory.writes,before)
    def test_tampered_projection_permissions_and_missing_donor_fail(self):
        p,memory=native.compiled()
        mutations=[lambda p:p['donor_occurrences'][0]['raw'].update(zero=99),lambda p:p.update(calls_eligible=True),lambda p:p.update(source_count=0),lambda p:p.update(acquisition_started_at='2099-01-01T00:00:00Z'),lambda p:p['source_inventory'].pop()]
        for mutate in mutations:
            changed=deepcopy(p);mutate(changed);raw=json.dumps(changed).encode();memory.data[op.public_ref(raw)['key']]=raw
            with self.assertRaises(ValueError):op.publication(memory,raw)
    def test_tampered_original_and_unrelated_objects_fail_before_access(self):
        p,memory=native.compiled();ref=p['source_inventory'][0]['capture']['original'];memory.data[ref['key']]=b'wrong'
        with self.assertRaises(ValueError):op.publication(memory,memory.data[native.KEY])
        memory=native.Memory()
        for key in ['data/pm-decision.json','private/accounts.json','learning/log.json','audit-private/other/file.bin','data/scarcity-radar/history/../private.json']:
            with self.assertRaises(ValueError):op.retained(memory,{'key':key})
            with self.assertRaises(ValueError):op.retained(memory,{'key':key},True)
        self.assertEqual(memory.reads,[])
    def test_old_packet_explicitly_pending_without_original_reads(self):
        memory=native.Memory();result,protected=op.publication(memory,b'{"version":"1.1.0"}')
        self.assertEqual(result['status'],'pending_original_schedule_publication');self.assertEqual(protected,[]);self.assertEqual(memory.reads,[])


if __name__=='__main__':unittest.main(verbosity=2)
