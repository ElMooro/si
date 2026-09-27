from pathlib import Path
from copy import deepcopy
from datetime import datetime,timezone
import importlib.util,json,sys,unittest
ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops/staged','aws/ops','aws/ops/checks','aws/shared')]
import ops_6248_hiring_statement_acceptance as op
spec=importlib.util.spec_from_file_location('isolated_hiring_native_tests',ROOT/'aws/lambdas/justhodl-hiring-velocity/tests/run_tests.py');native=importlib.util.module_from_spec(spec);spec.loader.exec_module(native)


class Tests(unittest.TestCase):
    def publication(self):
        memory=native.Memory();prior=memory.previous;ns=native.native(memory)
        ns['_hiring_company']=lambda *a:[native.envelope(json.dumps(native.employees()).encode(),datetime.now(timezone.utc).isoformat(),'historical-employee-count')]
        ns['lambda_handler']();return memory.data['data/hiring-velocity.json'],prior,memory
    def test_full_actual_native_original_replay_and_public_archives(self):
        raw,prior,memory=self.publication();out=op.publication(raw,prior)
        self.assertEqual(out['status'],'published_workforce_originals_replayed');self.assertEqual(out['employee_observations'],6)
        self.assertEqual(op.read_public_archive(memory,op.archive_ref(raw)),raw);self.assertEqual(op.read_public_archive(memory,op.archive_ref(prior)),prior)
        writes=list(memory.writes);op.publication(raw,prior);self.assertEqual(writes,memory.writes)
    def test_tampered_measurements_population_clocks_permissions_fail(self):
        raw,prior,_=self.publication();p=json.loads(raw)
        changes=[lambda d:d['request_records'][0]['employee_observations'][0].update(employee_count=999),
                 lambda d:d['request_records'][0]['annual_comparisons'][0].update(annual_interval_change_pct=999),
                 lambda d:d.update(unselected_universe_indices=[1]),lambda d:d.update(n_employee_observations=99),
                 lambda d:d.update(acquisition_started_at='2099-01-01T00:00:00Z'),lambda d:d.update(calls_eligible=True),
                 lambda d:d.update(previous_publication=None),lambda d:d.update(cap_buckets=['small'])]
        for change in changes:
            altered=deepcopy(p);change(altered)
            with self.assertRaises(ValueError):op.publication(json.dumps(altered).encode(),prior)
    def test_old_data_pending_and_only_declared_public_history_readable(self):
        self.assertEqual(op.publication(b'{"version":"1.0"}')['status'],'pending_original_schedule_publication')
        m=native.Memory()
        for key in ['private/account.json','data/pm-decision.json','learning/hiring.json','data/hiring-velocity/history/../private.json']:
            with self.assertRaises(ValueError):op.read_public_archive(m,{'key':key})
        self.assertEqual(m.reads,[])


if __name__=='__main__':unittest.main(verbosity=2)
