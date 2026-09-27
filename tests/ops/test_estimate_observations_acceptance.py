from pathlib import Path
from copy import deepcopy
from datetime import datetime,timezone
from io import BytesIO
import importlib.util,json,sys,unittest
ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops/staged','aws/ops','aws/ops/checks','aws/shared')]
import ops_6243_estimate_observations_acceptance as op
spec=importlib.util.spec_from_file_location('isolated_estimate_native_tests',ROOT/'aws/lambdas/justhodl-estimate-revisions/tests/run_tests.py');native=importlib.util.module_from_spec(spec);spec.loader.exec_module(native)


class Tests(unittest.TestCase):
    def publication(self):
        memory=native.Memory();prior=memory.data['data/estimate-revisions.json'];ns=native.native(memory)
        ns['_estimate_fetch']=lambda symbol:native.acquisition(stamp=datetime.now(timezone.utc).isoformat())
        ns['lambda_handler']();return memory.data['data/estimate-revisions.json'],prior,memory
    def test_actual_native_output_replays_full_original_response_and_public_archives(self):
        raw,prior,memory=self.publication();result=op.publication(raw,prior)
        self.assertEqual(result['status'],'published_original_estimate_observations_replayed');self.assertEqual(result['estimate_observations'],1)
        self.assertEqual(op.read_public_archive(memory,op.archive_ref(raw)),raw);self.assertEqual(op.read_public_archive(memory,op.archive_ref(prior)),prior)
        writes=list(memory.writes);op.publication(raw,prior);self.assertEqual(writes,memory.writes)
    def test_tampered_values_clocks_counts_permissions_and_history_fail(self):
        raw,prior,_=self.publication();source=json.loads(raw)
        mutations=[lambda p:p['request_records'][0]['observations'][0]['values'].update(epsAvg=99),
                   lambda p:p.update(calls_eligible=True),lambda p:p.update(n_estimate_observations=2),
                   lambda p:p.update(acquisition_started_at='2099-01-01T00:00:00Z'),
                   lambda p:p.update(not_selected_calendar_indices=[0]),
                   lambda p:p.update(previous_publication=None)]
        for mutate in mutations:
            p=deepcopy(source);mutate(p)
            with self.assertRaises(ValueError):op.publication(json.dumps(p).encode(),prior)
    def test_old_packet_is_pending_and_private_or_unrelated_archives_rejected_before_read(self):
        self.assertEqual(op.publication(b'{"version":"3.1.0"}')['status'],'pending_original_schedule_publication')
        memory=native.Memory()
        for key in ['estimate-revisions/state.json','private/account.json','data/pm-decision.json','data/estimate-revisions/history/../private.json']:
            with self.assertRaises(ValueError):op.read_public_archive(memory,{'key':key})
        self.assertEqual(memory.reads,[])


if __name__=='__main__':unittest.main(verbosity=2)
