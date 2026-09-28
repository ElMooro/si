from pathlib import Path
from copy import deepcopy
from concurrent.futures import Future
import importlib.util,json,sys,unittest
ROOT=Path(__file__).resolve().parents[2];sys.path[:0]=[str(ROOT/p) for p in ('aws/ops/staged','aws/ops','aws/ops/checks','aws/shared')]
import ops_6287_price_resumption_acceptance as op
spec=importlib.util.spec_from_file_location('price_resumption_native_tests',ROOT/'aws/lambdas/justhodl-volatility-squeeze-hunter/tests/run_tests.py')
n=importlib.util.module_from_spec(spec);spec.loader.exec_module(n)


class Immediate:
    """Control completion order while retaining the actual configured limits."""
    def __init__(self,**kwargs):pass
    def __enter__(self):return self
    def __exit__(self,*args):pass
    def submit(self,fn,*args):
        f=Future();f.set_result(fn(*args));return f


class Tests(unittest.TestCase):
    def test_complete_originals_and_resumption_replay(self):
        memory,p=n.publication();sources=op.original.read_sources(memory,p);writes=list(memory.writes)
        r=op.publication(memory.data[n.m.HEAD],memory.previous,sources)
        self.assertTrue(r['resumption_publication_verified']);self.assertTrue(r['complete_selected_visit_cycle'])
        self.assertFalse(r['investment_authority']);self.assertEqual(memory.writes,writes)
    def test_actual_second_window_replays_from_exact_prior_bytes(self):
        memory=n.Memory();memory.data['data/universe.json']=n.m.encode({'stocks':[{'symbol':s,'cap_bucket':'small'} for s in ('TEST','SECOND','THIRD')]})
        n.resumed_publication(memory,workers=12,executor=Immediate);prior=memory.data[n.m.HEAD]
        p,calls=n.resumed_publication(memory,workers=12,executor=Immediate)
        result=op.publication(memory.data[n.m.HEAD],prior,op.original.read_sources(memory,p))
        self.assertEqual(calls,['THIRD']);self.assertEqual(result['visited_occurrences'],1);self.assertEqual(result['pending_occurrences'],0)
    def test_false_progress_types_order_budget_and_gate_reason_rejected(self):
        memory,p=n.publication();sources=op.original.read_sources(memory,p)
        edits=[lambda p:p['acquisition_progress'].update(visited_occurrences=True),
               lambda p:p['acquisition_progress'].update(visit_does_not_mean_success=1),
               lambda p:p['acquisition_progress'].update(planned_request_indices=[1,0]),
               lambda p:p['acquisition_progress'].update(planned_request_indices=[False,1]),
               lambda p:p['acquisition_progress'].update(retained_source_bytes=1),
               lambda p:p['acquisition_progress'].update(stop_reason='provider_denial_or_rate_limit')]
        for edit in edits:
            changed=deepcopy(p);edit(changed)
            with self.assertRaises(ValueError):op.publication(n.m.encode(changed),memory.previous,sources)
    def test_prior_version_is_pending_not_new_compiler_acceptance(self):
        result=op.publication(b'{"measurement_contract":"price-compression-observations.v1","version":"2.0.0"}')
        self.assertFalse(result['resumption_publication_verified'])


if __name__=='__main__':unittest.main(verbosity=2)
